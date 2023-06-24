import {In} from 'typeorm'
import * as ss58 from '@subsquid/ss58'
import {Store, TypeormDatabase} from '@subsquid/typeorm-store'
import {Account, Transfer} from './model'
import {EventItem, ProcessorContext, processor} from './processor'
import {BalancesTransferEvent} from './types/events'

processor.run(new TypeormDatabase(), async (ctx) => {
    let transfersData: TransferEventData[] = []

    for (let block of ctx.blocks) {
        for (let item of block.items) {
            if (item.name == 'Balances.Transfer') {
                let e = getTransfer(ctx, item)
                transfersData.push({
                    id: item.event.id,
                    blockNumber: block.header.height,
                    timestamp: new Date(block.header.timestamp),
                    extrinsicHash: item.event.extrinsic?.hash,
                    call: item.event.call?.name,
                    from: encodeId(e.from),
                    to: encodeId(e.to),
                    amount: e.amount,
                })
            }
        }
    }

    await saveTransfers(ctx, transfersData)
})

interface TransferEventData {
    id: string
    blockNumber: number
    timestamp: Date
    extrinsicHash?: string
    call?: string
    from: string
    to: string
    amount: bigint
}

async function saveTransfers(ctx: ProcessorContext<Store>, transfersData: TransferEventData[]) {
    let accountIds = new Set<string>()
    for (let t of transfersData) {
        accountIds.add(t.from)
        accountIds.add(t.to)
    }

    let accounts = await ctx.store
        .findBy(Account, {id: In([...accountIds])})
        .then((q) => new Map(q.map((i) => [i.id, i])))

    let transfers: Transfer[] = []
    for (let t of transfersData) {
        let {id, blockNumber, timestamp, extrinsicHash, amount} = t

        let from = getAccount(accounts, t.from)
        let to = getAccount(accounts, t.to)

        transfers.push(
            new Transfer({
                id,
                blockNumber,
                timestamp,
                extrinsicHash,
                from,
                to,
                amount,
            })
        )
    }

    await ctx.store.upsert([...accounts.values()])
    await ctx.store.insert(transfers)
}

function getAccount(m: Map<string, Account>, id: string): Account {
    let acc = m.get(id)
    if (acc == null) {
        acc = new Account()
        acc.id = id
        m.set(id, acc)
    }
    return acc
}

function encodeId(id: Uint8Array): string {
    return ss58.codec('kusama').encode(id)
}

type TransferEventItem = Extract<EventItem, {name: 'Balances.Transfer'}>

function getTransfer(
    ctx: ProcessorContext<Store>,
    item: TransferEventItem
): {from: Uint8Array; to: Uint8Array; amount: bigint} {
    let e = new BalancesTransferEvent(ctx, item.event)
    if (e.isV1020) {
        let [from, to, amount] = e.asV1020
        return {from, to, amount}
    } else if (e.isV1050) {
        let [from, to, amount] = e.asV1050
        return {from, to, amount}
    } else if (e.isV9130) {
        return e.asV9130
    } else {
        throw new UknownVersionError()
    }
}

class UknownVersionError extends Error {
    constructor() {
        super('Uknown verson')
    }
}
