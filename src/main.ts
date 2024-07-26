import {In} from 'typeorm'
import * as ss58 from '@subsquid/ss58'
import {Store, TypeormDatabase} from '@subsquid/typeorm-store'
import {Account, Transfer} from './model'
import {events} from './types'
import {Event, ProcessorContext, processor} from './processor'

processor.run(new TypeormDatabase(), async (ctx) => {
    let transfersData: TransferEventData[] = []

    for (let block of ctx.blocks) {
        for (let event of block.events) {
            if (event.name===events.balances.transfer.name) {
                if (block.header.timestamp==null) {
                    throw new Error(`No timestamp for block ${block.header.height}`)
                }
                let tr = getTransfer(ctx, event)
                transfersData.push({
                    id: event.id,
                    blockNumber: block.header.height,
                    timestamp: new Date(block.header.timestamp),
                    extrinsicHash: event.extrinsic?.hash,
                    call: event.call?.name,
                    from: encodeId(tr.from),
                    to: encodeId(tr.to),
                    amount: tr.amount,
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

function encodeId(id: string): string {
    let idBytes = Uint8Array.from(Buffer.from(id.slice(2), 'hex'))
    return ss58.codec('kusama').encode(idBytes)
}

function getTransfer(
    ctx: ProcessorContext<Store>,
    e: Event
): {from: string; to: string; amount: bigint} {
    if (events.balances.transfer.v1020.is(e)) {
        let [from, to, amount] = events.balances.transfer.v1020.decode(e)
        return {from, to, amount}
    } else if (events.balances.transfer.v1050.is(e)) {
        let [from, to, amount] = events.balances.transfer.v1050.decode(e)
        return {from, to, amount}
    } else if (events.balances.transfer.v9130.is(e)) {
        return events.balances.transfer.v9130.decode(e)
    } else {
        throw new UnknownVersionError()
    }
}

class UnknownVersionError extends Error {
    constructor() {
        super('Unknown verson')
    }
}
