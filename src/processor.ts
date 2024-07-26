import {assertNotNull} from '@subsquid/util-internal'
import {
    BlockHeader,
    DataHandlerContext,
    SubstrateBatchProcessor,
    SubstrateBatchProcessorFields,
    Event as _Event,
    Call as _Call,
    Extrinsic as _Extrinsic
} from '@subsquid/substrate-processor'

import {events} from './types'

export const processor = new SubstrateBatchProcessor()
    .setGateway('https://v2.archive.subsquid.io/network/kusama')
    .setRpcEndpoint({
        url: assertNotNull(process.env.RPC_KUSAMA_HTTP, 'No RPC endpoint supplied'),
        rateLimit: 10,
    })
    .addEvent({
        name: [events.balances.transfer.name],
        extrinsic: true,
        call: true,
     })
    .setFields({
        block: {
            timestamp: true,
        },
        extrinsic: {
            hash: true,
        },
    })

export type Fields = SubstrateBatchProcessorFields<typeof processor>
export type Block = BlockHeader<Fields>
export type Event = _Event<Fields>
export type Call = _Call<Fields>
export type Extrinsic = _Extrinsic<Fields>
export type ProcessorContext<Store> = DataHandlerContext<Store, Fields>
