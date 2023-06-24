import {lookupArchive} from '@subsquid/archive-registry'
import {
    BatchContext,
    BatchProcessorCallItem,
    BatchProcessorEventItem,
    BatchProcessorItem,
    SubstrateBatchProcessor,
} from '@subsquid/substrate-processor'

export const processor = new SubstrateBatchProcessor()
    .setDataSource({
        archive: lookupArchive('kusama', {release: 'FireSquid'}),
    })
    .addEvent('Balances.Transfer', {
        data: {
            event: {
                args: true,
                extrinsic: {
                    hash: true,
                },
                call: {},
            },
        },
    } as const)

export type Item = BatchProcessorItem<typeof processor>
export type EventItem = BatchProcessorEventItem<typeof processor>
export type CallItem = BatchProcessorCallItem<typeof processor>
export type ProcessorContext<Store> = BatchContext<Store, Item>
