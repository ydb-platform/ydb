#pragma once

#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/system/types.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_TRANSACTIONS(xxx, ...)                               \
    xxx(InitSchema, __VA_ARGS__)                                               \
    xxx(LoadState, __VA_ARGS__)                                                \
    xxx(StorePartitionTabletId, __VA_ARGS__)

// BLOCKSTORE_VOLUME_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

// Arguments of the volume tablet local-database transactions.
struct TTxVolume
{
    //
    // InitSchema
    //

    struct TInitSchema
    {
        void Clear()
        {
            // nothing to do
        }
    };

    //
    // LoadState
    //

    // PartitionTabletId is 0 when the volume has not stored one yet.
    struct TLoadState
    {
        ui64 PartitionTabletId = 0;

        void Clear()
        {
            PartitionTabletId = 0;
        }
    };

    //
    // StorePartitionTabletId
    //

    // The id is written before UpdateVolumeConfig is forwarded.
    struct TStorePartitionTabletId
    {
        const ui64 PartitionTabletId;
        const NKikimrBlockStore::TUpdateVolumeConfig Record;

        TStorePartitionTabletId(
            ui64 partitionTabletId,
            NKikimrBlockStore::TUpdateVolumeConfig record)
            : PartitionTabletId(partitionTabletId)
            , Record(std::move(record))
        {}

        void Clear()
        {
            // nothing to do
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NStorage
