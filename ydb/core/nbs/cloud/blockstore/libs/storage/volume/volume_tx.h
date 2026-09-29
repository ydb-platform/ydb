#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_TRANSACTIONS(xxx, ...)                               \
    xxx(InitSchema, __VA_ARGS__)                                               \
    xxx(LoadState, __VA_ARGS__)                                                \
    xxx(StorePartitionTabletId, __VA_ARGS__)

// BLOCKSTORE_VOLUME_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

// Arguments of the volume tablet's local DB transactions.
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
    struct TLoadState
    {
        // Filled by Prepare; 0 until the first partition tablet id is stored.
        ui64 PartitionTabletId = 0;

        void Clear()
        {
            PartitionTabletId = 0;
        }
    };

    //
    // StorePartitionTabletId
    //
    struct TStorePartitionTabletId
    {
        const ui64 PartitionTabletId;
        // SchemeShard's UpdateVolumeConfig transaction being answered.
        const ui64 TxId;
        const TRequestInfoPtr RequestInfo;

        TStorePartitionTabletId(
            ui64 partitionTabletId,
            ui64 txId,
            TRequestInfoPtr requestInfo)
            : PartitionTabletId(partitionTabletId)
            , TxId(txId)
            , RequestInfo(std::move(requestInfo))
        {}

        void Clear()
        {
            // nothing to do
        }
    };
};

}   // namespace NYdb::NBS::NStorage
