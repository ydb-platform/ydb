#pragma once

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_TRANSACTIONS(xxx, ...) \
    xxx(InitSchema, __VA_ARGS__)                    \
    xxx(ReadWriteMeta, __VA_ARGS__)

// BLOCKSTORE_PARTITION_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxPartition
{
    //
    // InitSchema
    //
    struct TInitSchema
    {
        explicit TInitSchema()
        {}

        void Clear()
        {
            // nothing to do
        }
    };

    //
    // ReadWriteMeta
    //
    struct TReadWriteMeta
    {
        TString Meta;

        explicit TReadWriteMeta(TString meta)
            : Meta(meta)
        {}

        void Clear()
        {
            Meta.clear();
        }
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
