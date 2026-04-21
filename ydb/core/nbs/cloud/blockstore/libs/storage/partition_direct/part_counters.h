#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionCounters
{
    enum ETransactionType
    {
#define BLOCKSTORE_TRANSACTION_TYPE(name, ...) TX_##name,

        BLOCKSTORE_PARTITION_TRANSACTIONS(BLOCKSTORE_TRANSACTION_TYPE) TX_SIZE

#undef BLOCKSTORE_TRANSACTION_TYPE
    };
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
