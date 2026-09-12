#pragma once

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_counters.h
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_tx.h>
=======
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range_field.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>
>>>>>>> 78a20188184 (Field map set optimization (#52565)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/block_field_serializer.h

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_counters.h
struct TPartitionCounters
{
    enum ETransactionType
    {
#define BLOCKSTORE_TRANSACTION_TYPE(name, ...) TX_##name,

        BLOCKSTORE_PARTITION_TRANSACTIONS(BLOCKSTORE_TRANSACTION_TYPE) TX_SIZE

#undef BLOCKSTORE_TRANSACTION_TYPE
    };
};
=======
void SaveBlockField(const TBlockRangeField& field, TBlockFieldProto* proto);
void LoadBlockField(const TBlockFieldProto& proto, TBlockRangeField* field);
>>>>>>> 78a20188184 (Field map set optimization (#52565)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/block_field_serializer.h

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
