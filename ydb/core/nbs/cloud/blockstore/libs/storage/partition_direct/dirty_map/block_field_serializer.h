#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_field.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void SaveBlockField(
    const TBlockRangeField& field,
    ui64 blockCount,
    TBlockFieldProto* proto);
void LoadBlockField(const TBlockFieldProto& proto, TBlockRangeField* field);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
