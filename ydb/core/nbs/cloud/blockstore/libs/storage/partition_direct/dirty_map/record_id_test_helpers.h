#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/record_id.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Makes a record id for tests, which run within a single tablet generation.
constexpr TRecordId MakeId(ui64 lsn)
{
    return {.Generation = 1, .Lsn = lsn};
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
