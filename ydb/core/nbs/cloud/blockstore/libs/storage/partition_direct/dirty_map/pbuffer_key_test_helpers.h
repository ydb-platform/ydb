#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/pbuffer_key.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Makes a PBuffer key for tests, which run within a single tablet generation.
constexpr TPBufferKey MakeKey(ui64 lsn)
{
    return {.Generation = 1, .Lsn = lsn};
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
