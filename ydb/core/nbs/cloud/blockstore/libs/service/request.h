#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class EBlockStoreRequest
{
    ReadBlocks = 1,
    WriteBlocks = 2,
    ZeroBlocks = 3,
    MAX
};

}   // namespace NYdb::NBS::NBlockStore
