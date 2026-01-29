#pragma once

namespace NCloud::NBlockStore {

enum class EBlockStoreRequest
{
    ReadBlocks = 1,
    WriteBlocks = 2,
    ZeroBlocks = 3,
    MAX
};

} // namespace NCloud::NBlockStore
