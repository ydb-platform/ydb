#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IBlockStore;
using IBlockStorePtr = std::shared_ptr<IBlockStore>;

using TStorageBuffer = std::shared_ptr<char>;

}   // namespace NCloud::NBlockStore
