#pragma once

#include <memory>

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IBlockStore;
using IBlockStorePtr = std::shared_ptr<IBlockStore>;

using TStorageBuffer = std::shared_ptr<char>;

}   // namespace NYdb::NBS::NNbs1CompatApi::NBlockStore
