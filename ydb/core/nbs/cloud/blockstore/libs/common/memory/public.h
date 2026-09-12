#pragma once

#include <util/generic/size_literals.h>
#include <util/system/types.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

class IArenaAllocator;
using IArenaAllocatorPtr = std::shared_ptr<IArenaAllocator>;

class TArenaAllocatorPool;
class TArenaAllocatorIndexPool;

template <class T>
class TArenaPoolAdapter;

template <typename TKey, typename TCmp>
class TArenaSet;

template <class TKey, class TValue, class TCmp>
class TArenaMap;

template <typename T>
class TArenaArrayUniquePtr;

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
