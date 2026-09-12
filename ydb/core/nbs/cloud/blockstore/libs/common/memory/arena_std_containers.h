#pragma once

#include "public.h"

#include "arena_allocator_adapter.h"
#include "arena_allocator_pool.h"

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

#include <unordered_map>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

template <typename K, typename TCmp = TLess<K>>
class TArenaSet: public TSet<K, TCmp, TArenaPoolAdapter<K>>
{
public:
    using TPoolAdapter = TArenaPoolAdapter<K>;

    explicit TArenaSet(TArenaAllocatorPool* pool)
        : TSet<K, TCmp, TPoolAdapter>(TCmp(), TPoolAdapter(pool))
    {}
};

//////////////////////////////////////////////////////////////////////////////

template <class K, class V, class Less = TLess<K>>
class TArenaMap: public TMap<K, V, Less, TArenaPoolAdapter<K>>
{
public:
    using TPoolAdapter = TArenaPoolAdapter<K>;

    explicit TArenaMap(TArenaAllocatorPool* pool)
        : TMap<K, V, Less, TPoolAdapter>(Less(), TPoolAdapter(pool))
    {}
};

//////////////////////////////////////////////////////////////////////////////

template <
    class Key,
    class T,
    class HashFcn = THash<Key>,
    class EqualKey = TEqualTo<Key>>
class TArenaHashMap
    : public std::unordered_map<
          Key,
          T,
          HashFcn,
          EqualKey,
          TArenaPoolAdapter<std::pair<const Key, T>>>
{
public:
    using TPoolAdapter = TArenaPoolAdapter<std::pair<const Key, T>>;
    using TBase = std::unordered_map<Key, T, HashFcn, EqualKey, TPoolAdapter>;

    explicit TArenaHashMap(TArenaAllocatorPool* pool)
        : TBase(0, HashFcn(), EqualKey(), TPoolAdapter(pool))
    {}
};

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
