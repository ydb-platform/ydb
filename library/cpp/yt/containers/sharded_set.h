#pragma once

#include <util/generic/hash_set.h>

#include <array>
#include <cstddef>
#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A set that stores elements divided into fixed amount of shards.
//! Provides access to whole set and particular shards.
//! The interface is pretty minimalistic, feel free to extend it when needed.
template <class T, int N, class F, class S = THashSet<T>>
class TShardedSet
{
public:
    using size_type = size_t;
    using difference_type = ptrdiff_t;

    using value_type = T;

    class const_iterator;

    explicit TShardedSet(F elementToShard = F());

    [[nodiscard]] bool empty() const;

    size_type size() const;

    const T& front() const;

    size_type count(const T& value) const;

    bool contains(const T& value) const;

    std::pair<const_iterator, bool> insert(const T& value);

    bool erase(const T& value);

    void clear();

    const_iterator begin() const;
    const_iterator cbegin() const;

    const_iterator end() const;
    const_iterator cend() const;

    const S& Shard(int shardIndex) const;
    S& MutableShard(int shardIndex);

private:
    std::array<S, N> Shards_;

    const F ElementToShard_;

    S& GetShard(const T& value);
    const S& GetShard(const T& value) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SHARDED_SET_INL_H_
#include "sharded_set-inl.h"
#undef SHARDED_SET_INL_H_
