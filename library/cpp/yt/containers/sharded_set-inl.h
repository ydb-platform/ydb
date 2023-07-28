#ifndef SHARDED_SET_INL_H_
#error "Direct inclusion of this file is not allowed, include sharded_set.h"
// For the sake of sane code completion.
#include "sharded_set.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, int N, class F, class S>
class TShardedSet<T, N, F, S>::const_iterator
{
private:
    friend class TShardedSet<T, N, F, S>;

    using TOwner = TShardedSet<T, N, F, S>;
    using TShardIterator = typename S::const_iterator;

    const TOwner* const Owner_;

    int ShardIndex_;
    TShardIterator ShardIterator_;

    const_iterator(
        const TOwner* owner,
        int shardIndex,
        TShardIterator shardIterator)
        : Owner_(owner)
        , ShardIndex_(shardIndex)
        , ShardIterator_(shardIterator)
    { }

    bool IsValid() const
    {
        return ShardIterator_ != Owner_->Shards_[ShardIndex_].end();
    }

    void FastForward()
    {
        while (ShardIndex_ != N - 1 && !IsValid()) {
            ++ShardIndex_;
            ShardIterator_ = Owner_->Shards_[ShardIndex_].begin();
        }
    }

public:
    using difference_type = typename std::iterator_traits<TShardIterator>::difference_type;
    using value_type = typename std::iterator_traits<TShardIterator>::value_type;
    using pointer = typename std::iterator_traits<TShardIterator>::pointer;
    using reference = typename std::iterator_traits<TShardIterator>::reference;
    using iterator_category = std::forward_iterator_tag;

    const_iterator& operator++()
    {
        ++ShardIterator_;
        FastForward();

        return *this;
    }

    const_iterator operator++(int)
    {
        auto result = *this;

        ++ShardIterator_;
        FastForward();

        return result;
    }

    bool operator==(const const_iterator& rhs) const
    {
        return
            ShardIndex_ == rhs.ShardIndex_ &&
            ShardIterator_ == rhs.ShardIterator_;
    }

    bool operator!=(const const_iterator& rhs) const
    {
        return !(*this == rhs);
    }

    const T& operator*() const
    {
        return *ShardIterator_;
    }

    const T* operator->() const
    {
        return &operator*();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T, int N, class F, class S>
TShardedSet<T, N, F, S>::TShardedSet(F elementToShard)
    : ElementToShard_(elementToShard)
{ }

template <class T, int N, class F, class S>
bool TShardedSet<T, N, F, S>::empty() const
{
    return size() == 0;
}

template <class T, int N, class F, class S>
typename TShardedSet<T, N, F, S>::size_type TShardedSet<T, N, F, S>::size() const
{
    size_type result = 0;
    for (const auto& shard : Shards_) {
        result += shard.size();
    }

    return result;
}

template <class T, int N, class F, class S>
const T& TShardedSet<T, N, F, S>::front() const
{
    return *begin();
}

template <class T, int N, class F, class S>
typename TShardedSet<T, N, F, S>::size_type TShardedSet<T, N, F, S>::count(const T& value) const
{
    return GetShard(value).count(value);
}

template <class T, int N, class F, class S>
bool TShardedSet<T, N, F, S>::contains(const T& value) const
{
    return GetShard(value).contains(value);
}

template <class T, int N, class F, class S>
std::pair<typename TShardedSet<T, N, F, S>::const_iterator, bool> TShardedSet<T, N, F, S>::insert(const T& value)
{
    auto shardIndex = ElementToShard_(value);
    auto& shard = Shards_[shardIndex];
    auto [shardIterator, inserted] = shard.insert(value);

    const_iterator iterator(this, shardIndex, shardIterator);
    return {iterator, inserted};
}

template <class T, int N, class F, class S>
bool TShardedSet<T, N, F, S>::erase(const T& value)
{
    return GetShard(value).erase(value);
}

template <class T, int N, class F, class S>
void TShardedSet<T, N, F, S>::clear()
{
    for (auto& shard : Shards_) {
        shard.clear();
    }
}

template <class T, int N, class F, class S>
typename TShardedSet<T, N, F, S>::const_iterator TShardedSet<T, N, F, S>::begin() const
{
    const_iterator iterator(this, /*shardIndex*/ 0, /*shardIterator*/ Shards_[0].begin());
    iterator.FastForward();

    return iterator;
}

template <class T, int N, class F, class S>
typename TShardedSet<T, N, F, S>::const_iterator TShardedSet<T, N, F, S>::cbegin() const
{
    return begin();
}

template <class T, int N, class F, class S>
typename TShardedSet<T, N, F, S>::const_iterator TShardedSet<T, N, F, S>::end() const
{
    return const_iterator(this, /*shardIndex*/ N - 1, /*shardIterator*/ Shards_[N - 1].end());
}

template <class T, int N, class F, class S>
typename TShardedSet<T, N, F, S>::const_iterator TShardedSet<T, N, F, S>::cend() const
{
    return end();
}

template <class T, int N, class F, class S>
const S& TShardedSet<T, N, F, S>::Shard(int shardIndex) const
{
    return Shards_[shardIndex];
}

template <class T, int N, class F, class S>
S& TShardedSet<T, N, F, S>::MutableShard(int shardIndex)
{
    return Shards_[shardIndex];
}

template <class T, int N, class F, class S>
S& TShardedSet<T, N, F, S>::GetShard(const T& value)
{
    return Shards_[ElementToShard_(value)];
}

template <class T, int N, class F, class S>
const S& TShardedSet<T, N, F, S>::GetShard(const T& value) const
{
    return Shards_[ElementToShard_(value)];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
