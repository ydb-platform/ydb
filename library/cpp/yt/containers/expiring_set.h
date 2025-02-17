#pragma once

#include <util/generic/hash.h>

#include <util/datetime/base.h>

#include <queue>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Maintains a set of items that expire after a certain time.
template <class TItem, class THash = THash<TItem>, class TEqual = TEqualTo<TItem>>
class TExpiringSet
{
public:
    void SetTtl(TDuration ttl);

    void Insert(TInstant now, const TItem& item);
    template <class TItems>
    void InsertMany(TInstant now, const TItems& items);

    void Remove(const TItem& item);

    void Expire(TInstant now);

    void Clear();

    template <class TItemLike>
    bool Contains(const TItemLike& item) const;

    int GetSize() const;

private:
    TDuration Ttl_;

    struct TItemPack
    {
        std::vector<TItem> Items;
        TInstant Deadline;

        bool operator<(const TItemPack& other) const;
    };

    THashMap<TItem, TInstant, THash, TEqual> ItemToDeadline_;
    std::priority_queue<TItemPack> ExpirationQueue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRING_SET_INL_H_
#include "expiring_set-inl.h"
#undef EXPIRING_SET_INL_H_
