#ifndef EXPIRING_SET_INL_H_
#error "Direct inclusion of this file is not allowed, include expiring_set.h"
// For the sake of sane code completion.
#include "expiring_set.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TItem, class THash, class TEqual>
void TExpiringSet<TItem, THash, TEqual>::SetTtl(TDuration ttl)
{
    Ttl_ = ttl;
}

template <class TItem, class THash, class TEqual>
void TExpiringSet<TItem, THash, TEqual>::Insert(TInstant now, const TItem& item)
{
    Expire(now);
    auto deadline = now + Ttl_;
    ItemToDeadline_[item] = deadline;
    ExpirationQueue_.push(TItemPack{.Items = {item}, .Deadline = deadline});
}

template <class TItem, class THash, class TEqual>
template <class TItems>
void TExpiringSet<TItem, THash, TEqual>::InsertMany(TInstant now, const TItems& items)
{
    Expire(now);
    auto deadline = now + Ttl_;
    for (const auto& item : items) {
        ItemToDeadline_[item] = deadline;
    }
    ExpirationQueue_.push(TItemPack{.Items = {items.begin(), items.end()}, .Deadline = deadline});
}

template <class TItem, class THash, class TEqual>
void TExpiringSet<TItem, THash, TEqual>::Remove(const TItem& item)
{
    ItemToDeadline_.erase(item);
}

template <class TItem, class THash, class TEqual>
void TExpiringSet<TItem, THash, TEqual>::Expire(TInstant now)
{
    while (!ExpirationQueue_.empty() && ExpirationQueue_.top().Deadline <= now) {
        for (const auto& item : ExpirationQueue_.top().Items) {
            if (auto it = ItemToDeadline_.find(item); it != ItemToDeadline_.end()) {
                if (it->second <= now) {
                    ItemToDeadline_.erase(it);
                }
            }
        }
        ExpirationQueue_.pop();
    }
}

template <class TItem, class THash, class TEqual>
void TExpiringSet<TItem, THash, TEqual>::Clear()
{
    ItemToDeadline_ = {};
    ExpirationQueue_ = {};
}

template <class TItem, class THash, class TEqual>
template <class TItemLike>
bool TExpiringSet<TItem, THash, TEqual>::Contains(const TItemLike& item) const
{
    return ItemToDeadline_.contains(item);
}

template <class TItem, class THash, class TEqual>
int TExpiringSet<TItem, THash, TEqual>::GetSize() const
{
    return std::ssize(ItemToDeadline_);
}

template <class TItem, class THash, class TEqual>
bool TExpiringSet<TItem, THash, TEqual>::TItemPack::operator<(const TItemPack& other) const
{
    // Reversed ordering for the priority queue.
    return Deadline > other.Deadline;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
