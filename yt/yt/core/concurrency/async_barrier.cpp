#include "async_barrier.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TAsyncBarrierCookie TAsyncBarrier::Insert()
{
    auto guard = Guard(Lock_);
    SlotOccupied_.push(true);
    return TAsyncBarrierCookie(FirstSlotCookie_ + std::ssize(SlotOccupied_) - 1);
}

void TAsyncBarrier::Remove(TAsyncBarrierCookie cookie)
{
    std::vector<TPromise<void>> promises;

    {
        auto guard = Guard(Lock_);

        YT_VERIFY(static_cast<i64>(cookie) >= FirstSlotCookie_);
        YT_VERIFY(std::exchange(SlotOccupied_[static_cast<i64>(cookie) - FirstSlotCookie_], false));

        while (!SlotOccupied_.empty() && !SlotOccupied_.front()) {
            SlotOccupied_.pop();
            ++FirstSlotCookie_;
        }

        while (!Subscriptions_.empty() && Subscriptions_.front().BarrierCookie <= FirstSlotCookie_) {
            promises.push_back(std::move(Subscriptions_.front().Promise));
            Subscriptions_.pop();
        }
    }

    for (const auto& promise : promises) {
        promise.Set();
    }
}

TFuture<void> TAsyncBarrier::GetBarrierFuture()
{
    auto guard = Guard(Lock_);

    if (SlotOccupied_.empty()) {
        return VoidFuture;
    }

    auto barrierCookie = FirstSlotCookie_ + std::ssize(SlotOccupied_);
    if (!Subscriptions_.empty() && Subscriptions_.back().BarrierCookie == barrierCookie) {
        return Subscriptions_.back().Future;
    }

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture().ToUncancelable();
    Subscriptions_.push({
        .BarrierCookie = barrierCookie,
        .Promise = std::move(promise),
        .Future = future,
    });
    return future;
}

void TAsyncBarrier::Clear(const TError& error)
{
    std::vector<TPromise<void>> promises;

    {
        auto guard = Guard(Lock_);

        SlotOccupied_.clear();

        while (!Subscriptions_.empty()) {
            promises.push_back(std::move(Subscriptions_.front().Promise));
            Subscriptions_.pop();
        }

        FirstSlotCookie_ = 1;
    }

    for (const auto& promise : promises) {
        promise.Set(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
