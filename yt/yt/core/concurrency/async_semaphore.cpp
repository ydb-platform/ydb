#include "async_semaphore.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphore::TAsyncSemaphore(i64 totalSlots)
    : TotalSlots_(totalSlots)
    , FreeSlots_(totalSlots)
{
    YT_VERIFY(TotalSlots_ >= 0);
}

void TAsyncSemaphore::SetTotal(i64 totalSlots)
{
    YT_VERIFY(totalSlots >= 0);

    {
        auto guard = WriterGuard(SpinLock_);
        auto delta = totalSlots - TotalSlots_;
        TotalSlots_ += delta;
        FreeSlots_ += delta;
    }

    Release(0);
}

void TAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    YT_VERIFY(slots >= 0);

    {
        auto guard = WriterGuard(SpinLock_);

        FreeSlots_ += slots;
        YT_ASSERT(FreeSlots_ <= TotalSlots_);

        if (Releasing_) {
            return;
        }

        Releasing_ = true;
    }

    while (true) {
        std::vector<TWaiter> waitersToRelease;
        TPromise<void> readyEventToSet;

        {
            auto guard = WriterGuard(SpinLock_);

            while (!Waiters_.empty() && FreeSlots_ >= Waiters_.front().Slots) {
                auto& waiter = Waiters_.front();
                FreeSlots_ -= waiter.Slots;
                waitersToRelease.push_back(std::move(waiter));
                Waiters_.pop();
            }

            if (ReadyEvent_ && FreeSlots_ > 0) {
                swap(readyEventToSet, ReadyEvent_);
            }

            if (waitersToRelease.empty() && !readyEventToSet) {
                Releasing_ = false;
                break;
            }
        }

        for (const auto& waiter : waitersToRelease) {
            // NB: This may lead to a reentrant invocation of Release if the invoker discards the callback.
            waiter.Invoker->Invoke(BIND(waiter.Handler, Passed(TAsyncSemaphoreGuard(this, waiter.Slots))));
        }

        if (readyEventToSet) {
            readyEventToSet.Set();
        }
    }
}

void TAsyncSemaphore::Acquire(i64 slots /* = 1 */)
{
    YT_VERIFY(slots >= 0);

    auto guard = WriterGuard(SpinLock_);
    FreeSlots_ -= slots;
}

bool TAsyncSemaphore::TryAcquire(i64 slots /*= 1*/)
{
    YT_VERIFY(slots >= 0);

    auto guard = WriterGuard(SpinLock_);
    if (FreeSlots_ < slots) {
        return false;
    }
    FreeSlots_ -= slots;
    return true;
}

void TAsyncSemaphore::AsyncAcquire(
    const TCallback<void(TAsyncSemaphoreGuard)>& handler,
    IInvokerPtr invoker,
    i64 slots)
{
    YT_VERIFY(slots >= 0);

    auto guard = WriterGuard(SpinLock_);
    if (FreeSlots_ >= slots) {
        FreeSlots_ -= slots;
        guard.Release();
        invoker->Invoke(BIND(handler, Passed(TAsyncSemaphoreGuard(this, slots))));
    } else {
        Waiters_.push(TWaiter{handler, std::move(invoker), slots});
    }
}

bool TAsyncSemaphore::IsReady() const
{
    auto guard = ReaderGuard(SpinLock_);

    return FreeSlots_ > 0;
}

bool TAsyncSemaphore::IsFree() const
{
    auto guard = ReaderGuard(SpinLock_);

    return FreeSlots_ == TotalSlots_;
}

i64 TAsyncSemaphore::GetTotal() const
{
    auto guard = ReaderGuard(SpinLock_);

    return TotalSlots_;
}

i64 TAsyncSemaphore::GetUsed() const
{
    auto guard = ReaderGuard(SpinLock_);

    return TotalSlots_ - FreeSlots_;
}

i64 TAsyncSemaphore::GetFree() const
{
    auto guard = ReaderGuard(SpinLock_);

    return FreeSlots_;
}

TFuture<void> TAsyncSemaphore::GetReadyEvent()
{
    auto guard = WriterGuard(SpinLock_);

    if (FreeSlots_ > 0) {
        return VoidFuture;
    } else if (!ReadyEvent_) {
        ReadyEvent_ = NewPromise<void>();
    }

    return ReadyEvent_;
}

////////////////////////////////////////////////////////////////////////////////

TProfiledAsyncSemaphore::TProfiledAsyncSemaphore(
    i64 totalSlots,
    NProfiling::TGauge gauge)
    : TAsyncSemaphore(totalSlots)
    , Gauge_(std::move(gauge))
{ }

void TProfiledAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    TAsyncSemaphore::Release(slots);
    Profile();
}

void TProfiledAsyncSemaphore::Acquire(i64 slots /* = 1 */)
{
    TAsyncSemaphore::Acquire(slots);
    Profile();
}

bool TProfiledAsyncSemaphore::TryAcquire(i64 slots /* = 1 */)
{
    if (TAsyncSemaphore::TryAcquire(slots)) {
        Profile();
        return true;
    }
    return false;
}

void TProfiledAsyncSemaphore::Profile()
{
    Gauge_.Update(GetUsed());
}

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard(TAsyncSemaphoreGuard&& other) noexcept
{
    MoveFrom(std::move(other));
}

TAsyncSemaphoreGuard::~TAsyncSemaphoreGuard()
{
    Release();
}

TAsyncSemaphoreGuard& TAsyncSemaphoreGuard::operator=(TAsyncSemaphoreGuard&& other)
{
    if (this != &other) {
        Release();
        MoveFrom(std::move(other));
    }
    return *this;
}

void TAsyncSemaphoreGuard::MoveFrom(TAsyncSemaphoreGuard&& other)
{
    Semaphore_ = other.Semaphore_;
    Slots_ = other.Slots_;

    other.Semaphore_ = nullptr;
    other.Slots_ = 0;
}

void swap(TAsyncSemaphoreGuard& lhs, TAsyncSemaphoreGuard& rhs)
{
    std::swap(lhs.Semaphore_, rhs.Semaphore_);
    std::swap(lhs.Slots_, rhs.Slots_);
}

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard()
    : Slots_(0)
{ }

TAsyncSemaphoreGuard::TAsyncSemaphoreGuard(TAsyncSemaphorePtr semaphore, i64 slots)
    : Slots_(slots)
    , Semaphore_(semaphore)
{ }

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::Acquire(TAsyncSemaphorePtr semaphore, i64 slots /*= 1*/)
{
    semaphore->Acquire(slots);
    return TAsyncSemaphoreGuard(semaphore, slots);
}

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::TryAcquire(TAsyncSemaphorePtr semaphore, i64 slots /*= 1*/)
{
    if (semaphore->TryAcquire(slots)) {
        return TAsyncSemaphoreGuard(semaphore, slots);
    } else {
        return TAsyncSemaphoreGuard();
    }
}

TAsyncSemaphoreGuard TAsyncSemaphoreGuard::TransferSlots(i64 slotsToTransfer)
{
    YT_VERIFY(slotsToTransfer >= 0 && slotsToTransfer <= Slots_);
    Slots_ -= slotsToTransfer;
    TAsyncSemaphoreGuard spawnedGuard;
    spawnedGuard.Semaphore_ = Semaphore_;
    spawnedGuard.Slots_ = slotsToTransfer;
    return spawnedGuard;
}

void TAsyncSemaphoreGuard::Release()
{
    if (Semaphore_) {
        Semaphore_->Release(Slots_);
        Semaphore_ = nullptr;
        Slots_ = 0;
    }
}

TAsyncSemaphoreGuard::operator bool() const
{
    return static_cast<bool>(Semaphore_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
