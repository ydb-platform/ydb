#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TAsyncSemaphoreGuard
    : private TNonCopyable
{
public:
    DEFINE_BYVAL_RO_PROPERTY(i64, Slots);

public:
    TAsyncSemaphoreGuard();
    TAsyncSemaphoreGuard(TAsyncSemaphoreGuard&& other) noexcept;
    ~TAsyncSemaphoreGuard();

    TAsyncSemaphoreGuard& operator=(TAsyncSemaphoreGuard&& other);

    static TAsyncSemaphoreGuard Acquire(TAsyncSemaphorePtr semaphore, i64 slots = 1);
    static TAsyncSemaphoreGuard TryAcquire(TAsyncSemaphorePtr semaphore, i64 slots = 1);

    friend void swap(TAsyncSemaphoreGuard& lhs, TAsyncSemaphoreGuard& rhs);

    TAsyncSemaphoreGuard TransferSlots(i64 slotsToTransfer);

    void Release();

    explicit operator bool() const;

private:
    friend class TAsyncSemaphore;

    TAsyncSemaphorePtr Semaphore_;

    TAsyncSemaphoreGuard(TAsyncSemaphorePtr semaphore, i64 slots);

    void MoveFrom(TAsyncSemaphoreGuard&& other);
};

////////////////////////////////////////////////////////////////////////////////

//! Custom semaphore class with async acquire operation.
class TAsyncSemaphore
    : public TRefCounted
{
public:
    explicit TAsyncSemaphore(i64 totalSlots);

    //! Updates the total number of slots.
    void SetTotal(i64 totalSlots);

    //! Releases a given number of slots.
    virtual void Release(i64 slots = 1);

    //! Acquires a given number of slots.
    //! Cannot fail, may lead to an overcommit.
    //! Returns whether overcommit happened.
    virtual bool Acquire(i64 slots = 1);

    //! Tries to acquire a given number of slots.
    //! Returns |true| on success (the number of remaining slots is non-negative).
    virtual bool TryAcquire(i64 slots = 1);

    //! Returns a future that becomes set when a given number of slots becomes available and acquired.
    TFuture<TAsyncSemaphoreGuard> AsyncAcquire(i64 slots = 1);

    //! Returns |true| iff at least one slot is free.
    bool IsReady() const;

    //! Returns |true| iff all slots are free.
    bool IsFree() const;

    //! Returns the total number of slots.
    i64 GetTotal() const;

    //! Returns the number of used slots.
    i64 GetUsed() const;

    //! Returns the number of free slots.
    i64 GetFree() const;

    TFuture<void> GetReadyEvent();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    i64 TotalSlots_;
    i64 FreeSlots_;

    bool Releasing_ = false;

    TPromise<void> ReadyEvent_;

    struct TWaiter
    {
        TPromise<TAsyncSemaphoreGuard> Promise;
        i64 Slots;
    };

    std::queue<TWaiter> Waiters_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncSemaphore)

////////////////////////////////////////////////////////////////////////////////

class TProfiledAsyncSemaphore
    : public TAsyncSemaphore
{
public:
    TProfiledAsyncSemaphore(
        i64 totalSlots,
        NProfiling::TGauge gauge);

    void Release(i64 slots = 1) override;
    bool Acquire(i64 slots = 1) override;
    bool TryAcquire(i64 slots = 1) override;

private:
    NProfiling::TGauge Gauge_;

    void Profile();
};

DEFINE_REFCOUNTED_TYPE(TProfiledAsyncSemaphore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
