#pragma once

#include "error.h"

#include <library/cpp/yt/memory/blob.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IMemoryUsageTracker
    : public TRefCounted
{
    virtual TError TryAcquire(i64 size) = 0;
    virtual TError TryChange(i64 size) = 0;
    //! Returns true unless overcommit occurred.
    virtual bool Acquire(i64 size) = 0;
    virtual void Release(i64 size) = 0;
    virtual void SetLimit(i64 size) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryUsageTracker)

IMemoryUsageTrackerPtr GetNullMemoryUsageTracker();

////////////////////////////////////////////////////////////////////////////////

class TMemoryUsageTrackerGuard
    : private TNonCopyable
{
public:
    TMemoryUsageTrackerGuard() = default;
    TMemoryUsageTrackerGuard(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard(TMemoryUsageTrackerGuard&& other);
    ~TMemoryUsageTrackerGuard();

    TMemoryUsageTrackerGuard& operator=(const TMemoryUsageTrackerGuard& other) = delete;
    TMemoryUsageTrackerGuard& operator=(TMemoryUsageTrackerGuard&& other);

    static TMemoryUsageTrackerGuard Build(
        IMemoryUsageTrackerPtr tracker,
        i64 granularity = 1);
    static TMemoryUsageTrackerGuard Acquire(
        IMemoryUsageTrackerPtr tracker,
        i64 size,
        i64 granularity = 1);
    static TErrorOr<TMemoryUsageTrackerGuard> TryAcquire(
        IMemoryUsageTrackerPtr tracker,
        i64 size,
        i64 granularity = 1);

    void Release();

    //! Releases the guard but does not return memory to the tracker.
    //! The caller should care about releasing memory itself.
    void ReleaseNoReclaim();

    explicit operator bool() const;

    i64 GetSize() const;
    void SetSize(i64 size);
    TError TrySetSize(i64 size);
    void IncrementSize(i64 sizeDelta);
    TMemoryUsageTrackerGuard TransferMemory(i64 size);

private:
    IMemoryUsageTrackerPtr Tracker_;
    i64 Size_ = 0;
    i64 AcquiredSize_ = 0;
    i64 Granularity_ = 0;

    void MoveFrom(TMemoryUsageTrackerGuard&& other);
    TError SetSizeGeneric(i64 size, auto acquirer);
};

////////////////////////////////////////////////////////////////////////////////

class TMemoryTrackedBlob
{
public:
    static TMemoryTrackedBlob Build(
        IMemoryUsageTrackerPtr tracker,
        TRefCountedTypeCookie tagCookie = GetRefCountedTypeCookie<TDefaultBlobTag>());

    TMemoryTrackedBlob() = default;
    TMemoryTrackedBlob(const TMemoryTrackedBlob& other) = delete;
    TMemoryTrackedBlob(TMemoryTrackedBlob&& other) = default;
    ~TMemoryTrackedBlob() = default;

    TMemoryTrackedBlob& operator=(const TMemoryTrackedBlob& other) = delete;
    TMemoryTrackedBlob& operator=(TMemoryTrackedBlob&& other) = default;

    void Resize(
        i64 size,
        bool initializeStorage = true);

    TError TryResize(
        i64 size,
        bool initializeStorage = true);

    void Reserve(i64 capacity);

    TError TryReserve(i64 capacity);

    char* Begin();

    char* End();

    size_t Capacity() const;

    size_t Size() const;

    void Append(TRef ref);

    void Clear();

private:
    TBlob Blob_;
    TMemoryUsageTrackerGuard Guard_;

    TMemoryTrackedBlob(
        TBlob&& blob,
        TMemoryUsageTrackerGuard&& guard);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
