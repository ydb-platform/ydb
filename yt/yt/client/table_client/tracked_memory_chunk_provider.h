#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTrackedMemoryChunkProvider
    : public IMemoryChunkProvider
{
public:
    TTrackedMemoryChunkProvider(
        TStringBuf key,
        TMemoryProviderMapByTagPtr parent,
        size_t limit,
        IMemoryUsageTrackerPtr memoryTracker);

    TTrackedMemoryChunkProvider(
        IMemoryUsageTrackerPtr memoryTracker,
        bool allowMemoryOvercommit);

    std::unique_ptr<TAllocationHolder> Allocate(size_t size, TRefCountedTypeCookie cookie) override;

    size_t GetMaxAllocated();

    ~TTrackedMemoryChunkProvider();

private:
    struct THolder;

    const std::string Key_;
    const TMemoryProviderMapByTagPtr Parent_;
    const size_t Limit_;
    const IMemoryUsageTrackerPtr MemoryTracker_;
    const bool AllowMemoryOvercommit_ = false;

    std::atomic<size_t> Allocated_ = 0;
    std::atomic<size_t> MaxAllocated_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TTrackedMemoryChunkProvider)

////////////////////////////////////////////////////////////////////////////////

IMemoryChunkProviderPtr GetTrackedMemoryChunkProvider(
    IMemoryUsageTrackerPtr memoryUsageTracker,
    bool allowMemoryOvercommit = false);

////////////////////////////////////////////////////////////////////////////////

class TMemoryProviderMapByTag
    : public TRefCounted
{
public:
    TTrackedMemoryChunkProviderPtr GetOrCreateProvider(
        TStringBuf tag,
        size_t limit,
        IMemoryUsageTrackerPtr memoryTracker);

    friend class TTrackedMemoryChunkProvider;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<std::string, TWeakPtr<TTrackedMemoryChunkProvider>, THash<TStringBuf>, TEqualTo<>> Map_;
};

DEFINE_REFCOUNTED_TYPE(TMemoryProviderMapByTag)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
