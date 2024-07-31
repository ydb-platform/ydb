#pragma once

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TTestNodeMemoryTracker
    : public IMemoryUsageTracker
{
public:
    explicit TTestNodeMemoryTracker(i64 limit);

    i64 GetLimit() const override;
    i64 GetUsed() const override;
    i64 GetFree() const override;
    bool IsExceeded() const override;

    TError TryAcquire(i64 size) override;
    TError TryChange(i64 size) override;
    bool Acquire(i64 size) override;
    void Release(i64 size) override;
    void SetLimit(i64 size) override;

    void ClearTotalUsage();
    i64 GetTotalUsage() const;

    TSharedRef Track(
        TSharedRef reference,
        bool keepHolder = false) override;
    TErrorOr<TSharedRef> TryTrack(
        TSharedRef reference,
        bool keepHolder = false) override;
private:

    class TTestTrackedReferenceHolder
        : public TSharedRangeHolder
    {
    public:
        TTestTrackedReferenceHolder(
            TSharedRef underlying,
            TMemoryUsageTrackerGuard guard);

        TSharedRangeHolderPtr Clone(const TSharedRangeHolderCloneOptions& options) override;

        std::optional<size_t> GetTotalByteSize() const override;

    private:
        const TSharedRef Underlying_;
        const TMemoryUsageTrackerGuard Guard_;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    i64 Limit_;
    i64 Usage_ = 0;
    i64 TotalUsage_ = 0;

    TError DoTryAcquire(i64 size);
    void DoAcquire(i64 size);
    void DoRelease(i64 size);
};

DECLARE_REFCOUNTED_CLASS(TTestNodeMemoryTracker)
DEFINE_REFCOUNTED_TYPE(TTestNodeMemoryTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
