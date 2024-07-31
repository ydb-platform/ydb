#include "test_memory_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTestNodeMemoryTracker::TTestNodeMemoryTracker(i64 limit)
    : Limit_(limit)
{ }

i64 TTestNodeMemoryTracker::GetLimit() const
{
    auto guard = Guard(Lock_);
    return Limit_;
}

i64 TTestNodeMemoryTracker::GetUsed() const
{
    auto guard = Guard(Lock_);
    return Usage_;
}

i64 TTestNodeMemoryTracker::GetFree() const
{
    auto guard = Guard(Lock_);
    return Limit_ - Usage_;
}

bool TTestNodeMemoryTracker::IsExceeded() const
{
    auto guard = Guard(Lock_);
    return Limit_ - Usage_ <= 0;
}

TError TTestNodeMemoryTracker::TryAcquire(i64 size)
{
    auto guard = Guard(Lock_);
    return DoTryAcquire(size);
}

TError TTestNodeMemoryTracker::DoTryAcquire(i64 size)
{
    if (Usage_ + size >= Limit_) {
        return TError("Memory exceeded");
    }

    Usage_ += size;
    TotalUsage_ += size;

    return {};
}

TError TTestNodeMemoryTracker::TryChange(i64 size)
{
    auto guard = Guard(Lock_);

    if (size > Usage_) {
        return DoTryAcquire(size - Usage_);
    } else if (size < Usage_) {
        DoRelease(Usage_ - size);
    }

    return {};
}

bool TTestNodeMemoryTracker::Acquire(i64 size)
{
    auto guard = Guard(Lock_);
    DoAcquire(size);
    return Usage_ >= Limit_;
}

void TTestNodeMemoryTracker::Release(i64 size)
{
    auto guard = Guard(Lock_);
    DoRelease(size);
}

void TTestNodeMemoryTracker::SetLimit(i64 size)
{
    auto guard = Guard(Lock_);
    Limit_ = size;
}

void TTestNodeMemoryTracker::DoAcquire(i64 size)
{
    Usage_ += size;
    TotalUsage_ += size;
}

void TTestNodeMemoryTracker::DoRelease(i64 size)
{
    Usage_ -= size;
}

void TTestNodeMemoryTracker::ClearTotalUsage()
{
    TotalUsage_ = 0;
}

i64 TTestNodeMemoryTracker::GetTotalUsage() const
{
    return TotalUsage_;
}

TSharedRef TTestNodeMemoryTracker::Track(TSharedRef reference, bool keepExistingTracking)
{
    if (!reference) {
        return reference;
    }

    auto rawReference = TRef(reference);
    const auto& holder = reference.GetHolder();

    // Reference could be without a holder, e.g. empty reference.
    if (!holder) {
        YT_VERIFY(reference.Begin() == TRef::MakeEmpty().Begin());
        return reference;
    }

    auto guard = TMemoryUsageTrackerGuard::Acquire(this, reference.Size());

    auto underlyingHolder = holder->Clone({.KeepMemoryReferenceTracking = keepExistingTracking});
    auto underlyingReference = TSharedRef(rawReference, std::move(underlyingHolder));
    return TSharedRef(
        rawReference,
        New<TTestTrackedReferenceHolder>(std::move(underlyingReference), std::move(guard)));
}

TErrorOr<TSharedRef> TTestNodeMemoryTracker::TryTrack(
    TSharedRef reference,
    bool keepHolder)
{
    return Track(reference, keepHolder);
}

////////////////////////////////////////////////////////////////////////////////

TTestNodeMemoryTracker::TTestTrackedReferenceHolder::TTestTrackedReferenceHolder(
    TSharedRef underlying,
    TMemoryUsageTrackerGuard guard)
    : Underlying_(std::move(underlying))
    , Guard_(std::move(guard))
{ }

TSharedRangeHolderPtr TTestNodeMemoryTracker::TTestTrackedReferenceHolder::Clone(const TSharedRangeHolderCloneOptions& options)
{
    if (options.KeepMemoryReferenceTracking) {
        return this;
    }
    return Underlying_.GetHolder()->Clone(options);
}

std::optional<size_t> TTestNodeMemoryTracker::TTestTrackedReferenceHolder::GetTotalByteSize() const
{
    return Underlying_.GetHolder()->GetTotalByteSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
