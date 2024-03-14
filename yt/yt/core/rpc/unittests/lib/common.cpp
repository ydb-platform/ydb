#include "common.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TString TRpcOverUdsImpl::SocketPath_ = "";

////////////////////////////////////////////////////////////////////////////////

TTestNodeMemoryTracker::TTestNodeMemoryTracker(size_t limit)
    : Usage_(0)
    , Limit_(limit)
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
    return GetFree() > 0;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
