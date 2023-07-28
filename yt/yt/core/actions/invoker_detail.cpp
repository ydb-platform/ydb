#include "invoker_detail.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tag.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TInvokerWrapper::TInvokerWrapper(IInvokerPtr underlyingInvoker)
    : UnderlyingInvoker_(std::move(underlyingInvoker))
{
    YT_VERIFY(UnderlyingInvoker_);
}

void TInvokerWrapper::Invoke(TClosure callback)
{
    return UnderlyingInvoker_->Invoke(std::move(callback));
}

void TInvokerWrapper::Invoke(TMutableRange<TClosure> callbacks)
{
    return UnderlyingInvoker_->Invoke(callbacks);
}

NThreading::TThreadId TInvokerWrapper::GetThreadId() const
{
    return UnderlyingInvoker_->GetThreadId();
}

bool TInvokerWrapper::CheckAffinity(const IInvokerPtr& invoker) const
{
    return
        invoker.Get() == this ||
        UnderlyingInvoker_->CheckAffinity(invoker);
}

bool TInvokerWrapper::IsSerialized() const
{
    return UnderlyingInvoker_->IsSerialized();
}

void TInvokerWrapper::RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver)
{
    return UnderlyingInvoker_->RegisterWaitTimeObserver(waitTimeObserver);
}

////////////////////////////////////////////////////////////////////////////////

TInvokerProfileWrapper::TInvokerProfileWrapper(NProfiling::IRegistryImplPtr registry, const TString& invokerFamily, const NProfiling::TTagSet& tagSet)
{
    auto profiler = NProfiling::TProfiler("/invoker", NProfiling::TProfiler::DefaultNamespace, tagSet, registry).WithHot();
    WaitTimer_ = profiler.Timer(invokerFamily + "/wait");
}

TClosure TInvokerProfileWrapper::WrapCallback(TClosure callback)
{
    auto invokedAt = GetCpuInstant();

    return BIND([invokedAt, waitTimer = WaitTimer_, callback = std::move(callback)] {
        // Measure the time from WrapCallback() to callback().
        auto waitTime = CpuDurationToDuration(GetCpuInstant() - invokedAt);
        waitTimer.Record(waitTime);
        callback();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
