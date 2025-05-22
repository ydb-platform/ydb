#include "invoker_detail.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tag.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <bool VirtualizeBase>
TInvokerWrapper<VirtualizeBase>::TInvokerWrapper(IInvokerPtr underlyingInvoker)
    : UnderlyingInvoker_(std::move(underlyingInvoker))
{
    YT_VERIFY(UnderlyingInvoker_);
}

template <bool VirtualizeBase>
void TInvokerWrapper<VirtualizeBase>::Invoke(TMutableRange<TClosure> callbacks)
{
    for (auto& callback : callbacks) {
        static_cast<IInvoker*>(this)->Invoke(std::move(callback));
    }
}

template <bool VirtualizeBase>
NThreading::TThreadId TInvokerWrapper<VirtualizeBase>::GetThreadId() const
{
    return UnderlyingInvoker_->GetThreadId();
}

template <bool VirtualizeBase>
bool TInvokerWrapper<VirtualizeBase>::CheckAffinity(const IInvokerPtr& invoker) const
{
    return
        invoker.Get() == this ||
        UnderlyingInvoker_->CheckAffinity(invoker);
}

template <bool VirtualizeBase>
bool TInvokerWrapper<VirtualizeBase>::IsSerialized() const
{
    return UnderlyingInvoker_->IsSerialized();
}

template <bool VirtualizeBase>
void TInvokerWrapper<VirtualizeBase>::SubscribeWaitTimeObserved(const IInvoker::TWaitTimeObserver& callback)
{
    return UnderlyingInvoker_->SubscribeWaitTimeObserved(callback);
}

template <bool VirtualizeBase>
void TInvokerWrapper<VirtualizeBase>::UnsubscribeWaitTimeObserved(const IInvoker::TWaitTimeObserver& callback)
{
    return UnderlyingInvoker_->SubscribeWaitTimeObserved(callback);
}

////////////////////////////////////////////////////////////////////////////////

template class TInvokerWrapper<true>;
template class TInvokerWrapper<false>;
// template struct NDetail::TMaybeVirtualInvokerBase<true>; // Primary template.
template struct NDetail::TMaybeVirtualInvokerBase<false>;

////////////////////////////////////////////////////////////////////////////////

TInvokerProfileWrapper::TInvokerProfileWrapper(NProfiling::IRegistryPtr registry, const TString& invokerFamily, const NProfiling::TTagSet& tagSet)
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
