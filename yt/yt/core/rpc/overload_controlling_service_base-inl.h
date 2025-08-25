#ifndef OVERLOAD_CONTROLLING_SERVICE_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include overload_controlling_service_base.h"
// For the sake of sane code completion.
#include "overload_controlling_service_base.h"
#endif

#include "overload_controller.h"

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

template <class TBaseService>
template <typename... TArgs>
TOverloadControllingServiceBase<TBaseService>::TOverloadControllingServiceBase(
    IOverloadControllerPtr controller,
    TArgs&&... args)
    : TBaseService(std::forward<TArgs>(args)...)
    , Controller_(std::move(controller))
{
    YT_VERIFY(Controller_);
}

template <class TBaseService>
void TOverloadControllingServiceBase<TBaseService>::SubscribeLoadAdjusted()
{
    Controller_->SubscribeLoadAdjusted(BIND(
        &TOverloadControllingServiceBase::HandleLoadAdjusted,
        MakeWeak(this)));
}

template <class TBaseService>
auto TOverloadControllingServiceBase<TBaseService>::RegisterMethod(
    const TMethodDescriptor& descriptor) -> TRuntimeMethodInfoPtr
{
    Methods_.insert(descriptor.Method);
    return TBaseService::RegisterMethod(descriptor);
}

template <class TBaseService>
void TOverloadControllingServiceBase<TBaseService>::HandleLoadAdjusted()
{
    const auto& serviceName = TBaseService::GetServiceId().ServiceName;

    for (const auto& method : Methods_) {
        auto* runtimeInfo = TBaseService::FindMethodInfo(method);
        YT_VERIFY(runtimeInfo);

        auto congestionState = Controller_->GetCongestionState(serviceName, method);
        runtimeInfo->ConcurrencyLimit.SetDynamicLimit(congestionState.CurrentWindow);
        runtimeInfo->WaitingTimeoutFraction.store(
            congestionState.WaitingTimeoutFraction,
            std::memory_order::relaxed);
    }
}

template <class TBaseService>
std::optional<TError> TOverloadControllingServiceBase<TBaseService>::GetThrottledError(
    const NRpc::NProto::TRequestHeader& requestHeader)
{
    auto congestionState = Controller_->GetCongestionState(requestHeader.service(), requestHeader.method());
    const auto& overloadedTrackers = congestionState.OverloadedTrackers;

    if (!overloadedTrackers.empty()) {
        return TError(NRpc::EErrorCode::Overloaded, "Instance is overloaded")
            << TErrorAttribute("overloaded_trackers", overloadedTrackers);
    }

    return TBaseService::GetThrottledError(requestHeader);
}

template <class TBaseService>
void TOverloadControllingServiceBase<TBaseService>::HandleRequest(
    std::unique_ptr<NRpc::NProto::TRequestHeader> header,
    TSharedRefArray message,
    NBus::IBusPtr replyBus)
{
    auto congestionState = Controller_->GetCongestionState(
        header->service(),
        header->method());

    if (ShouldThrottleCall(congestionState)) {
        // Give other handling routines chance to execute.
        NConcurrency::Yield();
    }

    TBaseService::HandleRequest(std::move(header), std::move(message), std::move(replyBus));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
