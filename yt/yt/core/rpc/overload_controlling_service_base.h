#pragma once

#include "public.h"

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TBaseService>
class TOverloadControllingServiceBase
    : public TBaseService
{
public:
    template <typename... TArgs>
    explicit TOverloadControllingServiceBase(IOverloadControllerPtr controller, TArgs&&... args);

    using TRuntimeMethodInfoPtr = NRpc::TServiceBase::TRuntimeMethodInfoPtr;
    using TMethodDescriptor = NRpc::TServiceBase::TMethodDescriptor;

    TRuntimeMethodInfoPtr RegisterMethod(const TMethodDescriptor& descriptor) override;
    void SubscribeLoadAdjusted();

protected:
    std::optional<TError> GetThrottledError(const NRpc::NProto::TRequestHeader& requestHeader) override;
    void HandleRequest(
        std::unique_ptr<NRpc::NProto::TRequestHeader> header,
        TSharedRefArray message,
        NBus::IBusPtr replyBus) override;

private:
    IOverloadControllerPtr Controller_;
    THashSet<std::string> Methods_;

    void HandleLoadAdjusted();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define OVERLOAD_CONTROLLING_SERVICE_BASE_INL_H_
#include "overload_controlling_service_base-inl.h"
#undef OVERLOAD_CONTROLLING_SERVICE_BASE_INL_H_
