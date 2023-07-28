#include "no_baggage_service.h"

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <gtest/gtest.h>

namespace NYT::NRpc {

using namespace NTracing;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TNoBaggageService
    : public TServiceBase
    , public virtual IService
{
public:
    TNoBaggageService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TNoBaggageProxy::GetDescriptor(),
            NLogging::TLogger("Main"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExpectNoBaggage));
    }

    DECLARE_RPC_SERVICE_METHOD(NNoBaggageRpc, ExpectNoBaggage)
    {
        context->SetRequestInfo();
        auto baggage = GetCurrentTraceContext()->UnpackBaggage();
        EXPECT_FALSE(baggage);
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateNoBaggageService(IInvokerPtr invoker)
{
    return New<TNoBaggageService>(invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
