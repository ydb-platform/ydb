#pragma once

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/rpc/unittests/lib/no_baggage_service.pb.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TNoBaggageProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TNoBaggageProxy, NoBaggageService,
        .SetProtocolVersion(1)
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NNoBaggageRpc, ExpectNoBaggage);
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateNoBaggageService(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
