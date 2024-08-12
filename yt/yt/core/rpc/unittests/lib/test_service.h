#pragma once

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/rpc/unittests/lib/test_service.pb.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestFeature,
    ((Cool)     (0))
    ((Great)    (1))
);

class TTestProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTestProxy, TestService,
        .SetProtocolVersion(1)
        .SetFeaturesType<ETestFeature>());

    DEFINE_RPC_PROXY_METHOD(NTestRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, PassCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, AllocationCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, RegularAttachments);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, NullAndEmptyAttachments);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, Compression);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, DoNothing);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, CustomMessageError);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, NotRegistered);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, SlowCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, SlowCanceledCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, NoReply);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, FlakyCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, RequireCoolFeature);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, RequestBytesThrottledCall);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, StreamingEcho,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NTestRpc, ServerStreamsAborted,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NTestRpc, ServerNotReading,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NTestRpc, ServerNotWriting,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NTestRpc, GetTraceBaggage);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, CustomMetadata);
    DEFINE_RPC_PROXY_METHOD(NTestRpc, GetChannelFailureError);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ITestService)

struct ITestService
    : public virtual IService
{
    virtual TFuture<void> GetSlowCallCanceled() const = 0;
    virtual TFuture<void> GetServerStreamsAborted() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITestService)

////////////////////////////////////////////////////////////////////////////////

using TTestCreateChannelCallback = TCallback<IChannelPtr(const TString& address)>;

ITestServicePtr CreateTestService(
    IInvokerPtr invoker,
    bool secure,
    TTestCreateChannelCallback createChannel,
    IMemoryUsageTrackerPtr memoryUsageTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
