#pragma once

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/rpc/unittests/lib/my_service.pb.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMyFeature,
    ((Cool)     (0))
    ((Great)    (1))
);

class TMyProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMyProxy, MyService,
        .SetProtocolVersion(1)
        .SetFeaturesType<EMyFeature>());

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, PassCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, RegularAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NullAndEmptyAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, Compression);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NotRegistered);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCanceledCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NoReply);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, FlakyCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, RequireCoolFeature);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, RequestBytesThrottledCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, StreamingEcho,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NMyRpc, ServerStreamsAborted,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NMyRpc, ServerNotReading,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NMyRpc, ServerNotWriting,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NMyRpc, GetTraceBaggage);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, CustomMetadata);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, GetChannelFailureError);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(IMyService)

class IMyService
    : public virtual IService
{
public:
    virtual TFuture<void> GetSlowCallCanceled() const = 0;
    virtual TFuture<void> GetServerStreamsAborted() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMyService)

////////////////////////////////////////////////////////////////////////////////

using TTestCreateChannelCallback = TCallback<IChannelPtr(const TString& address)>;

IMyServicePtr CreateMyService(
    IInvokerPtr invoker,
    bool secure,
    TTestCreateChannelCallback createChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
