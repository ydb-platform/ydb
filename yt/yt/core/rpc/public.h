#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/bus/public.h>

#include <library/cpp/yt/misc/guid.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqDiscover;
class TRspDiscover;
class TRequestHeader;
class TResponseHeader;
class TCredentialsExt;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TStreamingParameters;
struct TStreamingPayload;
struct TStreamingFeedback;

struct TServiceDescriptor;
struct TMethodDescriptor;

DECLARE_REFCOUNTED_CLASS(TRequestQueue)

DECLARE_REFCOUNTED_STRUCT(IRequestQueueProvider)
DECLARE_REFCOUNTED_CLASS(TPerUserRequestQueueProvider);

using TInvokerProvider = TCallback<IInvokerPtr(const NRpc::NProto::TRequestHeader&)>;

DECLARE_REFCOUNTED_CLASS(TClientRequest)
DECLARE_REFCOUNTED_CLASS(TClientResponse)

template <class TRequestMessage, class TResponse>
class TTypedClientRequest;

class TClientResponse;

template <class TResponseMessage>
class TTypedClientResponse;

struct TServiceId;

struct TAuthenticationContext;
struct TAuthenticationIdentity;
struct TAuthenticationResult;

DECLARE_REFCOUNTED_STRUCT(IClientRequest)
DECLARE_REFCOUNTED_STRUCT(IClientRequestControl)
DECLARE_REFCOUNTED_STRUCT(IClientResponseHandler)
DECLARE_REFCOUNTED_STRUCT(IServer)
DECLARE_REFCOUNTED_STRUCT(IService)
DECLARE_REFCOUNTED_STRUCT(IServiceWithReflection)
DECLARE_REFCOUNTED_STRUCT(IServiceContext)
DECLARE_REFCOUNTED_STRUCT(IChannel)
DECLARE_REFCOUNTED_STRUCT(IThrottlingChannel)
DECLARE_REFCOUNTED_STRUCT(IChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IRoamingChannelProvider)
DECLARE_REFCOUNTED_STRUCT(IAuthenticator)
DECLARE_REFCOUNTED_STRUCT(IResponseKeeper)
DECLARE_REFCOUNTED_STRUCT(IOverloadController)

DECLARE_REFCOUNTED_CLASS(TClientContext)
DECLARE_REFCOUNTED_CLASS(TServiceBase)
DECLARE_REFCOUNTED_CLASS(TChannelWrapper)
DECLARE_REFCOUNTED_CLASS(TStaticChannelFactory)
DECLARE_REFCOUNTED_CLASS(TClientRequestControlThunk)
DECLARE_REFCOUNTED_CLASS(TCachingChannelFactory)
DECLARE_REFCOUNTED_CLASS(TCongestionController)

DECLARE_REFCOUNTED_CLASS(TAttachmentsInputStream)
DECLARE_REFCOUNTED_CLASS(TAttachmentsOutputStream)

DECLARE_REFCOUNTED_STRUCT(IViablePeerRegistry)
DECLARE_REFCOUNTED_STRUCT(IDiscoverRequestHook)
DECLARE_REFCOUNTED_STRUCT(IPeerDiscovery)
DECLARE_REFCOUNTED_CLASS(TDynamicChannelPool)

template <
    class TServiceContext,
    class TServiceContextWrapper,
    class TRequestMessage,
    class TResponseMessage
>
class TGenericTypedServiceContext;

struct THandlerInvocationOptions;

class TServiceContextWrapper;

template <class TRequestMessage, class TResponseMessage>
using TTypedServiceContext = TGenericTypedServiceContext<
    IServiceContext,
    TServiceContextWrapper,
    TRequestMessage,
    TResponseMessage
>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(THistogramExponentialBounds)
DECLARE_REFCOUNTED_STRUCT(TTimeHistogramConfig)
DECLARE_REFCOUNTED_STRUCT(TServerConfig)
DECLARE_REFCOUNTED_STRUCT(TServiceCommonConfig)
DECLARE_REFCOUNTED_STRUCT(TServerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TServiceCommonDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TMethodConfig)
DECLARE_REFCOUNTED_STRUCT(TRetryingChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TViablePeerRegistryConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicChannelPoolConfig)
DECLARE_REFCOUNTED_STRUCT(TServiceDiscoveryEndpointsConfig)
DECLARE_REFCOUNTED_STRUCT(TBalancingChannelConfigBase)
DECLARE_REFCOUNTED_STRUCT(TBalancingChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TThrottlingChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TThrottlingChannelDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TResponseKeeperConfig)
DECLARE_REFCOUNTED_STRUCT(TDispatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TDispatcherDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TServiceMethodConfig)
DECLARE_REFCOUNTED_STRUCT(TOverloadTrackerMeanWaitTimeConfig)
DECLARE_REFCOUNTED_STRUCT(TOverloadTrackerBacklogQueueFillFractionConfig)
DECLARE_REFCOUNTED_STRUCT(TOverloadControllerConfig)

struct TRequestQueueThrottlerConfigs
{
    NConcurrency::TThroughputThrottlerConfigPtr WeightThrottlerConfig;
    NConcurrency::TThroughputThrottlerConfigPtr BytesThrottlerConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct TTimeoutOptions
{
    std::optional<TDuration> Timeout;
};

////////////////////////////////////////////////////////////////////////////////

using NBus::EMultiplexingBand;

using TRequestId = TGuid;
extern const TRequestId NullRequestId;

using TRealmId = TGuid;
extern const TRealmId NullRealmId;

using TMutationId = TGuid;
extern const TMutationId NullMutationId;

extern const std::string RootUserName;

constexpr int TypicalMessagePartCount = 8;

// COMPAT(nadya02): remove it when all timeouts are set
constexpr TDuration DefaultRpcRequestTimeout = TDuration::Hours(24);

using TFeatureIdFormatter = const std::function<std::optional<TStringBuf>(int featureId)>*;

////////////////////////////////////////////////////////////////////////////////

extern const std::string RequestIdAnnotation;
extern const std::string EndpointAnnotation;
extern const std::string RequestInfoAnnotation;
extern const std::string RequestUser;
extern const std::string ResponseInfoAnnotation;

extern const std::string FeatureIdAttributeKey;
extern const std::string FeatureNameAttributeKey;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((TransportError)               (static_cast<int>(NBus::EErrorCode::TransportError)))
    ((ProtocolError)                (101))
    ((NoSuchService)                (102))
    ((NoSuchMethod)                 (103))
    ((Unavailable)                  (105)) // The server is not capable of serving requests and
                                           // must not receive any more load.
    ((TransientFailure)             (116)) // Similar to Unavailable but indicates a transient issue,
                                           // which can be safely retried.
    ((PoisonPill)                   (106)) // The client must die upon receiving this error.
    ((RequestQueueSizeLimitExceeded)(108))
    ((AuthenticationError)          (109))
    ((InvalidCsrfToken)             (110))
    ((InvalidCredentials)           (111))
    ((StreamingNotSupported)        (112))
    ((UnsupportedClientFeature)     (113))
    ((UnsupportedServerFeature)     (114))
    ((PeerBanned)                   (115)) // The server is explicitly banned and thus must be dropped.
    ((NoSuchRealm)                  (117))
    ((Overloaded)                   (118)) // The server is currently overloaded and unable to handle additional requests.
                                           // The client should try to reduce their request rate until the server has had a chance to recover.
    ((SslError)                     (static_cast<int>(NBus::EErrorCode::SslError)))
    ((RequestMemoryPressure)        (120)) // There is no enough memory to handle RPC request.
    ((GlobalDiscoveryError)         (121)) // Single peer discovery interrupts discovery session.
    ((ResponseMemoryPressure)       (122)) // There is no enough memory to handle RPC response.
);

DEFINE_ENUM(EMessageFormat,
    ((Protobuf)    (0))
    ((Json)        (1))
    ((Yson)        (2))
);

YT_DECLARE_RECONFIGURABLE_SINGLETON(TDispatcherConfig, TDispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
