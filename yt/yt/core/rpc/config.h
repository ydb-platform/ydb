#pragma once

#include "public.h"

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/polymorphic_yson_struct.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <library/cpp/yt/misc/enum.h>

#include <vector>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestTracingMode,
    (Enable)  // Propagation only.
    (Disable) // Neither creates, nor propagates trace further.
    (Force)   // Forces trace creation.
);

////////////////////////////////////////////////////////////////////////////////

struct THistogramExponentialBounds
    : public NYTree::TYsonStruct
{
    TDuration Min;
    TDuration Max;

    REGISTER_YSON_STRUCT(THistogramExponentialBounds);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THistogramExponentialBounds)

////////////////////////////////////////////////////////////////////////////////

struct TTimeHistogramConfig
    : public NYTree::TYsonStruct
{
    std::optional<THistogramExponentialBoundsPtr> ExponentialBounds;
    std::optional<std::vector<TDuration>> CustomBounds;

    REGISTER_YSON_STRUCT(TTimeHistogramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTimeHistogramConfig)

////////////////////////////////////////////////////////////////////////////////

// Common options shared between all services in one server.
struct TServiceCommonConfig
    : public NYTree::TYsonStruct
{
    bool EnablePerUserProfiling;
    TTimeHistogramConfigPtr TimeHistogram;
    bool EnableErrorCodeCounter;
    ERequestTracingMode TracingMode;

    REGISTER_YSON_STRUCT(TServiceCommonConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceCommonConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public TServiceCommonConfig
{
    THashMap<std::string, NYTree::INodePtr> Services;

    REGISTER_YSON_STRUCT(TServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

// Common options shared between all services in one server.
struct TServiceCommonDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<bool> EnablePerUserProfiling;
    std::optional<TTimeHistogramConfigPtr> TimeHistogram;
    std::optional<bool> EnableErrorCodeCounter;
    std::optional<ERequestTracingMode> TracingMode;

    REGISTER_YSON_STRUCT(TServiceCommonDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceCommonDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServerDynamicConfig
    : public TServiceCommonDynamicConfig
{
    THashMap<std::string, NYTree::INodePtr> Services;

    REGISTER_YSON_STRUCT(TServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServiceConfig
    : public NYTree::TYsonStruct
{
    std::optional<bool> EnablePerUserProfiling;
    std::optional<bool> EnableErrorCodeCounter;
    std::optional<ERequestTracingMode> TracingMode;
    TTimeHistogramConfigPtr TimeHistogram;
    THashMap<std::string, TMethodConfigPtr> Methods;
    std::optional<int> AuthenticationQueueSizeLimit;
    std::optional<TDuration> PendingPayloadsTimeout;
    std::optional<bool> Pooled;

    REGISTER_YSON_STRUCT(TServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMethodConfig
    : public NYTree::TYsonStruct
{
    std::optional<bool> Heavy;
    std::optional<int> QueueSizeLimit;
    std::optional<i64> QueueByteSizeLimit;
    std::optional<int> ConcurrencyLimit;
    std::optional<i64> ConcurrencyByteLimit;
    std::optional<NLogging::ELogLevel> LogLevel;
    std::optional<NLogging::ELogLevel> ErrorLogLevel;
    std::optional<TDuration> LoggingSuppressionTimeout;
    NConcurrency::TThroughputThrottlerConfigPtr RequestBytesThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr RequestWeightThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr LoggingSuppressionFailedRequestThrottler;
    std::optional<ERequestTracingMode> TracingMode;
    std::optional<bool> Pooled;

    REGISTER_YSON_STRUCT(TMethodConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMethodConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRetryingChannelConfig
    : public virtual NYTree::TYsonStruct
{
    //! Time to wait between consequent attempts.
    TDuration RetryBackoffTime;

    //! Maximum number of retry attempts to make.
    int RetryAttempts;

    // COMPAT(danilalexeev): YT-23734.
    bool EnableExponentialRetryBackoffs;

    //! Retry backoff policy.
    TExponentialBackoffOptions RetryBackoff;

    //! Maximum time to spend while retrying.
    //! If null then no limit is enforced.
    std::optional<TDuration> RetryTimeout;

    REGISTER_YSON_STRUCT(TRetryingChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRetryingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPeerPriorityStrategy,
    (None)
    (PreferLocal)
);

struct TViablePeerRegistryConfig
    : public virtual NYTree::TYsonStruct
{
    //! Timeout for |Discover| requests.
    TDuration DiscoverTimeout;

    //! Timeout for acknowledgement of all RPC requests going through the channel.
    TDuration AcknowledgementTimeout;

    //! Interval between automatic rediscovery of active peers.
    /*!
     *  Discovery is started automatically if no active peers are known.
     *  In some cases, however, this is not enough.
     *  E.g. a follower may become active and thus eligible for load balancing.
     *  This setting controls the period of time after which the channel
     *  starts rediscovering peers even if an active one is known.
     */
    TDuration RediscoverPeriod;

    //! A random duration from 0 to #RediscoverSplay is added to #RediscoverPeriod on each
    //! rediscovery attempt.
    TDuration RediscoverSplay;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a hard failure (i.e. non-OK response) to |Discover| request.
    TDuration HardBackoffTime;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a soft failure (i.e. "down" response) to |Discover| request.
    TDuration SoftBackoffTime;

    //! In case too many peers are known, the registry will only maintain this many peers active.
    int MaxPeerCount;

    //! For sticky mode: number of consistent hash tokens to assign to each peer.
    int HashesPerPeer;

    //! Configures how random channels are selected.
    EPeerPriorityStrategy PeerPriorityStrategy;

    //! If set to a positive value, this number of active peers with the smallest priority will be required
    //! for priority to be taken into account when choosing a random peer according to the peer priority strategy.
    //! If it is not satisfied, peers will be chosen randomly from the whole pool of active peers.
    //!
    //! In practice: if EPeerPriorityStrategy::PreferLocal is set, it will only have an effect if there are at least
    //! MinPeerCountForPriorityAwareness active local peers, otherwise peers will be chosen uniformly from the whole set of active peers.
    //!
    //! NB: Please note that MaxPeerCount respects priorities, e.g. given EPeerPriorityStrategy::PreferLocal and
    //! MaxPeerCount = 100, if there are 200 available local and 400 available non-local peers, all active peers will be local.
    //! This means that setting MinPeerCountForPriorityAwareness close to MaxPeerCount is practically useless.
    //! If you want to set bigger values, you must also increase MaxPeerCount to accommodate more peers.
    int MinPeerCountForPriorityAwareness;

    bool EnablePowerOfTwoChoicesStrategy;

    REGISTER_YSON_STRUCT(TViablePeerRegistryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TViablePeerRegistryConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChannelPoolConfig
    : public TViablePeerRegistryConfig
{
    //! Maximum number of peers to query in parallel when locating alive ones.
    int MaxConcurrentDiscoverRequests;

    //! To avoid being stuck with the same peer set forever,
    //! one random peer could be evicted after #RandomPeerEvictionPeriod.
    TDuration RandomPeerEvictionPeriod;

    bool EnablePeerPolling;
    TDuration PeerPollingPeriod;
    TDuration PeerPollingPeriodSplay;
    TDuration PeerPollingRequestTimeout;

    TDuration DiscoverySessionTimeout;

    REGISTER_YSON_STRUCT(TDynamicChannelPoolConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChannelPoolConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServiceDiscoveryEndpointsConfig
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Cluster;
    //! NB: If empty (default) this vector is filled with the cluster above.
    std::vector<std::string> Clusters;
    std::string EndpointSetId;
    TDuration UpdatePeriod;

    //! Use IPv4 address of endpoint.
    bool UseIPv4;
    //! Use IPv6 address of endpoint.
    bool UseIPv6;

    REGISTER_YSON_STRUCT(TServiceDiscoveryEndpointsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceDiscoveryEndpointsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBalancingChannelConfigBase
    : public TDynamicChannelPoolConfig
{
    //! Disables discovery and balancing when just one address is given.
    //! This is vital for jobs since node's redirector is incapable of handling
    //! discover requests properly.
    bool DisableBalancingOnSingleAddress;

    //! Delay before sending a hedged request. If null then hedging is disabled.
    std::optional<TDuration> HedgingDelay;

    //! Whether to cancel the primary request when backup one is sent.
    bool CancelPrimaryRequestOnHedging;

    REGISTER_YSON_STRUCT(TBalancingChannelConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelConfigBase)

////////////////////////////////////////////////////////////////////////////////

struct TBalancingChannelConfig
    : public TBalancingChannelConfigBase
{
    //! First option: static list of addresses.
    std::optional<std::vector<std::string>> Addresses;

    //! Second option: SD endpoints.
    TServiceDiscoveryEndpointsConfigPtr Endpoints;

    REGISTER_YSON_STRUCT(TBalancingChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

struct TThrottlingChannelConfig
    : public virtual NYTree::TYsonStruct
{
    //! Maximum allowed number of requests per second.
    int RateLimit;

    REGISTER_YSON_STRUCT(TThrottlingChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TThrottlingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

struct TThrottlingChannelDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<int> RateLimit;

    REGISTER_YSON_STRUCT(TThrottlingChannelDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TThrottlingChannelDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TResponseKeeperConfig
    : public NYTree::TYsonStruct
{
    //! For how long responses are kept in memory.
    TDuration ExpirationTime;

    //! How often an eviction tick is initiated. Eviction drops old responses
    //! that need no longer be kept in memory.
    TDuration EvictionPeriod;

    //! Maximum time an eviction tick can spend.
    TDuration MaxEvictionTickTime;

    //! The number of responses to evict between checking whether the tick is
    //! taking too long (longer than MaxEvictionTickTime).
    int EvictionTickTimeCheckPeriod;

    //! If |true| then initial warmup is enabled. In particular, #WarmupTime and #ExpirationTime are
    //! checked against each other. If |false| then initial warmup is disabled and #WarmupTime is ignored.
    bool EnableWarmup;

    //! For how long the keeper remains passive after start and merely collects all responses.
    TDuration WarmupTime;

    REGISTER_YSON_STRUCT(TResponseKeeperConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResponseKeeperConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDispatcherConfig
    : public NYTree::TYsonStruct
{
    int HeavyPoolSize;
    int CompressionPoolSize;
    TDuration HeavyPoolPollingPeriod;
    TDuration DefaultRequestTimeout;

    bool AlertOnMissingRequestInfo;
    bool AlertOnUnsetRequestTimeout;

    bool SendTracingBaggage;

    TDispatcherConfigPtr ApplyDynamic(const TDispatcherDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TDispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDispatcherDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<int> HeavyPoolSize;
    std::optional<int> CompressionPoolSize;
    std::optional<TDuration> HeavyPoolPollingPeriod;
    std::optional<TDuration> DefaultRequestTimeout;

    std::optional<bool> AlertOnMissingRequestInfo;
    std::optional<bool> AlertOnUnsetRequestTimeout;

    std::optional<bool> SendTracingBaggage;

    REGISTER_YSON_STRUCT(TDispatcherDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDispatcherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TServiceMethod
    : public NYTree::TYsonStructLite
{
    std::string Service;
    std::string Method;

    REGISTER_YSON_STRUCT_LITE(TServiceMethod);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TServiceMethodConfig
    : public NYTree::TYsonStruct
{
    std::string Service;
    std::string Method;

    int MaxWindow;
    double WaitingTimeoutFraction;

    REGISTER_YSON_STRUCT(TServiceMethodConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceMethodConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOverloadTrackerConfigBase
    : public NYTree::TYsonStruct
{
    std::vector<TServiceMethod> MethodsToThrottle;

    REGISTER_YSON_STRUCT(TOverloadTrackerConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TOverloadTrackerMeanWaitTimeConfig
    : public TOverloadTrackerConfigBase
{
    TDuration MeanWaitTimeThreshold;

    REGISTER_YSON_STRUCT(TOverloadTrackerMeanWaitTimeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOverloadTrackerMeanWaitTimeConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOverloadTrackerBacklogQueueFillFractionConfig
    : public TOverloadTrackerConfigBase
{
    double BacklogQueueFillFractionThreshold;

    REGISTER_YSON_STRUCT(TOverloadTrackerBacklogQueueFillFractionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOverloadTrackerBacklogQueueFillFractionConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOverloadTrackerConfigType,
    (Base)
    (MeanWaitTime)
    (BacklogQueueFillFraction)
);

DEFINE_POLYMORPHIC_YSON_STRUCT_FOR_ENUM_WITH_DEFAULT(OverloadTrackerConfig, EOverloadTrackerConfigType, MeanWaitTime,
    ((Base)                     (TOverloadTrackerConfigBase))
    ((MeanWaitTime)             (TOverloadTrackerMeanWaitTimeConfig))
    ((BacklogQueueFillFraction) (TOverloadTrackerBacklogQueueFillFractionConfig))
);

////////////////////////////////////////////////////////////////////////////////

struct TOverloadControllerConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;
    THashMap<std::string, TOverloadTrackerConfig> Trackers;
    std::vector<TServiceMethodConfigPtr> Methods;
    TDuration LoadAdjustingPeriod;

    REGISTER_YSON_STRUCT(TOverloadControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOverloadControllerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
