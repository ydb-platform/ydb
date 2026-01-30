#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TRemoteTimestampProviderConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
    //! Timeout for RPC requests to timestamp provider.
    TDuration RpcTimeout;

    //! Interval between consecutive updates of latest timestamp.
    TDuration LatestTimestampUpdatePeriod;

    //! All generation requests coming within this period are batched
    //! together.
    TDuration BatchPeriod;

    bool EnableTimestampProviderDiscovery;
    TDuration TimestampProviderDiscoveryPeriod;
    TDuration TimestampProviderDiscoveryPeriodSplay;

    TRemoteTimestampProviderConfigPtr ApplyDynamic(
        const TRemoteTimestampProviderDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TRemoteTimestampProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRemoteTimestampProviderDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<TDuration> BatchPeriod;

    REGISTER_YSON_STRUCT(TRemoteTimestampProviderDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteTimestampProviderDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAlienTimestampProviderConfig
    : public  NYTree::TYsonStruct
{
    //! Clock server cell tag
    NObjectClient::TCellTag ClockClusterTag;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;

    REGISTER_YSON_STRUCT(TAlienTimestampProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlienTimestampProviderConfig)

DECLARE_REFCOUNTED_STRUCT(TAlienTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
