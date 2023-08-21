#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProviderConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
public:
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

    REGISTER_YSON_STRUCT(TRemoteTimestampProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
