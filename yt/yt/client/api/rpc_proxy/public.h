#pragma once

#include <yt/yt/client/api/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionOptions;

DECLARE_REFCOUNTED_STRUCT(IRowStreamEncoder)
DECLARE_REFCOUNTED_STRUCT(IRowStreamDecoder)

DECLARE_REFCOUNTED_CLASS(TConnectionConfig)

extern const TString ApiServiceName;
extern const TString DiscoveryServiceName;

constexpr int CurrentWireFormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(babenko): get rid of this in favor of NRpc::EErrorCode::PeerBanned
YT_DEFINE_ERROR_ENUM(
    ((ProxyBanned) (2100))
);

DEFINE_ENUM(ERpcProxyFeature,
    ((GetInSyncWithoutKeys)(0))
    ((WideLocks)           (1))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAddressType,
    ((InternalRpc)        (0))
    ((MonitoringHttp)     (1))
    ((TvmOnlyInternalRpc) (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
