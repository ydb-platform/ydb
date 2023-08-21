#pragma once

#include "public.h"

#include "protocol_version.h"

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/discovery_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDiscoveryServiceProxy, DiscoveryService,
        .SetProtocolVersion(NRpc::TProtocolVersion{0, 0}));

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, DiscoverProxies,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
