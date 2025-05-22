#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/channel.h>

#include <contrib/libs/grpc/include/grpc/impl/connectivity_state.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

struct IGrpcChannel
    : public NRpc::IChannel
{
public:
    virtual grpc_connectivity_state CheckConnectivityState(bool tryToConnect) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGrpcChannel)

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via GRPC.
IGrpcChannelPtr CreateGrpcChannel(TChannelConfigPtr config);

//! Returns the factory for creating GRPC channels.
NRpc::IChannelFactoryPtr GetGrpcChannelFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
