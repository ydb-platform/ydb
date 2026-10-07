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
    virtual grpc_connectivity_state CheckConnectivityState(bool tryToConnect) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGrpcChannel)

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via GRPC.
IGrpcChannelPtr CreateGrpcChannel(TChannelConfigPtr config);

//! Creates a factory for creating GRPC channels; #config provides the
//! credentials and GRPC arguments shared by all channels (the address is supplied
//! per channel).
NRpc::IChannelFactoryPtr CreateGrpcChannelFactory(TChannelFactoryConfigPtr config);

//! Returns the factory for creating GRPC channels with default settings.
NRpc::IChannelFactoryPtr GetDefaultGrpcChannelFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
