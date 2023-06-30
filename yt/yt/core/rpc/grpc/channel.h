#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via GRPC.
NRpc::IChannelPtr CreateGrpcChannel(TChannelConfigPtr config);

//! Returns the factory for creating GRPC channels.
NRpc::IChannelFactoryPtr GetGrpcChannelFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
