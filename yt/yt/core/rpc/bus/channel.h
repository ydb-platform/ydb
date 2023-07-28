#pragma once

#include "public.h"

#include <yt/yt/core/bus/tcp/public.h>

namespace NYT::NRpc::NBus {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via Bus.
IChannelPtr CreateBusChannel(NYT::NBus::IBusClientPtr client);

//! Creates a factory for creating TCP Bus channels.
IChannelFactoryPtr CreateTcpBusChannelFactory(NYT::NBus::TBusConfigPtr config);

//! Creates a factory for creating Unix domain socket (UDS) Bus channels.
IChannelFactoryPtr CreateUdsBusChannelFactory(NYT::NBus::TBusConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NBus
