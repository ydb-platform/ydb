#pragma once

#include "public.h"

#include <yt/yt/library/tvm/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateCredentialsInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options);

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateUserInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options);

NRpc::IChannelPtr CreateTokenInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options);

NRpc::IChannelPtr CreateCookieInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options);

NRpc::IChannelPtr CreateServiceTicketInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TAuthenticationOptions& options);

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelFactoryPtr CreateServiceTicketInjectingChannelFactory(
    NRpc::IChannelFactoryPtr underlyingFactory,
    IServiceTicketAuthPtr serviceTicketAuth);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
