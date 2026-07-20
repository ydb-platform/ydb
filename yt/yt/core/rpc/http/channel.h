#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>
#include <yt/yt/core/https/public.h>
#include <yt/yt/core/rpc/public.h>

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via HTTP/HTTPs.
NRpc::IChannelPtr CreateHttpChannel(
    const std::string& address,
    const NConcurrency::IPollerPtr& poller,
    bool secure = true,
    NHttps::TClientCredentialsConfigPtr credentials = nullptr);

//! Creates a factory for HTTP/HTTPs channels; all channels share a poller owned
//! by the factory and sized per #config.
NRpc::IChannelFactoryPtr CreateHttpChannelFactory(TClientConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
