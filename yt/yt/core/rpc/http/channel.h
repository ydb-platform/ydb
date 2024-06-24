#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>
#include <yt/yt/core/https/public.h>
#include <yt/yt/core/rpc/public.h>

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via HTTP/HTTPs.
NRpc::IChannelPtr CreateHttpChannel(
    const TString& address,
    const NConcurrency::IPollerPtr& poller,
    bool isHttps = true,
    NHttps::TClientCredentialsConfigPtr credentials = nullptr
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
