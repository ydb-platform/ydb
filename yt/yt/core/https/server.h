#pragma once

#include "public.h"

#include "yt/yt/core/actions/public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

NHttp::IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

// A private poller object will be created and owned by the server.
// When the server is stopped, the poller will be shut down, and all active requests will be promptly canceled.
NHttp::IServerPtr CreateServer(
    const TServerConfigPtr& config,
    int pollerThreadCount);

NHttp::IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller,
    const NConcurrency::IPollerPtr& acceptor,
    const IInvokerPtr& controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
