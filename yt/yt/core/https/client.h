#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

NHttp::IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
