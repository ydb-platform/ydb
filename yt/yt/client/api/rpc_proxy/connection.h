#pragma once

#include "public.h"

#include <yt/yt/client/api/connection.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    TConnectionOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
