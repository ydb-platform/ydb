#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServerPtr CreateServer(NYT::NHttp::IServerPtr httpServer);

//! Creates an RPC-over-HTTP(s) server, instantiating the underlying HTTP server
//! from #config (HTTPs is used iff #config carries TLS credentials).
NRpc::IServerPtr CreateServer(TServerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
