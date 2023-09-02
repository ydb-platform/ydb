#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/http/http.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

NHttp::IHttpHandlerPtr CreateCypressCookieLoginHandler(
    TCypressCookieGeneratorConfigPtr config,
    NApi::IClientPtr client,
    ICypressCookieStorePtr cookieStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
