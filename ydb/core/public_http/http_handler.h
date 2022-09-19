#pragma once

#include "http_req.h"
#include <library/cpp/actors/http/http_proxy.h>

namespace NKikimr::NPublicHttp {

using THttpHandler = std::function<NActors::IActor*(const THttpRequestContext& request)>;

} // namespace NKikimr::NPublicHttp
