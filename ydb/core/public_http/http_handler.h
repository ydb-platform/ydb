#pragma once

#include "http_req.h"
#include <ydb/library/actors/http/http_proxy.h>

namespace NKikimr::NPublicHttp {

using THttpHandler = std::function<NActors::IActor*(const THttpRequestContext& request)>;

} // namespace NKikimr::NPublicHttp
