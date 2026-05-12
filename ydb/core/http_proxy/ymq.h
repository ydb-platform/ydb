#pragma once

#include "http_req.h"

namespace NKikimr::NHttpProxy {

std::unique_ptr<IHttpRequestProcessor> CreateYmqHttpController();

} // NKikimr::NHttpProxy
