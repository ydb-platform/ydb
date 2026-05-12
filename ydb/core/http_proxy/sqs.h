#pragma once

#include "http_req.h"

namespace NKikimr::NHttpProxy {

std::shared_ptr<const IHttpController> CreateSqsHttpController();

} // NKikimr::NHttpProxy
