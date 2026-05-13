#pragma once

#include "http_req.h"

namespace NKikimr::NHttpProxy {

std::shared_ptr<const IHttpController> CreateSqsHttpController(const NKikimrConfig::TServerlessProxyConfig& config);

} // namespace NKikimr::NHttpProxy
