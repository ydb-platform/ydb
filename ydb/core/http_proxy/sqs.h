#pragma once

#include "http_req.h"

namespace NKikimr::NHttpProxy {

const IHttpController* GetSqsHttpController();

} // namespace NKikimr::NHttpProxy
