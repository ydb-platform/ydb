#pragma once

#include "exceptions_mapping.h"
#include "http_req.h"

#include <util/datetime/base.h>

namespace NKikimr::NHttpProxy {

TException MapToException(NYdb::EStatus status, const TString& method, size_t issueCode = ISSUE_CODE_ERROR);
TString LogHttpRequestResponseCommonInfoString(const THttpRequestContext& httpContext, TInstant startTime, TStringBuf api, TStringBuf topicPath, TStringBuf method, TStringBuf userSid, int httpCode, TStringBuf httpResponseMessage);

} // namespace NKikimr::NHttpProxy
