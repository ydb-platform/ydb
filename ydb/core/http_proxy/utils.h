#pragma once

#include "exceptions_mapping.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <util/datetime/base.h>

namespace NKikimr::NHttpProxy {

struct THttpRequestContext;

TException MapToException(NYdb::EStatus status, const TString& method, size_t issueCode);

TString LogHttpRequestResponseCommonInfoString(const THttpRequestContext& httpContext, TInstant startTime, TStringBuf api, TStringBuf topicPath, TStringBuf method, TStringBuf userSid, int httpCode, TStringBuf httpResponseMessage);

} // namespace NKikimr::NHttpProxy
