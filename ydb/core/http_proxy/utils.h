#pragma once

#include "exceptions_mapping.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <util/datetime/base.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NHttpProxy {

struct THttpRequestContext;

TException MapToException(NYdb::EStatus status, const TString& method, size_t issueCode);

TString LogHttpRequestResponseCommonInfoString(const THttpRequestContext& httpContext, TInstant startTime, TStringBuf api, TStringBuf topicPath, TStringBuf method, TStringBuf userSid, int httpCode, TStringBuf httpResponseMessage);

// host is the HTTP Host / :authority value. headers is the raw header blob.
// tlsSecure is true when the connection to http_proxy itself is TLS.
// Prefers the first X-Forwarded-Host token when it is a host[:port] without '/', '#' or '?';
// otherwise uses host. Unknown X-Forwarded-Proto values fall back to tlsSecure.
TString MakeSqsRequestEndpoint(TStringBuf host, TStringBuf headers, bool tlsSecure);
TString MakeSqsRequestEndpoint(const THttpRequestContext& httpContext);

// HTTP request path used as the SQS/Kinesis database endpoint.
// Query string is ignored; a trailing slash is treated as equivalent to no slash.
// "/" and empty path mean "no database".
TString ParseDatabasePathFromRequestUrl(TStringBuf url);

} // namespace NKikimr::NHttpProxy
