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
// Origin, highest wins per field:
//   host: RFC 7239 Forwarded host= (first valid, left to right), else first X-Forwarded-Host
//         token that is host[:port] without '/', '#' or '?', else Host.
//   proto: Forwarded proto=, else X-Forwarded-Proto; unknown values fall back to tlsSecure.
//   port: valid X-Forwarded-Port, then port in the chosen host, else the scheme default
//         (80/http, 443/https). Default ports are omitted from the URL.
TString MakeSqsRequestEndpoint(TStringBuf host, TStringBuf headers, bool tlsSecure);
TString MakeSqsRequestEndpoint(const THttpRequestContext& httpContext);

} // namespace NKikimr::NHttpProxy
