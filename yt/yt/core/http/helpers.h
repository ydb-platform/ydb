#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/tracing/public.h>

#include <util/generic/string.h>

namespace NYT::NHttp {

namespace NHeaders {

////////////////////////////////////////////////////////////////////////////////

inline const TString AcceptHeaderName("Accept");
inline const TString AccessControlAllowCredentialsHeaderName("Access-Control-Allow-Credentials");
inline const TString AccessControlAllowHeadersHeaderName("Access-Control-Allow-Headers");
inline const TString AccessControlAllowMethodsHeaderName("Access-Control-Allow-Methods");
inline const TString AccessControlAllowOriginHeaderName("Access-Control-Allow-Origin");
inline const TString AccessControlExposeHeadersHeaderName("Access-Control-Expose-Headers");
inline const TString AccessControlMaxAgeHeaderName("Access-Control-Max-Age");
inline const TString AuthorizationHeaderName("Authorization");
inline const TString CacheControlHeaderName("Cache-Control");
inline const TString ContentRangeHeaderName("Content-Range");
inline const TString ContentTypeHeaderName("Content-Type");
inline const TString CookieHeaderName("Cookie");
inline const TString ExpiresHeaderName("Expires");
inline const TString PragmaHeaderName("Pragma");
inline const TString RangeHeaderName("Range");
inline const TString RequestTimeoutHeaderName("Request-Timeout");
inline const TString UserAgentHeaderName("User-Agent");
inline const TString XContentTypeOptionsHeaderName("X-Content-Type-Options");
inline const TString XRequestTimeoutHeaderName("X-Request-Timeout");

inline const TString UserTicketHeaderName("X-Ya-User-Ticket");
inline const TString XDnsPrefetchControlHeaderName("X-DNS-Prefetch-Control");
inline const TString XForwardedForYHeaderName("X-Forwarded-For-Y");
inline const TString XFrameOptionsHeaderName("X-Frame-Options");
inline const TString XSourcePortYHeaderName("X-Source-Port-Y");

inline const TString ProtocolVersionMajor("X-YT-Rpc-Protocol-Version-Major");
inline const TString ProtocolVersionMinor("X-YT-Rpc-Protocol-Version-Minor");
inline const TString RequestFormatOptionsHeaderName("X-YT-Request-Format-Options");
inline const TString RequestIdHeaderName("X-YT-Request-Id");
inline const TString ResponseFormatOptionsHeaderName("X-YT-Response-Format-Options");
inline const TString UserNameHeaderName("X-YT-User-Name");
inline const TString UserTagHeaderName("X-YT-User-Tag");
inline const TString XYTErrorHeaderName("X-YT-Error");
inline const TString XYTResponseCodeHeaderName("X-YT-Response-Code");
inline const TString XYTResponseMessageHeaderName("X-YT-Response-Message");
inline const TString XYTSpanIdHeaderName("X-YT-Span-Id");
inline const TString XYTTraceIdHeaderName("X-YT-Trace-Id");

////////////////////////////////////////////////////////////////////////////////

} // namespace Headers

////////////////////////////////////////////////////////////////////////////////

void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error);
void FillYTErrorTrailers(const IResponseWriterPtr& rsp, const TError& error);

TError ParseYTError(const IResponsePtr& rsp, bool fromTrailers = false);

//! Catches exception thrown from underlying handler body and
//! translates it into HTTP error.
IHttpHandlerPtr WrapYTException(IHttpHandlerPtr underlying);

bool MaybeHandleCors(
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp,
    const TCorsConfigPtr& config = New<TCorsConfig>());

THashMap<TString, TString> ParseCookies(TStringBuf cookies);

void ProtectCsrfToken(const IResponseWriterPtr& rsp);

std::optional<TString> FindHeader(const IRequestPtr& req, const TString& headerName);
std::optional<TString> FindBalancerRequestId(const IRequestPtr& req);
std::optional<TString> FindBalancerRealIP(const IRequestPtr& req);

std::optional<TString> FindUserAgent(const IRequestPtr& req);
void SetUserAgent(const THeadersPtr& headers, const TString& value);

void ReplyJson(const IResponseWriterPtr& rsp, std::function<void(NYson::IYsonConsumer*)> producer);

void ReplyError(const IResponseWriterPtr& response, const TError& error);

NTracing::TTraceId GetTraceId(const IRequestPtr& req);
void SetTraceId(const IResponseWriterPtr& rsp, NTracing::TTraceId traceId);

void SetRequestId(const IResponseWriterPtr& rsp, NRpc::TRequestId requestId);

NTracing::TSpanId GetSpanId(const IRequestPtr& req);

NTracing::TTraceContextPtr GetOrCreateTraceContext(const IRequestPtr& req);

std::optional<std::pair<i64, i64>> FindBytesRange(const THeadersPtr& headers);
void SetBytesRange(const THeadersPtr& headers, std::pair<i64, i64> range);

TString SanitizeUrl(const TString& url);

std::vector<std::pair<TString, TString>> DumpUnknownHeaders(const THeadersPtr& headers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
