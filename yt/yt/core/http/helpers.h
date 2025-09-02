#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/library/formats/format.h>

#include <util/generic/string.h>

namespace NYT::NHttp {

namespace NHeaders {

////////////////////////////////////////////////////////////////////////////////

inline const std::string AcceptHeaderName("Accept");
inline const std::string AccessControlAllowCredentialsHeaderName("Access-Control-Allow-Credentials");
inline const std::string AccessControlAllowHeadersHeaderName("Access-Control-Allow-Headers");
inline const std::string AccessControlAllowMethodsHeaderName("Access-Control-Allow-Methods");
inline const std::string AccessControlAllowOriginHeaderName("Access-Control-Allow-Origin");
inline const std::string AccessControlExposeHeadersHeaderName("Access-Control-Expose-Headers");
inline const std::string AccessControlMaxAgeHeaderName("Access-Control-Max-Age");
inline const std::string AuthorizationHeaderName("Authorization");
inline const std::string CacheControlHeaderName("Cache-Control");
inline const std::string ContentRangeHeaderName("Content-Range");
inline const std::string ContentTypeHeaderName("Content-Type");
inline const std::string CookieHeaderName("Cookie");
inline const std::string ExpiresHeaderName("Expires");
inline const std::string PragmaHeaderName("Pragma");
inline const std::string RangeHeaderName("Range");
inline const std::string RequestTimeoutHeaderName("Request-Timeout");
inline const std::string UserAgentHeaderName("User-Agent");
inline const std::string XContentTypeOptionsHeaderName("X-Content-Type-Options");
inline const std::string XRequestTimeoutHeaderName("X-Request-Timeout");

inline const std::string UserTicketHeaderName("X-Ya-User-Ticket");
inline const std::string ServiceTicketHeaderName("X-Ya-Service-Ticket");
inline const std::string XDnsPrefetchControlHeaderName("X-DNS-Prefetch-Control");
inline const std::string XForwardedForYHeaderName("X-Forwarded-For-Y");
inline const std::string XFrameOptionsHeaderName("X-Frame-Options");
inline const std::string XSourcePortYHeaderName("X-Source-Port-Y");

inline const std::string ProtocolVersionMajor("X-YT-Rpc-Protocol-Version-Major");
inline const std::string ProtocolVersionMinor("X-YT-Rpc-Protocol-Version-Minor");
inline const std::string RequestFormatOptionsHeaderName("X-YT-Request-Format-Options");
inline const std::string RequestIdHeaderName("X-YT-Request-Id");
inline const std::string ResponseFormatOptionsHeaderName("X-YT-Response-Format-Options");
inline const std::string UserNameHeaderName("X-YT-User-Name");
inline const std::string UserTagHeaderName("X-YT-User-Tag");
inline const std::string XYTErrorHeaderName("X-YT-Error");
inline const std::string XYTResponseCodeHeaderName("X-YT-Response-Code");
inline const std::string XYTResponseMessageHeaderName("X-YT-Response-Message");
inline const std::string XYTSpanIdHeaderName("X-YT-Span-Id");
inline const std::string XYTTraceIdHeaderName("X-YT-Trace-Id");

////////////////////////////////////////////////////////////////////////////////

} // namespace Headers

////////////////////////////////////////////////////////////////////////////////

struct TJsonFactory
    : public NFormats::IFormatFactory
{
    std::unique_ptr<NYson::IFlushableYsonConsumer> CreateConsumer(IZeroCopyOutput* output) override;

    NYson::TYsonProducer CreateProducer(IInputStream* input) override;
};

void FillYTErrorHeaders(
    const IResponseWriterPtr& rsp,
    const TError& error,
    NFormats::IFormatFactoryPtr errorFormatFactory = New<TJsonFactory>());
void FillYTErrorTrailers(
    const IResponseWriterPtr& rsp,
    const TError& error,
    NFormats::IFormatFactoryPtr errorFormatFactory = New<TJsonFactory>());

TError ParseYTError(
    const IResponsePtr& rsp,
    bool fromTrailers = false,
    NFormats::IFormatFactoryPtr errorFormatFactory = New<TJsonFactory>());

//! Catches exception thrown from underlying handler body and
//! translates it into HTTP error.
IHttpHandlerPtr WrapYTException(IHttpHandlerPtr underlying);

bool MaybeHandleCors(
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp,
    const TCorsConfigPtr& config = New<TCorsConfig>());

THashMap<std::string, std::string> ParseCookies(TStringBuf cookies);

void ProtectCsrfToken(const IResponseWriterPtr& rsp);

std::optional<std::string> FindHeader(const IRequestPtr& req, TStringBuf headerName);
std::optional<std::string> FindBalancerRequestId(const IRequestPtr& req);
std::optional<std::string> FindBalancerRealIP(const IRequestPtr& req);

std::optional<std::string> FindUserAgent(const IRequestPtr& req);
void SetUserAgent(const THeadersPtr& headers, const std::string& value);

void ReplyJson(const IResponseWriterPtr& rsp, std::function<void(NYson::IYsonConsumer*)> producer);

void ReplyError(const IResponseWriterPtr& response, const TError& error);

NTracing::TTraceId GetTraceId(const IRequestPtr& req);
void SetTraceId(const IResponseWriterPtr& rsp, NTracing::TTraceId traceId);

void SetRequestId(const IResponseWriterPtr& rsp, NRpc::TRequestId requestId);

NTracing::TSpanId GetSpanId(const IRequestPtr& req);

NTracing::TTraceContextPtr GetOrCreateTraceContext(const IRequestPtr& req);

std::optional<std::pair<i64, i64>> FindBytesRange(const THeadersPtr& headers);
void SetBytesRange(const THeadersPtr& headers, std::pair<i64, i64> range);

std::string SanitizeUrl(TStringBuf url);

std::vector<std::pair<std::string, std::string>> DumpUnknownHeaders(const THeadersPtr& headers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
