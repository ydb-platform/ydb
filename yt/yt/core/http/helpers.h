#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/tracing/public.h>

namespace NYT::NHttp {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
