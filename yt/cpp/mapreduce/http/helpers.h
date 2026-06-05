#pragma once

#include "fwd.h"

#include "http.h"

#include <util/generic/fwd.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declaration as we don't want to include yt/yt/core/tracing/public.h to avoid possible namespaces conflicts
namespace NTracing {

using TTraceId = TGuid;
using TSpanId = ui64;

} // namespace NTracing

////////////////////////////////////////////////////////////////////////////////

TString CreateHostNameWithPort(const TString& name, const TClientContext& context);

TString GetFullUrl(const TString& hostName, const TClientContext& context, THttpHeader& header);

void UpdateHeaderForProxyIfNeed(const TString& hostName, const TClientContext& context, THttpHeader& header);

TString GetFullUrlForProxy(const TString& hostName, const TClientContext& context, THttpHeader& header);

TString TruncateForLogs(const TString& text, size_t maxSize);

TString GetLoggedAttributes(const THttpHeader& header, const TString& url, bool includeParameters, size_t sizeLimit);

void LogRequest(const THttpHeader& header, const TString& url, bool includeParameters, const TString& requestId, const TString& hostName);

TString FormatTraceParentHeader(const NTracing::TTraceId& traceId, const NTracing::TSpanId& spanId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
