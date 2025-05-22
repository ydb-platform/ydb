#pragma once

#include "fwd.h"

#include "http.h"

#include <util/generic/fwd.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString CreateHostNameWithPort(const TString& name, const TClientContext& context);

TString GetFullUrl(const TString& hostName, const TClientContext& context, THttpHeader& header);

void UpdateHeaderForProxyIfNeed(const TString& hostName, const TClientContext& context, THttpHeader& header);

TString GetFullUrlForProxy(const TString& hostName, const TClientContext& context, THttpHeader& header);

TString TruncateForLogs(const TString& text, size_t maxSize);

TString GetLoggedAttributes(const THttpHeader& header, const TString& url, bool includeParameters, size_t sizeLimit);

void LogRequest(const THttpHeader& header, const TString& url, bool includeParameters, const TString& requestId, const TString& hostName);

// Sometimes errors may not include any specific inner errors and appear to be generic.
// To differentiate these errors and reduce reliance on HTTP status codes, we need to extend inner error information.
//
// XXX(hiddenpath): Remove this method when server will respond with specific errors.
void ExtendGenericError(TErrorResponse& errorResponse, int code, TString message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
