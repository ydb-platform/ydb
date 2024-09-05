#pragma once

#include "fwd.h"
#include "http.h"

#include <util/generic/maybe.h>
#include <util/str_stl.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool ParseBoolFromResponse(const TString& response);

TGUID ParseGuidFromResponse(const TString& response);

////////////////////////////////////////////////////////////////////////////////

TString GetProxyForHeavyRequest(const TClientContext& context);

void LogRequestError(
    const TString& requestId,
    const THttpHeader& header,
    const TString& message,
    const TString& attemptDescription);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
