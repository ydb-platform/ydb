#pragma once

#include "http_req.h"

namespace NKikimr::NHttpProxy {

TVector<std::pair<TString, TString>> BuildLabels(const TString& method, const THttpRequestContext& httpContext, const TString& name, bool setStreamPrefix = false);

static const bool setStreamPrefix{true};


} // namespace NKikimr::NHttpProxy
