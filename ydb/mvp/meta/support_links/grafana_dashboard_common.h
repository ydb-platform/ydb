#pragma once

#include "param_bindings.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <util/generic/strbuf.h>

namespace NMVP::NSupportLinks {

TResolvedParamBindings BuildDefaultGrafanaDashboardParamBindings();
TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters,
    const TResolvedParamBindings& paramBindings);
TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const ILinkSource::TLinkResolveInput& input,
    const TResolvedParamBindings& paramBindings);

} // namespace NMVP::NSupportLinks
