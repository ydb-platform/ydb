#pragma once

#include "source.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <utility>

namespace NMVP::NSupportLinks {

struct TResolvedParamBindings {
    TVector<std::pair<TString, TString>> RequestMappings;
    TVector<std::pair<TString, TString>> ClusterInfoMappings;
    TVector<std::pair<TString, TString>> StaticMappings;
};

TResolvedParamBindings ResolveParamBindings(const TSupportLinkEntryConfig& config, const TResolvedParamBindings& defaultParamBindings);
void ValidateParamsAreUnique(const TResolvedParamBindings& paramBindings, const TSupportLinkEntryConfig& config);
TVector<std::pair<TString, TString>> BuildRequestParamValues(
    const TCgiParameters& requestParameters,
    const TVector<std::pair<TString, TString>>& requestMappings);
TVector<std::pair<TString, TString>> BuildNonIdentityRequestParamValues(const TCgiParameters& requestParameters);
TVector<std::pair<TString, TString>> BuildClusterInfoParamValues(
    const THashMap<TString, TString>& clusterInfo,
    const TVector<std::pair<TString, TString>>& clusterInfoMappings);
TVector<std::pair<TString, TString>> BuildStaticParamValues(const TVector<std::pair<TString, TString>>& staticMappings);

} // namespace NMVP::NSupportLinks
