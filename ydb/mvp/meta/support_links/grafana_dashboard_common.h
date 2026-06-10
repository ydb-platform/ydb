#pragma once

#include "param_bindings.h"
#include "source_common.h"
#include "types.h"

#include <library/cpp/cgiparam/cgiparam.h>

namespace NMVP::NSupportLinks {

inline TVector<TAdditionalParamBinding> BuildDefaultGrafanaDashboardAdditionalParamBindings() {
    return {
        TAdditionalParamBinding{
            .Label = "ds",
            .ValueSource = TAdditionalParamBinding::EValueSource::ClusterInfo,
            .SourceValue = "datasource",
        },
        TAdditionalParamBinding{
            .Label = "workspace",
            .ValueSource = TAdditionalParamBinding::EValueSource::ClusterInfo,
            .SourceValue = "k8s_namespace",
        },
    };
}

inline std::pair<TString, TCgiParameters> BuildGrafanaDashboardUrlParts(
    TStringBuf grafanaEndpoint,
    TStringBuf url)
{
    TString resolvedUrl = IsAbsoluteUrl(url)
        ? TString(url)
        : JoinUrl(grafanaEndpoint, url);
    TString path = TString(TStringBuf(resolvedUrl).Before('?'));
    const TStringBuf queryString = TStringBuf(resolvedUrl).After('?');

    TCgiParameters queryParameters;
    if (!queryString.empty()) {
        queryParameters.Scan(queryString);
    }

    return {std::move(path), std::move(queryParameters)};
}

inline void InsertOrReplaceDashboardVar(TCgiParameters& queryParameters, TStringBuf label, TStringBuf value) {
    const TString varName = TStringBuilder() << "var-" << label;
    while (queryParameters.Erase(varName, 0)) {
    }
    queryParameters.InsertUnescaped(varName, value);
}

inline void ApplyGrafanaDashboardForwardedRequestParameters(
    TCgiParameters& queryParameters,
    const TCgiParameters& requestQueryParameters)
{
    for (const auto& [name, value] : requestQueryParameters) {
        InsertOrReplaceDashboardVar(queryParameters, name, value);
    }
}

inline void ApplyGrafanaDashboardBindingPolicy(
    TCgiParameters& queryParameters,
    const ILinkSource::TLinkResolveInput& input,
    const TResolvedParamBindings& paramBindings)
{
    const TCgiParameters forwardedParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    ApplyGrafanaDashboardForwardedRequestParameters(queryParameters, forwardedParameters);

    for (const auto& [label, value] : BuildAdditionalParamValues(input.ClusterInfo, paramBindings.AdditionalParams)) {
        InsertOrReplaceDashboardVar(queryParameters, label, value);
    }

    for (const auto& [label, value] : BuildRequestParamValues(forwardedParameters, paramBindings.RequestParams)) {
        InsertOrReplaceDashboardVar(queryParameters, label, value);
    }
}

inline TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters,
    const TResolvedParamBindings& paramBindings)
{
    auto [path, queryParameters] = BuildGrafanaDashboardUrlParts(grafanaEndpoint, url);

    ApplyGrafanaDashboardForwardedRequestParameters(queryParameters, requestQueryParameters);

    for (const auto& [label, value] : BuildAdditionalParamValues(clusterInfo, paramBindings.AdditionalParams)) {
        InsertOrReplaceDashboardVar(queryParameters, label, value);
    }

    for (const auto& [label, value] : BuildRequestParamValues(requestQueryParameters, paramBindings.RequestParams)) {
        InsertOrReplaceDashboardVar(queryParameters, label, value);
    }

    return queryParameters.empty()
        ? path
        : TStringBuilder() << path << '?' << queryParameters.Print();
}

inline TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const ILinkSource::TLinkResolveInput& input,
    const TResolvedParamBindings& paramBindings)
{
    const TCgiParameters forwardedParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    return BuildGrafanaDashboardUrl(grafanaEndpoint, url, input.ClusterInfo, forwardedParameters, paramBindings);
}

inline TResolvedParamBindings ResolveGrafanaDashboardParamBindings(
    const TSupportLinkEntryConfig& config,
    EEntityType entityType)
{
    if (config.LinkParameterMappingsSize() != 0) {
        return ResolveConfiguredParamMappings(config, entityType);
    }

    return TResolvedParamBindings{
        .RequestParams = {},
        .AdditionalParams = BuildDefaultGrafanaDashboardAdditionalParamBindings(),
    };
}

} // namespace NMVP::NSupportLinks
