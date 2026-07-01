#include "grafana_dashboard_common.h"

#include "source_common.h"

namespace NMVP::NSupportLinks {
namespace {

constexpr TStringBuf GRAFANA_WORKSPACE_KEY = "k8s_namespace";
constexpr TStringBuf GRAFANA_DATASOURCE_KEY = "datasource";

std::pair<TString, TCgiParameters> BuildGrafanaDashboardUrlParts(TStringBuf grafanaEndpoint, TStringBuf url) {
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

void InsertOrReplaceDashboardVar(TCgiParameters& queryParameters, TStringBuf label, TStringBuf value) {
    const TString varName = TStringBuilder() << "var-" << label;
    queryParameters.EraseAll(varName);
    queryParameters.InsertUnescaped(varName, value);
}

void ApplyGrafanaDashboardBindingPolicy(
    TCgiParameters& queryParameters,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters,
    const TResolvedParamBindings& paramBindings)
{
    for (const auto& [name, value] : BuildNonIdentityRequestParamValues(requestQueryParameters)) {
        InsertOrReplaceDashboardVar(queryParameters, name, value);
    }

    for (const auto& [label, value] : BuildClusterInfoParamValues(clusterInfo, paramBindings.ClusterInfoMappings)) {
        InsertOrReplaceDashboardVar(queryParameters, label, value);
    }

    for (const auto& [label, value] : BuildStaticParamValues(paramBindings.StaticMappings)) {
        InsertOrReplaceDashboardVar(queryParameters, label, value);
    }

    for (const auto& [label, value] : BuildRequestParamValues(requestQueryParameters, paramBindings.RequestMappings)) {
        InsertOrReplaceDashboardVar(queryParameters, label, value);
    }
}

} // namespace

TResolvedParamBindings BuildDefaultGrafanaDashboardParamBindings() {
    return TResolvedParamBindings{
        .RequestMappings = {
            {"cluster", "cluster"},
            {"database", "database"},
            {"node", "node"},
            {"host", "host"},
        },
        .ClusterInfoMappings = {
            {TString(GRAFANA_DATASOURCE_KEY), "ds"},
            {TString(GRAFANA_WORKSPACE_KEY), "workspace"},
        },
        .StaticMappings = {},
    };
}

TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const THashMap<TString, TString>& clusterInfo,
    const TCgiParameters& requestQueryParameters,
    const TResolvedParamBindings& paramBindings)
{
    auto [path, queryParameters] = BuildGrafanaDashboardUrlParts(grafanaEndpoint, url);
    ApplyGrafanaDashboardBindingPolicy(queryParameters, clusterInfo, requestQueryParameters, paramBindings);

    return queryParameters.empty()
        ? path
        : TStringBuilder() << path << '?' << queryParameters.Print();
}

TString BuildGrafanaDashboardUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const ILinkSource::TLinkResolveInput& input,
    const TResolvedParamBindings& paramBindings)
{
    const TCgiParameters forwardedParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    return BuildGrafanaDashboardUrl(grafanaEndpoint, url, input.ClusterInfo, forwardedParameters, paramBindings);
}

} // namespace NMVP::NSupportLinks
