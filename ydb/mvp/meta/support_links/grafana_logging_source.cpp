#include "grafana_logging_source.h"

#include "param_bindings.h"
#include "source_common.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/string/escape.h>

namespace NMVP::NSupportLinks {
namespace {

constexpr TStringBuf GRAFANA_LOGGING_DEFAULT_URL = "/explore";

TResolvedParamBindings BuildDefaultGrafanaLoggingParamBindings() {
    return TResolvedParamBindings{
        .RequestMappings = {
            {"database", "database"},
            {"node", "node_id"},
            {"host", "k8s_node_name"},
        },
        .ClusterInfoMappings = {
            {"k8s_namespace", "__workspace__"},
        },
        .StaticMappings = {
            {"ydb", "__bucket__"},
        },
    };
}

TString BuildGrafanaLoggingExpr(const TVector<std::pair<TString, TString>>& bindings) {
    TString expr = "{";
    bool first = true;
    for (const auto& [name, value] : bindings) {
        if (!first) {
            expr += ", ";
        }
        first = false;
        expr += TStringBuilder() << name << "=\"" << EscapeC(value) << '"';
    }
    expr += "}";
    return expr;
}

NJson::TJsonValue BuildGrafanaLoggingPanesJson(
    const TString& datasource,
    const TVector<std::pair<TString, TString>>& bindings)
{
    NJson::TJsonValue panesJson(NJson::JSON_MAP);
    NJson::TJsonValue paneJson(NJson::JSON_MAP);
    NJson::TJsonValue queryJson(NJson::JSON_MAP);

    queryJson["refId"] = "A";
    queryJson["expr"] = BuildGrafanaLoggingExpr(bindings);
    queryJson["queryType"] = "range";
    queryJson["direction"] = "backward";
    queryJson["datasource"]["type"] = "loki";
    queryJson["datasource"]["uid"] = datasource;

    paneJson["datasource"] = datasource;
    paneJson["queries"].AppendValue(std::move(queryJson));
    paneJson["range"]["from"] = "now-1h";
    paneJson["range"]["to"] = "now";

    panesJson["x"] = std::move(paneJson);
    return panesJson;
}

bool TryBuildGrafanaLoggingUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const ILinkSource::TLinkResolveInput& input,
    const TResolvedParamBindings& paramBindings,
    TString& resolvedUrl,
    TString& errorMessage)
{
    const TStringBuf path = url.empty() ? GRAFANA_LOGGING_DEFAULT_URL : url;
    resolvedUrl = IsAbsoluteUrl(path) ? TString(path) : JoinUrl(grafanaEndpoint, path);

    if (resolvedUrl.Contains('?')) {
        errorMessage = "query parameters are not supported in url for source=grafana/logging";
        return false;
    }

    TString datasource;
    if (const auto datasourceIt = input.ClusterInfo.find("datasource_logging");
        datasourceIt != input.ClusterInfo.end() && !datasourceIt->second.empty()) {
        datasource = datasourceIt->second;
    }

    const TCgiParameters forwardedParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    TVector<std::pair<TString, TString>> bindings = BuildNonIdentityRequestParamValues(forwardedParameters);
    for (auto& binding : BuildRequestParamValues(forwardedParameters, paramBindings.RequestMappings)) {
        bindings.push_back(std::move(binding));
    }
    for (auto& binding : BuildClusterInfoParamValues(input.ClusterInfo, paramBindings.ClusterInfoMappings)) {
        bindings.push_back(std::move(binding));
    }
    for (auto& binding : BuildStaticParamValues(paramBindings.StaticMappings)) {
        bindings.push_back(std::move(binding));
    }

    TVector<std::pair<TString, TString>> resolvedBindings;
    resolvedBindings.reserve(bindings.size());
    for (auto& binding : bindings) {
        if (binding.first == "datasource") {
            datasource = std::move(binding.second);
            continue;
        }
        resolvedBindings.push_back(std::move(binding));
    }

    if (datasource.empty()) {
        errorMessage = "datasource_logging is required in cluster info for source=grafana/logging";
        return false;
    }

    TCgiParameters queryParameters;
    queryParameters.InsertUnescaped("schemaVersion", "1");
    queryParameters.InsertUnescaped("panes", NJson::WriteJson(
        BuildGrafanaLoggingPanesJson(datasource, resolvedBindings),
        false));
    queryParameters.InsertUnescaped("orgId", "1");

    resolvedUrl += TStringBuilder() << '?' << queryParameters.Print();
    return true;
}

class TGrafanaLoggingSource : public ILinkSource {
public:
    TGrafanaLoggingSource(TString sourceName, TString title, TString url, TString grafanaEndpoint, TResolvedParamBindings paramBindings)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , ParamBindings(std::move(paramBindings))
    {}

    TResolveOutput Resolve(const ILinkSource::TLinkResolveInput& input, const ILinkSource::TResolveContext&) const override {
        TResolveOutput result{
            .Name = SourceName,
        };

        TString resolvedUrl;
        TString errorMessage;
        if (!TryBuildGrafanaLoggingUrl(
                GrafanaEndpoint,
                Url,
                input,
                ParamBindings,
                resolvedUrl,
                errorMessage))
        {
            result.Errors.emplace_back(TSupportError{
                .Source = SourceName,
                .Message = std::move(errorMessage),
            });
            return result;
        }

        result.Links.emplace_back(TResolvedLink{
            .Title = Title,
            .Url = std::move(resolvedUrl),
        });
        return result;
    }

private:
    TString SourceName;
    TString Title;
    TString Url;
    TString GrafanaEndpoint;
    TResolvedParamBindings ParamBindings;
};

} // namespace

void ValidateGrafanaLoggingSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    const TStringBuf url = config.GetUrl().empty()
        ? GRAFANA_LOGGING_DEFAULT_URL
        : TStringBuf(config.GetUrl());
    if (url.Contains('?')) {
        ythrow yexception() << "query parameters are not supported in url for source=" << config.GetSource();
    }
    if (!IsAbsoluteUrl(url) && metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
    ValidateParamsAreUnique(ResolveParamBindings(config, BuildDefaultGrafanaLoggingParamBindings()), config);
}

std::shared_ptr<ILinkSource> MakeGrafanaLoggingSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateGrafanaLoggingSourceConfig(config, metaSettings);
    auto paramBindings = ResolveParamBindings(config, BuildDefaultGrafanaLoggingParamBindings());
    return std::make_shared<TGrafanaLoggingSource>(
        config.GetSource(), config.GetTitle(), config.GetUrl(), metaSettings.SupportLinks.GrafanaEndpoint, std::move(paramBindings));
}

} // namespace NMVP::NSupportLinks
