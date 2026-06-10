#pragma once

#include "param_bindings.h"
#include "source_common.h"

#include <ydb/mvp/meta/support_links/source.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/string/escape.h>

#include <utility>

namespace NMVP::NSupportLinks {

inline constexpr TStringBuf GRAFANA_LOGGING_DEFAULT_URL = "/explore";

inline TVector<TAdditionalParamBinding> BuildDefaultGrafanaLoggingAdditionalParamBindings() {
    return {
        TAdditionalParamBinding{
            .Label = "__workspace__",
            .ValueSource = TAdditionalParamBinding::EValueSource::ClusterInfo,
            .SourceValue = "k8s_namespace",
        },
        TAdditionalParamBinding{
            .Label = "__bucket__",
            .ValueSource = TAdditionalParamBinding::EValueSource::Value,
            .SourceValue = "ydb",
        },
    };
}

inline TVector<std::pair<TString, TString>> BuildDefaultGrafanaLoggingRequestParamValues(
    const TCgiParameters& requestQueryParameters)
{
    TVector<std::pair<TString, TString>> paramValues;
    for (const auto& [name, value] : requestQueryParameters) {
        if (name != "cluster" && !value.empty()) {
            paramValues.emplace_back(name, value);
        }
    }
    return paramValues;
}

inline TString BuildGrafanaLoggingExpr(const TVector<std::pair<TString, TString>>& bindings) {
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

inline NJson::TJsonValue BuildGrafanaLoggingPanesJson(const TString& datasource, const TVector<std::pair<TString, TString>>& bindings) {
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

inline TResolvedParamBindings ResolveGrafanaLoggingParamBindings(
    const TSupportLinkEntryConfig& config,
    EEntityType entityType)
{
    if (config.LinkParameterMappingsSize() != 0) {
        return ResolveConfiguredParamMappings(config, entityType);
    }

    return TResolvedParamBindings{
        .RequestParams = {},
        .AdditionalParams = BuildDefaultGrafanaLoggingAdditionalParamBindings(),
    };
}

inline bool TryBuildGrafanaLoggingUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const ILinkSource::TLinkResolveInput& input,
    const TResolvedParamBindings& paramBindings,
    bool hasCustomParamMappings,
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
        datasourceIt != input.ClusterInfo.end() && !datasourceIt->second.empty())
    {
        datasource = datasourceIt->second;
    }

    const TCgiParameters forwardedParameters = BuildForwardedParameters(input.Identity, input.AdditionalRequestParams);
    TVector<std::pair<TString, TString>> bindings = hasCustomParamMappings
        ? BuildRequestParamValues(forwardedParameters, paramBindings.RequestParams)
        : BuildDefaultGrafanaLoggingRequestParamValues(forwardedParameters);
    for (auto& binding : BuildAdditionalParamValues(input.ClusterInfo, paramBindings.AdditionalParams)) {
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

} // namespace NMVP::NSupportLinks

namespace NMVP::NSupportLinks {

class TGrafanaLoggingSource : public ILinkSource {
public:
    TGrafanaLoggingSource(
        TString sourceName,
        TString title,
        TString url,
        TString grafanaEndpoint,
        NSupportLinks::TResolvedParamBindings paramBindings,
        bool hasCustomParamMappings)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , ParamBindings(std::move(paramBindings))
        , HasCustomParamMappings(hasCustomParamMappings)
    {}

    TResolveOutput Resolve(const TLinkResolveInput& input, const TResolveContext&) const override {
        TResolveOutput result{
            .Name = SourceName,
        };

        TString resolvedUrl;
        TString errorMessage;
        if (!NSupportLinks::TryBuildGrafanaLoggingUrl(
                GrafanaEndpoint,
                Url,
                input,
                ParamBindings,
                HasCustomParamMappings,
                resolvedUrl,
                errorMessage))
        {
            result.Errors.emplace_back(NSupportLinks::TSupportError{
                .Source = SourceName,
                .Message = std::move(errorMessage),
            });
            return result;
        }

        result.Links.emplace_back(NSupportLinks::TResolvedLink{
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
    NSupportLinks::TResolvedParamBindings ParamBindings;
    bool HasCustomParamMappings = false;
};

inline void ValidateGrafanaLoggingSourceConfig(
    const TSupportLinkEntryConfig& config,
    EEntityType entityType,
    const TMetaSettings& metaSettings)
{
    const TStringBuf url = config.GetUrl().empty()
        ? NSupportLinks::GRAFANA_LOGGING_DEFAULT_URL
        : TStringBuf(config.GetUrl());
    if (url.Contains('?')) {
        ythrow yexception() << "query parameters are not supported in url for source=" << config.GetSource();
    }
    if (!NSupportLinks::IsAbsoluteUrl(url) && metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
    NSupportLinks::ValidateResolvedParamBindings(
        NSupportLinks::ResolveGrafanaLoggingParamBindings(config, entityType),
        config);
}

inline std::shared_ptr<ILinkSource> MakeGrafanaLoggingSource(
    TSupportLinkEntryConfig config,
    EEntityType entityType,
    const TMetaSettings& metaSettings)
{
    ValidateGrafanaLoggingSourceConfig(config, entityType, metaSettings);
    const bool hasCustomParamMappings = config.LinkParameterMappingsSize() != 0;
    auto paramBindings = NSupportLinks::ResolveGrafanaLoggingParamBindings(config, entityType);
    return std::make_shared<TGrafanaLoggingSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        metaSettings.SupportLinks.GrafanaEndpoint,
        std::move(paramBindings),
        hasCustomParamMappings
    );
}

} // namespace NMVP::NSupportLinks
