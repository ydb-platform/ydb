#pragma once

#include "source_common.h"

#include <ydb/mvp/meta/support_links/source.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/string/escape.h>

#include <utility>

namespace NMVP::NSupportLinks {

inline constexpr TStringBuf GRAFANA_LOGGING_DEFAULT_URL = "/explore";

inline TVector<std::pair<TString, TString>> BuildGrafanaLoggingBindings(const TEntityIdentity& entityIdentity) {
    TVector<std::pair<TString, TString>> bindings;

    if (!entityIdentity.Cluster.empty()) {
        bindings.emplace_back("cluster", entityIdentity.Cluster);
    }
    if (entityIdentity.Database && !entityIdentity.Database->empty()) {
        bindings.emplace_back("database", *entityIdentity.Database);
    }
    if (entityIdentity.Node && !entityIdentity.Node->empty()) {
        bindings.emplace_back("node", *entityIdentity.Node);
    }
    if (entityIdentity.Host && !entityIdentity.Host->empty()) {
        bindings.emplace_back("host", *entityIdentity.Host);
    }

    return bindings;
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

inline bool TryBuildGrafanaLoggingUrl(
    TStringBuf grafanaEndpoint,
    TStringBuf url,
    const ILinkSource::TLinkResolveInput& input,
    TString& resolvedUrl,
    TString& errorMessage)
{
    const TStringBuf path = url.empty() ? GRAFANA_LOGGING_DEFAULT_URL : url;
    resolvedUrl = IsAbsoluteUrl(path) ? TString(path) : JoinUrl(grafanaEndpoint, path);

    if (resolvedUrl.Contains('?')) {
        errorMessage = "query parameters are not supported in url for source=grafana/logging";
        return false;
    }

    const auto datasourceIt = input.ClusterInfo.find("datasource_logging");
    if (datasourceIt == input.ClusterInfo.end() || datasourceIt->second.empty()) {
        errorMessage = "datasource_logging is required in cluster info for source=grafana/logging";
        return false;
    }

    TVector<std::pair<TString, TString>> bindings = BuildGrafanaLoggingBindings(input.Identity);

    TCgiParameters queryParameters;
    queryParameters.InsertUnescaped("schemaVersion", "1");
    queryParameters.InsertUnescaped("panes", NJson::WriteJson(
        BuildGrafanaLoggingPanesJson(datasourceIt->second, bindings),
        false));
    queryParameters.InsertUnescaped("orgId", "1");

    resolvedUrl += TStringBuilder() << '?' << queryParameters.Print();
    return true;
}

} // namespace NMVP::NSupportLinks

namespace NMVP::NSupportLinks {

class TGrafanaLoggingSource : public ILinkSource {
public:
    TGrafanaLoggingSource(TString sourceName, TString title, TString url, TString grafanaEndpoint)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
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
};

inline void ValidateGrafanaLoggingSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    const TStringBuf url = config.GetUrl().empty()
        ? GRAFANA_LOGGING_DEFAULT_URL
        : TStringBuf(config.GetUrl());
    if (url.Contains('?')) {
        ythrow yexception() << "query parameters are not supported in url for source=" << config.GetSource();
    }
    if (!IsAbsoluteUrl(url) && metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
}

inline std::shared_ptr<ILinkSource> MakeGrafanaLoggingSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateGrafanaLoggingSourceConfig(config, metaSettings);
    return std::make_shared<TGrafanaLoggingSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        metaSettings.SupportLinks.GrafanaEndpoint
    );
}

} // namespace NMVP::NSupportLinks
