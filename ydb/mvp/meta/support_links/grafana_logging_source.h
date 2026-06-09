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

struct TGrafanaLoggingLabels {
    TString Database;
    TString Node;
    TString Host;
    TString Cluster;
};

inline TVector<std::pair<TString, TString>> BuildGrafanaLoggingBindings(
    ESupportLinksEntityType entityType,
    const TGrafanaLoggingLabels& labels,
    const NHttp::TUrlParameters& requestQueryParameters)
{
    TVector<std::pair<TString, TString>> bindings;

    switch (entityType) {
        case ESupportLinksEntityType::Cluster:
            if (!labels.Cluster.empty()) {
                const TString cluster = requestQueryParameters["cluster"];
                if (!cluster.empty()) {
                    bindings.emplace_back(labels.Cluster, cluster);
                }
            }
            break;
        case ESupportLinksEntityType::Database: {
            const TString database = requestQueryParameters["database"];
            if (!database.empty()) {
                bindings.emplace_back(labels.Database, database);
            }
            break;
        }
        case ESupportLinksEntityType::Node: {
            const TString node = requestQueryParameters["node"];
            if (!node.empty()) {
                bindings.emplace_back(labels.Node, node);
            }
            break;
        }
        case ESupportLinksEntityType::Host: {
            const TString host = requestQueryParameters["host"];
            if (!host.empty()) {
                bindings.emplace_back(labels.Host, host);
            }
            break;
        }
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
    const TGrafanaLoggingLabels& labels,
    ESupportLinksEntityType entityType,
    const THashMap<TString, TString>& clusterInfo,
    const NHttp::TUrlParameters& requestQueryParameters,
    TString& resolvedUrl,
    TString& errorMessage)
{
    const TStringBuf path = url.empty() ? GRAFANA_LOGGING_DEFAULT_URL : url;
    resolvedUrl = IsAbsoluteUrl(path) ? TString(path) : JoinUrl(grafanaEndpoint, path);

    if (resolvedUrl.Contains('?')) {
        errorMessage = "query parameters are not supported in url for source=grafana/logging";
        return false;
    }

    const auto datasourceIt = clusterInfo.find("datasource_logging");
    if (datasourceIt == clusterInfo.end() || datasourceIt->second.empty()) {
        errorMessage = "datasource_logging is required in cluster info for source=grafana/logging";
        return false;
    }

    TVector<std::pair<TString, TString>> bindings = BuildGrafanaLoggingBindings(entityType, labels, requestQueryParameters);

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

namespace NMVP {

class TGrafanaLoggingSource : public ILinkSource {
public:
    TGrafanaLoggingSource(
        TString sourceName,
        TString title,
        TString url,
        NSupportLinks::TGrafanaLoggingLabels loggingLabels,
        TString grafanaEndpoint)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , LoggingLabels(std::move(loggingLabels))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
    {}

    TResolveOutput Resolve(const TLinkResolveInput& input, const TResolveContext& context) const override {
        TResolveOutput result{
            .Name = SourceName,
        };

        TString resolvedUrl;
        TString errorMessage;
        if (!NSupportLinks::TryBuildGrafanaLoggingUrl(
                GrafanaEndpoint,
                Url,
                LoggingLabels,
                context.EntityType,
                input.ClusterInfo,
                input.UrlParameters,
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
    NSupportLinks::TGrafanaLoggingLabels LoggingLabels;
    TString GrafanaEndpoint;
};

inline void ValidateGrafanaLoggingSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    const TStringBuf url = config.GetUrl().empty()
        ? NSupportLinks::GRAFANA_LOGGING_DEFAULT_URL
        : TStringBuf(config.GetUrl());
    if (url.Contains('?')) {
        ythrow yexception() << "query parameters are not supported in url for source=" << config.GetSource();
    }
    if (!NSupportLinks::IsAbsoluteUrl(url) && metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
}

inline std::shared_ptr<ILinkSource> MakeGrafanaLoggingSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateGrafanaLoggingSourceConfig(config, metaSettings);
    return std::make_shared<TGrafanaLoggingSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        NSupportLinks::TGrafanaLoggingLabels{
            .Database = metaSettings.SupportLinks.GrafanaLogging.DatabaseLabel,
            .Node = metaSettings.SupportLinks.GrafanaLogging.NodeLabel,
            .Host = metaSettings.SupportLinks.GrafanaLogging.HostLabel,
            .Cluster = metaSettings.SupportLinks.GrafanaLogging.ClusterLabel,
        },
        metaSettings.SupportLinks.GrafanaEndpoint
    );
}

} // namespace NMVP
