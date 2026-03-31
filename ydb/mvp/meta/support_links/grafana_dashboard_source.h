#pragma once

#include "source_common.h"
#include "grafana_dashboard_common.h"

#include <ydb/mvp/meta/support_links/source.h>

#include <util/generic/yexception.h>

namespace NMVP {

class TGrafanaDashboardSource : public ILinkSource {
public:
    TGrafanaDashboardSource(TString sourceName, TString title, TString url, TString grafanaEndpoint)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
    {}

    TResolveOutput Resolve(const TLinkResolveInput& input, const TResolveContext&) const override {
        TString resolvedUrl = NSupportLinks::BuildGrafanaDashboardUrl(GrafanaEndpoint, Url, input.ClusterInfo, input.UrlParameters);
        TResolveOutput result{
            .Name = SourceName,
        };
        if (!resolvedUrl.empty()) {
            result.Links.emplace_back(NSupportLinks::TResolvedLink{
                .Title = Title,
                .Url = std::move(resolvedUrl),
            });
        }
        return result;
    }

private:
    TString SourceName;
    TString Title;
    TString Url;
    TString GrafanaEndpoint;
};

inline void ValidateGrafanaDashboardSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    if (config.GetUrl().empty()) {
        ythrow yexception() << "url is required for source=" << config.GetSource();
    }
    if (!NSupportLinks::IsAbsoluteUrl(config.GetUrl()) && metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
}

inline std::shared_ptr<ILinkSource> MakeGrafanaDashboardSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateGrafanaDashboardSourceConfig(config, metaSettings);
    return std::make_shared<TGrafanaDashboardSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        metaSettings.SupportLinks.GrafanaEndpoint
    );
}

} // namespace NMVP
