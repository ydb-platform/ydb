#pragma once

#include "source_common.h"
#include "grafana_dashboard_common.h"

#include <ydb/mvp/meta/support_links/source.h>

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

class TGrafanaDashboardSource : public ILinkSource {
public:
    TGrafanaDashboardSource(
        TString sourceName,
        TString title,
        TString url,
        TString grafanaEndpoint,
        NSupportLinks::TResolvedParamBindings paramBindings)
        : SourceName(std::move(sourceName))
        , Title(std::move(title))
        , Url(std::move(url))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , ParamBindings(std::move(paramBindings))
    {}

    TResolveOutput Resolve(const TLinkResolveInput& input, const TResolveContext&) const override {
        TString resolvedUrl = NSupportLinks::BuildGrafanaDashboardUrl(
            GrafanaEndpoint,
            Url,
            input,
            ParamBindings);
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
    NSupportLinks::TResolvedParamBindings ParamBindings;
};

inline void ValidateGrafanaDashboardSourceConfig(
    const TSupportLinkEntryConfig& config,
    EEntityType entityType,
    const TMetaSettings& metaSettings)
{
    if (config.GetUrl().empty()) {
        ythrow yexception() << "url is required for source=" << config.GetSource();
    }
    if (!NSupportLinks::IsAbsoluteUrl(config.GetUrl()) && metaSettings.SupportLinks.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
    NSupportLinks::ValidateResolvedParamBindings(
        NSupportLinks::ResolveGrafanaDashboardParamBindings(config, entityType),
        config);
}

inline std::shared_ptr<ILinkSource> MakeGrafanaDashboardSource(
    TSupportLinkEntryConfig config,
    EEntityType entityType,
    const TMetaSettings& metaSettings)
{
    ValidateGrafanaDashboardSourceConfig(config, entityType, metaSettings);
    auto paramBindings = NSupportLinks::ResolveGrafanaDashboardParamBindings(config, entityType);
    return std::make_shared<TGrafanaDashboardSource>(
        config.GetSource(),
        config.GetTitle(),
        config.GetUrl(),
        metaSettings.SupportLinks.GrafanaEndpoint,
        std::move(paramBindings)
    );
}

} // namespace NMVP::NSupportLinks
