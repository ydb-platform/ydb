#pragma once

#include "source_common.h"
#include "grafana_dashboard_common.h"

#include <ydb/mvp/meta/support_links/source.h>

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

inline void ValidateGrafanaDashboardResolverConfig(const TResolverValidationContext& context)
{
    if (context.LinkConfig.GetUrl().empty()) {
        ythrow yexception() << "url is required for source=" << context.LinkConfig.GetSource();
    }
    if (!IsAbsoluteUrl(context.LinkConfig.GetUrl()) && context.GrafanaEndpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
}

} // namespace NMVP::NSupportLinks

namespace NMVP {

class TGrafanaDashboardSource : public ILinkSource {
public:
    TGrafanaDashboardSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings)
        : Config_(std::move(config))
        , MetaSettings_(metaSettings)
    {}

    const TSupportLinkEntryConfig& Config() const override {
        return Config_;
    }

    TResolveOutput Resolve(const TResolveInput& input) const override
    {
        NSupportLinks::TLinkResolveContext resolveContext{
            .Place = input.Place,
            .SourceName = Config_.GetSource(),
            .LinkConfig = Config_,
            .ClusterColumns = input.ClusterColumns,
            .UrlParameters = input.UrlParameters,
            .Parent = input.Parent,
            .HttpProxyId = input.HttpProxyId,
        };

        TResolveOutput result{
            .Name = Config_.GetSource(),
            .Ready = true,
        };
        TString url = NSupportLinks::ResolveGrafanaDashboardUrl(MetaSettings_.GrafanaEndpoint, resolveContext, result.Errors);
        if (!url.empty()) {
            result.Links.emplace_back(NSupportLinks::TResolvedLink{
                .Title = Config_.GetTitle(),
                .Url = std::move(url),
            });
        }
        return result;
    }

private:
    TSupportLinkEntryConfig Config_;
    const TMetaSettings& MetaSettings_;
};

inline std::shared_ptr<ILinkSource> BuildGrafanaDashboardSource(
    TSupportLinkEntryConfig config,
    const TMetaSettings& metaSettings)
{
    NSupportLinks::ValidateGrafanaDashboardResolverConfig(NSupportLinks::TResolverValidationContext{
        .LinkConfig = config,
        .GrafanaEndpoint = metaSettings.GrafanaEndpoint,
    });
    return std::make_shared<TGrafanaDashboardSource>(std::move(config), metaSettings);
}

} // namespace NMVP
