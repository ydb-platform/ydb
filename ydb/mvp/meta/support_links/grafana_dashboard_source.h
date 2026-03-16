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
    if (!IsAbsoluteUrl(context.LinkConfig.GetUrl()) && context.GrafanaConfig.Endpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
}

} // namespace NMVP::NSupportLinks

namespace NMVP {

class TGrafanaDashboardSource : public ILinkSource {
public:
    TGrafanaDashboardSource(TSupportLinkEntry config, const TMetaSettings& metaSettings)
        : Config_(std::move(config))
        , MetaSettings_(metaSettings)
    {}

    size_t Place() const override {
        return 0;
    }

    const TSupportLinkEntry& Config() const override {
        return Config_;
    }

    TResolveOutput Resolve(const TResolveInput& input) const override
    {
        NSupportLinks::TLinkResolveContext resolveContext{
            .Place = input.Place,
            .SourceName = Config_.GetSource(),
            .LinkConfig = Config_,
            .ClusterColumns = input.ClusterColumns,
            .QueryParams = input.QueryParams,
            .Parent = input.Parent,
            .HttpProxyId = input.HttpProxyId,
        };

        TResolveOutput result{
            .Name = Config_.GetSource(),
            .Ready = true,
        };
        TString url = NSupportLinks::ResolveGrafanaDashboardUrl(MetaSettings_.GrafanaConfig, resolveContext, result.Errors);
        if (!url.empty()) {
            result.Links.emplace_back(NSupportLinks::TResolvedLink{
                .Title = Config_.GetTitle(),
                .Url = std::move(url),
            });
        }
        return result;
    }

private:
    TSupportLinkEntry Config_;
    const TMetaSettings& MetaSettings_;
};

inline std::shared_ptr<ILinkSource> BuildGrafanaDashboardSource(
    TSupportLinkEntry config,
    const TMetaSettings& metaSettings)
{
    NSupportLinks::ValidateGrafanaDashboardResolverConfig(NSupportLinks::TResolverValidationContext{
        .LinkConfig = config,
        .GrafanaConfig = metaSettings.GrafanaConfig,
    });
    return std::make_shared<TGrafanaDashboardSource>(std::move(config), metaSettings);
}

} // namespace NMVP
