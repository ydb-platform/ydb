#pragma once

#include <ydb/mvp/meta/support_links/grafana_dashboard_resolver.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_search_resolver.h>
#include <ydb/mvp/meta/support_links/resolution_context.h>
#include <ydb/mvp/meta/support_links/resolver_base.h>

#include <ydb/mvp/meta/meta_settings.h>
#include <ydb/mvp/meta/protos/config.pb.h>

#include <ydb/mvp/meta/support_links/common.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/yexception.h>

#include <memory>
#include <utility>

namespace NMVP {

class ILinkSource {
public:
    struct TResolveInput {
        size_t Place = 0;
        const THashMap<TString, TString>& ClusterColumns;
        const TVector<std::pair<TString, TString>>& QueryParams;
        const NActors::TActorId& Parent;
        const NActors::TActorId& HttpProxyId;
    };

    virtual ~ILinkSource() = default;
    virtual size_t Place() const = 0;
    virtual const TSupportLinkEntry& Config() const = 0;
    virtual TSourceOutput Resolve(const TResolveInput& input) const = 0;
};

class TGrafanaLinkSourceBase : public ILinkSource {
public:
    explicit TGrafanaLinkSourceBase(TSupportLinkEntry config)
        : Config_(std::move(config))
    {}

    size_t Place() const override {
        return 0;
    }

    const TSupportLinkEntry& Config() const override {
        return Config_;
    }

protected:
    NActors::IActor* BuildGrafanaResolver(const TResolveInput& input) const {
        NSupportLinks::TLinkResolveContext resolveContext{
            .Place = input.Place,
            .SourceName = Config_.GetSource(),
            .LinkConfig = Config_,
            .ClusterColumns = input.ClusterColumns,
            .QueryParams = input.QueryParams,
            .Parent = input.Parent,
            .HttpProxyId = input.HttpProxyId,
        };
        return NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(resolveContext));
    }

private:
    TSupportLinkEntry Config_;
};

class TGrafanaDashboardSource : public TGrafanaLinkSourceBase {
public:
    using TGrafanaLinkSourceBase::TGrafanaLinkSourceBase;

    TSourceOutput Resolve(const TResolveInput& input) const override
    {
        NSupportLinks::TLinkResolveContext resolveContext{
            .Place = input.Place,
            .SourceName = Config().GetSource(),
            .LinkConfig = Config(),
            .ClusterColumns = input.ClusterColumns,
            .QueryParams = input.QueryParams,
            .Parent = input.Parent,
            .HttpProxyId = input.HttpProxyId,
        };

        TSourceOutput result{
            .Name = Config().GetSource(),
            .Waiting = false,
        };
        TString url = NSupportLinks::ResolveGrafanaDashboardUrl(InstanceMVP->GrafanaSupportConfig, resolveContext, result.Errors);
        if (!url.empty()) {
            result.Links.emplace_back(NSupportLinks::TResolvedLink{
                .Title = Config().GetTitle(),
                .Url = std::move(url),
            });
        }
        return result;
    }
};

class TGrafanaDashboardSearchSource : public TGrafanaLinkSourceBase {
public:
    using TGrafanaLinkSourceBase::TGrafanaLinkSourceBase;

    TSourceOutput Resolve(const TResolveInput& input) const override
    {
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        Y_ABORT_UNLESS(actorSystem, "ActorSystem is unavailable in activation context");
        actorSystem->Register(BuildGrafanaResolver(input));
        return TSourceOutput{
            .Name = Config().GetSource(),
            .Waiting = true,
        };
    }
};

inline std::unique_ptr<ILinkSource> BuildGrafanaDashboardSource(
    TSupportLinkEntry config,
    const TGrafanaSupportConfig& grafanaConfig)
{
    NSupportLinks::ValidateGrafanaDashboardResolverConfig(NSupportLinks::TResolverValidationContext{
        .LinkConfig = config,
        .GrafanaConfig = grafanaConfig,
    });
    return std::make_unique<TGrafanaDashboardSource>(std::move(config));
}

inline std::unique_ptr<ILinkSource> BuildGrafanaDashboardSearchSource(
    TSupportLinkEntry config,
    const TGrafanaSupportConfig& grafanaConfig)
{
    NSupportLinks::ValidateGrafanaDashboardSearchResolverConfig(NSupportLinks::TResolverValidationContext{
        .LinkConfig = config,
        .GrafanaConfig = grafanaConfig,
    });
    return std::make_unique<TGrafanaDashboardSearchSource>(std::move(config));
}

inline std::unique_ptr<ILinkSource> MakeLinkSourceUnchecked(size_t place, TSupportLinkEntry config) {
    (void)place;
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD) {
        return std::make_unique<TGrafanaDashboardSource>(std::move(config));
    }
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH) {
        return std::make_unique<TGrafanaDashboardSearchSource>(std::move(config));
    }
    ythrow yexception() << "unsupported source=" << config.GetSource();
}

inline std::unique_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config, const TGrafanaSupportConfig& grafanaConfig) {
    (void)place;
    if (config.GetSource().empty()) {
        ythrow yexception() << "source is required";
    }
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD) {
        return BuildGrafanaDashboardSource(std::move(config), grafanaConfig);
    }
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH) {
        return BuildGrafanaDashboardSearchSource(std::move(config), grafanaConfig);
    }
    ythrow yexception() << "unsupported source=" << config.GetSource();
}

inline std::unique_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config) {
    Y_ABORT_UNLESS(InstanceMVP);
    return MakeLinkSource(place, std::move(config), InstanceMVP->MetaSettings.GrafanaConfig);
}

inline std::unique_ptr<ILinkSource> MakeGrafanaLinkSource(size_t place, TSupportLinkEntry config) {
    return MakeLinkSourceUnchecked(place, std::move(config));
}

} // namespace NMVP
