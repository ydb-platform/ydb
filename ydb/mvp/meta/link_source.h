#pragma once

#include <ydb/mvp/meta/support_links/grafana_dashboard_resolver.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_search_resolver.h>
#include <ydb/mvp/meta/support_links/support_links_resolver.h>
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
    virtual TResolveOutput Resolve(const TResolveInput& input) const = 0;
};

class TGrafanaLinkSourceBase : public ILinkSource {
public:
    explicit TGrafanaLinkSourceBase(TSupportLinkEntry config, const TMetaSettings& metaSettings)
        : Config_(std::move(config))
        , MetaSettings_(metaSettings)
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
        return NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(resolveContext), MetaSettings_);
    }

    const TMetaSettings& GetMetaSettings() const {
        return MetaSettings_;
    }

private:
    TSupportLinkEntry Config_;
    const TMetaSettings& MetaSettings_;
};

class TGrafanaDashboardSource : public TGrafanaLinkSourceBase {
public:
    TGrafanaDashboardSource(TSupportLinkEntry config, const TMetaSettings& metaSettings)
        : TGrafanaLinkSourceBase(std::move(config), metaSettings)
    {}

    TResolveOutput Resolve(const TResolveInput& input) const override
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

        TResolveOutput result{
            .Name = Config().GetSource(),
            .Ready = true,
        };
        TString url = NSupportLinks::ResolveGrafanaDashboardUrl(GetMetaSettings().GrafanaConfig, resolveContext, result.Errors);
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
    TGrafanaDashboardSearchSource(TSupportLinkEntry config, const TMetaSettings& metaSettings)
        : TGrafanaLinkSourceBase(std::move(config), metaSettings)
    {}

    TResolveOutput Resolve(const TResolveInput& input) const override
    {
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        Y_ABORT_UNLESS(actorSystem, "ActorSystem is unavailable in activation context");
        NActors::TActorId actorId = actorSystem->Register(BuildGrafanaResolver(input));
        return TResolveOutput{
            .Name = Config().GetSource(),
            .Ready = false,
            .Actors = {actorId},
        };
    }
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

inline std::shared_ptr<ILinkSource> BuildGrafanaDashboardSearchSource(
    TSupportLinkEntry config,
    const TMetaSettings& metaSettings)
{
    NSupportLinks::ValidateGrafanaDashboardSearchResolverConfig(NSupportLinks::TResolverValidationContext{
        .LinkConfig = config,
        .GrafanaConfig = metaSettings.GrafanaConfig,
    });
    return std::make_shared<TGrafanaDashboardSearchSource>(std::move(config), metaSettings);
}

inline std::shared_ptr<ILinkSource> MakeLinkSourceUnchecked(size_t place, TSupportLinkEntry config, const TMetaSettings& metaSettings) {
    (void)place;
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD) {
        return std::make_shared<TGrafanaDashboardSource>(std::move(config), metaSettings);
    }
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH) {
        return std::make_shared<TGrafanaDashboardSearchSource>(std::move(config), metaSettings);
    }
    ythrow yexception() << "unsupported source=" << config.GetSource();
}

inline std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config, const TMetaSettings& metaSettings) {
    (void)place;
    if (config.GetSource().empty()) {
        ythrow yexception() << "source is required";
    }
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD) {
        return BuildGrafanaDashboardSource(std::move(config), metaSettings);
    }
    if (config.GetSource() == NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH) {
        return BuildGrafanaDashboardSearchSource(std::move(config), metaSettings);
    }
    ythrow yexception() << "unsupported source=" << config.GetSource();
}

inline std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config) {
    Y_ABORT_UNLESS(InstanceMVP);
    return MakeLinkSource(place, std::move(config), InstanceMVP->MetaSettings);
}

inline std::shared_ptr<ILinkSource> MakeGrafanaLinkSource(size_t place, TSupportLinkEntry config) {
    Y_ABORT_UNLESS(InstanceMVP);
    return MakeLinkSourceUnchecked(place, std::move(config), InstanceMVP->MetaSettings);
}

} // namespace NMVP
