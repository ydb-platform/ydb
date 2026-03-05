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
        TVector<NActors::IActor*>* ActorsToRegister = nullptr;
    };

    virtual ~ILinkSource() = default;
    virtual size_t Place() const = 0;
    virtual const TSupportLinkEntryConfig& Config() const = 0;
    virtual TSourceOutput Resolve(const TResolveInput& input) const = 0;
};

class TGrafanaLinkSourceBase : public ILinkSource {
public:
    TGrafanaLinkSourceBase(size_t place, TSupportLinkEntryConfig config)
        : Place_(place)
        , Config_(std::move(config))
    {}

    size_t Place() const override {
        return Place_;
    }

    const TSupportLinkEntryConfig& Config() const override {
        return Config_;
    }

protected:
    NActors::IActor* BuildGrafanaResolver(const TResolveInput& input) const
    {
        NSupportLinks::TLinkResolveContext resolveContext{
            .Place = input.Place,
            .SourceName = Config_.Source,
            .LinkConfig = Config_,
            .ClusterColumns = input.ClusterColumns,
            .QueryParams = input.QueryParams,
            .Parent = input.Parent,
            .HttpProxyId = input.HttpProxyId,
        };
        return NSupportLinks::BuildGrafanaDashboardSearchResolver(std::move(resolveContext));
    }

private:
    size_t Place_;
    TSupportLinkEntryConfig Config_;
};

class TGrafanaDashboardSource : public TGrafanaLinkSourceBase {
public:
    using TGrafanaLinkSourceBase::TGrafanaLinkSourceBase;

    TSourceOutput Resolve(const TResolveInput& input) const override
    {
        NSupportLinks::TLinkResolveContext resolveContext{
            .Place = input.Place,
            .SourceName = Config().Source,
            .LinkConfig = Config(),
            .ClusterColumns = input.ClusterColumns,
            .QueryParams = input.QueryParams,
            .Parent = input.Parent,
            .HttpProxyId = input.HttpProxyId,
        };

        TSourceOutput result{
            .Name = Config().Source,
            .Waiting = false,
        };
        TString url = NSupportLinks::ResolveGrafanaDashboardUrl(InstanceMVP->GrafanaSupportConfig, resolveContext, result.Errors);
        if (!url.empty()) {
            result.Links.emplace_back(NSupportLinks::TResolvedLink{
                .Title = Config().Title,
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
        Y_ABORT_UNLESS(input.ActorsToRegister, "ActorsToRegister must be provided for async source");
        input.ActorsToRegister->push_back(BuildGrafanaResolver(input));
        return TSourceOutput{
            .Name = Config().Source,
            .Waiting = true,
        };
    }
};

inline std::unique_ptr<ILinkSource> BuildGrafanaDashboardSource(
    size_t place,
    TSupportLinkEntryConfig config,
    const TGrafanaSupportConfig& grafanaConfig,
    TStringBuf where)
{
    NSupportLinks::ValidateGrafanaDashboardResolverConfig(NSupportLinks::TResolverValidationContext{
        .LinkConfig = config,
        .GrafanaConfig = grafanaConfig,
        .Where = where,
    });
    return std::make_unique<TGrafanaDashboardSource>(place, std::move(config));
}

inline std::unique_ptr<ILinkSource> BuildGrafanaDashboardSearchSource(
    size_t place,
    TSupportLinkEntryConfig config,
    const TGrafanaSupportConfig& grafanaConfig,
    TStringBuf where)
{
    NSupportLinks::ValidateGrafanaDashboardSearchResolverConfig(NSupportLinks::TResolverValidationContext{
        .LinkConfig = config,
        .GrafanaConfig = grafanaConfig,
        .Where = where,
    });
    return std::make_unique<TGrafanaDashboardSearchSource>(place, std::move(config));
}

inline std::unique_ptr<ILinkSource> MakeLinkSourceUnchecked(size_t place, TSupportLinkEntryConfig config) {
    if (config.Source == NSupportLinks::SOURCE_GRAFANA_DASHBOARD) {
        return std::make_unique<TGrafanaDashboardSource>(place, std::move(config));
    }
    if (config.Source == NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH) {
        return std::make_unique<TGrafanaDashboardSearchSource>(place, std::move(config));
    }
    ythrow yexception() << "unsupported source=" << config.Source;
}

inline std::unique_ptr<ILinkSource> MakeLinkSource(
    size_t place,
    TSupportLinkEntryConfig config,
    const TGrafanaSupportConfig& grafanaConfig,
    TStringBuf where)
{
    if (config.Source.empty()) {
        ythrow yexception() << where << ": source is required";
    }
    if (config.Source == NSupportLinks::SOURCE_GRAFANA_DASHBOARD) {
        return BuildGrafanaDashboardSource(place, std::move(config), grafanaConfig, where);
    }
    if (config.Source == NSupportLinks::SOURCE_GRAFANA_DASHBOARD_SEARCH) {
        return BuildGrafanaDashboardSearchSource(place, std::move(config), grafanaConfig, where);
    }
    ythrow yexception() << where << ": unsupported source=" << config.Source;
}

inline std::unique_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntryConfig config, TStringBuf where) {
    Y_ABORT_UNLESS(InstanceMVP);
    return MakeLinkSource(place, std::move(config), InstanceMVP->MetaSettings.GrafanaConfig, where);
}

inline std::unique_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntryConfig config) {
    return MakeLinkSource(place, std::move(config), TStringBuilder() << "support_links[" << place << "]");
}

inline TSupportLinkEntryConfig MakeSupportLinkEntryConfig(
    const NMvp::NMeta::TMetaConfig::TSupportLinksConfig::TSupportLinkEntry& link)
{
    TSupportLinkEntryConfig entry;
    entry.Source = link.GetSource();
    entry.Title = link.GetTitle();
    entry.Url = link.GetUrl();
    entry.Tag = link.GetTag();
    entry.Folder = link.GetFolder();
    return entry;
}

inline std::unique_ptr<ILinkSource> MakeLinkSource(
    size_t place,
    const NMvp::NMeta::TMetaConfig::TSupportLinksConfig::TSupportLinkEntry& link,
    const TGrafanaSupportConfig& grafanaConfig,
    TStringBuf where)
{
    return MakeLinkSource(place, MakeSupportLinkEntryConfig(link), grafanaConfig, where);
}

inline std::unique_ptr<ILinkSource> MakeLinkSource(
    size_t place,
    const NMvp::NMeta::TMetaConfig::TSupportLinksConfig::TSupportLinkEntry& link)
{
    Y_ABORT_UNLESS(InstanceMVP);
    return MakeLinkSource(place, link, InstanceMVP->MetaSettings.GrafanaConfig, TStringBuilder() << "support_links[" << place << "]");
}

inline std::unique_ptr<ILinkSource> MakeGrafanaLinkSource(size_t place, TSupportLinkEntryConfig config) {
    return MakeLinkSourceUnchecked(place, std::move(config));
}

} // namespace NMVP
