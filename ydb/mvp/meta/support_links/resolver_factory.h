#pragma once

#include "grafana_dashboard_search_resolver.h"

#include <util/generic/hash.h>
#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

class TLinkSourceCollections {
public:
    using TBuilderFn = NActors::IActor* (*)(TLinkResolveContext);

    struct TResolverRegistration {
        TString SourceName;
        TBuilderFn Build = nullptr;
    };

    class TResolverBuilder {
    public:
        TResolverBuilder() = default;

        TResolverBuilder(TString sourceName, TBuilderFn builder)
            : SourceName(std::move(sourceName))
            , Builder(builder)
        {}

        NActors::IActor* Build(TLinkResolveContext context) const {
            context.SourceName = SourceName;
            return Builder(std::move(context));
        }

    private:
        TString SourceName;
        TBuilderFn Builder = nullptr;
    };

    struct TSourceEntry {
        TResolverBuilder Builder;
    };

    void Register(TResolverRegistration registration) {
        TString normalizedSourceName = registration.SourceName;
        Builders[std::move(registration.SourceName)] = TSourceEntry{
            .Builder = TResolverBuilder(std::move(normalizedSourceName), registration.Build),
        };
    }

    NActors::IActor* Build(TLinkResolveContext context) const {
        const TString sourceName = context.LinkConfig.Source;
        if (sourceName.empty()) {
            ythrow yexception() << "support_links source is empty";
        }

        auto it = Builders.find(sourceName);
        if (it == Builders.end()) {
            ythrow yexception() << "unsupported support_links source: " << sourceName;
        }
        return it->second.Builder.Build(std::move(context));
    }

    static const TLinkSourceCollections& Default() {
        static const TLinkSourceCollections collections = [] {
            TLinkSourceCollections c;
            c.Register(TResolverRegistration{
                .SourceName = SOURCE_GRAFANA_DASHBOARD_SEARCH,
                .Build = &BuildGrafanaDashboardSearchResolver,
            });
            return c;
        }();
        return collections;
    }

private:
    THashMap<TString, TSourceEntry> Builders;
};

inline NActors::IActor* BuildSourceHandler(TLinkResolveContext context) {
    return TLinkSourceCollections::Default().Build(std::move(context));
}

} // namespace NMVP::NSupportLinks
