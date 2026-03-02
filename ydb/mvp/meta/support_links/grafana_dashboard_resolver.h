#pragma once

#include "common.h"
#include "events.h"
#include "grafana_resolver_base.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

class TGrafanaDashboardResolver
    : public NActors::TActorBootstrapped<TGrafanaDashboardResolver>
    , public TGrafanaResolverBase<TGrafanaDashboardResolver> {
public:
    explicit TGrafanaDashboardResolver(TLinkResolveContext context)
        : TGrafanaResolverBase<TGrafanaDashboardResolver>(std::move(context))
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString url = ResolveGrafanaDashboardUrl(Context.LinkConfig.Url, Errors);
        if (!url.empty()) {
            Links.emplace_back(TResolvedLink{
                .Title = Context.LinkConfig.Title,
                .Url = std::move(url)
            });
        }
        SendResultAndDie(ctx);
    }

    void DieResolver(const NActors::TActorContext& ctx) {
        Die(ctx);
    }
};

inline NActors::IActor* BuildGrafanaDashboardResolver(TLinkResolveContext context) {
    return new TGrafanaDashboardResolver(std::move(context));
}

inline void ValidateGrafanaDashboardResolverConfig(const TResolverValidationContext& context)
{
    if (context.LinkConfig.Url.empty()) {
        ythrow yexception() << context.Where << ": url is required for source=" << context.LinkConfig.Source;
    }
    if (!IsAbsoluteUrl(context.LinkConfig.Url) && context.GrafanaConfig.Endpoint.empty()) {
        ythrow yexception() << context.Where << ": grafana.endpoint is required for relative url";
    }
}

} // namespace NMVP::NSupportLinks
