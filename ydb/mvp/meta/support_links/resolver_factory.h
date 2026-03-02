#pragma once

#include "common.h"
#include "events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

class TUnsupportedSourceResolver
    : public NActors::TActorBootstrapped<TUnsupportedSourceResolver> {
public:
    explicit TUnsupportedSourceResolver(TLinkResolveContext context)
        : Context(std::move(context))
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TVector<TSupportError> errors;
        errors.emplace_back(TSupportError{
            .Source = Context.LinkConfig.Source,
            .Message = TStringBuilder() << "unsupported support_links source: " << Context.LinkConfig.Source
        });
        ctx.Send(Context.Parent, new TEvPrivate::TEvSourceResponse(Context.Place, {}, std::move(errors)));
        Die(ctx);
    }

private:
    TLinkResolveContext Context;
};

inline NActors::IActor* BuildSourceHandler(TLinkResolveContext context) {
    if (context.LinkConfig.Source.empty()) {
        ythrow yexception() << "support_links source is empty";
    }
    return new TUnsupportedSourceResolver(std::move(context));
}

} // namespace NMVP::NSupportLinks
