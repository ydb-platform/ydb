#pragma once

#include "events.h"

#include <ydb/library/actors/http/http.h>

#include <util/string/cast.h>

namespace NMVP::NSupportLinks {

template <class TDerived>
class TResolverBase {
protected:
    TVector<TResolvedLink> Links;
    TVector<TSupportError> Errors;
    TLinkResolveContext Context;

    explicit TResolverBase(TLinkResolveContext context)
        : Context(std::move(context))
    {}

    void AddHttpError(const NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        TSupportError error;
        error.Source = Context.SourceName;
        if (event->Get()->Response) {
            ui32 status = 0;
            if (TryFromString<ui32>(event->Get()->Response->Status, status)) {
                error.Status = status;
            }
            error.Reason = TString(event->Get()->Response->Message);
        }
        if (!event->Get()->Error.empty()) {
            error.Message = event->Get()->Error;
        } else if (event->Get()->Response) {
            error.Message = TStringBuilder() << event->Get()->Response->Status << " " << event->Get()->Response->Message;
        } else {
            error.Message = "Unknown Grafana request error";
        }
        Errors.emplace_back(std::move(error));
    }

    void SendResultAndDie(const NActors::TActorContext& ctx) {
        ctx.Send(Context.Parent, new TEvPrivate::TEvSourceResponse(Context.Place, std::move(Links), std::move(Errors)));
        static_cast<TDerived*>(this)->DieResolver(ctx);
    }
};

} // namespace NMVP::NSupportLinks
