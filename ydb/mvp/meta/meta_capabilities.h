#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP {

class THandlerActorMetaCapabilities : public NActors::TActor<THandlerActorMetaCapabilities> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCapabilities>;

    THandlerActorMetaCapabilities()
        : TBase(&THandlerActorMetaCapabilities::StateWork)
    {}

    void Handle(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event, const NActors::TActorContext& ctx) {
        // Stub endpoint for UI compatibility so clients do not receive 404 while capabilities are not implemented yet.
        static constexpr TStringBuf ResponseBody = "{\n  \"Capabilities\": {}\n}\n";
        auto response = event->Get()->Request->CreateResponseOK(ResponseBody, "application/json; charset=utf-8");
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
