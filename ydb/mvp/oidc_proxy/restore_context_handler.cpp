#include <utility>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include "context_storage.h"
#include "restore_context_handler.h"

namespace NMVP {
namespace NOIDC {

class TRestoreContextImpl : public NActors::TActorBootstrapped<TRestoreContextImpl> {
private:
    using TBase = NActors::TActorBootstrapped<TRestoreContextImpl>;

    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    TContextStorage* const ContextStorage;

public:
    TRestoreContextImpl(const NActors::TActorId& sender,
                        const NHttp::THttpIncomingRequestPtr& request,
                        const NActors::TActorId& httpProxyId,
                        TContextStorage* const contextStorage);

    void Bootstrap(const NActors::TActorContext& ctx);
};

TRestoreContextHandler::TRestoreContextHandler(const NActors::TActorId& httpProxyId, TContextStorage* const contextStorage)
    : TBase(&TRestoreContextHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , ContextStorage(contextStorage)
{}

void TRestoreContextHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
    NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
    if (request->Method == "GET") {
        ctx.Register(new TRestoreContextImpl(event->Sender, request, HttpProxyId, ContextStorage));
        return;
    }
    auto response = request->CreateResponseBadRequest();
    ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
}

TRestoreContextImpl::TRestoreContextImpl(const NActors::TActorId& sender,
                                         const NHttp::THttpIncomingRequestPtr& request,
                                         const NActors::TActorId& httpProxyId,
                                         TContextStorage* const contextStorage)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , ContextStorage(contextStorage)
{}

void TRestoreContextImpl::Bootstrap(const NActors::TActorContext& ctx) {
    NHttp::TUrlParameters urlParameters(Request->URL);
    TString state = urlParameters["state"];

    std::pair<bool, TContextRecord> restoreContextResult = ContextStorage->Find(state);
    NHttp::THttpOutgoingResponsePtr response = nullptr;
    if (restoreContextResult.first) {
        TContext context = restoreContextResult.second.GetContext();
        TStringBuilder body;
        body << "{\"requested_address\":\"" << context.GetRequestedAddress() << "\","
                 "\"is_ajax_request\":\"" << context.GetIsAjaxRequest() << "\","
                 "\"expiration_time\":" << ToString(restoreContextResult.second.GetExpirationTime().TimeT()) << "}";
        response = Request->CreateResponseOK(body, "application/json; charset=utf-8");

    } else {
        response = Request->CreateResponse("401", "Unauthorized");
    }
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    Die(ctx);
}

} // NOIDC
} // NMVP
