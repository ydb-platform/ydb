#pragma once

#include "extension_manager.h"

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP::NOIDC {

class THandlerSessionServiceCheck : public NActors::TActorBootstrapped<THandlerSessionServiceCheck> {
protected:
    using TBase = NActors::TActorBootstrapped<THandlerSessionServiceCheck>;

    const NActors::TActorId Sender;
    NHttp::THttpIncomingRequestPtr Request;
    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    const TCrackedPage ProtectedPage;

    THolder<TExtensionManager> ExtensionManager;
    NHttp::THttpOutgoingResponsePtr StreamResponse;
    NActors::TActorId StreamConnection;

public:
    THandlerSessionServiceCheck(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings);

    virtual void Bootstrap(const NActors::TActorContext& ctx);
    void HandleProxy(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);
    void HandleIncompleteProxy(NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse::TPtr event);
    void HandleDataChunk(NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk::TPtr event);
    void HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr event);
    void HandleCancelled();
    void HandleEnrichmentTimeout();

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleProxy);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse, HandleIncompleteProxy);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk, HandleDataChunk);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, HandleCancelled);
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleEnrichmentTimeout);
        }
    }

protected:
    virtual void StartOidcProcess(const NActors::TActorContext& ctx) = 0;
    virtual void ForwardUserRequest(TStringBuf authHeader, bool secure = false);
    virtual bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const = 0;
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
    static bool IsAuthorizedRequest(TStringBuf authHeader);

private:
    void SendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response);
};

} // NMVP::NOIDC
