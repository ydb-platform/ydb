#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "oidc_settings.h"

namespace NMVP::NOIDC {

class THandlerSessionServiceCheck : public NActors::TActorBootstrapped<THandlerSessionServiceCheck> {
protected:
    using TBase = NActors::TActorBootstrapped<THandlerSessionServiceCheck>;

    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    const TString ProtectedPageUrl;
    TString RequestedPageScheme;
    NHttp::THttpOutgoingResponsePtr StreamResponse;
    NActors::TActorId StreamConnection;

    const static inline TStringBuf IAM_TOKEN_SCHEME = "Bearer ";
    const static inline TStringBuf IAM_TOKEN_SCHEME_LOWER = "bearer ";
    const static inline TStringBuf AUTH_HEADER_NAME = "Authorization";

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

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleProxy);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse, HandleIncompleteProxy);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk, HandleDataChunk);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, HandleCancelled);
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
        }
    }

protected:
    virtual void StartOidcProcess(const NActors::TActorContext& ctx) = 0;
    virtual void ForwardUserRequest(TStringBuf authHeader, bool secure = false);
    virtual bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const = 0;

    bool CheckRequestedHost();
    NHttp::THttpOutgoingRequestPtr CreateProxiedRequest(TStringBuf authHeader, bool secure) const;
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);

    static bool IsAuthorizedRequest(TStringBuf authHeader);
    static TString FixReferenceInHtml(TStringBuf html, TStringBuf host, TStringBuf findStr);
    static TString FixReferenceInHtml(TStringBuf html, TStringBuf host);

private:
    NHttp::THeadersBuilder ProxyResponseHeaders(const NHttp::THttpIncomingResponsePtr& response);
    TString ProxyResponseBody(const NHttp::THttpIncomingResponsePtr& response);
    NHttp::THttpOutgoingResponsePtr CreateProxiedResponse(const NHttp::THttpIncomingResponsePtr& response);
    void SendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response);
    TString GetFixedLocationHeader(TStringBuf location);
    NHttp::THttpOutgoingResponsePtr CreateResponseForbiddenHost();
    NHttp::THttpOutgoingResponsePtr CreateResponseForNotExistingResponseFromProtectedResource(const TString& errorMessage);
};

} // NMVP::NOIDC
