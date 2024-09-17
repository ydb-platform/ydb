#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "oidc_settings.h"

namespace NMVP {
namespace NOIDC {

class THandlerSessionServiceCheck : public NActors::TActorBootstrapped<THandlerSessionServiceCheck> {
protected:
    using TBase = NActors::TActorBootstrapped<THandlerSessionServiceCheck>;

    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    TString ProtectedPageUrl;
    TString RequestedPageScheme;
    bool IsAjaxRequest = false;

    const static inline TStringBuf IAM_TOKEN_SCHEME = "Bearer ";
    const static inline TStringBuf IAM_TOKEN_SCHEME_LOWER = "bearer ";
    const static inline TStringBuf AUTH_HEADER_NAME = "Authorization";

public:
    THandlerSessionServiceCheck(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings);

    virtual void Bootstrap(const NActors::TActorContext& ctx);
    void HandleProxy(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx);

protected:
    virtual void StartOidcProcess(const NActors::TActorContext& ctx) = 0;
    virtual void ForwardUserRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure = false);
    virtual bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const = 0;

    bool CheckRequestedHost();
    void ForwardRequestHeaders(NHttp::THttpOutgoingRequestPtr& request) const;

    static bool IsAuthorizedRequest(TStringBuf authHeader);
    static TString FixReferenceInHtml(TStringBuf html, TStringBuf host, TStringBuf findStr);
    static TString FixReferenceInHtml(TStringBuf html, TStringBuf host);

private:
    NHttp::THeadersBuilder GetResponseHeaders(const NHttp::THttpIncomingResponsePtr& response);
    void SendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response, const NActors::TActorContext& ctx);
    TString GetFixedLocationHeader(TStringBuf location);
    NHttp::THttpOutgoingResponsePtr CreateResponseForbiddenHost();
    NHttp::THttpOutgoingResponsePtr CreateResponseForNotExistingResponseFromProtectedResource(const TString& errorMessage);
};

}  // NOIDC
}  // NMVP
