#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "oidc_settings.h"
#include "context.h"

namespace NMVP::NOIDC {

class THandlerSessionCreate : public NActors::TActorBootstrapped<THandlerSessionCreate> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerSessionCreate>;

protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    TContext Context;

public:
    THandlerSessionCreate(const NActors::TActorId& sender,
                          const NHttp::THttpIncomingRequestPtr& request,
                          const NActors::TActorId& httpProxyId,
                          const TOpenIdConnectSettings& settings);

    virtual void RequestSessionToken(const TString&) = 0;
    virtual void ProcessSessionToken(const NJson::TJsonValue& jsonValue) = 0;

    void Bootstrap();
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);

protected:
    TString ChangeSameSiteFieldInSessionCookie(const TString& cookie);
    void RetryRequestToProtectedResourceAndDie();
    void RetryRequestToProtectedResourceAndDie(NHttp::THeadersBuilder* responseHeaders);
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
    void ReplyBadRequestAndPassAway(TString errorMessage);

private:
    void SendUnknownErrorResponseAndDie();
};

} // NMVP::NOIDC
