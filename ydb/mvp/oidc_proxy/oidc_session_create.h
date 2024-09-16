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

class THandlerSessionCreate : public NActors::TActorBootstrapped<THandlerSessionCreate> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerSessionCreate>;

protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    TString RedirectUrl;
    bool IsAjaxRequest = false;
    NHttp::THeadersBuilder ResponseHeaders;

public:
    THandlerSessionCreate(const NActors::TActorId& sender,
                          const NHttp::THttpIncomingRequestPtr& request,
                          const NActors::TActorId& httpProxyId,
                          const TOpenIdConnectSettings& settings);

    virtual void RequestSessionToken(const TString&, const NActors::TActorContext&) = 0;
    virtual void ProcessSessionToken(const TString& accessToken, const NActors::TActorContext&) = 0;

    void Bootstrap(const NActors::TActorContext& ctx);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx);

protected:
    void RemoveAppliedCookie(const TString& cookieName);
    bool IsStateValid(const TString& state, const NHttp::TCookies& cookies, const NActors::TActorContext& ctx);
    TString ChangeSameSiteFieldInSessionCookie(const TString& cookie);
};

}  // NOIDC
}  // NMVP
