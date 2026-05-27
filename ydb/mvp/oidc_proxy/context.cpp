#include "context.h"
#include "oidc_cookie.h"
#include "oidc_settings.h"
#include "openid_connect.h"

#include <ydb/library/actors/http/http.h>

#include <util/generic/string.h>

namespace NMVP::NOIDC {

TContext::TContext(const TInitializer& initializer)
    : State(initializer.State)
    , NavigationRequest(initializer.NavigationRequest)
    , RequestedAddress(initializer.RequestedAddress)
{}

TContext::TContext(const NHttp::THttpIncomingRequestPtr& request)
    : State(GenerateRandomBase64())
    , NavigationRequest(IsPageNavigationRequest(request))
    , RequestedAddress(GetRequestedUrl(request, NavigationRequest))
{}

TString TContext::GetState(const TString& key) const {
    TState payload;
    payload.AntiForgeryToken = State;
    payload.RequestedAddress = RequestedAddress;
    payload.ExpirationTime = TInstant::Now() + TOpenIdConnectSettings::DEFAULT_AUTH_STATE_LIFETIME;
    return EncodeState(payload, key);
}

bool TContext::IsNavigationRequest() const {
    return NavigationRequest;
}

TString TContext::GetRequestedAddress() const {
    return RequestedAddress;
}

TString TContext::CreateYdbOidcCookie(const TString& secret) const {
    return CreateYdbOidcCookie(secret, "");
}

TString TContext::CreateYdbOidcCookie(const TString& secret, TStringBuf currentCookieValue) const {
    const TString updatedCookieValue = UpdateAuthFlowCookieValue(currentCookieValue, secret, State);
    return CreateAuthFlowCookie(updatedCookieValue);
}

bool TContext::IsPageNavigationRequest(const NHttp::THttpIncomingRequestPtr& request) {
    NHttp::THeaders headers(request->Headers);

    const TStringBuf mode = headers.Get("Sec-Fetch-Mode");
    const TStringBuf dest = headers.Get("Sec-Fetch-Dest");
    const bool hasFetchMetadata = mode && dest;
    if (hasFetchMetadata) {
        return mode == "navigate" && dest == "document";
    }

    if (headers.Get("X-Requested-With") == "XMLHttpRequest") {
        return false;
    }

    const TStringBuf accept = headers.Get("Accept");
    if (accept && accept.find("application/json") != TStringBuf::npos) {
        return false;
    }

    return true;
}

TStringBuf TContext::GetRequestedUrl(const NHttp::THttpIncomingRequestPtr& request, bool isNavigationRequest) {
    NHttp::THeaders headers(request->Headers);
    TStringBuf requestedUrl = headers.Get("Referer");
    if (isNavigationRequest || requestedUrl.empty()) {
        return request->URL;
    }
    return requestedUrl;
}

} // NMVP::NOIDC
