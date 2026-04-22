#include <util/generic/string.h>
#include <util/string/builder.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include "openid_connect.h"
#include "oidc_settings.h"
#include "context.h"

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
    static const TDuration STATE_LIFE_TIME = TDuration::Minutes(10);
    TState payload;
    payload.AntiForgeryToken = State;
    payload.ExpirationTime = (TInstant::Now() + STATE_LIFE_TIME).TimeT();
    if (!NavigationRequest) {
        payload.CookieSuffix = TString(TOpenIdConnectSettings::YDB_OIDC_COOKIE_BACKGROUND_SUFFIX);
    }
    return EncodeState(payload, key);
}

bool TContext::IsNavigationRequest() const {
    return NavigationRequest;
}

TString TContext::GetRequestedAddress() const {
    return RequestedAddress;
}

TString TContext::CreateYdbOidcCookie(const TString& secret) const {
    static constexpr size_t COOKIE_MAX_AGE_SEC = 3600;
    return TStringBuilder() << CreateNameYdbOidcCookie(NavigationRequest ? TStringBuf() : TOpenIdConnectSettings::YDB_OIDC_COOKIE_BACKGROUND_SUFFIX) << "="
                            << GenerateCookie(secret) << ";"
                            " Path=" << GetAuthCallbackUrl() << ";"
                            " Max-Age=" << COOKIE_MAX_AGE_SEC << ";"
                            " SameSite=None; Secure";
}

TString TContext::GenerateCookie(const TString& key) const {
    TStringBuilder requestedAddressContext;
    requestedAddressContext << "{\"requested_address\":\"" << RequestedAddress << "\"}";
    TString digest = HmacSHA256(key, requestedAddressContext);
    TStringBuilder signedRequestedAddress;
    signedRequestedAddress << "{\"requested_address_context\":\"" << Base64Encode(requestedAddressContext)
                           << "\",\"digest\":\"" << Base64Encode(digest) << "\"}";
    return Base64Encode(signedRequestedAddress);
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
