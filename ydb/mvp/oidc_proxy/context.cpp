#include "context.h"
#include "oidc_settings.h"
#include "openid_connect.h"

#include <ydb/library/actors/http/http.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NMVP::NOIDC {

TContext::TContext(const TInitializer& initializer)
    : State(initializer.State)
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
    payload.ExpirationTime = TInstant::Now() + TOpenIdConnectSettings::DEFAULT_OIDC_FLOW_LIFETIME;
    payload.CookieSuffix = CreateFlowId(key, RequestedAddress);
    return EncodeState(payload, key);
}

bool TContext::IsNavigationRequest() const {
    return NavigationRequest;
}

TString TContext::GetRequestedAddress() const {
    return RequestedAddress;
}

TString TContext::CreateAuthFlowCookie(const TString& secret) const {
    return TStringBuilder() << CreateAuthFlowCookieName(CreateFlowId(secret, RequestedAddress)) << "="
                            << CreateAuthFlowCookieValue(secret) << ";"
                            " Path=" << GetAuthCallbackUrl() << ";"
                            " Max-Age=" << TOpenIdConnectSettings::DEFAULT_OIDC_FLOW_LIFETIME.Seconds() << ";"
                            " SameSite=None; Secure";
}

TString TContext::CreateAuthFlowCookieValue(const TString& key) const {
    NJson::TJsonValue root(NJson::JSON_MAP);
    root["requested_address"] = RequestedAddress;
    root["digest"] = Base64Encode(HmacSHA256(key, RequestedAddress));
    return Base64Encode(NJson::WriteJson(root, false));
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
