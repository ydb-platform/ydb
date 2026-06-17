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
    , NavigationRequest(initializer.NavigationRequest)
    , RequestedAddress(initializer.RequestedAddress)
{}

TContext::TContext(const NHttp::THttpIncomingRequestPtr& request)
    : State(GenerateRandomBase64())
    , NavigationRequest(IsPageNavigationRequest(request))
    , RequestedAddress(GetRequestedUrl(request, NavigationRequest))
{}

TString TContext::GetState(const TString& key, bool includeRequestedAddress) const {
    TState payload;
    payload.AntiForgeryToken = State;
    if (includeRequestedAddress) {
        payload.RequestedAddress = RequestedAddress;
    }
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
    const TString cookieValue = GenerateCookie(secret);
    if (cookieValue.size() > TOpenIdConnectSettings::MAX_AUTH_FLOW_COOKIE_VALUE_SIZE) {
        return {};
    }

    return TStringBuilder()
        << TOpenIdConnectSettings::YDB_OIDC_COOKIE << "=" << cookieValue << "; "
        << "Path=" << GetAuthCallbackUrl() << "; "
        << "Max-Age=" << TOpenIdConnectSettings::DEFAULT_AUTH_STATE_LIFETIME.Seconds() << "; "
        << "Secure; "
        << "HttpOnly; "
        << "SameSite=None";
}

TString TContext::GenerateCookie(const TString& key) const {
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["requested_address"] = RequestedAddress;
    const TString requestedAddressContext = NJson::WriteJson(json, false);

    TString digest = HmacSHA256(key, requestedAddressContext);

    NJson::TJsonValue root(NJson::JSON_MAP);
    root["requested_address_context"] = Base64Encode(requestedAddressContext);
    root["digest"] = Base64Encode(digest);
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
