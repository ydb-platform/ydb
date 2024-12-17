#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include "openid_connect.h"
#include "oidc_settings.h"
#include "context.h"

namespace NMVP::NOIDC {

TContext::TContext(const TInitializer& initializer)
    : State(initializer.State)
    , AjaxRequest(initializer.AjaxRequest)
    , RequestedAddress(initializer.RequestedAddress)
{}

TContext::TContext(const NHttp::THttpIncomingRequestPtr& request)
    : State(GenerateState())
    , AjaxRequest(DetectAjaxRequest(request))
    , RequestedAddress(GetRequestedUrl(request, AjaxRequest))
{}

TString TContext::GetState(const TString& key) const {
    static const TDuration STATE_LIFE_TIME = TDuration::Minutes(10);
    TInstant expirationTime = TInstant::Now() + STATE_LIFE_TIME;
    TStringBuilder json;
    json << "{\"state\":\"" << State
         << "\",\"expiration_time\":\"" << ToString(expirationTime.TimeT()) << "\"}";
    TString digest = HmacSHA1(key, json);
    TStringBuilder signedState;
    signedState << "{\"container\":\"" << Base64Encode(json) << "\","
                  "\"digest\":\"" << Base64Encode(digest) << "\"}";
    return Base64EncodeNoPadding(signedState);
}

bool TContext::IsAjaxRequest() const {
    return AjaxRequest;
}

TString TContext::GetRequestedAddress() const {
    return RequestedAddress;
}

TString TContext::CreateYdbOidcCookie(const TString& secret) const {
    static constexpr size_t COOKIE_MAX_AGE_SEC = 3600;
    return TStringBuilder() << TOpenIdConnectSettings::YDB_OIDC_COOKIE << "="
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

TString TContext::GenerateState() {
    TStringBuilder sb;
    static constexpr size_t CHAR_NUMBER = 15;
    for (size_t i{0}; i < CHAR_NUMBER; i++) {
        sb << RandomNumber<char>();
    }
    return Base64EncodeUrlNoPadding(sb);
}

bool TContext::DetectAjaxRequest(const NHttp::THttpIncomingRequestPtr& request) {
    static const THashMap<TStringBuf, TStringBuf> expectedHeaders {
        {"Accept", "application/json"}
    };
    NHttp::THeaders headers(request->Headers);
    for (const auto& el : expectedHeaders) {
        TStringBuf headerValue = headers.Get(el.first);
        if (!headerValue || headerValue.find(el.second) == TStringBuf::npos) {
            return false;
        }
    }
    return true;
}

TStringBuf TContext::GetRequestedUrl(const NHttp::THttpIncomingRequestPtr& request, bool isAjaxRequest) {
    NHttp::THeaders headers(request->Headers);
    TStringBuf requestedUrl = headers.Get("Referer");
    if (!isAjaxRequest || requestedUrl.empty()) {
        return request->URL;
    }
    return requestedUrl;
}

} // NMVP::NOIDC
