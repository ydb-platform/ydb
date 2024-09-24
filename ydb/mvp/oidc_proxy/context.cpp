#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include "openid_connect.h"
#include "context.h"

namespace NMVP {
namespace NOIDC {

TContext::TContext(const TString& state, const TString& requestedAddress, bool isAjaxRequest)
    : State(state)
    , AjaxRequest(isAjaxRequest)
    , RequestedAddress(requestedAddress)
{}

TContext::TContext(const NHttp::THttpIncomingRequestPtr& request)
    : State(GenerateState())
    , AjaxRequest(DetectAjaxRequest(request))
    , RequestedAddress(GetRequestedUrl(request, AjaxRequest))
{}

TString TContext::GetState() const {
    return State;
}

bool TContext::IsAjaxRequest() const {
    return AjaxRequest;
}

TString TContext::GetRequestedAddress() const {
    return RequestedAddress;
}

TString TContext::CreateYdbOidcCookie(const TString& secret) const {
    static constexpr size_t COOKIE_MAX_AGE_SEC = 420;
    return TStringBuilder() << CreateNameYdbOidcCookie(secret, State) << "="
                            << GenerateCookie(secret) << ";"
                            " Path=" << GetAuthCallbackUrl() << ";"
                            " Max-Age=" << COOKIE_MAX_AGE_SEC << ";"
                            " SameSite=None; Secure";
}

TString TContext::GenerateCookie(const TString& secret) const {
    const TDuration StateLifeTime = TDuration::Minutes(10);
    TInstant expirationTime = TInstant::Now() + StateLifeTime;
    TStringBuilder stateStruct;
    stateStruct << "{\"state\":\"" << State
                << "\",\"requested_address\":\"" << RequestedAddress
                << "\",\"expiration_time\":" << ToString(expirationTime.TimeT())
                << ",\"ajax_request\":" << (AjaxRequest ? "true" : "false") << "}";
    TString digest = HmacSHA256(secret, stateStruct);
    TString cookieStruct {"{\"state_struct\":\"" + Base64Encode(stateStruct) + "\",\"digest\":\"" + Base64Encode(digest) + "\"}"};
    return Base64Encode(cookieStruct);
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

} // NOIDC
} // NMVP
