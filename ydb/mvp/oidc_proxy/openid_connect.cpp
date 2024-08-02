#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/hex.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include "openid_connect.h"

namespace {

TString GenerateState() {
    TStringBuilder sb;
    const size_t CHAR_NUMBER = 15;
    for (size_t i{0}; i < CHAR_NUMBER; i++) {
        sb << RandomNumber<char>();
    }
    return Base64EncodeUrlNoPadding(sb);
}

struct TRedirectUrlParameters {
    TStringBuf SessionServerCheckDetails;
    TOpenIdConnectSettings OidcSettings;
    TStringBuf CallbackUrl;
    TStringBuf State;
    TStringBuf Scheme;
    TStringBuf Host;
    NMvp::EAccessServiceType AccessServiceType;
    TStringBuf AuthEndpoint;
};

bool TryAppendAuthEndpointFromDetailsYandexProfile(const TRedirectUrlParameters& parameters, TStringBuilder& locationHeaderValue) {
    if (parameters.AccessServiceType != NMvp::yandex_v2) {
        return false;
    }
    const auto& eventDetails = parameters.SessionServerCheckDetails;
    size_t posAuthUrl = eventDetails.find(parameters.AuthEndpoint);
    if (posAuthUrl != TStringBuf::npos) {
        size_t pos = eventDetails.rfind("https://", posAuthUrl);
        locationHeaderValue << eventDetails.substr(pos, posAuthUrl - pos) << parameters.AuthEndpoint;
        return true;
    }
    return false;
}

TString CreateRedirectUrl(const TRedirectUrlParameters& parameters) {
    TStringBuilder locationHeaderValue;
    if (!TryAppendAuthEndpointFromDetailsYandexProfile(parameters, locationHeaderValue)) {
        locationHeaderValue << parameters.OidcSettings.GetAuthEndpointURL();
    }
    locationHeaderValue << "?response_type=code"
                        << "&scope=openid"
                        << "&state=" << parameters.State
                        << "&client_id=" << parameters.OidcSettings.ClientId
                        << "&redirect_uri=" << parameters.Scheme << parameters.Host << parameters.CallbackUrl;
    return locationHeaderValue;
}

void SetCORS(const NHttp::THttpIncomingRequestPtr& request, NHttp::THeadersBuilder* const headers) {
    TString origin = TString(NHttp::THeaders(request->Headers)["Origin"]);
    if (origin.empty()) {
        origin = "*";
    }
    headers->Set("Access-Control-Allow-Origin", origin);
    headers->Set("Access-Control-Allow-Credentials", "true");
    headers->Set("Access-Control-Allow-Headers", "Content-Type,Authorization,Origin,Accept");
    headers->Set("Access-Control-Allow-Methods", "OPTIONS, GET, POST");
}

NHttp::THttpOutgoingResponsePtr CreateResponseForAjaxRequest(const NHttp::THttpIncomingRequestPtr& request, NHttp::THeadersBuilder& headers, const TString& redirectUrl) {
    headers.Set("Content-Type", "application/json; charset=utf-8");
    SetCORS(request, &headers);
    TString body {"{\"error\":\"Authorization Required\",\"authUrl\":\"" + redirectUrl + "\"}"};
    return request->CreateResponse("401", "Unauthorized", headers, body);
}

TStringBuf GetRequestedUrl(const NHttp::THttpIncomingRequestPtr& request, bool isAjaxRequest) {
    NHttp::THeaders headers(request->Headers);
    TStringBuf requestedUrl = headers.Get("Referer");
    if (!isAjaxRequest || requestedUrl.empty()) {
        return request->URL;
    }
    return requestedUrl;
}

} // namespace

TString HmacSHA256(TStringBuf key, TStringBuf data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    ui32 hl = SHA256_DIGEST_LENGTH;
    const auto* res = HMAC(EVP_sha256(), key.data(), key.size(), reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash, &hl);
    Y_ENSURE(res);
    Y_ENSURE(hl == SHA256_DIGEST_LENGTH);
    return TString{reinterpret_cast<const char*>(res), hl};
}

void SetHeader(NYdbGrpc::TCallMeta& meta, const TString& name, const TString& value) {
    for (auto& [exname, exvalue] : meta.Aux) {
        if (exname == name) {
            exvalue = value;
            return;
        }
    }
    meta.Aux.emplace_back(name, value);
}

TString GenerateCookie(TStringBuf state, TStringBuf redirectUrl, const TString& secret, bool isAjaxRequest) {
    const TDuration StateLifeTime = TDuration::Minutes(10);
    TInstant expirationTime = TInstant::Now() + StateLifeTime;
    TStringBuilder stateStruct;
    stateStruct << "{\"state\":\"" << state
                << "\",\"redirect_url\":\"" << redirectUrl
                << "\",\"expiration_time\":" << ToString(expirationTime.TimeT())
                << ",\"ajax_request\":" << (isAjaxRequest ? "true" : "false") << "}";
    TString digest = HmacSHA256(secret, stateStruct);
    TString cookieStruct {"{\"state_struct\":\"" + Base64Encode(stateStruct) + "\",\"digest\":\"" + Base64Encode(digest) + "\"}"};
    return Base64Encode(cookieStruct);
}

NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(TStringBuf eventDetails, const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings, NHttp::THeadersBuilder& responseHeaders, bool isAjaxRequest) {
    TString state = GenerateState();
    const TString redirectUrl = CreateRedirectUrl({.SessionServerCheckDetails = eventDetails,
                                                    .OidcSettings = settings,
                                                    .CallbackUrl = GetAuthCallbackUrl(),
                                                    .State = state,
                                                    .Scheme = (request->Endpoint->Secure ? "https://" : "http://"),
                                                    .Host = request->Host,
                                                    .AccessServiceType = settings.AccessServiceType,
                                                    .AuthEndpoint = settings.AuthEndpoint});
    const size_t cookieMaxAgeSec = 420;
    TStringBuilder setCookieBuilder;
    setCookieBuilder << CreateNameYdbOidcCookie(settings.ClientSecret, state) << "=" << GenerateCookie(state, GetRequestedUrl(request, isAjaxRequest), settings.ClientSecret, isAjaxRequest)
                     << "; Path=" << GetAuthCallbackUrl() << "; Max-Age=" << cookieMaxAgeSec <<"; SameSite=None; Secure";
    responseHeaders.Set("Set-Cookie", setCookieBuilder);
    if (isAjaxRequest) {
        return CreateResponseForAjaxRequest(request, responseHeaders, redirectUrl);
    }
    responseHeaders.Set("Location", redirectUrl);
    return request->CreateResponse("302", "Authorization required", responseHeaders);
}

NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(TStringBuf eventDetails, const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings, bool isAjaxRequest) {
    NHttp::THeadersBuilder responseHeaders;
    return GetHttpOutgoingResponsePtr(eventDetails, request, settings, responseHeaders, isAjaxRequest);
}

bool DetectAjaxRequest(const NHttp::THeaders& headers) {
    const static THashMap<TStringBuf, TStringBuf> expectedHeaders {
        {"Accept", "application/json"}
    };
    for (const auto& el : expectedHeaders) {
        TStringBuf headerValue = headers.Get(el.first);
        if (!headerValue || headerValue.find(el.second) == TStringBuf::npos) {
            return false;
        }
    }
    return true;
}

TString CreateNameYdbOidcCookie(TStringBuf key, TStringBuf state) {
    return TOpenIdConnectSettings::YDB_OIDC_COOKIE + "_" + HexEncode(HmacSHA256(key, state));
}

TString CreateNameSessionCookie(TStringBuf key) {
    return "__Host_" + TOpenIdConnectSettings::SESSION_COOKIE + "_" + HexEncode(key);
}

const TString& GetAuthCallbackUrl() {
    static const TString callbackUrl = "/auth/callback";
    return callbackUrl;
}

TString CreateSecureCookie(const TString& key, const TString& value) {
    TStringBuilder cookieBuilder;
    cookieBuilder << CreateNameSessionCookie(key) << "=" << Base64Encode(value)
            << "; Path=/; Secure; HttpOnly; SameSite=Lax";
    return cookieBuilder;
}
