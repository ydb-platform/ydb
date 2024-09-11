#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/hex.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include "context.h"
#include "openid_connect.h"
#include "oidc_settings.h"

namespace NMVP {
namespace NOIDC {

namespace {

NHttp::THttpOutgoingResponsePtr CreateResponseForAjaxRequest(const NHttp::THttpIncomingRequestPtr& request, NHttp::THeadersBuilder& headers, const TString& redirectUrl) {
    headers.Set("Content-Type", "application/json; charset=utf-8");
    SetCORS(request, &headers);
    TString body {"{\"error\":\"Authorization Required\",\"authUrl\":\"" + redirectUrl + "\"}"};
    return request->CreateResponse("401", "Unauthorized", headers, body);
}

} // namespace

TRestoreOidcContextResult::TRestoreOidcContextResult(const TStatus& status, const TContext& context)
    : Context(context)
    , Status(status)
{}


bool TRestoreOidcContextResult::IsSuccess() const {
    return Status.IsSuccess;
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

TString HmacSHA256(TStringBuf key, TStringBuf data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    ui32 hl = SHA256_DIGEST_LENGTH;
    const auto* res = HMAC(EVP_sha256(), key.data(), key.size(), reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash, &hl);
    Y_ENSURE(res);
    Y_ENSURE(hl == SHA256_DIGEST_LENGTH);
    return TString{reinterpret_cast<const char*>(res), hl};
}

TString HmacSHA1(TStringBuf key, TStringBuf data) {
    unsigned char hash[SHA_DIGEST_LENGTH];
    ui32 hl = SHA_DIGEST_LENGTH;
    const auto* res = HMAC(EVP_sha1(), key.data(), key.size(), reinterpret_cast<const unsigned char*>(data.data()), data.size(), hash, &hl);
    Y_ENSURE(res);
    Y_ENSURE(hl == SHA_DIGEST_LENGTH);
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

NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings, TContextStorage* const contextStorage) {
    TContext context(request);
    TString state;
    NHttp::THeadersBuilder responseHeaders;
    state = context.CreateStateContainer(settings.ClientSecret, TString(request->Host));
    // Cerr << "+++New state: " << state << Endl;
    // Cerr << "+++decoded sate: " << Base64DecodeUneven(state) << Endl;
    // Cerr << "+++decoded sate: " << HexDecode(state) << Endl;
    if (settings.StoreContextOnHost) {
        state = context.CreateStateContainer(settings.ClientSecret, TString(request->Endpoint->WorkerName));
        contextStorage->Write(context);
    } else {
        state = context.GetState();
        responseHeaders.Set("Set-Cookie", context.CreateYdbOidcCookie(settings.ClientSecret));
    }
    const TString redirectUrl = TStringBuilder() << settings.GetAuthEndpointURL()
                                                 << "?response_type=code"
                                                 << "&scope=openid"
                                                 << "&state=" << state
                                                 << "&client_id=" << settings.ClientId
                                                 << "&redirect_uri=" << (request->Endpoint->Secure ? "https://" : "http://")
                                                                     << request->Host
                                                                     << GetAuthCallbackUrl();
    if (context.GetIsAjaxRequest()) {
        return CreateResponseForAjaxRequest(request, responseHeaders, redirectUrl);
    }
    responseHeaders.Set("Location", redirectUrl);
    return request->CreateResponse("302", "Authorization required", responseHeaders);
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
            << "; Path=/; Secure; HttpOnly; SameSite=None; Partitioned";
    return cookieBuilder;
}

TRestoreOidcContextResult RestoreSessionStoredOnClientSide(const TString& state, const NHttp::TCookies& cookies, const TString& secret) {
    TStringBuilder errorMessage;
    errorMessage << "Restore oidc session failed: ";
    const TString cookieName {CreateNameYdbOidcCookie(secret, state)};
    if (!cookies.Has(cookieName)) {
        return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = false,
                                         .ErrorMessage = errorMessage << "Cannot find cookie " << cookieName});
    }
    TString cookieStruct = Base64Decode(cookies.Get(cookieName));
    TString stateStruct;
    TString expectedDigest;
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (NJson::ReadJsonTree(cookieStruct, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonStateStruct = nullptr;
        if (jsonValue.GetValuePointer("state_struct", &jsonStateStruct)) {
            stateStruct = jsonStateStruct->GetStringRobust();
            stateStruct = Base64Decode(stateStruct);
        }
        const NJson::TJsonValue* jsonDigest = nullptr;
        if (jsonValue.GetValuePointer("digest", &jsonDigest)) {
            expectedDigest = jsonDigest->GetStringRobust();
            expectedDigest = Base64Decode(expectedDigest);
        }
    }
    if (stateStruct.Empty() || expectedDigest.Empty()) {
        return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = false,
                                         .ErrorMessage = errorMessage << "Struct with state and expected digest are empty"});
    }
    TString digest = HmacSHA256(secret, stateStruct);
    if (expectedDigest != digest) {
        return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = false,
                                         .ErrorMessage = errorMessage << "Calculated digest is not equal expected digest"});
    }
    TString expectedState;
    TString redirectUrl;
    bool isAjaxRequest = false;
    if (NJson::ReadJsonTree(stateStruct, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonState = nullptr;
        if (jsonValue.GetValuePointer("state", &jsonState)) {
            expectedState = jsonState->GetStringRobust();
        }
        const NJson::TJsonValue* jsonRedirectUrl = nullptr;
        if (jsonValue.GetValuePointer("requested_address", &jsonRedirectUrl)) {
            redirectUrl = jsonRedirectUrl->GetStringRobust();
        } else {
            return TRestoreOidcContextResult({.IsSuccess = false,
                                             .IsErrorRetryable = false,
                                             .ErrorMessage = errorMessage << "Requested address not found in cookie"});
        }
        const NJson::TJsonValue* jsonExpirationTime = nullptr;
        if (jsonValue.GetValuePointer("expiration_time", &jsonExpirationTime)) {
            timeval timeVal {
                .tv_sec = jsonExpirationTime->GetIntegerRobust(),
                .tv_usec = 0
            };
            if (TInstant::Now() > TInstant(timeVal)) {
                return TRestoreOidcContextResult({.IsSuccess = false,
                                                 .IsErrorRetryable = true,
                                                 .ErrorMessage = errorMessage << "State life time expired"}, TContext(state, redirectUrl));
            }
        } else {
            return TRestoreOidcContextResult({.IsSuccess = false,
                                             .IsErrorRetryable = true,
                                             .ErrorMessage = errorMessage << "Expiration time not found in json"}, TContext(state, redirectUrl));
        }
        const NJson::TJsonValue* jsonAjaxRequest = nullptr;
        if (jsonValue.GetValuePointer("ajax_request", &jsonAjaxRequest)) {
            isAjaxRequest = jsonAjaxRequest->GetBooleanRobust();
        } else {
            return TRestoreOidcContextResult({.IsSuccess = false,
                                             .IsErrorRetryable = true,
                                             .ErrorMessage = errorMessage << "Can not detect ajax request"}, TContext(state, redirectUrl));
        }
    }
    if (expectedState.Empty() || expectedState != state) {
        return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = true,
                                         .ErrorMessage = errorMessage << "Unknown state"}, TContext(state, redirectUrl, isAjaxRequest));
    }
    return TRestoreOidcContextResult({.IsSuccess = true,
                                     .IsErrorRetryable = true,
                                     .ErrorMessage = ""}, TContext(state, redirectUrl, isAjaxRequest));
}

}  // NOIDC
}  // NMVP
