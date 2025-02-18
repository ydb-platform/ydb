#include "context.h"
#include "openid_connect.h"
#include "oidc_settings.h"
#include <ydb/library/security/util.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/hex.h>

namespace NMVP::NOIDC {

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

TCheckStateResult::TCheckStateResult(bool success, const TString& errorMessage)
    : Success(success)
    , ErrorMessage(errorMessage)
{}

bool TCheckStateResult::IsSuccess() const {
    return Success;
}

void SetCORS(const NHttp::THttpIncomingRequestPtr& request, NHttp::THeadersBuilder* const headers) {
    TString origin = TString(NHttp::THeaders(request->Headers)["Origin"]);
    if (origin.empty()) {
        origin = "*";
    }
    headers->Set("Access-Control-Allow-Origin", origin);
    headers->Set("Access-Control-Allow-Credentials", "true");
    headers->Set("Access-Control-Allow-Headers", "Content-Type,Authorization,Origin,Accept,X-Trace-Verbosity,X-Want-Trace,traceparent");
    headers->Set("Access-Control-Expose-Headers", "traceresponse,X-Worker-Name");
    headers->Set("Access-Control-Allow-Methods", "OPTIONS,GET,POST,PUT,DELETE");
    headers->Set("Allow", "OPTIONS,GET,POST,PUT,DELETE");
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

NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings) {
    TContext context(request);
    const TString redirectUrl = TStringBuilder() << settings.GetAuthEndpointURL()
                                                 << "?response_type=code"
                                                 << "&scope=openid"
                                                 << "&state=" << context.GetState(settings.ClientSecret)
                                                 << "&client_id=" << settings.ClientId
                                                 << "&redirect_uri=" << (request->Endpoint->Secure ? "https://" : "http://")
                                                                     << request->Host
                                                                     << GetAuthCallbackUrl();
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(request, &responseHeaders);
    responseHeaders.Set("Set-Cookie", context.CreateYdbOidcCookie(settings.ClientSecret));
    if (context.IsAjaxRequest()) {
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

TString CreateNameImpersonatedCookie(TStringBuf key) {
    return "__Host_" + TOpenIdConnectSettings::IMPERSONATED_COOKIE + "_" + HexEncode(key);
}

const TString& GetAuthCallbackUrl() {
    static const TString callbackUrl = "/auth/callback";
    return callbackUrl;
}

TString CreateSecureCookie(const TString& name, const TString& value, const ui32 expiredSeconds) {
    TStringBuilder cookieBuilder;
    cookieBuilder << name << "=" << value
            << "; Path=/; Secure; HttpOnly; SameSite=None; Partitioned"
            << "; Max-Age=" << expiredSeconds;
    return cookieBuilder;
}

TString ClearSecureCookie(const TString& name) {
    TStringBuilder cookieBuilder;
    cookieBuilder << name << "=; Path=/; Secure; HttpOnly; SameSite=None; Partitioned; Max-Age=0";
    return cookieBuilder;
}

TRestoreOidcContextResult RestoreOidcContext(const NHttp::TCookies& cookies, const TString& key) {
    TStringBuilder errorMessage;
    errorMessage << "Restore oidc context failed: ";
    if (!cookies.Has(TOpenIdConnectSettings::YDB_OIDC_COOKIE)) {
        return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = false,
                                         .ErrorMessage = errorMessage << "Cannot find cookie " << TOpenIdConnectSettings::YDB_OIDC_COOKIE});
    }
    TString signedRequestedAddress = Base64Decode(cookies.Get(TOpenIdConnectSettings::YDB_OIDC_COOKIE));
    TString requestedAddressContext;
    TString expectedDigest;
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (NJson::ReadJsonTree(signedRequestedAddress, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonRequestedAddressContext = nullptr;
        if (jsonValue.GetValuePointer("requested_address_context", &jsonRequestedAddressContext)) {
            requestedAddressContext = jsonRequestedAddressContext->GetStringRobust();
            requestedAddressContext = Base64Decode(requestedAddressContext);
        }
        if (requestedAddressContext.empty()) {
            return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = false,
                                         .ErrorMessage = errorMessage << "Struct with state is empty"});
        }
        const NJson::TJsonValue* jsonDigest = nullptr;
        if (jsonValue.GetValuePointer("digest", &jsonDigest)) {
            expectedDigest = jsonDigest->GetStringRobust();
            expectedDigest = Base64Decode(expectedDigest);
        }
        if (expectedDigest.empty()) {
            return TRestoreOidcContextResult({.IsSuccess = false,
                                            .IsErrorRetryable = false,
                                            .ErrorMessage = errorMessage << "Expected digest is empty"});
        }
    }
    TString digest = HmacSHA256(key, requestedAddressContext);
    if (expectedDigest != digest) {
        return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = false,
                                         .ErrorMessage = errorMessage << "Calculated digest is not equal expected digest"});
    }
    TString requestedAddress;
    if (NJson::ReadJsonTree(requestedAddressContext, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonRequestedAddress = nullptr;
        if (jsonValue.GetValuePointer("requested_address", &jsonRequestedAddress)) {
            requestedAddress = jsonRequestedAddress->GetStringRobust();
        } else {
            return TRestoreOidcContextResult({.IsSuccess = false,
                                             .IsErrorRetryable = false,
                                             .ErrorMessage = errorMessage << "Requested address was not found in the cookie"});
        }
    }
    return TRestoreOidcContextResult({.IsSuccess = true,
                                     .IsErrorRetryable = true,
                                     .ErrorMessage = ""}, TContext({.RequestedAddress = requestedAddress}));
}

TCheckStateResult CheckState(const TString& state, const TString& key) {
    TStringBuilder errorMessage;
    errorMessage << "Check state failed: ";
    TString signedState = Base64DecodeUneven(state);
    TString stateContainer;
    TString expectedDigest;
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (NJson::ReadJsonTree(signedState, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonStateContainer = nullptr;
        if (jsonValue.GetValuePointer("container", &jsonStateContainer)) {
            stateContainer = jsonStateContainer->GetStringRobust();
            stateContainer = Base64Decode(stateContainer);
        }
        if (stateContainer.empty()) {
            return TCheckStateResult(false, errorMessage << "Container with state is empty");
        }
        const NJson::TJsonValue* jsonDigest = nullptr;
        if (jsonValue.GetValuePointer("digest", &jsonDigest)) {
            expectedDigest = jsonDigest->GetStringRobust();
            expectedDigest = Base64Decode(expectedDigest);
        }
        if (expectedDigest.empty()) {
            return TCheckStateResult(false, errorMessage << "Expected digest is empty");
        }
    }
    TString digest = HmacSHA1(key, stateContainer);
    if (expectedDigest != digest) {
        return TCheckStateResult(false, errorMessage << "Calculated digest is not equal expected digest");
    }
    if (NJson::ReadJsonTree(stateContainer, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonExpirationTime = nullptr;
        if (jsonValue.GetValuePointer("expiration_time", &jsonExpirationTime)) {
            timeval timeVal {
                .tv_sec = jsonExpirationTime->GetIntegerRobust(),
                .tv_usec = 0
            };
            if (TInstant::Now() > TInstant(timeVal)) {
                return TCheckStateResult(false, errorMessage << "State life time expired");
            }
        } else {
            return TCheckStateResult(false, errorMessage << "Expiration time not found in json");
        }
    }
    return TCheckStateResult();
}

TString DecodeToken(const TStringBuf& cookie) {
    TString token;
    try {
        Base64StrictDecode(cookie, token);
    } catch (std::exception& e) {
        BLOG_D("Base64Decode " << NKikimr::MaskTicket(cookie) << " cookie: " << e.what());
        token.clear();
    }
    return token;
}

TStringBuf GetCookie(const NHttp::TCookies& cookies, const TString& cookieName) {
    TStringBuf cookieValue = cookies.Get(cookieName);
    if (!cookieValue.Empty()) {
        BLOG_D("Using cookie (" << cookieName << ": " << NKikimr::MaskTicket(cookieValue) << ")");
    }
    return cookieValue;
}

} // NMVP::NOIDC
