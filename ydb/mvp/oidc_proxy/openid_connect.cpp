#include "openid_connect.h"
#include "context.h"

#include <ydb/core/util/random.h>
#include <ydb/core/util/wildcard.h>
#include <ydb/library/security/util.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <util/string/builder.h>
#include <util/string/hex.h>

namespace NMVP::NOIDC {

TRestoreOidcContextResult::TRestoreOidcContextResult(const TStatus& status, const TContext& context)
    : Context(context)
    , Status(status)
{}


bool TRestoreOidcContextResult::IsSuccess() const {
    return Status.IsSuccess;
}

TCheckStateResult::TCheckStateResult(bool success, const TString& cookieSuffix, const TString& errorMessage)
    : Success(success)
    , ErrorMessage(errorMessage)
    , CookieSuffix(cookieSuffix)
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
    headers->Set("Access-Control-Expose-Headers", "traceresponse,X-Worker-Name,X-Request-Id");
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

NHttp::THttpOutgoingResponsePtr GetHttpOutgoingResponsePtr(const NHttp::THttpIncomingRequestPtr& request, const TOpenIdConnectSettings& settings, TStringBuf requestId) {
    TContext context(request);
    const TString redirectUri = TStringBuilder()
        << (request->Endpoint->Secure ? "https://" : "http://")
        << request->Host
        << GetAuthCallbackUrl();

    TCgiParameters authParams;
    authParams.InsertUnescaped("response_type", "code");
    authParams.InsertUnescaped("scope", "openid");
    authParams.InsertUnescaped("state", context.GetState(settings.ClientSecret));
    authParams.InsertUnescaped("client_id", settings.ClientId);
    authParams.InsertUnescaped("redirect_uri", redirectUri);

    const TString redirectUrl = settings.GetAuthEndpointURL() + "?" + authParams.Print();
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(request, &responseHeaders);
    SetRequestIdHeader(responseHeaders, requestId);
    responseHeaders.Set("Set-Cookie", context.CreateYdbOidcCookie(settings.ClientSecret));
    if (context.IsNavigationRequest()) {
        responseHeaders.Set(LOCATION_HEADER, redirectUrl);
        return request->CreateResponse("302", "Authorization required", responseHeaders);
    }
    responseHeaders.Set("Content-Type", "application/json; charset=utf-8");
    TString body {"{\"error\":\"Authorization Required\",\"authUrl\":\"" + redirectUrl + "\"}"};
    return request->CreateResponse("401", "Unauthorized", responseHeaders, body);
}

TString CreateNameYdbOidcCookie(TStringBuf suffix) {
    return TString(TOpenIdConnectSettings::YDB_OIDC_COOKIE) + TString(suffix);
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

TRestoreOidcContextResult RestoreOidcContext(const NHttp::TCookies& cookies, const TString& key, TStringBuf cookieSuffix) {
    TStringBuilder errorMessage;
    errorMessage << "Restore oidc context failed: ";
    TString cookieName = CreateNameYdbOidcCookie(cookieSuffix);
    if (!cookies.Has(cookieName)) {
        return TRestoreOidcContextResult({.IsSuccess = false,
                                         .IsErrorRetryable = false,
                                         .ErrorMessage = errorMessage << "Cannot find cookie " << cookieName});
    }
    TString signedRequestedAddress = Base64Decode(cookies.Get(cookieName));
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

TCheckStateResult TDecodeStateResult::Check(const TString& key) const {
    const TString errorPrefix = "Check state failed: ";
    if (StateContainer.empty()) {
        return TCheckStateResult(false, "", errorPrefix + "Container with state is empty");
    }
    if (ExpectedDigest.empty()) {
        return TCheckStateResult(false, "", errorPrefix + "Expected digest is empty");
    }
    if (!HasSignedStateJson) {
        return TCheckStateResult(false, "", errorPrefix + "Calculated digest is not equal expected digest");
    }

    TString digest = HmacSHA1(key, StateContainer);
    if (ExpectedDigest != digest) {
        return TCheckStateResult(false, "", errorPrefix + "Calculated digest is not equal expected digest");
    }
    if (!HasStateContainerJson) {
        return TCheckStateResult(false, "", errorPrefix + "State container is not valid json");
    }
    if (!Payload.ExpirationTime.Defined()) {
        return TCheckStateResult(false, Payload.CookieSuffix, errorPrefix + "Expiration time not found in json");
    }
    timeval timeVal {
        .tv_sec = Payload.ExpirationTime.GetRef(),
        .tv_usec = 0
    };
    if (TInstant::Now() > TInstant(timeVal)) {
        return TCheckStateResult(false, Payload.CookieSuffix, errorPrefix + "State life time expired");
    }
    return TCheckStateResult(true, Payload.CookieSuffix, "");
}

TString EncodeState(const TState& payload, TStringBuf signingKey) {
    TStringBuilder json;
    json << "{\"state\":\"" << payload.AntiForgeryToken << "\"";
    if (payload.ExpirationTime.Defined()) {
        json << ",\"expiration_time\":\"" << ToString(payload.ExpirationTime.GetRef()) << "\"";
    }
    if (!payload.CookieSuffix.empty()) {
        json << ",\"cookie_suffix\":\"" << payload.CookieSuffix << "\"";
    }
    json << "}";
    const TString stateContainer = json;

    TString digest = HmacSHA1(signingKey, stateContainer);
    TStringBuilder signedState;
    signedState << "{\"container\":\"" << Base64Encode(stateContainer)
                << "\",\"digest\":\"" << Base64Encode(digest) << "\"}";
    return Base64EncodeNoPadding(signedState);
}

TDecodeStateResult DecodeState(TStringBuf encodedState) {
    TDecodeStateResult result;
    TString signedState = Base64DecodeUneven(encodedState);
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    result.HasSignedStateJson = NJson::ReadJsonTree(signedState, &jsonConfig, &jsonValue);
    if (result.HasSignedStateJson) {
        const NJson::TJsonValue* jsonStateContainer = nullptr;
        if (jsonValue.GetValuePointer("container", &jsonStateContainer)) {
            result.StateContainer = jsonStateContainer->GetStringRobust();
            result.StateContainer = Base64Decode(result.StateContainer);
        }
        const NJson::TJsonValue* jsonDigest = nullptr;
        if (jsonValue.GetValuePointer("digest", &jsonDigest)) {
            result.ExpectedDigest = jsonDigest->GetStringRobust();
            result.ExpectedDigest = Base64Decode(result.ExpectedDigest);
        }
    } else {
        const TStringBuf signedStateBuf(signedState);
        auto extractJsonStringField = [signedStateBuf](TStringBuf fieldName) -> TString {
            const TString pattern = TStringBuilder() << "\"" << fieldName << "\":\"";
            const size_t valueBegin = signedStateBuf.find(pattern);
            if (valueBegin == TString::npos) {
                return {};
            }
            const size_t stringBegin = valueBegin + pattern.size();
            const size_t stringEnd = signedStateBuf.find('"', stringBegin);
            if (stringEnd == TString::npos) {
                return {};
            }
            return TString(signedStateBuf.SubString(stringBegin, stringEnd - stringBegin));
        };

        result.StateContainer = Base64Decode(extractJsonStringField("container"));
        result.ExpectedDigest = Base64Decode(extractJsonStringField("digest"));
    }

    if (!result.StateContainer.empty()) {
        result.HasStateContainerJson = NJson::ReadJsonTree(result.StateContainer, &jsonConfig, &jsonValue);
        if (result.HasStateContainerJson) {
            const NJson::TJsonValue* jsonState = nullptr;
            if (jsonValue.GetValuePointer("state", &jsonState)) {
                result.Payload.AntiForgeryToken = jsonState->GetStringRobust();
            }
            const NJson::TJsonValue* jsonCookieSuffix = nullptr;
            if (jsonValue.GetValuePointer("cookie_suffix", &jsonCookieSuffix)) {
                result.Payload.CookieSuffix = jsonCookieSuffix->GetStringRobust();
            }
            const NJson::TJsonValue* jsonExpirationTime = nullptr;
            if (jsonValue.GetValuePointer("expiration_time", &jsonExpirationTime)) {
                result.Payload.ExpirationTime = jsonExpirationTime->GetIntegerRobust();
            }
        }
    }

    return result;
}

TCheckStateResult CheckState(const TString& state, const TString& key) {
    return DecodeState(state).Check(key);
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
    if (!cookies.Has(cookieName)) {
        return {};
    }
    TStringBuf cookieValue = cookies.Get(cookieName);
    if (!cookieValue.Empty()) {
        BLOG_D("Using cookie (" << cookieName << ": " << NKikimr::MaskTicket(cookieValue) << ")");
    }
    return cookieValue;
}

TString GetAddressWithoutPort(const TString& address) {
    // IPv6 with brackets: [addr]:port -> addr
    if (address.StartsWith('[')) {
        auto end = address.find(']');
        if (end != TString::npos) {
            return address.substr(1, end - 1);
        }
    }

    // IPv6 without brackets - leave unchanged just in case
    if (std::count(address.begin(), address.end(), ':') > 1) {
        return address;
    }

    // IPv4 with port: addr:port → addr
    auto pos = address.rfind(':');
    if (pos != TString::npos) {
        return address.substr(0, pos);
    }

    return address;
}

TString GenerateRandomBase64(size_t byteNumber) {
    TString bytes = TString::Uninitialized(byteNumber);
    NKikimr::SafeEntropyPoolRead(bytes.Detach(), bytes.size());
    return Base64EncodeUrlNoPadding(bytes);
}

// Append request address to X-Forwarded-For header
// Useful for logging and audit
TString MakeXForwardedFor(const TProxiedRequestParams& params) {
    NHttp::THeaders headers(params.Request->Headers);

    TStringBuilder forwarded;
    forwarded << headers.Get(X_FORWARDED_FOR_HEADER);
    if (params.Request->Address) {
        auto address = GetAddressWithoutPort(params.Request->Address->ToString());
        if (!address.empty()) {
            if (!forwarded.empty()) {
                forwarded << ", ";
            }
            forwarded << address;
        }
    }
    return std::move(forwarded);
}

NHttp::THttpOutgoingRequestPtr CreateProxiedRequest(const TProxiedRequestParams& params) {
    auto outRequest = NHttp::THttpOutgoingRequest::CreateRequest(params.Request->Method, params.ProtectedPage.Url);
    NHttp::THeaders headers(params.Request->Headers);
    for (const auto& header : params.Settings.REQUEST_HEADERS_WHITE_LIST) {
        if (headers.Has(header)) {
            outRequest->Set(header, headers.Get(header));
        }
    }
    outRequest->Set("Accept-Encoding", "deflate");

    if (!params.AuthHeader.empty()) {
        outRequest->Set(AUTHORIZATION_HEADER, params.AuthHeader);
    }

    outRequest->Set(X_FORWARDED_FOR_HEADER, MakeXForwardedFor(params));

    if (params.Request->HaveBody()) {
        outRequest->SetBody(params.Request->Body);
    }
    if (params.ProtectedPage.Scheme.empty()) {
        outRequest->Secure = params.Secure;
    }

    return outRequest;
}

NHttp::THttpOutgoingResponsePtr CreateResponseForbiddenHost(const NHttp::THttpIncomingRequestPtr request, const TCrackedPage& protectedPage, TStringBuf requestId) {
    NHttp::THeadersBuilder headers;
    headers.Set("Content-Type", "text/html");
    SetCORS(request, &headers);
    SetRequestIdHeader(headers, requestId);

    TStringBuilder html;
    html << "<html><head><title>403 Forbidden</title></head><body bgcolor=\"white\"><center><h1>";
    html << "403 Forbidden host: " << protectedPage.Host;
    html << "</h1></center></body></html>";

    return request->CreateResponse("403", "Forbidden", headers, html);
}

NHttp::THttpOutgoingResponsePtr CreateResponseForNotExistingResponseFromProtectedResource(const NHttp::THttpIncomingRequestPtr request, const TString& errorMessage, TStringBuf requestId) {
    NHttp::THeadersBuilder headers;
    headers.Set("Content-Type", "text/html");
    SetCORS(request, &headers);
    SetRequestIdHeader(headers, requestId);

    TStringBuilder html;
    html << "<html><head><title>400 Bad Request</title></head><body bgcolor=\"white\"><center><h1>";
    html << "400 Bad Request. Can not process request to protected resource: " << errorMessage;
    html << "</h1></center></body></html>";
    return request->CreateResponse("400", "Bad Request", headers, html);
}

} // NMVP::NOIDC
