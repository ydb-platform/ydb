#include "openid_connect.h"
#include "context.h"

#include <ydb/core/util/random.h>
#include <ydb/core/util/wildcard.h>
#include <ydb/library/security/util.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
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

TCheckStateResult::TCheckStateResult(bool ok, const TString& errorMessage, const TString& requestedAddress)
    : Ok(ok)
    , ErrorMessage(errorMessage)
    , RequestedAddress(requestedAddress)
{}

TCheckStateResult TCheckStateResult::Error(const TString& errorMessage, const TString& requestedAddress) {
    return TCheckStateResult(false, errorMessage, requestedAddress);
}

TCheckStateResult TCheckStateResult::Success(const TString& requestedAddress) {
    return TCheckStateResult(true, "", requestedAddress);
}

TString CreateAuthorizationServerRedirectUrl(const TOpenIdConnectSettings& settings, TStringBuf redirectUri, TStringBuf state) {
    TCgiParameters authParams;
    authParams.InsertUnescaped("response_type", "code");
    authParams.InsertUnescaped("scope", "openid");
    authParams.InsertUnescaped("state", state);
    authParams.InsertUnescaped("client_id", settings.ClientId);
    authParams.InsertUnescaped("redirect_uri", redirectUri);

    return TStringBuilder()
        << settings.GetAuthEndpointURL()
        << "?"
        << authParams.Print();
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

    TString state = context.GetState(settings.ClientSecret);
    TString redirectUrl = CreateAuthorizationServerRedirectUrl(settings, redirectUri, state);
    static constexpr size_t AUTH_CALLBACK_CODE_RESERVE_BYTES = 200;
    if (redirectUrl.size() + AUTH_CALLBACK_CODE_RESERVE_BYTES > NHttp::THttpRequestParser::MaxURLSize) {
        // Keep return_to in the temp cookie when the authorization request URL is too large
        // to leave room for the code returned by IAM in the callback.
        state = context.GetState(settings.ClientSecret, false);
        redirectUrl = CreateAuthorizationServerRedirectUrl(settings, redirectUri, state);
    }
    const TString authFlowCookie = context.CreateYdbOidcCookie(settings.ClientSecret);
    if (authFlowCookie.empty()) {
        return CreateResponseForNotExistingResponseFromProtectedResource(
            request,
            "requested address is too large to preserve during authentication",
            requestId
        );
    }

    NHttp::THeadersBuilder responseHeaders;
    SetCORS(request, &responseHeaders);
    SetRequestIdHeader(responseHeaders, requestId);
    responseHeaders.Set("Set-Cookie", authFlowCookie);
    if (context.IsNavigationRequest()) {
        responseHeaders.Set(LOCATION_HEADER, redirectUrl);
        return request->CreateResponse("302", "Authorization required", responseHeaders);
    }
    responseHeaders.Set("Content-Type", "application/json; charset=utf-8");
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["error"] = "Authorization Required";
    json["authUrl"] = redirectUrl;
    const TString body = NJson::WriteJson(json, false);
    return request->CreateResponse("401", "Unauthorized", responseHeaders, body);
}

TString CreateNameSessionCookie(TStringBuf key) {
    return "__Host-" + TOpenIdConnectSettings::SESSION_COOKIE + "_" + HexEncode(key);
}

TString CreateNameImpersonatedCookie(TStringBuf key) {
    return "__Host-" + TOpenIdConnectSettings::IMPERSONATED_COOKIE + "_" + HexEncode(key);
}

const TString& GetAuthCallbackUrl() {
    static const TString callbackUrl = "/auth/callback";
    return callbackUrl;
}

TString CreateSecureCookie(const TString& name, const TString& value, const ui32 expiredSeconds) {
    TStringBuilder cookieBuilder;
    cookieBuilder << name << "=" << value
            << "; Path=/; Secure; HttpOnly; SameSite=None"
            << "; Max-Age=" << expiredSeconds;
    return cookieBuilder;
}

TString ClearSecureCookie(const TString& name) {
    TStringBuilder cookieBuilder;
    cookieBuilder << name << "=; Path=/; Secure; HttpOnly; SameSite=None; Max-Age=0";
    return cookieBuilder;
}

TCheckStateResult TDecodeStateResult::Check(const TString& key) const {
    static constexpr TStringBuf ErrorPrefix = "Check state failed: ";
    if (!HasSignedStateJson) {
        return TCheckStateResult::Error(TString(ErrorPrefix) + "Signed state is not valid json");
    }
    if (StateContainer.empty()) {
        return TCheckStateResult::Error(TString(ErrorPrefix) + "Container with state is empty");
    }
    if (ExpectedDigest.empty()) {
        return TCheckStateResult::Error(TString(ErrorPrefix) + "Expected digest is empty");
    }

    TString digest = HmacSHA1(key, StateContainer);
    if (ExpectedDigest != digest) {
        return TCheckStateResult::Error(TString(ErrorPrefix) + "Calculated digest is not equal expected digest");
    }
    if (!HasStateContainerJson) {
        return TCheckStateResult::Error(TString(ErrorPrefix) + "State container is not valid json");
    }
    if (!Payload.ExpirationTime) {
        return TCheckStateResult::Error(TString(ErrorPrefix) + "Expiration time not found in json", Payload.RequestedAddress);
    }
    if (TInstant::Now() > *Payload.ExpirationTime) {
        return TCheckStateResult::Error(TString(ErrorPrefix) + "State life time expired", Payload.RequestedAddress);
    }
    return TCheckStateResult::Success(Payload.RequestedAddress);
}

TString EncodeState(const TState& payload, TStringBuf signingKey) {
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["state"] = payload.AntiForgeryToken;
    if (!payload.RequestedAddress.empty()) {
        json["requested_address"] = payload.RequestedAddress;
    }
    if (payload.ExpirationTime) {
        json["expiration_time"] = ToString(payload.ExpirationTime->TimeT());
    }
    const TString stateContainer = NJson::WriteJson(json, false);

    TString digest = HmacSHA1(signingKey, stateContainer);

    NJson::TJsonValue root(NJson::JSON_MAP);
    root["container"] = Base64Encode(stateContainer);
    root["digest"] = Base64Encode(digest);
    return Base64EncodeNoPadding(NJson::WriteJson(root, false));
}

TDecodeStateResult DecodeState(TStringBuf encodedState) {
    TDecodeStateResult result;
    TString signedState = Base64DecodeUneven(encodedState);
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    result.HasSignedStateJson = NJson::ReadJsonTree(signedState, &jsonConfig, &jsonValue);
    if (result.HasSignedStateJson) {
        const NJson::TJsonValue* jsonStateContainer = nullptr;
        if (jsonValue.GetValuePointer("container", &jsonStateContainer) && jsonStateContainer->IsString()) {
            result.StateContainer = jsonStateContainer->GetString();
            result.StateContainer = Base64Decode(result.StateContainer);
        }
        const NJson::TJsonValue* jsonDigest = nullptr;
        if (jsonValue.GetValuePointer("digest", &jsonDigest) && jsonDigest->IsString()) {
            result.ExpectedDigest = jsonDigest->GetString();
            result.ExpectedDigest = Base64Decode(result.ExpectedDigest);
        }
    }

    if (!result.StateContainer.empty()) {
        result.HasStateContainerJson = NJson::ReadJsonTree(result.StateContainer, &jsonConfig, &jsonValue);
        if (result.HasStateContainerJson) {
            const NJson::TJsonValue* jsonState = nullptr;
            if (jsonValue.GetValuePointer("state", &jsonState) && jsonState->IsString()) {
                result.Payload.AntiForgeryToken = jsonState->GetString();
            }
            const NJson::TJsonValue* jsonRequestedAddress = nullptr;
            if (jsonValue.GetValuePointer("requested_address", &jsonRequestedAddress) && jsonRequestedAddress->IsString()) {
                result.Payload.RequestedAddress = jsonRequestedAddress->GetString();
            }
            const NJson::TJsonValue* jsonExpirationTime = nullptr;
            if (jsonValue.GetValuePointer("expiration_time", &jsonExpirationTime)) {
                result.Payload.ExpirationTime = TInstant::Seconds(jsonExpirationTime->GetIntegerRobust());
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
