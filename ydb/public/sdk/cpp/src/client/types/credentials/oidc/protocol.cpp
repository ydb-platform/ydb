#include "protocol.h"

#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

constexpr size_t MaxResponseSize = 1024 * 1024;
const TDuration SocketTimeout = TDuration::Seconds(5);
const TDuration ConnectTimeout = TDuration::Seconds(30);

class TResponseBuffer: public IOutputStream {
public:
    TString Body;

private:
    void DoWrite(const void* buffer, size_t size) override;
};

std::string String(const NJson::TJsonValue& json, const TString& name, bool required);
std::string ScopeString(const TOidcConfig& config);
bool IsBearer(const std::string& value);
void CheckToken(const std::string& token);

void TResponseBuffer::DoWrite(const void* buffer, size_t size) {
    if (size > MaxResponseSize - Body.size()) {
        throw TError("response exceeds size limit", false, {});
    }
    Body.append(static_cast<const char*>(buffer), size);
}

std::string String(const NJson::TJsonValue& json, const TString& name, bool required) {
    const auto* field = Field(json, name);
    if (field == nullptr && !required) {
        return {};
    }
    if (field == nullptr || !field->IsString() || field->GetString().empty()) {
        throw TError("missing or invalid " + std::string(name), false, {});
    }
    return std::string(field->GetString());
}

std::string ScopeString(const TOidcConfig& config) {
    auto scopes = Scopes(config);
    if (std::find(scopes.begin(), scopes.end(), "openid") == scopes.end()) {
        scopes.push_back("openid");
    }
    std::string result;
    for (const auto& scope : scopes) {
        if (!result.empty()) {
            result += ' ';
        }
        result += scope;
    }
    return result;
}

bool IsBearer(const std::string& value) {
    return to_lower(TString(value)) == "bearer";
}

void CheckToken(const std::string& token) {
    if (std::any_of(token.begin(), token.end(), [](unsigned char c) { return c <= 0x20 || c >= 0x7f; })) {
        throw TError("invalid token characters", false, {});
    }
}

} // namespace

TProtocol::TProtocol(const TOidcConfig& config, NThreading::TCancellationToken cancellation)
    : Config(config)
    , Cancellation(std::move(cancellation))
{
}

TProtocol::~TProtocol() = default;

NJson::TJsonValue TProtocol::Request(const std::string& endpoint, const TCgiParameters* form, bool authenticate, TInstant deadline) {
    Cancellation.ThrowIfCancellationRequested();
    const auto url = ParseUrl(endpoint, false);
    TKeepAliveHttpClient::THeaders headers;
    TCgiParameters body = (form != nullptr) ? *form : TCgiParameters{};
    if (authenticate) {
        TString secret(ClientSecret(Config));
        if (!secret.empty()) {
            TString clientId(ClientId(Config));
            Quote(clientId, "");
            Quote(secret, "");
            headers["Authorization"] = "Basic " + Base64Encode(clientId + ":" + secret);
        } else {
            body.InsertUnescaped("client_id", ClientId(Config));
        }
    }
    headers["Accept"] = "application/json";
    if (form != nullptr) {
        headers["Content-Type"] = "application/x-www-form-urlencoded";
    }
    const auto now = TInstant::Now();
    if (deadline <= now) {
        throw TError("HTTP request deadline exceeded", true, {});
    }
    const auto remaining = deadline - now;
    const auto host = url.PrintS(NUri::TUri::FlagScheme | NUri::TUri::FlagHost | NUri::TUri::FlagHostAscii);
    const auto path = url.PrintS(NUri::TUri::FlagPath | NUri::TUri::FlagQuery);
    const auto encodedBody = body.Print();
    TStringBuilder request;
    request << (form != nullptr ? "POST " : "GET ") << path << " HTTP/1.1\r\n"
            << "Host: " << url.PrintS(NUri::TUri::FlagHost | NUri::TUri::FlagHostAscii | NUri::TUri::FlagPort) << "\r\n"
            << "Content-Length: " << encodedBody.size() << "\r\n";
    for (const auto& [name, value] : headers) {
        request << name << ": " << value << "\r\n";
    }
    request << "\r\n" << encodedBody;
    TResponseBuffer response;
    unsigned status;
    try {
        // Shutdown waits for this synchronous request. Keep HTTP cancellation
        // subscriptions local to the request instead of retaining them until shutdown.
        NThreading::TCancellationTokenSource requestCancellation;
        TKeepAliveHttpClient client(host, url.GetPort(),
            std::min(SocketTimeout, remaining), std::min(ConnectTimeout, remaining), false, false, true);
        status = client.DoRequestRaw(request, &response, nullptr, requestCancellation.Token());
        Cancellation.ThrowIfCancellationRequested();
    } catch (const TError&) {
        throw;
    } catch (const std::exception&) {
        Cancellation.ThrowIfCancellationRequested();
        throw TError("HTTP transport failed", true, {});
    }
    if (TInstant::Now() >= deadline) {
        throw TError("HTTP request deadline exceeded", true, {});
    }
    NJson::TJsonValue json;
    NJson::TJsonReaderConfig reader;
    reader.MaxDepth = 32;
    const bool valid = NJson::ReadJsonTree(response.Body, &reader, &json) && json.IsMap();
    if (status != 200) {
        std::string code;
        if (valid) {
            if (const auto* error = Field(json, "error"); error != nullptr && error->IsString()) {
                for (const char* known : {"invalid_grant", "invalid_client", "invalid_scope", "unauthorized_client",
                                          "unsupported_grant_type", "authorization_pending", "slow_down", "access_denied", "expired_token"}) {
                    if (error->GetString() == known) {
                        code = known;
                        break;
                    }
                }
            }
        }
        throw TError("HTTP " + std::to_string(status) + (code.empty() ? "" : " (" + code + ")"),
                     status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504, code);
    }
    if (!valid) {
        throw TError("invalid JSON response", false, {});
    }
    return json;
}

void TProtocol::Discover() {
    if (!TokenEndpoint.empty()) {
        return;
    }
    auto discovery = Config.Issuer;
    while (!discovery.empty() && discovery.back() == '/') {
        discovery.pop_back();
    }
    const auto metadata = Request(discovery + "/.well-known/openid-configuration", nullptr, false, TInstant::Max());
    // Only the discovery request path is normalized; the issuer identifier
    // must match exactly (OpenID Connect Discovery 1.0, sections 4.1 and 4.3).
    const auto advertisedIssuer = String(metadata, "issuer", true);
    if (advertisedIssuer != Config.Issuer) {
        try {
            // Reject userinfo, queries and control characters before including
            // an untrusted metadata value in diagnostics.
            ParseUrl(advertisedIssuer, true);
        } catch (const std::invalid_argument&) {
            throw TError("discovery issuer mismatch: invalid advertised issuer URL", false, {});
        }
        throw TError("discovery issuer mismatch: configured '" + Config.Issuer +
            "', advertised '" + advertisedIssuer + "'", false, {});
    }
    auto tokenEndpoint = String(metadata, "token_endpoint", true);
    auto deviceEndpoint = String(metadata, "device_authorization_endpoint", false);
    ParseUrl(tokenEndpoint, false);
    if (!deviceEndpoint.empty()) {
        ParseUrl(deviceEndpoint, false);
    }
    if (!ClientSecret(Config).empty()) {
        if (const auto* methods = Field(metadata, "token_endpoint_auth_methods_supported"); methods != nullptr) {
            if (!methods->IsArray()) {
                throw TError("invalid token_endpoint_auth_methods_supported", false, {});
            }
            bool supported = false;
            for (const auto& method : methods->GetArray()) {
                if (!method.IsString()) {
                    throw TError("invalid token_endpoint_auth_methods_supported", false, {});
                }
                supported |= (method.GetString() == "client_secret_basic");
            }
            if (!supported) {
                throw TError("client_secret_basic is not supported", false, {});
            }
        }
    }
    TokenEndpoint = std::move(tokenEndpoint);
    DeviceEndpoint = std::move(deviceEndpoint);
}

TTokenCache TProtocol::TokenRequest(TCgiParameters form, const std::optional<TOAuthToken>& refresh, TInstant deadline) {
    Discover();
    const auto now = TInstant::Now();
    const auto response = Request(TokenEndpoint, &form, true, deadline);
    // Include JSON response processing in the device authorization deadline.
    if (TInstant::Now() >= deadline) {
        throw TError("device authorization expired", false, "expired_token");
    }
    if (!IsBearer(String(response, "token_type", true))) {
        throw TError("unsupported token_type", false, {});
    }
    TTokenCache result;
    result.AccessToken.Token = String(response, "access_token", true);
    CheckToken(result.AccessToken.Token);
    if (const auto* expires = Field(response, "expires_in"); expires != nullptr) {
        result.AccessToken.ExpiresAt = now + TDuration::Seconds(Seconds(*expires, "expires_in", false));
    } else {
        result.AccessToken.ExpiresAt = JwtExpiry(result.AccessToken.Token);
    }
    if (!result.AccessToken.IsValid(TInstant::Now())) {
        throw TError("access token expired", false, {});
    }
    const auto refreshToken = String(response, "refresh_token", false);
    if (!refreshToken.empty()) {
        CheckToken(refreshToken);
        result.RefreshToken = TOAuthToken{refreshToken, std::nullopt};
    } else {
        result.RefreshToken = refresh;
    }
    if (result.RefreshToken.has_value()) {
        if (const auto* expires = Field(response, "refresh_expires_in"); expires != nullptr) {
            const auto seconds = Seconds(*expires, "refresh_expires_in", true);
            result.RefreshToken->ExpiresAt = seconds
                                                 ? std::optional<TInstant>(now + TDuration::Seconds(seconds))
                                                 : std::nullopt;
        }
    }
    return result;
}

TTokenCache TProtocol::Refresh(const TOAuthToken& refresh) {
    TCgiParameters form;
    form.InsertUnescaped("grant_type", "refresh_token");
    form.InsertUnescaped("refresh_token", refresh.Token);
    return TokenRequest(std::move(form), refresh, TInstant::Max());
}

TTokenCache TProtocol::ClientGrant() {
    TCgiParameters form;
    form.InsertUnescaped("grant_type", "client_credentials");
    form.InsertUnescaped("scope", ScopeString(Config));
    return TokenRequest(std::move(form), std::nullopt, TInstant::Max());
}

TTokenCache TProtocol::DeviceGrant(const std::function<bool(TDuration)>& wait) {
    if (Config.Acceptor_ == nullptr) {
        throw TError("device authorization requires an auth acceptor", false, {});
    }
    Discover();
    if (DeviceEndpoint.empty()) {
        throw TError("discovery is missing device_authorization_endpoint", false, {});
    }
    TCgiParameters form;
    form.InsertUnescaped("client_id", ClientId(Config));
    form.InsertUnescaped("scope", ScopeString(Config));
    const auto started = TInstant::Now();
    const auto response = Request(DeviceEndpoint, &form, false, TInstant::Max());
    TDeviceAuthInfo info;
    info.UserCode = String(response, "user_code", true);
    info.VerificationUrl = String(response, "verification_uri", true);
    ParseUrl(info.VerificationUrl, false);
    const auto complete = String(response, "verification_uri_complete", false);
    if (!complete.empty()) {
        ParseUrl(complete, false);
        info.VerificationUrlComplete = complete;
    }
    const auto deviceCode = String(response, "device_code", true);
    const auto* expires = Field(response, "expires_in");
    if (expires == nullptr) {
        throw TError("missing device expires_in", false, {});
    }
    info.ExpiresAt = started + TDuration::Seconds(Seconds(*expires, "expires_in", false));
    TDevicePolling polling;
    polling.Deadline = info.ExpiresAt;
    if (const auto* interval = Field(response, "interval"); interval != nullptr) {
        polling.Interval = TDuration::Seconds(Seconds(*interval, "interval", false));
    }
    Config.Acceptor_->Accept(info);
    TCgiParameters tokenForm;
    tokenForm.InsertUnescaped("grant_type", "urn:ietf:params:oauth:grant-type:device_code");
    tokenForm.InsertUnescaped("device_code", deviceCode);
    for (;;) {
        if (!wait(polling.NextDelay(TInstant::Now()))) {
            throw TError("provider stopped", false, {});
        }
        // Do not start another token request if the wait overshot the device
        // deadline. This also preserves expired_token instead of an HTTP timeout.
        polling.NextDelay(TInstant::Now());
        try {
            return TokenRequest(tokenForm, std::nullopt, polling.Deadline);
        } catch (const TError& error) {
            polling.HandleError(error);
        }
    }
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
