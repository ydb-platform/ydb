#include "private.h"

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/uri/uri.h>

#include <util/stream/output.h>

#include <algorithm>
#include <limits>
#include <thread>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

constexpr size_t MaxResponseSize = 1024 * 1024;

class TResponseBuffer: public IOutputStream {
public:
    TString Body;

private:
    void DoWrite(const void* buffer, size_t size) override {
        if (size > MaxResponseSize - Body.size()) {
            throw TError("response exceeds size limit", false, {});
        }
        Body.append(static_cast<const char*>(buffer), size);
    }
};

const NJson::TJsonValue* Field(const NJson::TJsonValue& json, const TString& name) {
    const auto& map = json.GetMapSafe();
    const auto it = map.find(name);
    return it == map.end() ? nullptr : &it->second;
}

std::string String(const NJson::TJsonValue& json, const TString& name, bool required) {
    const auto* field = Field(json, name);
    if (!field && !required) {
        return {};
    }
    if (!field || !field->IsString() || field->GetString().empty()) {
        throw TError("missing or invalid " + std::string(name), false, {});
    }
    return std::string(field->GetString());
}

ui64 Seconds(const NJson::TJsonValue& value, const std::string& field, bool allowZero) {
    if ((!value.IsInteger() && !value.IsUInteger()) ||
        (value.IsInteger() && value.GetInteger() < 0)) {
        throw TError("invalid " + field, false, {});
    }
    const auto seconds = value.GetUInteger();
    // Bound conversions to microseconds and addition to the current instant.
    if ((!seconds && !allowZero) || seconds > std::numeric_limits<i64>::max() / 2 / 1'000'000) {
        throw TError("invalid " + field, false, {});
    }
    return seconds;
}

std::string FormEncode(const std::string& value) {
    static constexpr char Hex[] = "0123456789ABCDEF";
    std::string result;
    for (const unsigned char c : value) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~') {
            result += static_cast<char>(c);
        } else if (c == ' ') {
            result += '+';
        } else {
            result += '%';
            result += Hex[c >> 4];
            result += Hex[c & 15];
        }
    }
    return result;
}

std::string ScopeString(const TOidcConfig& config) {
    std::string result;
    for (const auto& scope : Scopes(config)) {
        if (!result.empty()) {
            result += ' ';
        }
        result += scope;
    }
    return result;
}

bool IsBearer(std::string value) {
    for (char& c : value) {
        if (c >= 'A' && c <= 'Z') {
            c += 'a' - 'A';
        }
    }
    return value == "bearer";
}

void CheckToken(const std::string& token) {
    if (std::any_of(token.begin(), token.end(), [](unsigned char c) { return c <= 0x20 || c >= 0x7f; })) {
        throw TError("invalid token characters", false, {});
    }
}

} // namespace

struct THttpResult {
    unsigned Status;
    TString Body;
};

// The resolver in the synchronous HTTP client cannot be interrupted. Isolate
// each request from the credentials worker, so its public deadline remains
// enforceable even during resolution. At most one job per protocol is alive;
// retries cannot accumulate more threads while an old request is draining.
struct THttpJob {
    NThreading::TCancellationTokenSource Cancellation;
    NThreading::TFuture<THttpResult> Result;
    std::thread Worker;

    THttpJob(TUrl url, TString body, bool post, TKeepAliveHttpClient::THeaders headers,
             TDuration socketTimeout, TDuration connectTimeout)
    {
        auto promise = NThreading::NewPromise<THttpResult>();
        Result = promise.GetFuture();
        Worker = std::thread([url = std::move(url), body = std::move(body), post, headers = std::move(headers),
                              socketTimeout, connectTimeout, cancellation = Cancellation.Token(), promise]() mutable {
            try {
                cancellation.ThrowIfCancellationRequested();
                TKeepAliveHttpClient client(url.Host, url.Port, socketTimeout, connectTimeout, false, false, true);
                TResponseBuffer response;
                const auto status = post
                                        ? client.DoPost(url.Path, body, &response, headers, nullptr, cancellation)
                                        : client.DoGet(url.Path, &response, headers, nullptr, cancellation);
                promise.TrySetValue(THttpResult{status, std::move(response.Body)});
            } catch (...) {
                promise.TrySetException(std::current_exception());
            }
        });
    }

    bool Ready() const {
        return Result.HasValue() || Result.HasException();
    }

    ~THttpJob() {
        Cancellation.Cancel();
        if (Ready()) {
            Worker.join();
        } else {
            // The worker owns all its request inputs, token and promise. It
            // never accesses the protocol/provider after this handle is gone.
            Worker.detach();
        }
    }
};

TUrl ParseUrl(const std::string& value, bool allowHttp, bool issuer) {
    NUri::TUri url;
    if (value.empty() ||
        std::any_of(value.begin(), value.end(), [](unsigned char c) { return c <= 0x20 || c == 0x7f; }) ||
        url.Parse(value, NUri::TFeature::FeaturesAll) != NUri::TUri::TState::EParsed::ParsedOK ||
        url.GetHost().empty() || !url.IsNull(NUri::TUri::FieldUser) || !url.IsNull(NUri::TUri::FieldPass) ||
        !url.IsNull(NUri::TUri::FieldFrag) || (issuer && !url.IsNull(NUri::TUri::FieldQuery))) {
        throw std::invalid_argument("OIDC credentials: invalid endpoint URL");
    }
    if (url.GetScheme() != NUri::TScheme::SchemeHTTPS &&
        !(allowHttp && url.GetScheme() == NUri::TScheme::SchemeHTTP)) {
        throw std::invalid_argument("OIDC credentials: endpoint requires HTTPS (allow_insecure_http enables development HTTP)");
    }
    TUrl result;
    result.Host = TString(url.GetField(NUri::TUri::FieldScheme)) + "://" + url.GetHost();
    result.Port = url.GetPort();
    if (!result.Port) {
        throw std::invalid_argument("OIDC credentials: invalid endpoint port");
    }
    result.Path = url.GetField(NUri::TUri::FieldPath);
    if (result.Path.empty()) {
        result.Path = "/";
    }
    if (!url.IsNull(NUri::TUri::FieldQuery)) {
        result.Path += "?";
        result.Path += url.GetField(NUri::TUri::FieldQuery);
    }
    return result;
}

std::optional<TInstant> JwtExpiry(const std::string& token) {
    const auto first = token.find('.');
    const auto second = first == std::string::npos ? first : token.find('.', first + 1);
    if (second == std::string::npos || second - first > MaxResponseSize) {
        return std::nullopt;
    }
    NJson::TJsonValue payload;
    try {
        const auto decoded = Base64DecodeUneven(TStringBuf(token.data() + first + 1, second - first - 1));
        if (!NJson::ReadJsonTree(decoded, &payload) || !payload.IsMap()) {
            return std::nullopt;
        }
    } catch (const std::exception&) {
        return std::nullopt; // Opaque and malformed JWT-shaped tokens need not have an exp hint.
    }
    if (const auto* expiry = Field(payload, "exp")) {
        return TInstant::Seconds(Seconds(*expiry, "exp", true));
    }
    return std::nullopt;
}

TDuration TDevicePolling::NextDelay(TInstant now) const {
    if (now >= Deadline) {
        throw TError("device authorization expired", false, "expired_token");
    }
    return std::min(Interval, Deadline - now);
}

void TDevicePolling::HandleError(const TError& error) {
    if (error.Code == "authorization_pending") {
        return;
    }
    if (error.Code == "slow_down") {
        Interval = std::min(Interval + TDuration::Seconds(5), TDuration::Hours(24));
        return;
    }
    if (error.Retryable) {
        Interval = std::min(Interval * 2, TDuration::Hours(24));
        return;
    }
    throw error;
}

TProtocol::TProtocol(const TOidcConfig& config, NThreading::TCancellationToken cancellation)
    : Config(config)
    , Cancellation(std::move(cancellation))
{
}

TProtocol::~TProtocol() = default;

NJson::TJsonValue TProtocol::Request(const std::string& endpoint, const TCgiParameters* form, bool authenticate, TInstant deadline) {
    Cancellation.ThrowIfCancellationRequested();
    if (HttpJob && !HttpJob->Ready()) {
        throw TError("previous HTTP request is still stopping", true, {});
    }
    HttpJob.reset();
    const auto url = ParseUrl(endpoint, Config.AllowInsecureHttp_, false);
    TKeepAliveHttpClient::THeaders headers;
    TCgiParameters body = form ? *form : TCgiParameters{};
    if (authenticate) {
        const auto secret = ClientSecret(Config);
        if (!secret.empty() && Config.TokenEndpointAuthMethod_ == "client_secret_basic") {
            const auto credentials = FormEncode(ClientId(Config)) + ":" + FormEncode(secret);
            headers["Authorization"] = "Basic " + Base64Encode(credentials);
        } else {
            body.InsertUnescaped("client_id", ClientId(Config));
            if (!secret.empty()) {
                body.InsertUnescaped("client_secret", secret);
            }
        }
    }
    headers["Accept"] = "application/json";
    if (form) {
        headers["Content-Type"] = "application/x-www-form-urlencoded";
    }
    const auto now = TInstant::Now();
    if (deadline <= now) {
        throw TError("HTTP request deadline exceeded", true, {});
    }
    const auto timeout = std::min(deadline - now, Config.SocketTimeout_ + Config.ConnectTimeout_);
    const auto monotonicDeadline = std::chrono::steady_clock::now() + std::chrono::microseconds(timeout.MicroSeconds());
    HttpJob = std::make_unique<THttpJob>(url, body.Print(), form != nullptr, std::move(headers),
                                         Config.SocketTimeout_, Config.ConnectTimeout_);
    THttpResult response;
    try {
        while (!HttpJob->Ready()) {
            if (Cancellation.IsCancellationRequested()) {
                HttpJob->Cancellation.Cancel();
                Cancellation.ThrowIfCancellationRequested();
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(monotonicDeadline - std::chrono::steady_clock::now());
            if (remaining <= std::chrono::microseconds::zero()) {
                HttpJob->Cancellation.Cancel();
                throw TError("HTTP request deadline exceeded", true, {});
            }
            HttpJob->Result.Wait(TDuration::MicroSeconds(std::min<i64>(remaining.count(), 100'000)));
        }
        response = HttpJob->Result.GetValueSync();
        HttpJob.reset();
    } catch (const TError&) {
        throw;
    } catch (const std::exception&) {
        Cancellation.ThrowIfCancellationRequested();
        throw TError("HTTP transport failed", true, {});
    }
    NJson::TJsonValue json;
    NJson::TJsonReaderConfig reader;
    reader.MaxDepth = 32;
    const bool valid = NJson::ReadJsonTree(response.Body, &reader, &json) && json.IsMap();
    const auto status = response.Status;
    if (status != 200) {
        std::string code;
        if (valid) {
            if (const auto* error = Field(json, "error"); error && error->IsString()) {
                // Only known protocol codes are safe for public diagnostics.
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
    if (String(metadata, "issuer", true) != Config.Issuer) {
        throw TError("discovery issuer mismatch", false, {});
    }
    auto tokenEndpoint = String(metadata, "token_endpoint", true);
    auto deviceEndpoint = String(metadata, "device_authorization_endpoint", false);
    ParseUrl(tokenEndpoint, Config.AllowInsecureHttp_, false);
    if (!deviceEndpoint.empty()) {
        ParseUrl(deviceEndpoint, Config.AllowInsecureHttp_, false);
    }
    if (!ClientSecret(Config).empty()) {
        if (const auto* methods = Field(metadata, "token_endpoint_auth_methods_supported")) {
            if (!methods->IsArray()) {
                throw TError("invalid token_endpoint_auth_methods_supported", false, {});
            }
            bool supported = false;
            for (const auto& method : methods->GetArray()) {
                if (!method.IsString()) {
                    throw TError("invalid token_endpoint_auth_methods_supported", false, {});
                }
                supported |= method.GetString() == Config.TokenEndpointAuthMethod_;
            }
            if (!supported) {
                throw TError("configured client authentication method is not supported", false, {});
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
    if (TInstant::Now() >= deadline) {
        throw TError("device authorization expired", false, "expired_token");
    }
    if (!IsBearer(String(response, "token_type", true))) {
        throw TError("unsupported token_type", false, {});
    }
    TTokenCache result;
    result.AccessToken.Token = String(response, "access_token", true);
    CheckToken(result.AccessToken.Token);
    if (const auto* expires = Field(response, "expires_in")) {
        result.AccessToken.ExpiresAt = now + TDuration::Seconds(Seconds(*expires, "expires_in", false));
    } else {
        result.AccessToken.ExpiresAt = JwtExpiry(result.AccessToken.Token);
    }
    if (!result.AccessToken.ExpiresAt || !result.AccessToken.IsValid(TInstant::Now())) {
        throw TError("missing or expired access token lifetime", false, {});
    }
    const auto refreshToken = String(response, "refresh_token", false);
    if (!refreshToken.empty()) {
        CheckToken(refreshToken);
        result.RefreshToken = TOAuthToken{refreshToken, std::nullopt};
    } else {
        result.RefreshToken = refresh;
    }
    if (result.RefreshToken) {
        if (const auto* expires = Field(response, "refresh_expires_in")) {
            const auto seconds = Seconds(*expires, "refresh_expires_in", true);
            // Keycloak uses zero to represent a refresh token without a timeout.
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
    const auto scope = ScopeString(Config);
    if (!scope.empty()) {
        form.InsertUnescaped("scope", scope);
    }
    return TokenRequest(std::move(form), std::nullopt, TInstant::Max());
}

TTokenCache TProtocol::DeviceGrant(const std::function<bool(TDuration)>& wait) {
    if (!Config.Acceptor_) {
        throw TError("device authorization requires an auth acceptor", false, {});
    }
    Discover();
    if (DeviceEndpoint.empty()) {
        throw TError("discovery is missing device_authorization_endpoint", false, {});
    }
    TCgiParameters form;
    form.InsertUnescaped("client_id", ClientId(Config));
    const auto scope = ScopeString(Config);
    if (!scope.empty()) {
        form.InsertUnescaped("scope", scope);
    }
    const auto started = TInstant::Now();
    const auto response = Request(DeviceEndpoint, &form, false, TInstant::Max());
    TDeviceAuthInfo info;
    info.UserCode = String(response, "user_code", true);
    info.VerificationUrl = String(response, "verification_uri", true);
    ParseUrl(info.VerificationUrl, Config.AllowInsecureHttp_, false);
    const auto complete = String(response, "verification_uri_complete", false);
    if (!complete.empty()) {
        ParseUrl(complete, Config.AllowInsecureHttp_, false);
        info.VerificationUrlComplete = complete;
    }
    const auto deviceCode = String(response, "device_code", true);
    const auto* expires = Field(response, "expires_in");
    if (!expires) {
        throw TError("missing device expires_in", false, {});
    }
    info.ExpiresAt = started + TDuration::Seconds(Seconds(*expires, "expires_in", false));
    TDevicePolling polling;
    polling.Deadline = info.ExpiresAt;
    if (const auto* interval = Field(response, "interval")) {
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
        polling.NextDelay(TInstant::Now()); // Do not poll after the device deadline.
        try {
            return TokenRequest(tokenForm, std::nullopt, polling.Deadline);
        } catch (const TError& error) {
            polling.HandleError(error);
        }
    }
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
