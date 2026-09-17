#include "protocol.h"
#include "private.h"

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

#include <algorithm>
#include <thread>

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

struct THttpResult {
    unsigned Status;
    TString Body;
};

struct THttpJob {
    NThreading::TCancellationTokenSource Cancellation;
    NThreading::TFuture<THttpResult> Result;
    std::thread Worker;

    THttpJob(NUri::TUri url, TString body, bool post, TKeepAliveHttpClient::THeaders headers,
             TDuration socketTimeout, TDuration connectTimeout);

    bool Ready() const;

    ~THttpJob();
};

THttpJob::THttpJob(NUri::TUri url, TString body, bool post, TKeepAliveHttpClient::THeaders headers,
                   TDuration socketTimeout, TDuration connectTimeout)
{
    auto promise = NThreading::NewPromise<THttpResult>();
    Result = promise.GetFuture();
    Worker = std::thread([url = std::move(url), body = std::move(body), post, headers = std::move(headers),
                          socketTimeout, connectTimeout, cancellation = Cancellation.Token(), promise]() mutable {
        try {
            cancellation.ThrowIfCancellationRequested();
            const auto host = url.PrintS(NUri::TUri::FlagScheme | NUri::TUri::FlagHost | NUri::TUri::FlagHostAscii);
            const auto path = url.PrintS(NUri::TUri::FlagPath | NUri::TUri::FlagQuery);
            TKeepAliveHttpClient client(host, url.GetPort(), socketTimeout, connectTimeout, false, false, true);
            TResponseBuffer response;
            const auto status = post
                                    ? client.DoPost(path, body, &response, headers, nullptr, cancellation)
                                    : client.DoGet(path, &response, headers, nullptr, cancellation);
            promise.TrySetValue(THttpResult{status, std::move(response.Body)});
        } catch (...) {
            promise.TrySetException(std::current_exception());
        }
    });
}

bool THttpJob::Ready() const {
    return Result.HasValue() || Result.HasException();
}

THttpJob::~THttpJob() {
    Cancellation.Cancel();
    if (Ready()) {
        Worker.join();
    } else {
        // DNS/connect/TLS setup does not observe cancellation. The worker owns
        // all request data, so it can finish without keeping the provider alive.
        Worker.detach();
    }
}

TProtocol::TProtocol(const TOidcConfig& config, NThreading::TCancellationToken cancellation)
    : Config(config)
    , Cancellation(std::move(cancellation))
{
}

TProtocol::~TProtocol() = default;

NJson::TJsonValue TProtocol::Request(const std::string& endpoint, const TCgiParameters* form, bool authenticate, TInstant deadline) {
    Cancellation.ThrowIfCancellationRequested();
    if (HttpJob != nullptr && !HttpJob->Ready()) {
        throw TError("previous HTTP request is still stopping", true, {});
    }
    HttpJob.reset();
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
    if (form) {
        headers["Content-Type"] = "application/x-www-form-urlencoded";
    }
    const auto now = TInstant::Now();
    if (deadline <= now) {
        throw TError("HTTP request deadline exceeded", true, {});
    }
    const auto timeout = std::min(deadline - now, SocketTimeout + ConnectTimeout);
    const auto monotonicDeadline = std::chrono::steady_clock::now() + std::chrono::microseconds(timeout.MicroSeconds());
    HttpJob = std::make_unique<THttpJob>(url, body.Print(), form != nullptr, std::move(headers), SocketTimeout, ConnectTimeout);
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
    if (String(metadata, "issuer", true) != Config.Issuer) {
        throw TError("discovery issuer mismatch", false, {});
    }
    auto tokenEndpoint = String(metadata, "token_endpoint", true);
    auto deviceEndpoint = String(metadata, "device_authorization_endpoint", false);
    ParseUrl(tokenEndpoint, false);
    if (!deviceEndpoint.empty()) {
        ParseUrl(deviceEndpoint, false);
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
    // A ready HTTP future can race with the deadline check inside Request().
    // Keep the entire device grant bounded, including response processing.
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
    if (result.RefreshToken) {
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
