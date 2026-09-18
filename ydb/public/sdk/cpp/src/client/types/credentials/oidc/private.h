#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/threading/cancellation/cancellation_token.h>

#include <functional>
#include <stdexcept>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TError: public std::runtime_error {
public:
    explicit TError(const std::string& message, bool retryable, std::string code)
        : std::runtime_error("OIDC credentials: " + message)
        , Retryable(retryable)
        , Code(std::move(code))
    {
    }

    bool Retryable;
    std::string Code;
};

struct TUrl {
    TString Host;
    ui16 Port;
    TString Path;
};

TUrl ParseUrl(const std::string& value, bool allowHttp, bool issuer);
std::optional<TInstant> JwtExpiry(const std::string& token);
std::string ClientId(const TOidcConfig& config);
std::string ClientSecret(const TOidcConfig& config);
std::vector<std::string> Scopes(const TOidcConfig& config);

// Polling policy is independent of networking and wall-clock waiting.
struct TDevicePolling {
    TDuration Interval = TDuration::Seconds(5);
    TInstant Deadline;

    TDuration NextDelay(TInstant now) const;
    void HandleError(const TError& error);
};

struct THttpJob;

class TProtocol {
public:
    TProtocol(const TOidcConfig& config, NThreading::TCancellationToken cancellation);
    ~TProtocol();

    TTokenCache Refresh(const TOAuthToken& refresh);
    TTokenCache ClientGrant();
    TTokenCache DeviceGrant(const std::function<bool(TDuration)>& wait);

private:
    NJson::TJsonValue Request(const std::string& endpoint, const TCgiParameters* form, bool authenticate, TInstant deadline);
    void Discover();
    TTokenCache TokenRequest(TCgiParameters form, const std::optional<TOAuthToken>& refresh, TInstant deadline);

    const TOidcConfig& Config;
    NThreading::TCancellationToken Cancellation;
    std::string TokenEndpoint;
    std::string DeviceEndpoint;
    std::unique_ptr<THttpJob> HttpJob;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
