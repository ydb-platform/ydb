#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/uri/uri.h>

#include <stdexcept>

namespace NYdb::inline Dev::NOidc::NPrivate {

void ValidateOidcConfig(const TOidcConfig& config);
std::string HashIdentity(const std::string& data);

// Deterministic credential fingerprint; excludes cacher/acceptor instances.
std::string GetOidcClientIdentity(const TOidcConfig& config);

class TError: public std::runtime_error {
public:
    explicit TError(const std::string& message, bool retryable, std::string code);

    bool Retryable;
    std::string Code;
};

const NJson::TJsonValue* Field(const NJson::TJsonValue& json, const TString& name);
ui64 Seconds(const NJson::TJsonValue& value, const std::string& field, bool allowZero);

NUri::TUri ParseUrl(const std::string& value, bool issuer);
std::optional<TInstant> JwtExpiry(const std::string& token);
std::string ClientId(const TOidcConfig& config);
std::string ClientSecret(const TOidcConfig& config);
std::vector<std::string> Scopes(const TOidcConfig& config);

struct TDevicePolling {
    TDuration Interval = TDuration::Seconds(5);
    TInstant Deadline;

    TDuration NextDelay(TInstant now) const;
    void HandleError(const TError& error);
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
