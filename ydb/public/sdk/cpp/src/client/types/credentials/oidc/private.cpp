#include "private.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/openssl/crypto/sha.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/overloaded.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

constexpr size_t MaxJwtPayloadSize = 1024 * 1024;
// Seconds are converted to signed chrono microseconds. Reserve half the range
// for deadline arithmetic and doubling the polling interval without overflow.
constexpr ui64 MaxDurationSeconds = std::numeric_limits<i64>::max() / 2 / 1'000'000;

} // namespace

std::string HashIdentity(const std::string& data) {
    const auto hash = NOpenSsl::NSha256::Calc(data.data(), data.size());
    static constexpr char Hex[] = "0123456789abcdef";
    std::string identity = "oidc:";
    for (const auto byte : hash) {
        identity += Hex[byte >> 4];
        identity += Hex[byte & 0xF];
    }
    return identity;
}

void ValidateOidcConfig(const TOidcConfig& config) {
    ParseUrl(config.Issuer, true);
    std::visit(TOverloaded{
        [](const TStaticOidcConfig& flow) {
            if (flow.AccessToken.empty()) {
                throw std::invalid_argument("OIDC credentials: static_credentials requires access_token");
            }
        },
        [](const TClientOidcConfig& flow) {
            if (flow.ClientId.empty()) {
                throw std::invalid_argument("OIDC credentials: client_id is required");
            }
            if (flow.ClientSecret.empty()) {
                throw std::invalid_argument("OIDC credentials: client_secret is required");
            }
        },
        [](const TDeviceOidcConfig& flow) {
            if (flow.ClientId.empty()) {
                throw std::invalid_argument("OIDC credentials: client_id is required");
            }
        },
    }, config.FlowConfig);
    for (const auto& scope : Scopes(config)) {
        if (scope.empty() || std::any_of(scope.begin(), scope.end(), [](unsigned char c) {
                return c <= 0x20 || c >= 0x7f || c == '"' || c == '\\';
            })) {
            throw std::invalid_argument("OIDC credentials: invalid scope");
        }
    }
}

std::string GetOidcClientIdentity(const TOidcConfig& config) {
    std::string data;
    const auto append = [&data](const std::string& value) {
        data += std::to_string(value.size()) + ":" + value;
    };
    append(config.Issuer);
    append(std::to_string(config.FlowConfig.index()));
    append(ClientId(config));
    append(ClientSecret(config));
    auto scopes = Scopes(config);
    std::sort(scopes.begin(), scopes.end());
    scopes.erase(std::unique(scopes.begin(), scopes.end()), scopes.end());
    for (const auto& scope : scopes) {
        append(scope);
    }
    if (const auto* flow = std::get_if<TStaticOidcConfig>(&config.FlowConfig); flow != nullptr) {
        append(flow->AccessToken);
        append(flow->ExpiresAt.has_value() ? std::to_string(flow->ExpiresAt->MicroSeconds()) : "");
    }
    return HashIdentity(data);
}

TError::TError(const std::string& message, bool retryable, std::string code)
    : std::runtime_error("OIDC credentials: " + message)
    , Retryable(retryable)
    , Code(std::move(code))
{
}

const NJson::TJsonValue* Field(const NJson::TJsonValue& json, const TString& name) {
    const auto& map = json.GetMapSafe();
    const auto it = map.find(name);
    return (it == map.end()) ? nullptr : &it->second;
}

ui64 Seconds(const NJson::TJsonValue& value, const std::string& field, bool allowZero) {
    if ((!value.IsInteger() && !value.IsUInteger()) || (value.IsInteger() && value.GetInteger() < 0)) {
        throw TError("invalid " + field, false, {});
    }
    const auto seconds = value.GetUInteger();
    if ((!seconds && !allowZero) || seconds > MaxDurationSeconds) {
        throw TError("invalid " + field, false, {});
    }
    return seconds;
}

NUri::TUri ParseUrl(const std::string& value, bool issuer) {
    const std::string role = issuer ? "issuer" : "endpoint";
    const auto invalidUrl = [&role] {
        return std::invalid_argument("OIDC credentials: invalid " + role + " URL");
    };
    if (value.empty()) {
        throw invalidUrl();
    }
    const bool hasControlCharacters = std::any_of(value.begin(), value.end(), [](unsigned char c) {
        return c <= 0x20 || c == 0x7f;
    });
    if (hasControlCharacters) {
        throw invalidUrl();
    }

    NUri::TUri url;
    if (url.Parse(value, NUri::TFeature::FeaturesAll) != NUri::TUri::TState::EParsed::ParsedOK) {
        throw invalidUrl();
    }
    if (url.GetHost().empty()) {
        throw invalidUrl();
    }
    for (const auto field : {NUri::TUri::FieldUser, NUri::TUri::FieldPass, NUri::TUri::FieldFrag}) {
        if (!url.IsNull(field)) {
            throw invalidUrl();
        }
    }
    if (issuer && !url.IsNull(NUri::TUri::FieldQuery)) {
        throw invalidUrl();
    }
    if (url.GetScheme() != NUri::TScheme::SchemeHTTPS) {
        throw std::invalid_argument("OIDC credentials: " + role + " requires HTTPS");
    }
    if (!url.GetPort()) {
        throw std::invalid_argument("OIDC credentials: invalid " + role + " port");
    }
    return url;
}

std::optional<TInstant> JwtExpiry(const std::string& token) {
    const auto first = token.find('.');
    const auto second = (first == std::string::npos) ? first : token.find('.', first + 1);
    if (second == std::string::npos || second - first > MaxJwtPayloadSize) {
        return std::nullopt;
    }
    NJson::TJsonValue payload;
    try {
        const auto decoded = Base64DecodeUneven(TStringBuf(token.data() + first + 1, second - first - 1));
        if (!NJson::ReadJsonTree(decoded, &payload) || !payload.IsMap()) {
            return std::nullopt;
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }
    if (const auto* expiry = Field(payload, "exp"); expiry != nullptr) {
        // A negative NumericDate is in the past, not an unknown lifetime.
        if (expiry->IsInteger() && expiry->GetInteger() < 0) {
            return TInstant::Zero();
        }
        try {
            return TInstant::Seconds(Seconds(*expiry, "exp", true));
        } catch (const TError&) {
            // JWT decoding is only a scheduling hint; token validation belongs
            // to the server, just as it does for opaque access tokens.
            return std::nullopt;
        }
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

std::string ClientId(const TOidcConfig& config) {
    return std::visit(TOverloaded{
        [](const TStaticOidcConfig&) { return std::string{}; },
        [](const TClientOidcConfig& flow) { return flow.ClientId; },
        [](const TDeviceOidcConfig& flow) { return flow.ClientId; },
    }, config.FlowConfig);
}

std::string ClientSecret(const TOidcConfig& config) {
    return std::visit(TOverloaded{
        [](const TStaticOidcConfig&) { return std::string{}; },
        [](const TClientOidcConfig& flow) { return flow.ClientSecret; },
        [](const TDeviceOidcConfig&) { return std::string{}; },
    }, config.FlowConfig);
}

std::vector<std::string> Scopes(const TOidcConfig& config) {
    return std::visit(TOverloaded{
        [](const TStaticOidcConfig&) { return std::vector<std::string>{}; },
        [](const TClientOidcConfig& flow) { return flow.Scopes; },
        [](const TDeviceOidcConfig& flow) { return flow.Scopes; },
    }, config.FlowConfig);
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
