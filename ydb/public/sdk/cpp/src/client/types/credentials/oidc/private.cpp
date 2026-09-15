#include "private.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/overloaded.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

constexpr size_t MaxJwtPayloadSize = 1024 * 1024;

} // namespace

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
    if ((!seconds && !allowZero) || seconds > std::numeric_limits<i64>::max() / 2 / 1'000'000) {
        throw TError("invalid " + field, false, {});
    }
    return seconds;
}

NUri::TUri ParseUrl(const std::string& value, bool issuer) {
    NUri::TUri url;
    if (value.empty() ||
        std::any_of(value.begin(), value.end(), [](unsigned char c) { return c <= 0x20 || c == 0x7f; }) ||
        url.Parse(value, NUri::TFeature::FeaturesAll) != NUri::TUri::TState::EParsed::ParsedOK ||
        url.GetHost().empty() || !url.IsNull(NUri::TUri::FieldUser) || !url.IsNull(NUri::TUri::FieldPass) ||
        !url.IsNull(NUri::TUri::FieldFrag) || (issuer && !url.IsNull(NUri::TUri::FieldQuery))) {
        throw std::invalid_argument("OIDC credentials: invalid endpoint URL");
    }
    if (url.GetScheme() != NUri::TScheme::SchemeHTTPS) {
        throw std::invalid_argument("OIDC credentials: endpoint requires HTTPS");
    }
    if (!url.GetPort()) {
        throw std::invalid_argument("OIDC credentials: invalid endpoint port");
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
