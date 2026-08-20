#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/yaml/as/tstring.h>

#include <util/string/builder.h>

#include <algorithm>
#include <array>
#include <stdexcept>

namespace NYdb::inline Dev {

    namespace {

        constexpr std::string_view STATIC_FLOW = "static_credentials";
        constexpr std::string_view CLIENT_FLOW = "client_credentials_grant";
        constexpr std::string_view DEVICE_FLOW = "device_authorization_grant";
        constexpr std::array FLOW_NAMES = {STATIC_FLOW, CLIENT_FLOW, DEVICE_FLOW};

        std::string RequiredString(const YAML::Node& node, std::string_view name) {
            const YAML::Node value = node[std::string(name)];
            if (!value || !value.IsScalar()) {
                throw std::invalid_argument(TStringBuilder() << "OIDC config requires scalar field '" << name << "'");
            }
            std::string result = value.as<std::string>();
            if (result.empty()) {
                throw std::invalid_argument(TStringBuilder() << "OIDC config field '" << name << "' is empty");
            }
            return result;
        }

        std::optional<TInstant> OptionalInstant(const YAML::Node& node, std::string_view name) {
            const YAML::Node value = node[std::string(name)];
            if (!value) {
                return std::nullopt;
            }
            if (!value.IsScalar()) {
                throw std::invalid_argument(TStringBuilder() << "OIDC config field '" << name << "' must be scalar");
            }
            TInstant result;
            const std::string text = value.as<std::string>();
            if (!TInstant::TryParseIso8601(text, result)) {
                throw std::invalid_argument(TStringBuilder() << "OIDC config field '" << name << "' is not an ISO-8601 timestamp");
            }
            return result;
        }

        std::vector<std::string> OptionalScope(const YAML::Node& node) {
            const YAML::Node scope = node["scope"];
            if (!scope) {
                return {};
            }
            if (!scope.IsSequence()) {
                throw std::invalid_argument("OIDC config field 'scope' must be a sequence");
            }

            std::vector<std::string> result;
            result.reserve(scope.size());
            for (const auto& value : scope) {
                if (!value.IsScalar()) {
                    throw std::invalid_argument("OIDC config scope value must be scalar");
                }
                std::string item = value.as<std::string>();
                if (item.empty()) {
                    throw std::invalid_argument("OIDC config scope value is empty");
                }
                result.push_back(std::move(item));
            }
            return result;
        }

        TStaticOidcConfig ParseStaticFlow(const YAML::Node& node) {
            TOidcTokenSet tokens;
            tokens.AccessToken.Token = RequiredString(node, "access_token");
            tokens.AccessToken.ExpiresAt = OptionalInstant(node, "access_token_expires_at");

            TStaticOidcConfig result;
            if (const YAML::Node clientId = node["client_id"]) {
                if (!clientId.IsScalar()) {
                    throw std::invalid_argument("OIDC config field 'client_id' must be scalar");
                }
                result.ClientId(clientId.as<std::string>());
            }
            if (const YAML::Node refreshToken = node["refresh_token"]) {
                if (!refreshToken.IsScalar()) {
                    throw std::invalid_argument("OIDC config field 'refresh_token' must be scalar");
                }
                std::string value = refreshToken.as<std::string>();
                if (!value.empty()) {
                    tokens.RefreshToken = TOidcToken{
                        .Token = std::move(value),
                        .ExpiresAt = OptionalInstant(node, "refresh_token_expires_at"),
                    };
                }
            }
            return result.Tokens(tokens);
        }

        TClientOidcConfig ParseClientFlow(const YAML::Node& node) {
            TClientOidcConfig result;
            result.ClientId(RequiredString(node, "client_id"));
            result.ClientSecret(RequiredString(node, "client_secret"));
            for (const auto& scope : OptionalScope(node)) {
                result.AppendScope(scope);
            }
            return result;
        }

        TDeviceOidcConfig ParseDeviceFlow(const YAML::Node& node) {
            TDeviceOidcConfig result;
            result.ClientId(RequiredString(node, "client_id"));
            for (const auto& scope : OptionalScope(node)) {
                result.AppendScope(scope);
            }
            return result;
        }

    } // namespace

    TOidcConfig TOidcConfig::Parse(const std::string& configPath) {
        try {
            const YAML::Node root = YAML::LoadFile(configPath);
            if (!root.IsMap()) {
                throw std::invalid_argument("OIDC config root must be a map");
            }

            TOidcConfig result;
            result.Issuer(RequiredString(root, "issuer"));

            const size_t flowCount = std::ranges::count_if(FLOW_NAMES, [&root](const auto& name) {
                return static_cast<bool>(root[std::string(name)]);
            });
            if (flowCount != 1) {
                throw std::invalid_argument("OIDC config must contain exactly one authentication flow");
            }

            if (const YAML::Node flow = root[std::string(STATIC_FLOW)]) {
                result.Flow(ParseStaticFlow(flow));
            } else if (const YAML::Node flow = root[std::string(CLIENT_FLOW)]) {
                result.Flow(ParseClientFlow(flow));
            } else {
                result.Flow(ParseDeviceFlow(root[std::string(DEVICE_FLOW)]));
            }
            return result;
        } catch (const std::exception& error) {
            throw std::invalid_argument(TStringBuilder() << "Failed to parse OIDC config '" << configPath << "': " << error.what());
        }
    }

} // namespace NYdb::inline Dev
