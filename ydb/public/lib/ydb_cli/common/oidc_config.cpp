#include "oidc_config.h"

#include "oidc_token_cache.h"

#include <util/folder/path.h>

#include <yaml-cpp/yaml.h>

#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NYdb::NConsoleClient {
namespace {

constexpr std::string_view StaticGrant = "static_credentials";
constexpr std::string_view ClientGrant = "client_credentials_grant";
constexpr std::string_view DeviceGrant = "device_authorization_grant";

struct TParsedConfig {
    NOidc::TOidcConfig Config;
    std::string CachePath;
};

[[noreturn]] void ThrowConfigError(std::string_view field, std::string_view problem);
void RequireMapping(const YAML::Node& node, std::string_view field);
void CheckKeys(const YAML::Node& node, std::string_view field, const std::set<std::string>& allowed);
bool HasImplicitTypedScalar(const YAML::Node& value);
std::string ReadString(const YAML::Node& mapping, std::string_view key, bool required);
std::optional<TInstant> ReadInstant(const YAML::Node& mapping, std::string_view key);
std::vector<std::string> ReadScopes(const YAML::Node& mapping);
NOidc::TStaticOidcConfig ReadStaticGrant(const YAML::Node& node);
NOidc::TClientOidcConfig ReadClientGrant(const YAML::Node& node);
NOidc::TDeviceOidcConfig ReadDeviceGrant(const YAML::Node& node);
TParsedConfig ParseConfig(const std::string& configFilePath);

[[noreturn]] void ThrowConfigError(std::string_view field, std::string_view problem) {
    throw std::invalid_argument("Invalid OIDC configuration field '" + std::string(field) + "': " + std::string(problem));
}

void RequireMapping(const YAML::Node& node, std::string_view field) {
    if (!node.IsMap()) {
        ThrowConfigError(field, "expected a mapping");
    }
}

void CheckKeys(const YAML::Node& node, std::string_view field, const std::set<std::string>& allowed) {
    RequireMapping(node, field);
    std::set<std::string> seen;
    for (const auto& item : node) {
        if (!item.first.IsScalar()) {
            ThrowConfigError(field, "mapping keys must be strings");
        }
        std::string key;
        try {
            key = item.first.as<std::string>();
        } catch (const YAML::Exception&) {
            ThrowConfigError(field, "mapping keys must be strings");
        }
        if (!allowed.contains(key)) {
            ThrowConfigError(key, "unknown field");
        }
        if (!seen.insert(key).second) {
            ThrowConfigError(key, "duplicate field");
        }
    }
}

bool HasImplicitTypedScalar(const YAML::Node& value) {
    if (value.Tag() != "?") {
        return false;
    }
    bool boolean = false;
    if (YAML::convert<bool>::decode(value, boolean)) {
        return true;
    }
    double number = 0;
    return YAML::convert<double>::decode(value, number);
}

std::string ReadString(
    const YAML::Node& mapping,
    std::string_view key,
    bool required)
{
    const YAML::Node value = mapping[std::string(key)];
    if (!value.IsDefined()) {
        if (required) {
            ThrowConfigError(key, "field is required");
        }
        return {};
    }
    if (!value.IsScalar() || HasImplicitTypedScalar(value)) {
        ThrowConfigError(key, "expected a non-empty string");
    }
    std::string result;
    try {
        result = value.as<std::string>();
    } catch (const YAML::Exception&) {
        ThrowConfigError(key, "expected a non-empty string");
    }
    if (result.empty()) {
        ThrowConfigError(key, "must not be empty");
    }
    return result;
}

std::optional<TInstant> ReadInstant(const YAML::Node& mapping, std::string_view key) {
    const YAML::Node value = mapping[std::string(key)];
    if (!value.IsDefined()) {
        return std::nullopt;
    }
    if (!value.IsScalar() || value.Tag() == "!" || value.Tag() == "tag:yaml.org,2002:str") {
        ThrowConfigError(key, "expected Unix seconds");
    }
    try {
        const auto seconds = value.as<ui64>();
        if (seconds > TInstant::Max().Seconds()) {
            ThrowConfigError(key, "Unix seconds exceed the supported range");
        }
        return TInstant::Seconds(seconds);
    } catch (const YAML::Exception&) {
        ThrowConfigError(key, "expected non-negative Unix seconds");
    }
}

std::vector<std::string> ReadScopes(const YAML::Node& mapping) {
    const YAML::Node value = mapping["scope"];
    if (!value.IsDefined()) {
        return {};
    }
    if (!value.IsSequence()) {
        ThrowConfigError("scope", "expected a sequence of non-empty strings");
    }
    std::vector<std::string> result;
    result.reserve(value.size());
    for (const auto& item : value) {
        if (!item.IsScalar() || HasImplicitTypedScalar(item)) {
            ThrowConfigError("scope", "expected a sequence of non-empty strings");
        }
        std::string scope;
        try {
            scope = item.as<std::string>();
        } catch (const YAML::Exception&) {
            ThrowConfigError("scope", "expected a sequence of non-empty strings");
        }
        if (scope.empty()) {
            ThrowConfigError("scope", "scope values must not be empty");
        }
        result.push_back(std::move(scope));
    }
    return result;
}

NOidc::TStaticOidcConfig ReadStaticGrant(const YAML::Node& node) {
    CheckKeys(node, StaticGrant, {"access_token", "expires_at"});
    return {
        .AccessToken = ReadString(node, "access_token", true),
        .ExpiresAt = ReadInstant(node, "expires_at"),
    };
}

NOidc::TClientOidcConfig ReadClientGrant(const YAML::Node& node) {
    CheckKeys(node, ClientGrant, {"client_id", "client_secret", "scope"});
    return {
        .ClientId = ReadString(node, "client_id", true),
        .ClientSecret = ReadString(node, "client_secret", true),
        .Scopes = ReadScopes(node),
    };
}

NOidc::TDeviceOidcConfig ReadDeviceGrant(const YAML::Node& node) {
    CheckKeys(node, DeviceGrant, {"client_id", "scope"});
    return {
        .ClientId = ReadString(node, "client_id", true),
        .Scopes = ReadScopes(node),
    };
}

TParsedConfig ParseConfig(const std::string& configFilePath) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(configFilePath);
    } catch (const YAML::Exception&) {
        throw std::invalid_argument("Failed to load OIDC configuration file '" + configFilePath + "'");
    }

    CheckKeys(root, "root", {
        "issuer",
        "cache_path",
        std::string(StaticGrant),
        std::string(ClientGrant),
        std::string(DeviceGrant),
    });

    size_t grantCount = 0;
    grantCount += root[std::string(StaticGrant)].IsDefined();
    grantCount += root[std::string(ClientGrant)].IsDefined();
    grantCount += root[std::string(DeviceGrant)].IsDefined();
    if (grantCount != 1) {
        throw std::invalid_argument("OIDC configuration must contain exactly one credentials grant");
    }

    TParsedConfig result;
    result.Config.Issuer = ReadString(root, "issuer", true);
    if (const auto node = root[std::string(StaticGrant)]; node.IsDefined()) {
        result.Config.FlowConfig = ReadStaticGrant(node);
    } else if (const auto node = root[std::string(ClientGrant)]; node.IsDefined()) {
        result.Config.FlowConfig = ReadClientGrant(node);
    } else {
        result.Config.FlowConfig = ReadDeviceGrant(root[std::string(DeviceGrant)]);
    }
    if (root["cache_path"].IsDefined()) {
        result.CachePath = ReadString(root, "cache_path", true);
        TFsPath cachePath(result.CachePath);
        if (cachePath.IsRelative()) {
            result.CachePath = (TFsPath(configFilePath).Parent() / cachePath).GetPath();
        }
    }

    return result;
}

} // namespace

NOidc::TOidcConfig LoadOidcConfig(const std::string& configFilePath) {
    auto parsed = ParseConfig(configFilePath);
    // Validate through the public SDK API and obtain a persistent identity
    // before adding cacher/acceptor instances to the configuration.
    const auto factory = NOidc::CreateOidcProviderFactory(parsed.Config);
    if (!parsed.CachePath.empty()) {
        parsed.Config.Cacher(CreateFileTokenCacher(
            parsed.CachePath,
            factory->GetClientIdentity()));
    }
    return std::move(parsed.Config);
}

std::shared_ptr<ICredentialsProviderFactory> CreateOidcFileCredentialsProviderFactory(
    const std::string& configFilePath,
    std::shared_ptr<NOidc::IAuthAcceptor> acceptor)
{
    auto config = LoadOidcConfig(configFilePath);
    if (acceptor != nullptr) {
        config.Acceptor(acceptor);
    }
    return NOidc::CreateOidcProviderFactory(config);
}

} // namespace NYdb::NConsoleClient
