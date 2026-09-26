#include "oidc_options.h"

#include "client_command_options.h"
#include "oidc_config.h"
#include "oidc_token_cache.h"

#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/env.h>

#include <yaml-cpp/yaml.h>

#include <stdexcept>
#include <utility>

namespace NYdb::NConsoleClient {
namespace {

struct TField {
    const char* Option;
    const char* Env;
    const char* Profile;
    const char* Help;
    TString TOidcCliOptions::* Member;
};

const TField Fields[] = {
    {"oidc-issuer", "YDB_OIDC_ISSUER", "issuer", "OIDC issuer URL (HTTPS)", &TOidcCliOptions::Issuer},
    {"oidc-flow", "YDB_OIDC_FLOW", "flow", "OIDC flow: static, client or device (default: device)", &TOidcCliOptions::Flow},
    {"oidc-client-id", "YDB_OIDC_CLIENT_ID", "client_id", "OIDC client ID for client or device flow", &TOidcCliOptions::ClientId},
    {"oidc-client-secret", "YDB_OIDC_CLIENT_SECRET", "client_secret", "OIDC client secret for client flow", &TOidcCliOptions::ClientSecret},
    {"oidc-access-token", "YDB_OIDC_ACCESS_TOKEN", "access_token", "Access token for static OIDC flow", &TOidcCliOptions::AccessToken},
    {"oidc-expires-at", "YDB_OIDC_EXPIRES_AT", "expires_at", "Static OIDC access token expiration (Unix seconds)", &TOidcCliOptions::ExpiresAt},
    {"oidc-scope", "YDB_OIDC_SCOPE", "scope", "Space-separated OIDC scopes; may be repeated. openid is added automatically", &TOidcCliOptions::Scope},
    {"oidc-cache-path", "YDB_OIDC_CACHE_PATH", "cache_path", "OIDC token cache file path", &TOidcCliOptions::CachePath},
};

TAuthMethodOption::TProfileParser ProfileFieldParser(const TField& field);
bool IsProfileSource(EOptionValueSource source);
void CheckAllowedFields(const TOidcCliOptions& options, const TString& flow);

TAuthMethodOption::TProfileParser ProfileFieldParser(const TField& field) {
    return [key = TString(field.Profile)](const YAML::Node& data, TString* value, bool* isFileName,
        std::vector<TString>* errors, bool) {
        if (!data.IsMap()) {
            if (errors != nullptr) {
                errors->push_back("OIDC profile authentication data must be a mapping");
            }
            return false;
        }
        const auto node = data[std::string(key)];
        if (!node.IsDefined()) {
            if (key == "issuer" && errors != nullptr) {
                errors->push_back("OIDC profile authentication requires issuer");
            }
            return false;
        }
        if (!node.IsScalar()) {
            if (errors != nullptr) {
                errors->push_back("OIDC profile field '" + key + "' must be a string");
            }
            return false;
        }
        if (value != nullptr) {
            *value = node.as<std::string>();
        }
        if (isFileName != nullptr) {
            *isFileName = false;
        }
        return true;
    };
}

bool IsProfileSource(EOptionValueSource source) {
    return source == EOptionValueSource::ExplicitProfile || source == EOptionValueSource::ActiveProfile;
}

void CheckAllowedFields(const TOidcCliOptions& options, const TString& flow) {
    if (flow == "static") {
        if (!options.ClientId.empty() || !options.ClientSecret.empty() || !options.Scope.empty()) {
            throw std::invalid_argument("Static OIDC flow does not accept client ID, client secret or scopes");
        }
    } else if (flow == "client" || flow == "device") {
        if (!options.AccessToken.empty() || !options.ExpiresAt.empty()) {
            throw std::invalid_argument("Access token and expires-at require static OIDC flow");
        }
        if (flow == "device" && !options.ClientSecret.empty()) {
            throw std::invalid_argument("Client secret requires client OIDC flow");
        }
    } else {
        throw std::invalid_argument("--oidc-flow must be static, client or device");
    }
}

} // namespace

bool TOidcCliOptions::IsConfigured() const {
    return !ConfigFile.empty() || !Issuer.empty();
}

bool TOidcCliOptions::HasOptions() const {
    if (!ConfigFile.empty()) {
        return true;
    }
    for (const auto& field : Fields) {
        if (!(this->*field.Member).empty()) {
            return true;
        }
    }
    return false;
}

NOidc::TOidcConfig TOidcCliOptions::MakeConfig() const {
    if (!ConfigFile.empty()) {
        for (const auto& field : Fields) {
            if (!(this->*field.Member).empty()) {
                throw std::invalid_argument("--oidc-config cannot be combined with direct OIDC options");
            }
        }
        return LoadOidcConfig(std::string(ConfigFile));
    }
    if (Issuer.empty()) {
        throw std::invalid_argument("OIDC authentication requires --oidc-issuer or YDB_OIDC_ISSUER");
    }
    const TString flow = Flow.empty() ? TString("device") : Flow;
    CheckAllowedFields(*this, flow);

    NOidc::TOidcConfig config;
    config.Issuer = std::string(Issuer);
    std::vector<std::string> scopes;
    for (const auto scope : StringSplitter(Scope).SplitBySet(" \t\r\n").SkipEmpty()) {
        scopes.emplace_back(scope.Token());
    }
    if (flow == "static") {
        std::optional<TInstant> expiresAt;
        if (!ExpiresAt.empty()) {
            ui64 seconds = 0;
            if (!TryFromString(ExpiresAt, seconds) || seconds > TInstant::Max().Seconds()) {
                throw std::invalid_argument("--oidc-expires-at must be non-negative Unix seconds within the supported range");
            }
            expiresAt = TInstant::Seconds(seconds);
        }
        config.FlowConfig = NOidc::TStaticOidcConfig{
            .AccessToken = std::string(AccessToken),
            .ExpiresAt = expiresAt,
        };
    } else if (flow == "client") {
        config.FlowConfig = NOidc::TClientOidcConfig{
            .ClientId = std::string(ClientId),
            .ClientSecret = std::string(ClientSecret),
            .Scopes = std::move(scopes),
        };
    } else {
        config.FlowConfig = NOidc::TDeviceOidcConfig{
            .ClientId = std::string(ClientId),
            .Scopes = std::move(scopes),
        };
    }
    const auto factory = NOidc::CreateOidcProviderFactory(config);
    if (!CachePath.empty()) {
        config.Cacher(CreateFileTokenCacher(std::string(CachePath), factory->GetClientIdentity()));
    }
    return config;
}

YAML::Node TOidcCliOptions::MakeProfileAuth() const {
    YAML::Node auth;
    if (!ConfigFile.empty()) {
        auth["method"] = "oidc-config";
        auth["data"] = std::string(ConfigFile);
    } else {
        auth["method"] = "oidc";
        for (const auto& field : Fields) {
            const auto& value = this->*field.Member;
            if (!value.empty()) {
                auth["data"][field.Profile] = std::string(value);
            }
        }
    }
    return auth;
}

void TOidcCliOptions::Print(IOutputStream& output) const {
    if (!ConfigFile.empty()) {
        output << "oidc-config: " << ConfigFile << Endl;
        return;
    }
    if (Issuer.empty()) {
        return;
    }
    for (const auto& field : Fields) {
        const auto& value = this->*field.Member;
        if (!value.empty()) {
            const bool secret = field.Member == &TOidcCliOptions::ClientSecret || field.Member == &TOidcCliOptions::AccessToken;
            output << field.Option << ": " << (secret ? TString("***") : value) << Endl;
        }
    }
}

TOidcAuthOptions AddOidcOptions(TClientCommandOptions& options, TOidcCliOptions& values, bool profileCommand) {
    auto& config = options.AddAuthMethodOption("oidc-config", "OIDC credentials YAML configuration file", true);
    config.AuthMethod("oidc-config");
    config.RequiredArgument("PATH").StoreResult(&values.ConfigFile);
    if (profileCommand) {
        config.Handler([](const TString& value) {
            if (value.empty()) {
                throw std::invalid_argument("--oidc-config must not be empty");
            }
        });
    }
    if (!profileCommand) {
        config.SimpleProfileDataParam("oidc-config", false)
            .Env("YDB_OIDC_CONFIG", false)
            .LogToConnectionParams("oidc-config");
    }

    TAuthMethodOption* issuer = nullptr;
    for (const auto& field : Fields) {
        const bool mainOption = field.Member == &TOidcCliOptions::Issuer;
        auto& option = options.AddAuthMethodOption(field.Option, field.Help, mainOption);
        option.AuthMethod("oidc");
        option.RequiredArgument("VALUE");
        if (field.Member == &TOidcCliOptions::Scope) {
            option.Handler([&values, profileCommand](const TString& scope) {
                if (profileCommand && scope.empty()) {
                    throw std::invalid_argument("--oidc-scope must not be empty");
                }
                if (!values.Scope.empty()) {
                    values.Scope += ' ';
                }
                values.Scope += scope;
            });
        } else {
            option.StoreResult(&(values.*field.Member));
            if (profileCommand) {
                option.Handler([name = TString(field.Option)](const TString& value) {
                    if (value.empty()) {
                        throw std::invalid_argument(std::string("--") + std::string(name) + " must not be empty");
                    }
                });
            }
        }
        if (!profileCommand) {
            option.AuthProfileParser(ProfileFieldParser(field), "oidc")
                .Env(field.Env, false)
                .LogToConnectionParams(field.Option);
        }
        if (mainOption) {
            issuer = &option;
        }
    }
    return {config, *issuer};
}

void ResolveOidcOptions(TOidcCliOptions& values, const TOptionsParseResult& result) {
    const auto& method = result.GetChosenAuthMethod();
    for (const auto& field : Fields) {
        const auto* parsed = result.FindResult(field.Option);
        if (parsed != nullptr && parsed->GetValueSource() == EOptionValueSource::Explicit) {
            if (method != "oidc") {
                throw std::invalid_argument("Direct OIDC options require --oidc-issuer and cannot be combined with another authentication method");
            }
            if ((values.*field.Member).empty()) {
                throw std::invalid_argument(std::string("--") + field.Option + " must not be empty");
            }
        }
    }
    if (method == "oidc-config") {
        TString path = std::move(values.ConfigFile);
        values = {};
        values.ConfigFile = std::move(path);
        if (values.ConfigFile.empty()) {
            throw std::invalid_argument("--oidc-config must not be empty");
        }
    } else if (method == "oidc") {
        const auto* issuer = result.FindResult("oidc-issuer");
        const auto* clientId = result.FindResult("oidc-client-id");
        const auto* accessToken = result.FindResult("oidc-access-token");
        for (const auto& field : Fields) {
            const auto* parsed = result.FindResult(field.Option);
            if (parsed == nullptr || !IsProfileSource(parsed->GetValueSource())) {
                continue;
            }
            const bool differentIssuerSource = issuer != nullptr && parsed->GetValueSource() != issuer->GetValueSource();
            const bool differentClientSource = field.Member == &TOidcCliOptions::ClientSecret && clientId != nullptr &&
                parsed->GetValueSource() != clientId->GetValueSource();
            const bool differentTokenSource = field.Member == &TOidcCliOptions::ExpiresAt && accessToken != nullptr &&
                parsed->GetValueSource() != accessToken->GetValueSource();
            if (differentIssuerSource || differentClientSource || differentTokenSource) {
                values.*field.Member = GetEnv(field.Env);
            }
        }
    } else {
        values = {};
        return;
    }
    values.MakeConfig();
}

} // namespace NYdb::NConsoleClient
