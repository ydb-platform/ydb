#include "oidc_options.h"

#include "client_command_options.h"
#include "common.h"
#include "oidc_token_cache.h"

#include <util/stream/output.h>
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
    {"oidc-flow", "YDB_OIDC_FLOW", "flow", "OIDC flow: static, client or device (default: static with --oidc-token-file, otherwise device)", &TOidcCliOptions::Flow},
    {"oidc-client-id", "YDB_OIDC_CLIENT_ID", "client_id", "OIDC client ID for client or device flow", &TOidcCliOptions::ClientId},
    {"oidc-client-secret-file", "YDB_OIDC_CLIENT_SECRET", "client_secret_file", "File containing the OIDC client secret for client flow", &TOidcCliOptions::ClientSecretFile},
    {"oidc-token-file", nullptr, "access_token_file", "File containing an OIDC access token; Bearer prefix is optional", &TOidcCliOptions::AccessTokenFile},
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
        if (key == "issuer" && (data["access_token"].IsDefined() || data["client_secret"].IsDefined())) {
            if (errors != nullptr) {
                errors->push_back("Inline OIDC secrets are not supported; use access_token_file or client_secret_file");
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
        if (!options.ClientId.empty() || !options.ClientSecret.empty() || !options.ClientSecretFile.empty() || !options.Scope.empty()) {
            throw std::invalid_argument("Static OIDC flow does not accept client ID, client secret or scopes");
        }
    } else if (flow == "client" || flow == "device") {
        if (!options.AccessTokenFile.empty()) {
            throw std::invalid_argument("Access token file requires static OIDC flow");
        }
        if (flow == "device" && (!options.ClientSecret.empty() || !options.ClientSecretFile.empty())) {
            throw std::invalid_argument("Client secret requires client OIDC flow");
        }
    } else {
        throw std::invalid_argument("--oidc-flow must be static, client or device");
    }
}

} // namespace

bool TOidcCliOptions::IsConfigured() const {
    return !Issuer.empty();
}

bool TOidcCliOptions::HasOptions() const {
    if (!ClientSecret.empty()) {
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
    if (Issuer.empty()) {
        throw std::invalid_argument("OIDC authentication requires --oidc-issuer or YDB_OIDC_ISSUER");
    }
    const TString flow = Flow.empty() ? TString(AccessTokenFile.empty() ? "device" : "static") : Flow;
    CheckAllowedFields(*this, flow);

    NOidc::TOidcConfig config;
    config.Issuer = std::string(Issuer);
    std::vector<std::string> scopes;
    for (const auto scope : StringSplitter(Scope).SplitBySet(" \t\r\n").SkipEmpty()) {
        scopes.emplace_back(scope.Token());
    }
    if (flow == "static") {
        TString token = AccessTokenFile.empty() ? GetEnv("YDB_TOKEN")
            : ReadFromFile(AccessTokenFile, "OIDC access token", false);
        // The SDK adds Bearer itself; accept either raw or prefixed tokens.
        if (token.StartsWith("Bearer ")) {
            token = token.substr(7);
        }
        config.FlowConfig = NOidc::TStaticOidcConfig{
            .AccessToken = std::string(token),
            .ExpiresAt = std::nullopt,
        };
    } else if (flow == "client") {
        TString secret = ClientSecret;
        if (secret.empty()) {
            secret = ClientSecretFile.empty() ? GetEnv("YDB_OIDC_CLIENT_SECRET")
                : ReadFromFile(ClientSecretFile, "OIDC client secret", false);
        }
        config.FlowConfig = NOidc::TClientOidcConfig{
            .ClientId = std::string(ClientId),
            .ClientSecret = std::string(secret),
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
    auth["method"] = "oidc";
    for (const auto& field : Fields) {
        const auto& value = this->*field.Member;
        if (!value.empty()) {
            auth["data"][field.Profile] = std::string(value);
        }
    }
    return auth;
}

void TOidcCliOptions::Print(IOutputStream& output) const {
    if (Issuer.empty()) {
        return;
    }
    for (const auto& field : Fields) {
        const auto& value = this->*field.Member;
        if (!value.empty()) {
            output << field.Option << ": " << value << Endl;
        }
    }
    if (!ClientSecret.empty()) {
        output << "YDB_OIDC_CLIENT_SECRET: ***" << Endl;
    }
}

TAuthMethodOption& AddOidcOptions(TClientCommandOptions& options, TOidcCliOptions& values, bool profileCommand) {
    TAuthMethodOption* issuer = nullptr;
    for (const auto& field : Fields) {
        const bool mainOption = field.Member == &TOidcCliOptions::Issuer;
        auto& option = options.AddAuthMethodOption(field.Option, field.Help, mainOption);
        option.AuthMethod("oidc");
        option.RequiredArgument(field.Member == &TOidcCliOptions::ClientSecretFile || field.Member == &TOidcCliOptions::AccessTokenFile ? "PATH" : "VALUE");
        if (field.Member == &TOidcCliOptions::ClientSecretFile && !profileCommand) {
            // Keep the selected value until authentication and profile sources are resolved.
            // Reading a file here would also read secrets from an unselected profile.
            option.StoreResult(&values.ClientSecret);
        } else if (field.Member == &TOidcCliOptions::Scope) {
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
                .LogToConnectionParams(field.Option);
            if (field.Env != nullptr) {
                option.Env(field.Env, false);
            }
        }
        if (mainOption) {
            issuer = &option;
        }
    }
    return *issuer;
}

void ResolveOidcOptions(TOidcCliOptions& values, const TOptionsParseResult& result) {
    const auto& method = result.GetChosenAuthMethod();
    const auto* secret = result.FindResult("oidc-client-secret-file");
    if (method == "oidc" && secret != nullptr && secret->GetValueSource() != EOptionValueSource::EnvironmentVariable) {
        values.ClientSecretFile = std::move(values.ClientSecret);
        values.ClientSecret.clear();
    }
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
    if (method == "oidc") {
        const auto* issuer = result.FindResult("oidc-issuer");
        const auto* clientId = result.FindResult("oidc-client-id");
        for (const auto& field : Fields) {
            const auto* parsed = result.FindResult(field.Option);
            if (parsed == nullptr || !IsProfileSource(parsed->GetValueSource())) {
                continue;
            }
            const bool differentIssuerSource = issuer != nullptr && parsed->GetValueSource() != issuer->GetValueSource();
            const bool differentClientSource = field.Member == &TOidcCliOptions::ClientSecretFile && clientId != nullptr &&
                parsed->GetValueSource() != clientId->GetValueSource();
            if (differentIssuerSource || differentClientSource) {
                if (field.Member == &TOidcCliOptions::ClientSecretFile) {
                    values.ClientSecretFile.clear();
                    values.ClientSecret = GetEnv(field.Env);
                } else {
                    values.*field.Member = field.Env != nullptr ? GetEnv(field.Env) : TString();
                }
            }
        }
    } else {
        values = {};
        return;
    }
    values.MakeConfig();
}

} // namespace NYdb::NConsoleClient
