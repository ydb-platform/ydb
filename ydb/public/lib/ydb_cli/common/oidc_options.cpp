#include "oidc_options.h"

#include "client_command_options.h"
#include "common.h"
#include "oidc_token_cache.h"

#include <util/generic/strbuf.h>
#include <util/stream/output.h>
#include <util/string/split.h>
#include <util/system/env.h>

#include <yaml-cpp/yaml.h>

#include <stdexcept>
#include <utility>

namespace NYdb::NConsoleClient {
namespace {

constexpr TStringBuf OIDC_METHOD = "oidc";
constexpr TStringBuf ISSUER_OPTION = "oidc-issuer";
constexpr TStringBuf FLOW_OPTION = "oidc-flow";
constexpr TStringBuf CLIENT_ID_OPTION = "oidc-client-id";
constexpr TStringBuf CLIENT_SECRET_FILE_OPTION = "oidc-client-secret-file";
constexpr TStringBuf ACCESS_TOKEN_FILE_OPTION = "oidc-access-token-file";
constexpr TStringBuf SCOPE_OPTION = "oidc-scope";
constexpr TStringBuf CACHE_PATH_OPTION = "oidc-cache-path";
constexpr TStringBuf ISSUER_ENV = "YDB_OIDC_ISSUER";
constexpr TStringBuf FLOW_ENV = "YDB_OIDC_FLOW";
constexpr TStringBuf CLIENT_ID_ENV = "YDB_OIDC_CLIENT_ID";
constexpr TStringBuf CLIENT_SECRET_ENV = "YDB_OIDC_CLIENT_SECRET";
constexpr TStringBuf ACCESS_TOKEN_ENV = "YDB_OIDC_ACCESS_TOKEN";
constexpr TStringBuf SCOPE_ENV = "YDB_OIDC_SCOPE";
constexpr TStringBuf CACHE_PATH_ENV = "YDB_OIDC_CACHE_PATH";
constexpr TStringBuf ISSUER_KEY = "issuer";
constexpr TStringBuf STATIC_FLOW = "static";
constexpr TStringBuf CLIENT_FLOW = "client";
constexpr TStringBuf DEVICE_FLOW = "device";
constexpr TStringBuf EMPTY_OPTION_ERROR = " must not be empty";
constexpr TStringBuf OPTION_PREFIX = "--";
constexpr TStringBuf VALUE_SEPARATOR = ": ";
constexpr TStringBuf MASKED_VALUE = ": ***";

struct TField {
    const char* Option;
    const char* Env;
    const char* Profile;
    const char* Help;
    TString TOidcCliOptions::* Member;
};

constexpr TField FIELDS[] = {
    {ISSUER_OPTION.data(), ISSUER_ENV.data(), ISSUER_KEY.data(), "OIDC issuer URL (HTTPS)", &TOidcCliOptions::Issuer},
    {FLOW_OPTION.data(), FLOW_ENV.data(), "flow", "OIDC flow: static, client or device (default: static with --oidc-access-token-file or YDB_OIDC_ACCESS_TOKEN, otherwise device). Device flow requires browser sign-in, including noninteractive runs; use client or static flow for unattended automation", &TOidcCliOptions::Flow},
    {CLIENT_ID_OPTION.data(), CLIENT_ID_ENV.data(), "client_id", "OIDC client ID for client or device flow", &TOidcCliOptions::ClientId},
    {CLIENT_SECRET_FILE_OPTION.data(), CLIENT_SECRET_ENV.data(), "client_secret_file", "File containing the OIDC client secret for client flow", &TOidcCliOptions::ClientSecretFile},
    {ACCESS_TOKEN_FILE_OPTION.data(), ACCESS_TOKEN_ENV.data(), "access_token_file", "File containing an OIDC access token; Bearer prefix is optional", &TOidcCliOptions::AccessTokenFile},
    {SCOPE_OPTION.data(), SCOPE_ENV.data(), "scope", "Space-separated OIDC scopes; may be repeated. openid is added automatically", &TOidcCliOptions::Scope},
    {CACHE_PATH_OPTION.data(), CACHE_PATH_ENV.data(), "cache_path", "OIDC token cache file path", &TOidcCliOptions::CachePath},
};

TAuthMethodOption::TProfileParser ProfileFieldParser(const TField& field);
bool IsProfileSource(EOptionValueSource source);
bool IsSecretFile(const TField& field);
TString GetFlow(const TOidcCliOptions& options);
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
        if (key == ISSUER_KEY && (data["access_token"].IsDefined() || data["client_secret"].IsDefined())) {
            if (errors != nullptr) {
                errors->push_back("Inline OIDC secrets are not supported; use access_token_file or client_secret_file");
            }
            return false;
        }
        const auto node = data[std::string(key)];
        if (!node.IsDefined()) {
            if (key == ISSUER_KEY && errors != nullptr) {
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

bool IsSecretFile(const TField& field) {
    return field.Member == &TOidcCliOptions::ClientSecretFile || field.Member == &TOidcCliOptions::AccessTokenFile;
}

TString GetFlow(const TOidcCliOptions& options) {
    if (!options.Flow.empty()) {
        return options.Flow;
    }
    return TString((options.AccessTokenFile.empty() && GetEnv(TString(ACCESS_TOKEN_ENV)).empty()) ? DEVICE_FLOW : STATIC_FLOW);
}

void CheckAllowedFields(const TOidcCliOptions& options, const TString& flow) {
    if (flow == STATIC_FLOW) {
        if (!options.ClientId.empty() || !options.ClientSecretFile.empty() || !options.Scope.empty()) {
            throw std::invalid_argument("Static OIDC flow does not accept client ID, client secret or scopes");
        }
    } else if (flow == CLIENT_FLOW || flow == DEVICE_FLOW) {
        if (!options.AccessTokenFile.empty()) {
            throw std::invalid_argument("Access token file requires static OIDC flow");
        }
        if (flow == DEVICE_FLOW && !options.ClientSecretFile.empty()) {
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
    for (const auto& field : FIELDS) {
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
    const TString flow = GetFlow(*this);
    CheckAllowedFields(*this, flow);

    NOidc::TOidcConfig config;
    config.Issuer = std::string(Issuer);
    std::vector<std::string> scopes;
    for (const auto scope : StringSplitter(Scope).SplitBySet(" \t\r\n").SkipEmpty()) {
        scopes.emplace_back(scope.Token());
    }
    if (flow == STATIC_FLOW) {
        TString token = AccessTokenFile.empty() ? GetEnv(TString(ACCESS_TOKEN_ENV))
            : ReadFromFile(AccessTokenFile, "OIDC access token", false);
        // The SDK adds Bearer itself; accept either raw or prefixed tokens.
        if (token.StartsWith("Bearer ")) {
            token = token.substr(7);
        }
        config.FlowConfig = NOidc::TStaticOidcConfig{
            .AccessToken = std::string(token),
            .ExpiresAt = std::nullopt,
        };
    } else if (flow == CLIENT_FLOW) {
        const TString secret = ClientSecretFile.empty() ? GetEnv(TString(CLIENT_SECRET_ENV))
            : ReadFromFile(ClientSecretFile, "OIDC client secret", false);
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
    auth["method"] = std::string(OIDC_METHOD);
    for (const auto& field : FIELDS) {
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
    for (const auto& field : FIELDS) {
        const auto& value = this->*field.Member;
        if (!value.empty()) {
            output << field.Option << VALUE_SEPARATOR << value << Endl;
        }
    }
    const TString flow = GetFlow(*this);
    if (flow == CLIENT_FLOW && ClientSecretFile.empty() && !GetEnv(TString(CLIENT_SECRET_ENV)).empty()) {
        output << CLIENT_SECRET_ENV << MASKED_VALUE << Endl;
    }
    if (flow == STATIC_FLOW && AccessTokenFile.empty() && !GetEnv(TString(ACCESS_TOKEN_ENV)).empty()) {
        output << ACCESS_TOKEN_ENV << MASKED_VALUE << Endl;
    }
}

TAuthMethodOption& AddOidcOptions(TClientCommandOptions& options, TOidcCliOptions& values, bool profileCommand) {
    TAuthMethodOption* issuer = nullptr;
    for (const auto& field : FIELDS) {
        const bool mainOption = field.Member == &TOidcCliOptions::Issuer;
        auto& option = options.AddAuthMethodOption(field.Option, field.Help, mainOption);
        option.AuthMethod(TString(OIDC_METHOD));
        option.RequiredArgument(IsSecretFile(field) ? "PATH" : "VALUE");
        if (IsSecretFile(field) && !profileCommand) {
            // Resolve the source before storing a path: environment variables contain
            // literal credentials, and files from unselected profiles must not be read.
            option.Handler([](const TString&) {});
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
                        throw std::invalid_argument(std::string(OPTION_PREFIX) + std::string(name) + std::string(EMPTY_OPTION_ERROR));
                    }
                });
            }
        }
        if (!profileCommand) {
            option.AuthProfileParser(ProfileFieldParser(field), TString(OIDC_METHOD))
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
    for (const auto& field : FIELDS) {
        const auto* parsed = result.FindResult(field.Option);
        if (parsed != nullptr && parsed->GetValueSource() == EOptionValueSource::Explicit) {
            if (method != OIDC_METHOD) {
                throw std::invalid_argument("Direct OIDC options require --oidc-issuer and cannot be combined with another authentication method");
            }
            if (parsed->Values().back().empty()) {
                throw std::invalid_argument(std::string(OPTION_PREFIX) + field.Option + std::string(EMPTY_OPTION_ERROR));
            }
        }
    }
    if (method == OIDC_METHOD) {
        for (const auto& field : FIELDS) {
            const auto* parsed = result.FindResult(field.Option);
            if (IsSecretFile(field) && parsed != nullptr && parsed->GetValueSource() != EOptionValueSource::EnvironmentVariable) {
                values.*field.Member = parsed->Values().back();
            }
        }
        const auto* issuer = result.FindResult(TString(ISSUER_OPTION));
        const auto* clientId = result.FindResult(TString(CLIENT_ID_OPTION));
        for (const auto& field : FIELDS) {
            const auto* parsed = result.FindResult(field.Option);
            if (parsed == nullptr || !IsProfileSource(parsed->GetValueSource())) {
                continue;
            }
            const bool differentIssuerSource = issuer != nullptr && parsed->GetValueSource() != issuer->GetValueSource();
            const bool differentClientSource = field.Member == &TOidcCliOptions::ClientSecretFile && clientId != nullptr &&
                parsed->GetValueSource() != clientId->GetValueSource();
            if (differentIssuerSource || differentClientSource) {
                values.*field.Member = !IsSecretFile(field) && field.Env != nullptr ? GetEnv(field.Env) : TString();
            }
        }
    } else {
        values = {};
        return;
    }
    // An OIDC profile can coexist with environment settings for another flow.
    // Ignore incompatible ambient fields, but still reject incompatible CLI/profile fields.
    const auto* issuer = result.FindResult(TString(ISSUER_OPTION));
    if (issuer != nullptr && IsProfileSource(issuer->GetValueSource())) {
        const TString flow = GetFlow(values);
        for (const auto& field : FIELDS) {
            const auto* parsed = result.FindResult(field.Option);
            if (parsed != nullptr && parsed->GetValueSource() == EOptionValueSource::EnvironmentVariable &&
                flow == STATIC_FLOW && (field.Member == &TOidcCliOptions::ClientId || field.Member == &TOidcCliOptions::Scope))
            {
                (values.*field.Member).clear();
            }
        }
    }
    values.ResolvedConfig = values.MakeConfig();
}

} // namespace NYdb::NConsoleClient
