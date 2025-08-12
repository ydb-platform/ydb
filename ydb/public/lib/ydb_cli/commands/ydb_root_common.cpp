#include "ydb_root_common.h"
#include "ydb_profile.h"
#include "ydb_admin.h"
#include "ydb_debug.h"
#include "ydb_service_auth.h"
#include "ydb_service_discovery.h"
#include "ydb_service_export.h"
#include "ydb_service_import.h"
#include "ydb_service_operation.h"
#include "ydb_service_monitoring.h"
#include "ydb_service_scheme.h"
#include "ydb_service_scripting.h"
#include "ydb_service_table.h"
#include "ydb_service_topic.h"
#include "ydb_sql.h"
#include "ydb_tools.h"
#include "ydb_yql.h"
#include "ydb_workload.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/interactive_cli.h>
#include <ydb/public/lib/ydb_cli/common/cert_format_converter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/from_file.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/jwt_token_source.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>
#include <util/string/builder.h>
#include <util/system/env.h>

namespace NYdb {
namespace NConsoleClient {

TClientCommandRootCommon::TClientCommandRootCommon(const TString& name, const TClientSettings& settings)
    : TClientCommandRootBase(name)
    , Settings(settings)
{
    ValidateSettings();
    AddDangerousCommand(std::make_unique<TCommandAdmin>());
    AddCommand(std::make_unique<TCommandAuth>());
    AddCommand(std::make_unique<TCommandDiscovery>());
    AddCommand(std::make_unique<TCommandScheme>());
    AddHiddenCommand(std::make_unique<TCommandScripting>());
    AddCommand(std::make_unique<TCommandTable>());
    AddCommand(std::make_unique<TCommandTools>());
    AddCommand(std::make_unique<TCommandExport>(Settings.UseExportToYt.GetRef()));
    AddCommand(std::make_unique<TCommandImport>());
    AddCommand(std::make_unique<TCommandMonitoring>());
    AddCommand(std::make_unique<TCommandOperation>());
    AddCommand(std::make_unique<TCommandConfig>());
    AddCommand(std::make_unique<TCommandInit>());
    AddCommand(std::make_unique<TCommandSql>());
    AddCommand(std::make_unique<TCommandYql>());
    AddCommand(std::make_unique<TCommandTopic>());
    AddCommand(std::make_unique<TCommandWorkload>());
    AddCommand(std::make_unique<TCommandDebug>());
    PropagateFlags(TCommandFlags{.Dangerous = false, .OnlyExplicitProfile = false});
}

void TClientCommandRootCommon::ValidateSettings() {
    if (!Settings.EnableSsl.Defined()) {
        Cerr << "Missing ssl enabling flag in client settings" << Endl;
    } else if (!Settings.UseAccessToken.Defined()) {
        Cerr << "Missing access token usage flag in client settings" << Endl;
    } else if (!Settings.UseDefaultTokenFile.Defined()) {
        Cerr << "Missing default token file usage flag in client settings" << Endl;
    } else if (!Settings.UseIamAuth.Defined()) {
        Cerr << "Missing IAM authentication usage flag in client settings" << Endl;
    } else if (!Settings.UseExportToYt.Defined()) {
        Cerr << "Missing export to YT command usage flag in client settings" << Endl;
    } else if (!Settings.UseStaticCredentials.Defined()) {
        Cerr << "Missing static credentials usage flag in client settings" << Endl;
    } else if (!Settings.MentionUserAccount.Defined()) {
        Cerr << "Missing user account mentioning flag in client settings" << Endl;
    } else if (!Settings.UseOauth2TokenExchange.Defined()) {
        Cerr << "Missing OAuth 2.0 token exchange credentials usage flag in client settings" << Endl;
    } else if (!Settings.YdbDir) {
        Cerr << "Missing YDB directory in client settings" << Endl;
    } else {
        return;
    }
    throw yexception() << "Invalid client settings";
}

void TClientCommandRootCommon::FillConfig(TConfig& config) {
    config.UseAccessToken = Settings.UseAccessToken.GetRef();
    config.UseIamAuth = Settings.UseIamAuth.GetRef();
    config.UseStaticCredentials = Settings.UseStaticCredentials.GetRef();
    config.UseOauth2TokenExchange = Settings.UseOauth2TokenExchange.GetRef();
    config.UseExportToYt = Settings.UseExportToYt.GetRef();
    config.StorageUrl = Settings.StorageUrl;
    SetCredentialsGetter(config);
}

// Default CredentialsGetter that can be overridden in different CLI versions
void TClientCommandRootCommon::SetCredentialsGetter(TConfig& config) {
    config.CredentialsGetter = [](const TClientCommand::TConfig& config) {
        if (config.SecurityToken) {
            return CreateOAuthCredentialsProviderFactory(config.SecurityToken);
        }

        if (config.UseStaticCredentials) {
            if (!config.StaticCredentials.User.empty()) {
                return CreateLoginCredentialsProviderFactory(config.StaticCredentials);
            }
        }

        if (config.UseOauth2TokenExchange) {
            if (config.Oauth2KeyFile) {
                return CreateOauth2TokenExchangeFileCredentialsProviderFactory(config.Oauth2KeyFile, config.IamEndpoint);
            }
        }

        return CreateInsecureCredentialsProviderFactory();
    };
}

void TClientCommandRootCommon::Config(TConfig& config) {
    FillConfig(config);
    TClientCommandOptions& opts = *config.Opts;

    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    TStringBuilder endpointHelp;
    endpointHelp << "Endpoint to connect. Protocols: "
        << colors.BoldColor() << "grpc" << colors.OldColor() << ", "
        << colors.BoldColor() << "grpcs" << colors.OldColor() << " (default: "
        << colors.Cyan() << (Settings.EnableSsl.GetRef() ? "grpcs" : "grpc" ) << colors.OldColor() << ")";

    EnableSsl = Settings.EnableSsl.GetRef(); // default
    opts.AddLongOption('e', "endpoint", endpointHelp)
        .RequiredArgument("[PROTOCOL://]HOST[:PORT]")
        .ProfileParam("endpoint")
        .LogToConnectionParams("endpoint")
        .Handler([this](const TString& value) {
            // Parse address & enable SSL at one time
            Address = GetAddressFromString(value, &EnableSsl);
        })
        .Validator([](const TString& value) {
            // Get errors from parsing
            std::vector<TString> errors;
            GetAddressFromString(value, nullptr, &errors);
            return errors;
        });
    opts.AddLongOption('d', "database", "Database to work with")
        .RequiredArgument("PATH").StoreResult(&Database)
        .ProfileParam("database")
        .LogToConnectionParams("database");
    opts.GetOpts().AddLongOption('v', "verbose", "Increase verbosity of operations")
        .Optional().NoArgument().Handler0([&](){
            VerbosityLevel++;
        });
    opts.AddLongOption('p', "profile", "Profile name to use configuration parameters from")
        .RequiredArgument("NAME").StoreResult(&ProfileName);
    opts.AddLongOption('y', "assume-yes", "Automatic yes to prompts; assume \"yes\" as answer to all prompts and run non-interactively")
        .Optional().StoreTrue(&config.AssumeYes);

    if (config.HelpCommandVerbosiltyLevel >= 2) {
        opts.AddLongOption("no-discovery", "Do not perform discovery (client balancing) for ydb cluster connection."
            " If this option is set the user provided endpoint (by -e option) will be used to setup a connections")
            .Optional().StoreTrue(&config.SkipDiscovery);
    } else {
        opts.AddLongOption("no-discovery")
            .Optional().Hidden().StoreTrue(&config.SkipDiscovery);
    }

    TClientCommandRootBase::Config(config);

    TAuthMethodOption* iamTokenAuth = nullptr;
    TAuthMethodOption* ycTokenAuth = nullptr;
    TAuthMethodOption* metadataAuth = nullptr;
    TAuthMethodOption* saKeyAuth = nullptr;
    TAuthMethodOption* ydbTokenAuth = nullptr;
    TAuthMethodOption* ydbUserAuth = nullptr;
    TAuthMethodOption* oauth2TokenExchangeAuth = nullptr;

    if (config.UseIamAuth) {
        const TString docsUrl = "cloud.yandex.ru/docs";
        iamTokenAuth = &opts.AddAuthMethodOption("iam-token-file", "IAM token file. Note: IAM tokens expire in 12 hours");
        (*iamTokenAuth)
            .AuthMethod("token")
            .SimpleProfileDataParam("iam-token", false)
            .DocLink(TStringBuilder() << docsUrl << "/iam/concepts/authorization/iam-token")
            .LogToConnectionParams("token")
            .Env("IAM_TOKEN", false)
            .FileName("token").RequiredArgument("PATH")
            .StoreFilePath(&TokenFile)
            .StoreResult(&config.SecurityToken);

        ycTokenAuth = &opts.AddAuthMethodOption("yc-token-file", "YC token file. It should contain OAuth token of a Yandex Passport user to get IAM token with");
        (*ycTokenAuth)
            .AuthMethod("yc-token")
            .SimpleProfileDataParam("yc-token", false) // parse token directly from profile
            .SimpleProfileDataParam("yc-token-file", true) // parse token from file specified in profile
            .DocLink(TStringBuilder() << docsUrl << "/iam/concepts/authorization/oauth-token")
            .LogToConnectionParams("yc-token")
            .Env("YC_TOKEN", false)
            .FileName("YC token").RequiredArgument("PATH")
            .StoreResult(&config.YCToken);

        metadataAuth = &opts.AddAuthMethodOption("use-metadata-credentials", "Use metadata service on a virtual machine to get credentials");
        (*metadataAuth)
            .AuthMethod("use-metadata-credentials")
            .SimpleProfileDataParam()
            .DocLink(TStringBuilder() << docsUrl << "/compute/operations/vm-connect/auth-inside-vm")
            .LogToConnectionParams("use-metadata-credentials")
            .Env("USE_METADATA_CREDENTIALS", false)
            .StoreTrue(&config.UseMetadataCredentials);

        saKeyAuth = &opts.AddAuthMethodOption("sa-key-file", TStringBuilder() << "Service account" << (Settings.MentionUserAccount.GetRef() ? " (or user account) key file" : " key file"));
        (*saKeyAuth)
            .AuthMethod("sa-key-file")
            .SimpleProfileDataParam("sa-key-file", true)
            .DocLink(TStringBuilder() << docsUrl << "/iam/operations/iam-token/create-for-sa")
            .LogToConnectionParams("sa-key-file")
            .Env("SA_KEY_FILE", true, "SA key file")
            .FileName("SA key file").RequiredArgument("PATH")
            .StoreFilePath(&config.SaKeyFile)
            .StoreResult(&config.SaKeyParams);
    }

    if (config.UseAccessToken) {
        ydbTokenAuth = &opts.AddAuthMethodOption("token-file", "Access token file");
        (*ydbTokenAuth)
            .AuthMethod("token")
            .SimpleProfileDataParam()
            .SimpleProfileDataParam("token-file", true)
            .SimpleProfileDataParam("ydb-token", false)
            .LogToConnectionParams("token")
            .Env("YDB_TOKEN", false)
            .FileName("token").RequiredArgument("PATH")
            .StoreFilePath(&TokenFile)
            .StoreResult(&config.SecurityToken);
        if (Settings.UseDefaultTokenFile.GetRef()) {
            (*ydbTokenAuth)
                .DefaultValue(defaultTokenFile);
        }
    }

    if (config.UseStaticCredentials) {
        auto parser = [this](const YAML::Node& authData, TString* value, bool* isFileName, std::vector<TString>* errors, bool parseOnly) -> bool {
            Y_UNUSED(isFileName);
            TString user, password;
            bool hasPasswordOption = false;
            if (authData["user"]) {
                user = authData["user"].as<TString>();
            }
            if (authData["password"]) {
                hasPasswordOption = true;
                password = authData["password"].as<TString>();
            }
            if (authData["password-file"]) {
                if (hasPasswordOption) {
                    if (errors) {
                        errors->push_back(TStringBuilder() << "Profile has both password and password-file");
                    }
                    return false;
                }
                PasswordFile = authData["password-file"].as<TString>();
                TString fileContent;
                if (!ReadFromFileIfExists(PasswordFile, "password", fileContent, true)) {
                    if (errors) {
                        errors->push_back(TStringBuilder() << "Couldn't read password from file " << PasswordFile);
                    }
                    return false;
                }
                password = std::move(fileContent);
            }
            if (value) {
                *value = user;
            }
            // Assign values
            if (!parseOnly) {
                if (!password.empty()) {
                    DoNotAskForPassword = true;
                }
                UserName = std::move(user);
                Password = std::move(password);
            }
            return true;
        };
        ydbUserAuth = &opts.AddAuthMethodOption("user", "User name to authenticate with");
        (*ydbUserAuth)
            .AuthMethod("static-credentials")
            .AuthProfileParser(std::move(parser))
            .LogToConnectionParams("user")
            .Env("YDB_USER", false)
            .RequiredArgument("NAME").StoreResult(&UserName);

        auto& passwordOpt = opts.AddLongOption("password-file", "File with password to authenticate with")
            .Env("YDB_PASSWORD", false)
            .SetSupportsProfile()
            .FileName("password").RequiredArgument("PATH")
            .StoreFilePath(&PasswordFileOption)
            .StoreResult(&PasswordOption);

        auto& noPasswordOpt = opts.AddLongOption("no-password", "Do not ask for user password (if empty)")
            .Optional().StoreTrue(&DoNotAskForPassword);

        opts.MutuallyExclusiveOpt(passwordOpt, noPasswordOpt);
    }

    if (config.UseOauth2TokenExchange) {
        TOauth2TokenExchangeParams defaultParams;
        TJwtTokenSourceParams defaultJwtParams;

#define FIELD(name) "  " << colors.BoldColor() << name << colors.OldColor() << ": "
#define TYPE(type) "[" << colors.YellowColor() << type << colors.OldColor() << "] "
#define TYPE2(type1, type2) "[" << colors.YellowColor() << type1 << colors.OldColor() << " | " << colors.YellowColor() << type2 << colors.OldColor() << "] "
#define DEFAULT(value) " (default: " << colors.CyanColor() << value << colors.OldColor() << ")"

        oauth2TokenExchangeAuth = &opts.AddAuthMethodOption("oauth2-key-file", "OAuth 2.0 RFC8693 token exchange credentials parameters json file");
        (*oauth2TokenExchangeAuth)
            .AuthMethod("oauth2-key-file")
            .SimpleProfileDataParam("oauth2-key-file", true)
            .LogToConnectionParams("oauth2-key-file")
            .DocLink("ydb.tech/docs/en/reference/ydb-cli/connect")
            .Env("YDB_OAUTH2_KEY_FILE", true, "OAuth 2 key file")
            .RequiredArgument("PATH")
            .FileName("OAuth 2 key file")
            .StoreFilePath(&config.Oauth2KeyFile)
            .StoreResult(&config.Oauth2KeyParams);

        if (config.HelpCommandVerbosiltyLevel >= 2) {
            TStringBuilder additionalHelp;
            additionalHelp << "Detailed information about OAuth 2.0 token exchange protocol: https://www.rfc-editor.org/rfc/rfc8693" << Endl << Endl;

            TStringBuilder supportedJwtAlgorithms;
            for (const std::string& alg : GetSupportedOauth2TokenExchangeJwtAlgorithms()) {
                if (supportedJwtAlgorithms) {
                    supportedJwtAlgorithms << ", ";
                }
                supportedJwtAlgorithms << colors.BoldColor() << alg << colors.OldColor();
            }

            additionalHelp
                << "Fields of json file:" << Endl
                << FIELD("grant-type") "          " TYPE("string") "Grant type" DEFAULT(defaultParams.GrantType_) << Endl
                << FIELD("res") "                 " TYPE("string") "Resource (optional)" << Endl
                << FIELD("aud") "                 " TYPE2("string", "list of strings") "Audience option for token exchange request (optional)" << Endl
                << FIELD("scope") "               " TYPE2("string", "list of strings") "Scope (optional)" << Endl
                << FIELD("requested-token-type") "" TYPE("string") "Requested token type" DEFAULT(defaultParams.RequestedTokenType_) << Endl
                << FIELD("subject-credentials") " " TYPE("creds_json") "Subject credentials (optional)" << Endl
                << FIELD("actor-credentials") "   " TYPE("creds_json") "Actor credentials (optional)" << Endl
                << Endl
                << "Fields of " << colors.BoldColor() << "creds_json" << colors.OldColor() << " (JWT):" << Endl
                << FIELD("type") "                " TYPE("string") "Token source type. Set " << colors.BoldColor() << "JWT" << colors.OldColor() << Endl
                << FIELD("alg") "                 " TYPE("string") "Algorithm for JWT signature. Supported algorithms: " << supportedJwtAlgorithms << Endl
                << FIELD("private-key") "         " TYPE("string") "(Private) key in PEM format for JWT signature" << Endl
                << FIELD("kid") "                 " TYPE("string") "Key id JWT standard claim (optional)" << Endl
                << FIELD("iss") "                 " TYPE("string") "Issuer JWT standard claim (optional)" << Endl
                << FIELD("sub") "                 " TYPE("string") "Subject JWT standard claim (optional)" << Endl
                << FIELD("aud") "                 " TYPE2("string", "list of strings") "Audience JWT standard claim (optional)" << Endl
                << FIELD("jti") "                 " TYPE("string") "JWT ID JWT standard claim (optional)" << Endl
                << FIELD("ttl") "                 " TYPE("string") "Token TTL" DEFAULT(defaultJwtParams.TokenTtl_) << Endl
                << Endl
                << "Fields of " << colors.BoldColor() << "creds_json" << colors.OldColor() << " (FIXED):" << Endl
                << FIELD("type") "                " TYPE("string") "Token source type. Set " << colors.BoldColor() << "FIXED" << colors.OldColor() << Endl
                << FIELD("token") "               " TYPE("string") "Token value" << Endl
                << FIELD("token-type") "          " TYPE("string") "Token type value. It will become subject_token_type/actor_token_type parameter in token exchange request (https://www.rfc-editor.org/rfc/rfc8693)" << Endl
                << Endl;

            additionalHelp
                << "Note that additionally you need to set " << colors.BoldColor() << "--iam-endpoint" << colors.OldColor() << " option" << Endl
                << "  in url format (SCHEMA://HOST:PORT/PATH) to configure endpoint.";

            oauth2TokenExchangeAuth->AdditionalHelpNotes(additionalHelp);
        }

#undef DEFAULT
#undef TYPE2
#undef TYPE
#undef FIELD
    }

    if (config.UseIamAuth) {
        opts.AddLongOption("iam-endpoint", "Endpoint of IAM service")
            .RequiredArgument("STR")
            .StoreResult(&config.IamEndpoint)
            .LogToConnectionParams("iam-endpoint")
            .ProfileParam("iam-endpoint")
            .DefaultValue(config.IamEndpoint);
    }

    // Special auth method that is not parsed from command line,
    // but is parsed from profile
    opts.AddAnonymousAuthMethodOption();

    opts.AddLongOption("profile-file", "Path to config file with profile data in yaml format")
        .RequiredArgument("PATH").StoreResult(&ProfileFile);

    opts.SetAuthMethodsEnvPriority(
        iamTokenAuth,
        ycTokenAuth,
        metadataAuth,
        saKeyAuth,
        ydbTokenAuth,
        ydbUserAuth,
        oauth2TokenExchangeAuth
    );

    TStringStream stream;
    stream << " [options...] <subcommand>" << Endl << Endl
        << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    RenderCommandDescription(stream, config.HelpCommandVerbosiltyLevel > 1, colors, BEGIN, "", true);
    stream << Endl << Endl << colors.BoldColor() << "Commands in " << colors.Red() << colors.BoldColor() <<  "admin" << colors.OldColor() << colors.BoldColor() << " subtree may treat global flags and profile differently, see corresponding help" << colors.OldColor() << Endl;

    // detailed help
    stream << Endl << Endl << colors.BoldColor() << "Detailed help" << colors.OldColor() << ":" << Endl;
    stream << colors.Green() << "-hh" << colors.OldColor() << ": Print detailed help" << Endl;

    opts.GetOpts().SetCmdLineDescr(stream.Str());

    opts.GetOpts().GetLongOption("time").Hidden();
    opts.GetOpts().GetLongOption("progress").Hidden();

    config.SetFreeArgsMin(0);
}

void TClientCommandRootCommon::Parse(TConfig& config) {
    TClientCommandRootBase::Parse(config);
    config.VerbosityLevel = VerbosityLevel;
}

void TClientCommandRootCommon::ExtractParams(TConfig& config) {
    if (ProfileFile.empty()) {
        config.ProfileFile = TStringBuilder() << HomeDir << '/' << Settings.YdbDir << "/config/config.yaml";
    } else {
        config.ProfileFile = TFsPath(ProfileFile).RealLocation().GetPath();
    }
    if (TFsPath(config.ProfileFile).Exists() && !TFsPath(config.ProfileFile).IsFile()) {
        throw TMisuseException() << "\'" << config.ProfileFile << "\' is not a file";
    }
    ProfileManager = CreateProfileManager(config.ProfileFile);
    ParseProfile();

    if (std::vector<TString> errors = ParseResult->Validate(); !errors.empty()) {
        MisuseErrors.insert(MisuseErrors.end(), errors.begin(), errors.end());
    }
    if (std::vector<TString> errors = ParseResult->ParseFromProfilesAndEnv(Profile, !config.OnlyExplicitProfile ? ProfileManager->GetActiveProfile() : nullptr); !errors.empty()) {
        MisuseErrors.insert(MisuseErrors.end(), errors.begin(), errors.end());
    }
    if (IsVerbose()) {
        std::vector<TString> errors = ParseResult->LogConnectionParams([&](const TString& paramName, const TString& value, const TString& sourceText) {
            config.ConnectionParams[paramName].push_back({value, sourceText});
        });
        if (!errors.empty()) {
            MisuseErrors.insert(MisuseErrors.end(), errors.begin(), errors.end());
        }
    }

    config.EnableSsl = EnableSsl;

    ParseCaCerts(config);
    ParseClientCert(config);
    ParseStaticCredentials(config);

    config.Address = Address;
    config.Database = Database;
    config.ChosenAuthMethod = ParseResult->GetChosenAuthMethod();
}

void TClientCommandRootCommon::ParseCaCerts(TConfig& config) {
    if (!config.EnableSsl && config.CaCerts) {
        MisuseErrors.push_back("\"ca-file\" option is provided for a non-ssl connection. Use grpcs:// prefix for host to connect using SSL.");
    }
}

void TClientCommandRootCommon::ParseClientCert(TConfig& config) {
    if (!config.EnableSsl && config.ClientCert) {
        MisuseErrors.push_back("\"client-cert-file\"/\"client-cert-key-file\"/\"client-cert-key-password-file\" options are provided for a non-ssl connection. Use grpcs:// prefix for host to connect using SSL.");
    }

    if (config.ClientCert) {
        // Convert certificates from PKCS#12 to PEM or encrypted private key to nonencrypted
        // May ask for password
        std::tie(config.ClientCert, config.ClientCertPrivateKey) = ConvertCertToPEM(config.ClientCert, config.ClientCertPrivateKey, config.ClientCertPrivateKeyPassword);
    }
}

void TClientCommandRootCommon::ParseStaticCredentials(TConfig& config) {
    if (!config.UseStaticCredentials) {
        return;
    }

    if (ParseResult->GetChosenAuthMethod() != "static-credentials") {
        return;
    }

    // Check that we have username/password from one source
    const TOptionParseResult* userResult = ParseResult->FindResult("user");
    const TOptionParseResult* passwordResult = ParseResult->FindResult("password-file");
    if (passwordResult && passwordResult->GetValueSource() == userResult->GetValueSource()) { // Both from command line or both from env
        Password = PasswordOption;
        PasswordFile = PasswordFileOption;
    }

    if (passwordResult && static_cast<int>(passwordResult->GetValueSource()) < static_cast<int>(userResult->GetValueSource())) { // Password is set from command line/env, but user is taken from source with less priority
        MisuseErrors.push_back("User password was provided without user name");
        return;
    }

    // Assign values
    config.StaticCredentials.User = UserName;
    config.StaticCredentials.Password = Password;

    if (!config.StaticCredentials.Password.empty()) {
        DoNotAskForPassword = true;
    }

    // Interactively ask for password
    if (!config.StaticCredentials.User.empty()) {
        if (config.StaticCredentials.Password.empty() && !DoNotAskForPassword) {
            Cerr << "Enter password for user " << config.StaticCredentials.User << ": ";
            config.StaticCredentials.Password = InputPassword();
            if (IsVerbose()) {
                config.ConnectionParams["password"].push_back({TString{config.StaticCredentials.Password}, "standard input"});
            }
        }
    }
}

TString TClientCommandRootCommon::GetAddressFromString(const TString& address_, bool* enableSsl, std::vector<TString>* errors) {
    TString port = "2135";
    TString address = address_;
    TString message;
    if (!ParseProtocolNoConfig(address, enableSsl, message)) {
        if (errors) {
            errors->emplace_back(std::move(message));
        }
        return {};
    }

    const size_t colonPos = address.find(":");
    if (colonPos == TString::npos) {
        address = address + ':' + port;
    } else {
        if (colonPos != address.rfind(":")) {
            if (errors) {
                errors->emplace_back("Wrong format for option 'endpoint': more than one colon found.");
            }
            return {};
        }
    }
    return address;
}

bool TClientCommandRootCommon::ParseProtocolNoConfig(TString& address, bool* enableSsl, TString& message) {
    auto separatorPos = address.find("://");
    if (separatorPos != TString::npos) {
        TString protocol = address.substr(0, separatorPos);
        protocol.to_lower();
        if (protocol == "grpcs") {
            if (enableSsl) {
                *enableSsl = true;
            }
        } else if (protocol == "grpc") {
            if (enableSsl) {
                *enableSsl = false;
            }
        } else {
            message = TStringBuilder() << "Unknown protocol \"" << protocol << "\".";
            return false;
        }
        address = address.substr(separatorPos + 3);
    }
    return true;
}

void TClientCommandRootCommon::ParseProfile() {
    if (ProfileName) {
        if (ProfileManager->HasProfile(ProfileName)) {
            Profile = ProfileManager->GetProfile(ProfileName);
        } else {
            MisuseErrors.push_back(TStringBuilder() << "Profile " << ProfileName << " does not exist." << Endl
                << "Run \"ydb config profile list\" to see existing profiles");
            return;
        }
    }
}

void TClientCommandRootCommon::Validate(TConfig& config) {
    TClientCommandRootBase::Validate(config);
    if (!config.NeedToConnect && !IsVerbose()) {
        return;
    }

    if (!MisuseErrors.empty()) {
        TStringBuilder errors;
        for (auto it = MisuseErrors.begin(); it != MisuseErrors.end(); ++it) {
            if (it != MisuseErrors.begin()) {
                errors << Endl;
            }
            errors << *it;
        }
        if (!config.NeedToConnect) {
            Cerr << "Connection parameters parsing errors:" << Endl << errors << Endl;
        } else {
            throw TMisuseException() << errors;
        }
    }

    // TODO: Maybe NeedToConnect doesn't always mean that we don't need to check endpoint and database
    // TODO: Now we supplying only one error while it is possible to return all errors at once,
    //       maybe even errors from nested command's validate
    if (!config.NeedToConnect) {
        return;
    }

    if (config.Address.empty() && !config.AllowEmptyAddress) {
        throw TMisuseException() << "Missing required option 'endpoint'." << (config.OnlyExplicitProfile ? " Profile ignored due to admin command use." : "");
    }

    if (config.Database.empty() && config.AllowEmptyDatabase) {
        // just skip the Database check
    } else if (config.Database.empty()) {
        throw TMisuseException()
            << "Missing required option 'database'." << (config.OnlyExplicitProfile ? " Profile ignored due to admin command use." : "");
    } else if (!config.Database.StartsWith('/')) {
        throw TMisuseException() << "Path to a database \"" << config.Database
            << "\" is incorrect. It must be absolute and thus must begin with '/'.";
    }
}

int TClientCommandRootCommon::Run(TConfig& config) {
    if (HasSelectedCommand()) {
        return TClientCommandRootBase::Run(config);
    }

    TString prompt;
    if (!ProfileName.empty()) {
        prompt = ProfileName + "> ";
    } else {
        prompt = "ydb> ";
    }

    TInteractiveCLI interactiveCLI(prompt);
    return interactiveCLI.Run(config);
}

void TClientCommandRootCommon::ParseCredentials(TConfig& config) {
    Y_UNUSED(config);
}

}
}
