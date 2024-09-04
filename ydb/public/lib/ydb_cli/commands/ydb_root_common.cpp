#include "ydb_root_common.h"
#include "ydb_profile.h"
#include "ydb_admin.h"
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
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange/credentials.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange/from_file.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange/jwt_token_source.h>

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
    AddCommand(std::make_unique<TCommandAdmin>());
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
    exit(EXIT_FAILURE);
}

void TClientCommandRootCommon::FillConfig(TConfig& config) {
    config.UseAccessToken = Settings.UseAccessToken.GetRef();
    config.UseIamAuth = Settings.UseIamAuth.GetRef();
    config.UseStaticCredentials = Settings.UseStaticCredentials.GetRef();
    config.UseOauth2TokenExchange = Settings.UseOauth2TokenExchange.GetRef();
    config.UseExportToYt = Settings.UseExportToYt.GetRef();
    SetCredentialsGetter(config);
}

// Default CredentialsGetter that can be overridden in different CLI versions
void TClientCommandRootCommon::SetCredentialsGetter(TConfig& config) {
    config.CredentialsGetter = [](const TClientCommand::TConfig& config) {
        if (config.SecurityToken) {
            return CreateOAuthCredentialsProviderFactory(config.SecurityToken);
        }

        if (config.UseStaticCredentials) {
            if (config.StaticCredentials.User) {
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
    NLastGetopt::TOpts& opts = *config.Opts;

    TStringBuilder endpointHelp;
    endpointHelp << "[Required] Endpoint to connect. Protocols: grpc, grpcs (Default: "
        << (Settings.EnableSsl.GetRef() ? "grpcs" : "grpc" ) << ")." << Endl;

    endpointHelp << "  Endpoint search order:" << Endl
        << "    1. This option" << Endl
        << "    2. Profile specified with --profile option" << Endl
        << "    3. Active configuration profile";
    TStringBuilder databaseHelp;
    databaseHelp << "[Required] Database to work with." << Endl
        << "  Database search order:" << Endl
        << "    1. This option" << Endl
        << "    2. Profile specified with --profile option" << Endl
        << "    3. Active configuration profile";

    opts.AddLongOption('e', "endpoint", endpointHelp)
        .RequiredArgument("[PROTOCOL://]HOST[:PORT]").StoreResult(&Address);
    opts.AddLongOption('d', "database", databaseHelp)
        .RequiredArgument("PATH").StoreResult(&Database);
    opts.AddLongOption('v', "verbose", "Increase verbosity of operations")
        .Optional().NoArgument().Handler0([&](){
            VerbosityLevel++;
        });
    opts.AddLongOption('p', "profile", "Profile name to use configuration parameters from.")
        .RequiredArgument("NAME").StoreResult(&ProfileName);
    TClientCommandRootBase::Config(config);

    if (config.UseIamAuth) {
        const TString docsUrl = "cloud.yandex.ru/docs";
        TStringBuilder iamTokenHelp;
        iamTokenHelp << "IAM token file. Note: IAM tokens expire in 12 hours." << Endl
            << "  For more info go to: " << docsUrl << "/iam/concepts/authorization/iam-token" << Endl
            << "  Token search order:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"IAM_TOKEN\" environment variable" << Endl
            << "    4. Active configuration profile";
        opts.AddLongOption("iam-token-file", iamTokenHelp).RequiredArgument("PATH").StoreResult(&TokenFile);

        TStringBuilder ycTokenHelp;
        ycTokenHelp << "YC token file. It should contain OAuth token of a Yandex Passport user to get IAM token with." << Endl
            << "  For more info go to: " << docsUrl << "/iam/concepts/authorization/oauth-token" << Endl
            << "  Token search order:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"YC_TOKEN\" environment variable" << Endl
            << "    4. Active configuration profile";
        opts.AddLongOption("yc-token-file", ycTokenHelp).RequiredArgument("PATH").StoreResult(&YCTokenFile);

        TStringBuilder metadataHelp;
        metadataHelp << "Use metadata service on a virtual machine to get credentials" << Endl
            << "  For more info go to: " << docsUrl << "/compute/operations/vm-connect/auth-inside-vm" << Endl
            << "  Definition priority:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"USE_METADATA_CREDENTIALS\" environment variable" << Endl
            << "    4. Active configuration profile";
        opts.AddLongOption("use-metadata-credentials", metadataHelp).Optional().StoreTrue(&UseMetadataCredentials);

        TStringBuilder saKeyHelp;
        saKeyHelp << "Service account";
        if (Settings.MentionUserAccount.GetRef()) {
            saKeyHelp << " (or user account)";
        }
        saKeyHelp << " key file" << Endl
            << "  For more info go to: " << docsUrl << "/iam/operations/iam-token/create-for-sa" << Endl
            << "  Definition priority:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"SA_KEY_FILE\" environment variable" << Endl
            << "    4. Active configuration profile";
        opts.AddLongOption("sa-key-file", saKeyHelp).RequiredArgument("PATH").StoreResult(&SaKeyFile);
    }

    if (config.UseAccessToken) {
        TStringBuilder tokenHelp;
        tokenHelp << "Access token file" << Endl
            << "  Token search order:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"YDB_TOKEN\" environment variable" << Endl
            << "    4. Active configuration profile";
        if (Settings.UseDefaultTokenFile.GetRef()) {
            tokenHelp << Endl << "    5. Default token file \"" << defaultTokenFile << "\"";
        }
        opts.AddLongOption("token-file", tokenHelp).RequiredArgument("PATH").StoreResult(&TokenFile);
    }

    if (config.UseStaticCredentials) {
        TStringBuilder userHelp;
        userHelp << "User name to authenticate with" << Endl
            << "  User name search order:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"YDB_USER\" environment variable" << Endl
            << "    4. Active configuration profile";
        opts.AddLongOption("user", userHelp).RequiredArgument("STR").StoreResult(&UserName);

        TStringBuilder passwordHelp;
        passwordHelp << "File with password to authenticate with" << Endl
            << "  Password search order:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"YDB_PASSWORD\" environment variable" << Endl
            << "    4. Active configuration profile";
        opts.AddLongOption("password-file", passwordHelp).RequiredArgument("PATH").StoreResult(&PasswordFile);

        opts.AddLongOption("no-password", "Do not ask for user password (if empty)").Optional().StoreTrue(&DoNotAskForPassword);
    }

    if (config.UseOauth2TokenExchange) {
        TOauth2TokenExchangeParams defaultParams;
        TJwtTokenSourceParams defaultJwtParams;
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);

#define FIELD(name) "    " << colors.BoldColor() << name << colors.OldColor() << ": "
#define TYPE(type) "[" << colors.YellowColor() << type << colors.OldColor() << "] "
#define TYPE2(type1, type2) "[" << colors.YellowColor() << type1 << colors.OldColor() << " | " << colors.YellowColor() << type2 << colors.OldColor() << "] "
#define DEFAULT(value) " (default: " << colors.CyanColor() << value << colors.OldColor() << ")"

        TStringBuilder oauth2TokenExchangeHelp;
        oauth2TokenExchangeHelp << "OAuth 2.0 RFC8693 token exchange credentials parameters json file" << Endl;
        if (config.HelpCommandVerbosiltyLevel <= 1) {
            oauth2TokenExchangeHelp << "  Use -hh option to see file format description" << Endl;
        }
        oauth2TokenExchangeHelp
            << "  Parameters file search order:" << Endl
            << "    1. This option" << Endl
            << "    2. Profile specified with --profile option" << Endl
            << "    3. \"YDB_OAUTH2_KEY_FILE\" environment variable" << Endl
            << "    4. Active configuration profile" << Endl << Endl
            << "  Detailed information about OAuth 2.0 token exchange protocol: https://www.rfc-editor.org/rfc/rfc8693" << Endl
            << "  Detailed description about file parameters: https://ydb.tech/docs/en/reference/ydb-cli/connect" << Endl
            << Endl;

        if (config.HelpCommandVerbosiltyLevel >= 2) {
            TStringBuilder supportedJwtAlgorithms;
            for (const TString& alg : GetSupportedOauth2TokenExchangeJwtAlgorithms()) {
                if (supportedJwtAlgorithms) {
                    supportedJwtAlgorithms << ", ";
                }
                supportedJwtAlgorithms << colors.BoldColor() << alg << colors.OldColor();
            }

            oauth2TokenExchangeHelp
                << "  Fields of json file:" << Endl
                << FIELD("grant-type") "          " TYPE("string") "Grant type" DEFAULT(defaultParams.GrantType_) << Endl
                << FIELD("res") "                 " TYPE("string") "Resource (optional)" << Endl
                << FIELD("aud") "                 " TYPE2("string", "list of strings") "Audience option for token exchange request (optional)" << Endl
                << FIELD("scope") "               " TYPE2("string", "list of strings") "Scope (optional)" << Endl
                << FIELD("requested-token-type") "" TYPE("string") "Requested token type" DEFAULT(defaultParams.RequestedTokenType_) << Endl
                << FIELD("subject-credentials") " " TYPE("creds_json") "Subject credentials (optional)" << Endl
                << FIELD("actor-credentials") "   " TYPE("creds_json") "Actor credentials (optional)" << Endl
                << Endl
                << "  Fields of " << colors.BoldColor() << "creds_json" << colors.OldColor() << " (JWT):" << Endl
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
                << "  Fields of " << colors.BoldColor() << "creds_json" << colors.OldColor() << " (FIXED):" << Endl
                << FIELD("type") "                " TYPE("string") "Token source type. Set " << colors.BoldColor() << "FIXED" << colors.OldColor() << Endl
                << FIELD("token") "               " TYPE("string") "Token value" << Endl
                << FIELD("token-type") "          " TYPE("string") "Token type value. It will become subject_token_type/actor_token_type parameter in token exchange request (https://www.rfc-editor.org/rfc/rfc8693)" << Endl
                << Endl;
        }

        oauth2TokenExchangeHelp
            << "  Note that additionally you need to set " << colors.BoldColor() << "--iam-endpoint" << colors.OldColor() << " option" << Endl
            << "    in url format (SCHEMA://HOST:PORT/PATH) to configure endpoint.";

        opts.AddLongOption("oauth2-key-file", oauth2TokenExchangeHelp).RequiredArgument("PATH").StoreResult(&Oauth2KeyFile);

#undef DEFAULT
#undef TYPE2
#undef TYPE
#undef FIELD
    }

    if (config.UseIamAuth) {
        TStringBuilder iamEndpointHelp;
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        iamEndpointHelp << "Endpoint of IAM service (default: " << colors.Cyan();
        iamEndpointHelp << "\"" << config.IamEndpoint << "\"" << colors.OldColor() << ")";
        opts.AddLongOption("iam-endpoint", iamEndpointHelp)
            .RequiredArgument("STR")
            .StoreResult(&IamEndpoint);
    }

    opts.AddLongOption("profile-file", "Path to config file with profile data in yaml format")
        .RequiredArgument("PATH").StoreResult(&ProfileFile);

    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << " [options...] <subcommand>" << Endl << Endl
        << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    RenderCommandsDescription(stream, colors);
    opts.SetCmdLineDescr(stream.Str());

    opts.GetLongOption("time").Hidden();
    opts.GetLongOption("progress").Hidden();

    config.SetFreeArgsMin(0);
}

void TClientCommandRootCommon::Parse(TConfig& config) {
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

    TClientCommandRootBase::Parse(config);
    ParseDatabase(config);
    ParseCaCerts(config);
    ParseIamEndpoint(config);

    config.VerbosityLevel = std::min(static_cast<TConfig::EVerbosityLevel>(VerbosityLevel), TConfig::EVerbosityLevel::DEBUG);
}

namespace {
    inline void PrintSettingFromProfile(const TString& setting, std::shared_ptr<IProfile> profile, bool explicitOption) {
        Cerr << "Using " << setting << " due to configuration in" << (explicitOption ? "" : " active") << " profile \""
            << profile->GetName() << "\"" << (explicitOption ? " from explicit --profile option" : "") << Endl;
    }

    inline TString GetProfileSource(std::shared_ptr<IProfile> profile, bool explicitOption) {
        Y_ABORT_UNLESS(profile, "No profile to get source");
        if (explicitOption) {
            return TStringBuilder() << "profile \"" << profile->GetName() << "\" from explicit --profile option";
        }
        return TStringBuilder() << "active profile \"" << profile->GetName() << "\"";
    }
}

bool TClientCommandRootCommon::TryGetParamFromProfile(const TString& name, std::shared_ptr<IProfile> profile, bool explicitOption,
                                                      std::function<bool(const TString&, const TString&, bool)> callback) {
    if (profile && profile->Has(name)) {
        return callback(profile->GetValue(name).as<TString>(), GetProfileSource(profile, explicitOption), explicitOption);
    }
    return false;
}

void TClientCommandRootCommon::ParseCaCerts(TConfig& config) {
    auto getCaFile = [this, &config] (const TString& param, const TString& sourceText, bool explicitOption) {
        if (!IsCaCertsFileSet && (explicitOption || !Profile)) {
            config.CaCertsFile = param;
            IsCaCertsFileSet = true;
            GetCaCerts(config);
        }
        if (!IsVerbose()) {
            return true;
        }
        config.ConnectionParams["ca-file"].push_back({param, sourceText});
        return false;
    };
    // Priority 1. Explicit --ca-file option
    if (CaCertsFile && getCaFile(CaCertsFile, "explicit --ca-file option", true)) {
        return;
    }
    // Priority 2. Explicit --profile option
    if (TryGetParamFromProfile("ca-file", Profile, true, getCaFile)) {
        return;
    }
    // Priority 3. Active profile (if --profile option is not specified)
    if (TryGetParamFromProfile("ca-file", ProfileManager->GetActiveProfile(), false, getCaFile)) {
        return;
    }
}

void TClientCommandRootCommon::GetCaCerts(TConfig& config) {
    if (!config.EnableSsl && !config.CaCertsFile.empty()) {
        throw TMisuseException()
            << "\"ca-file\" option provided for a non-ssl connection. Use grpcs:// prefix for host to connect using SSL.";
    }
    if (!config.CaCertsFile.empty()) {
        config.CaCerts = ReadFromFile(config.CaCertsFile, "CA certificates");
    }
}

void TClientCommandRootCommon::ParseAddress(TConfig& config) {
    auto getAddress = [this, &config] (const TString& param, const TString& sourceText, bool explicitOption) {
        TString address;
        if (!IsAddressSet && (explicitOption || !Profile)) {
            config.Address = param;
            IsAddressSet = true;
            GetAddressFromString(config);
            address = config.Address;
        }
        if (!IsVerbose()) {
            return true;
        }
        if (!address) {
            address = param;
            GetAddressFromString(config, &address);
        }
        config.ConnectionParams["endpoint"].push_back({address, sourceText});
        return false;
    };
    // Priority 1. Explicit --endpoint option
    if (Address && getAddress(Address, "explicit --endpoint option", true)) {
        return;
    }
    // Priority 2. Explicit --profile option
    if (TryGetParamFromProfile("endpoint", Profile, true, getAddress)) {
        return;
    }
    // Priority 3. Active profile (if --profile option is not specified)
    if (TryGetParamFromProfile("endpoint", ProfileManager->GetActiveProfile(), false, getAddress)) {
        return;
    }
}

void TClientCommandRootCommon::GetAddressFromString(TConfig& config, TString* result) {
    TString port = "2135";
    Address = result ? *result : config.Address;
    if (!Address.empty()) {
        if (!result) {
            config.EnableSsl = Settings.EnableSsl.GetRef();
        }
        TString message;
        if ((result && !ParseProtocolNoConfig(message)) || (!result && !ParseProtocol(config, message))) {
            MisuseErrors.push_back(message);
        }
        auto colon_pos = Address.find(":");
        if (colon_pos == TString::npos) {
            if (result) {
                *result = Address + ":" + port;
            } else {
                config.Address =  Address + ":" + port;
            }
        } else {
            if (colon_pos == Address.rfind(":")) {
                if (result) {
                    *result = Address;
                } else {
                    config.Address =  Address;
                }
            } else {
                MisuseErrors.push_back("Wrong format for option 'endpoint': more than one colon found.");
            }
        }
    }
}

bool TClientCommandRootCommon::ParseProtocolNoConfig(TString& message) {
    auto separator_pos = Address.find("://");
    if (separator_pos != TString::npos) {
        TString protocol = Address.substr(0, separator_pos);
        protocol.to_lower();
        if (protocol != "grpcs" && protocol != "grpc") {
            message = TStringBuilder() << "Unknown protocol \"" << protocol << "\".";
            return false;
        }
        Address = Address.substr(separator_pos + 3);
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

void TClientCommandRootCommon::ParseDatabase(TConfig& config) {
    auto getDatabase = [this, &config] (const TString& param, const TString& sourceText, bool explicitOption) {
        if (!IsDatabaseSet && (explicitOption || !Profile)) {
            config.Database = param;
            IsDatabaseSet = true;
        }
        if (!IsVerbose()) {
            return true;
        }
        config.ConnectionParams["database"].push_back({param, sourceText});
        return false;
    };
    // Priority 1. Explicit --database option
    if (Database && getDatabase(Database, "explicit --database option", true)) {
        return;
    }
    // Priority 2. Explicit --profile option
    if (TryGetParamFromProfile("database", Profile, true, getDatabase)) {
        return;
    }
    // Priority 3. Active profile (if --profile option is not specified)
    if (TryGetParamFromProfile("database", ProfileManager->GetActiveProfile(), false, getDatabase)) {
        return;
    }
}

void TClientCommandRootCommon::ParseIamEndpoint(TConfig& config) {
    auto getIamEndpoint = [this, &config] (const TString& param, const TString& sourceText, bool explicitOption) {
        if (!IsIamEndpointSet && (explicitOption || !Profile)) {
            config.IamEndpoint = param;
            IsIamEndpointSet = true;
        }
        if (!IsVerbose()) {
            return true;
        }
        config.ConnectionParams["iam-endpoint"].push_back({param, sourceText});
        return false;
    };
    // Priority 1. Explicit --iam-endpoint option
    if (IamEndpoint && getIamEndpoint(IamEndpoint, "explicit --iam-endpoint option", true)) {
        return;
    }
    // Priority 2. Explicit --profile option
    if (TryGetParamFromProfile("iam-endpoint", Profile, true, getIamEndpoint)) {
        return;
    }
    // Priority 3. Active profile (if --profile option is not specified)
    if (TryGetParamFromProfile("iam-endpoint", ProfileManager->GetActiveProfile(), false, getIamEndpoint)) {
        return;
    }
    // Priority 4. Default value
    if (IsVerbose()) {
        config.ConnectionParams["iam-endpoint"].push_back({config.IamEndpoint, "default value"});
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
    if (!config.NeedToConnect) {
        return;
    }

    if (config.Address.empty()) {
        throw TMisuseException() << "Missing required option 'endpoint'.";
    }

    if (config.Database.empty()) {
        throw TMisuseException()
            << "Missing required option 'database'.";
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
    if (!ProfileName.Empty()) {
        prompt = ProfileName + "> ";
    } else {
        prompt = "ydb> ";
    }

    TInteractiveCLI interactiveCLI(config, prompt);
    interactiveCLI.Run();

    return EXIT_SUCCESS;
}

bool TClientCommandRootCommon::GetCredentialsFromProfile(std::shared_ptr<IProfile> profile, TConfig& config, bool explicitOption) {
    if (!profile || !profile->Has("authentication")) {
        return false;
    }
    auto authValue = profile->GetValue("authentication");
    if (!authValue["method"]) {
        MisuseErrors.push_back("Configuration profile has \"authentication\" but does not have \"method\" in it");
        return false;
    }
    TString authMethod = authValue["method"].as<TString>();

    if (authMethod == "use-metadata-credentials") {
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("metadata service", profile, explicitOption);
            }
            config.UseMetadataCredentials = true;
            config.ChosenAuthMethod = "use-metadata-credentials";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["use-metadata-credentials"].push_back({"true", GetProfileSource(profile, explicitOption)});
        }
        return true;
    }
    if (authMethod == "anonymous-auth") {
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("anonymous authentication", profile, explicitOption);
            }
            config.ChosenAuthMethod = "anonymous-auth";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["anonymous-auth"].push_back({"true", GetProfileSource(profile, explicitOption)});
        }
        return true;
    }
    bool knownMethod = false;
    if (config.UseOauth2TokenExchange) {
        knownMethod |= (authMethod == "oauth2-key-file");
    }
    if (config.UseIamAuth) {
        knownMethod |= (authMethod == "iam-token" || authMethod == "yc-token" || authMethod == "sa-key-file" ||
                        authMethod == "token-file" || authMethod == "yc-token-file");
    }
    if (config.UseAccessToken) {
        knownMethod |= (authMethod == "ydb-token" || authMethod == "token-file");
    }
    if (config.UseStaticCredentials) {
        knownMethod |= (authMethod == "static-credentials");
    }
    if (!knownMethod) {
        MisuseErrors.push_back(TStringBuilder() << "Unknown authentication method in configuration profile: \""
            << authMethod << "\"");
        return false;
    }
    if (!authValue["data"]) {
        MisuseErrors.push_back(TStringBuilder() << "Active configuration profile has \"authentication\" with method \""
            << authMethod << "\" in it, but no \"data\"");
        return false;
    }
    auto authData = authValue["data"];

    if (authMethod == "iam-token") {
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("iam token", profile, explicitOption);
            }
            config.SecurityToken = authData.as<TString>();
            config.ChosenAuthMethod = "token";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["token"].push_back({authData.as<TString>(), GetProfileSource(profile, explicitOption)});
        }
    } else if (authMethod == "token-file") {
        TString filename = authData.as<TString>();
        TString fileContent;
        if (!ReadFromFileIfExists(filename, "token", fileContent, true)) {
            MisuseErrors.push_back(TStringBuilder() << "Couldn't read token from file " << filename);
            return false;
        }
        if (!fileContent) {
            MisuseErrors.push_back(TStringBuilder() << "Empty token file " << filename << " provided");
            return false;
        }
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("token file", profile, explicitOption);
            }
            config.SecurityToken = fileContent;
            config.ChosenAuthMethod = "token";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["token"].push_back({fileContent, GetProfileSource(profile, explicitOption)});
        }
    } else if (authMethod == "oauth2-key-file") {
        TString filePath = authData.as<TString>();
        if (filePath.StartsWith("~")) {
            filePath = HomeDir + filePath.substr(1);
        }
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("oauth2 key file (oauth2-key-file)", profile, explicitOption);
            }
            config.Oauth2KeyFile = filePath;
            config.ChosenAuthMethod = "oauth2-key-file";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["oauth2-key-file"].push_back({filePath, GetProfileSource(profile, explicitOption)});
        }
    } else if (authMethod == "yc-token") {
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("Yandex.Cloud Passport token (yc-token)", profile, explicitOption);
            }
            config.YCToken = authData.as<TString>();
            config.ChosenAuthMethod = "yc-token";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["yc-token"].push_back({authData.as<TString>(), GetProfileSource(profile, explicitOption)});
        }
    } else if (authMethod == "yc-token-file") {
        TString filename = authData.as<TString>();
        TString fileContent;
        if (!ReadFromFileIfExists(filename, "token", fileContent, true)) {
            MisuseErrors.push_back(TStringBuilder() << "Couldn't read token from file " << filename);
            return false;
        }
        if (!fileContent) {
            MisuseErrors.push_back(TStringBuilder() << "Empty token file " << filename << " provided");
            return false;
        }
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("Yandex.Cloud Passport token file (yc-token-file)", profile, explicitOption);
            }
            config.YCToken = fileContent;
            config.ChosenAuthMethod = "yc-token";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["yc-token"].push_back({fileContent, GetProfileSource(profile, explicitOption)});
        }
    } else if (authMethod == "sa-key-file") {
        TString filePath = authData.as<TString>();
        if (filePath.StartsWith("~")) {
            filePath = HomeDir + filePath.substr(1);
        }
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("service account key file (sa-key-file)", profile, explicitOption);
            }
            config.SaKeyFile = filePath;
            config.ChosenAuthMethod = "sa-key-file";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["sa-key-file"].push_back({filePath, GetProfileSource(profile, explicitOption)});
        }
    } else if (authMethod == "ydb-token") {
        if (!IsAuthSet && (explicitOption || !Profile)) {
            if (IsVerbose()) {
                PrintSettingFromProfile("Access token (ydb-token)", profile, explicitOption);
            }
            config.SecurityToken = authData.as<TString>();
            config.ChosenAuthMethod = "token";
            IsAuthSet = true;
        }
        if (IsVerbose()) {
            config.ConnectionParams["token"].push_back({authData.as<TString>(), GetProfileSource(profile, explicitOption)});
        }
    } else if (authMethod == "static-credentials") {
        if (!IsAuthSet && (explicitOption || !Profile) && IsVerbose()) {
            PrintSettingFromProfile("user name & password", profile, explicitOption);
        }
        if (authData["user"]) {
            if (!IsAuthSet && (explicitOption || !Profile)) {
                config.StaticCredentials.User = authData["user"].as<TString>();
            }
            if (IsVerbose()) {
                config.ConnectionParams["user"].push_back({authData["user"].as<TString>(), GetProfileSource(profile, explicitOption)});
            }
        }
        if (authData["password"]) {
            if (!IsAuthSet && (explicitOption || !Profile)) {
                config.StaticCredentials.Password = authData["password"].as<TString>();
                if (!config.StaticCredentials.Password) {
                    DoNotAskForPassword = true;
                }
            }
            if (IsVerbose()) {
                config.ConnectionParams["password"].push_back({authData["password"].as<TString>(), GetProfileSource(profile, explicitOption)});
            }
        }
        if (authData["password-file"]) {
            TString filename = authData["password-file"].as<TString>();
            TString fileContent;
            if (!ReadFromFileIfExists(filename, "password", fileContent, true)) {
                MisuseErrors.push_back(TStringBuilder() << "Couldn't read password from file " << filename);
                DoNotAskForPassword = true;
                return false;
            }
            if (!IsAuthSet && (explicitOption || !Profile)) {
                config.StaticCredentials.Password = fileContent;
                if (!config.StaticCredentials.Password) {
                    DoNotAskForPassword = true;
                }
            }
            if (IsVerbose()) {
                config.ConnectionParams["password"].push_back({fileContent, GetProfileSource(profile, explicitOption)});
            }
        }
        config.ChosenAuthMethod = "static-credentials";
        IsAuthSet = true;
    } else {
        return false;
    }
    return true;
}

void TClientCommandRootCommon::ParseCredentials(TConfig& config) {
    size_t explicitAuthMethodCount = (size_t)(config.ParseResult->Has("iam-token-file")) + (size_t)(config.ParseResult->Has("token-file"))
        + (size_t)(!YCTokenFile.empty())
        + (size_t)UseMetadataCredentials + (size_t)(!SaKeyFile.empty())
        + (size_t)(!UserName.empty() || !PasswordFile.empty() || DoNotAskForPassword)
        + (size_t)(!Oauth2KeyFile.empty());

    switch (explicitAuthMethodCount) {
    case 1:
        // Priority 1. Exactly one explicit auth method. Using it.
        if (config.ParseResult->Has("token-file")) {
            config.SecurityToken = ReadFromFile(TokenFile, "token");
            config.ChosenAuthMethod = "token";
            if (IsVerbose()) {
                Cerr << "Using token from file provided with explicit option" << Endl;
                config.ConnectionParams["token"].push_back({config.SecurityToken, "file provided with explicit --token-file option"});
            }
        } else if (Oauth2KeyFile) {
            config.Oauth2KeyFile = Oauth2KeyFile;
            config.ChosenAuthMethod = "oauth2-key-file";
            if (IsVerbose()) {
                Cerr << "Using oauth2 key file provided with --oauth2-key-file option" << Endl;
                config.ConnectionParams["oauth2-key-file"].push_back({config.Oauth2KeyFile, "explicit --oauth2-key-file option"});
            }
        } else if (config.ParseResult->Has("iam-token-file")) {
            config.SecurityToken = ReadFromFile(TokenFile, "token");
            config.ChosenAuthMethod = "token";
            if (IsVerbose()) {
                Cerr << "Using IAM token from file provided with explicit option" << Endl;
                config.ConnectionParams["token"].push_back({config.SecurityToken, "file provided with explicit --iam-token-file option"});
            }
        } else if (YCTokenFile) {
            config.YCToken = ReadFromFile(YCTokenFile, "token");
            config.ChosenAuthMethod = "yc-token";
            if (IsVerbose()) {
                Cerr << "Using Yandex.Cloud Passport token from file provided with --yc-token-file option" << Endl;
                config.ConnectionParams["yc-token"].push_back({config.YCToken, "file provided with explicit --yc-token-file option"});
            }
        } else if (UseMetadataCredentials) {
            config.ChosenAuthMethod = "use-metadata-credentials";
            config.UseMetadataCredentials = true;
            if (IsVerbose()) {
                Cerr << "Using metadata service due to --use-metadata-credentials option" << Endl;
                config.ConnectionParams["use-metadata-credentials"].push_back({"true", "explicit --use-metadata-credentials option"});
            }
        } else if (SaKeyFile) {
            config.SaKeyFile = SaKeyFile;
            config.ChosenAuthMethod = "sa-key-file";
            if (IsVerbose()) {
                Cerr << "Using service account key file provided with --sa-key-file option" << Endl;
                config.ConnectionParams["sa-key-file"].push_back({config.SaKeyFile, "explicit --sa-key-file option"});
            }
        } else if (UserName || PasswordFile) {
            if (UserName) {
                config.StaticCredentials.User = UserName;
                if (IsVerbose()) {
                    Cerr << "Using user name provided with --user option" << Endl;
                    config.ConnectionParams["user"].push_back({UserName, "explicit --user option"});
                }
            }
            if (PasswordFile) {
                config.StaticCredentials.Password = ReadFromFile(PasswordFile, "password", true);
                if (!config.StaticCredentials.Password) {
                    DoNotAskForPassword = true;
                }
                if (IsVerbose()) {
                    Cerr << "Using user password from file provided with --password-file option" << Endl;
                    config.ConnectionParams["password"].push_back({config.StaticCredentials.Password, "file provided with explicit --password-file option"});
                }
            }
            config.ChosenAuthMethod = "static-credentials";
        }
        IsAuthSet = true;
        if (!IsVerbose()) {
            break;
        }
    case 0:
    {
        // Priority 2. No explicit auth methods. Checking configuration profile given via --profile option.
        if (GetCredentialsFromProfile(Profile, config, true) && !IsVerbose()) {
            break;
        }

        // Priority 3. No auth methods from --profile either. Checking environment variables.
        if (config.UseIamAuth) {
            TString envIamToken = GetEnv("IAM_TOKEN");
            if (!envIamToken.empty()) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using iam token from IAM_TOKEN env variable" << Endl;
                    }
                    config.ChosenAuthMethod = "token";
                    config.SecurityToken = envIamToken;
                    IsAuthSet = true;
                }
                if (!IsVerbose()) {
                    break;
                }
                config.ConnectionParams["token"].push_back({envIamToken, "IAM_TOKEN enviroment variable"});
            }
            TString envYcToken = GetEnv("YC_TOKEN");
            if (!envYcToken.empty()) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using Yandex.Cloud Passport token from YC_TOKEN env variable" << Endl;
                    }
                    config.ChosenAuthMethod = "yc-token";
                    config.YCToken = envYcToken;
                    IsAuthSet = true;
                }
                if (!IsVerbose()) {
                    break;
                }
                config.ConnectionParams["yc-token"].push_back({envYcToken, "YC_TOKEN enviroment variable"});
            }
            if (GetEnv("USE_METADATA_CREDENTIALS") == "1") {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using metadata service due to USE_METADATA_CREDENTIALS=\"1\" env variable" << Endl;
                    }
                    config.ChosenAuthMethod = "use-metadata-credentials";
                    config.UseMetadataCredentials = true;
                    IsAuthSet = true;
                }
                if (!IsVerbose()) {
                    break;
                }
                config.ConnectionParams["use-metadata-credentials"].push_back({"true", "USE_METADATA_CREDENTIALS enviroment variable"});
            }
            TString envSaKeyFile = GetEnv("SA_KEY_FILE");
            if (!envSaKeyFile.empty()) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using service account key file from SA_KEY_FILE env variable" << Endl;
                    }
                    config.ChosenAuthMethod = "sa-key-file";
                    config.SaKeyFile = envSaKeyFile;
                    IsAuthSet = true;
                }
                if (!IsVerbose()) {
                    break;
                }
                config.ConnectionParams["sa-key-file"].push_back({envSaKeyFile, "SA_KEY_FILE enviroment variable"});
            }
        }
        if (config.UseAccessToken) {
            TString envYdbToken = GetEnv("YDB_TOKEN");
            if (!envYdbToken.empty()) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using access token from YDB_TOKEN env variable" << Endl;
                    }
                    config.ChosenAuthMethod = "token";
                    config.SecurityToken = envYdbToken;
                    IsAuthSet = true;
                }
                if (!IsVerbose()) {
                    break;
                }
                config.ConnectionParams["token"].push_back({envYdbToken, "YDB_TOKEN enviroment variable"});
            }
        }
        if (config.UseStaticCredentials) {
            TString userName = GetEnv("YDB_USER");
            bool hasStaticCredentials = false;
            if (!userName.empty()) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using user name from YDB_USER env variable" << Endl;
                    }
                    hasStaticCredentials = true;
                    config.StaticCredentials.User = userName;
                }
                if (IsVerbose()) {
                    config.ConnectionParams["user"].push_back({userName, "YDB_USER enviroment variable"});
                }
            }

            TString password = GetEnv("YDB_PASSWORD");
            if (!password.empty()) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using user password from YDB_PASSWORD env variable" << Endl;
                    }
                    hasStaticCredentials = true;
                    config.StaticCredentials.Password = password;
                }
                if (IsVerbose()) {
                    config.ConnectionParams["password"].push_back({password, "YDB_PASSWORD enviroment variable"});
                }
            }
            if (hasStaticCredentials) {
                config.ChosenAuthMethod = "static-credentials";
                IsAuthSet = true;
            }
            if (!IsVerbose() && (!userName.empty() || !password.empty())) {
                break;
            }
        }

        if (config.UseOauth2TokenExchange) {
            TString envOauth2KeyFile = GetEnv("YDB_OAUTH2_KEY_FILE");
            if (!envOauth2KeyFile.empty()) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using oauth2 key file from YDB_OAUTH2_KEY_FILE env variable" << Endl;
                    }
                    config.ChosenAuthMethod = "oauth2-key-file";
                    config.Oauth2KeyFile = envOauth2KeyFile;
                    IsAuthSet = true;
                }
                if (!IsVerbose()) {
                    break;
                }
                config.ConnectionParams["oauth2-key-file"].push_back({envOauth2KeyFile, "YDB_OAUTH2_KEY_FILE enviroment variable"});
            }
        }

        // Priority 4. No auth methods from environment variables too. Checking active configuration profile.
        // (if --profile option is not set)
        if (GetCredentialsFromProfile(ProfileManager->GetActiveProfile(), config, false) && !IsVerbose()) {
            break;
        }

        if (Settings.UseDefaultTokenFile.GetRef()) {
            // Priority 5. No auth methods from active configuration profile. Checking default token file.
            TString tokenFile = defaultTokenFile;
            TString fileContent;
            if (ReadFromFileIfExists(tokenFile, "default token", fileContent)) {
                if (!IsAuthSet) {
                    if (IsVerbose()) {
                        Cerr << "Using auth token from default token file " << defaultTokenFile << Endl;
                    }
                    config.ChosenAuthMethod = "token";
                    config.SecurityToken = fileContent;
                }
                if (IsVerbose()) {
                    config.ConnectionParams["token"].push_back({fileContent, "default token file"});
                }
            } else {
                if (!IsAuthSet && IsVerbose()) {
                    Cerr << "No authentication methods were found. Going without authentication" << Endl;
                }
            }
        }
        break;
    }
    default:
        TStringBuilder str;
        str << explicitAuthMethodCount << " methods were provided via options:";
        if (!TokenFile.empty()) {
            str << " TokenFile (" << TokenFile << ")";
        }
        if (!YCTokenFile.empty()) {
            str << " YCTokenFile (" << YCTokenFile << ")";
        }
        if (!SaKeyFile.empty()) {
            str << " SaKeyFile (" << SaKeyFile << ")";
        }
        if (UseMetadataCredentials) {
            str << " UseMetadataCredentials (true)";
        }
        if (Oauth2KeyFile) {
            str << " OAuth2KeyFile (" << Oauth2KeyFile << ")";
        }

        MisuseErrors.push_back(TStringBuilder() << str << ". Choose exactly one of them");
        return;
    }

    if (config.UseStaticCredentials) {
        if (config.StaticCredentials.User) {
            if (!config.StaticCredentials.Password && !DoNotAskForPassword) {
                Cerr << "Enter password for user " << config.StaticCredentials.User << ": ";
                config.StaticCredentials.Password = InputPassword();
                if (IsVerbose()) {
                    config.ConnectionParams["password"].push_back({config.StaticCredentials.Password, "standard input"});
                }
            }
        } else {
            if (config.StaticCredentials.Password) {
                MisuseErrors.push_back("User password was provided without user name");
                return;
            }
        }
    }
}

}
}
