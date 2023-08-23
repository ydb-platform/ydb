#include "ydb_root_common.h"
#include "ydb_profile.h"
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
#include "ydb_tools.h"
#include "ydb_yql.h"
#include "ydb_benchmark.h"
#include "ydb_workload.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/interactive_cli.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>
#include <util/system/env.h>

namespace NYdb {
namespace NConsoleClient {

TClientCommandRootCommon::TClientCommandRootCommon(const TString& name, const TClientSettings& settings)
    : TClientCommandRootBase(name)
    , Settings(settings)
{
    ValidateSettings();
    AddCommand(std::make_unique<TCommandAuth>());
    AddCommand(std::make_unique<TCommandDiscovery>());
    AddCommand(std::make_unique<TCommandScheme>());
    AddCommand(std::make_unique<TCommandScripting>());
    AddCommand(std::make_unique<TCommandTable>());
    AddCommand(std::make_unique<TCommandTools>());
    AddCommand(std::make_unique<TCommandExport>(Settings.UseExportToYt.GetRef()));
    AddCommand(std::make_unique<TCommandImport>());
    AddCommand(std::make_unique<TCommandMonitoring>());
    AddCommand(std::make_unique<TCommandOperation>());
    AddCommand(std::make_unique<TCommandConfig>());
    AddCommand(std::make_unique<TCommandInit>());
    AddCommand(std::make_unique<TCommandYql>());
    AddCommand(std::make_unique<TCommandTopic>());
    AddCommand(std::make_unique<TCommandWorkload>());
    AddCommand(std::make_unique<TCommandBenchmark>());
}

void TClientCommandRootCommon::ValidateSettings() {
    if (!Settings.EnableSsl.Defined()) {
        Cerr << "Missing ssl enabling flag in client settings" << Endl;
    } else if (!Settings.UseOAuthToken.Defined()) {
        Cerr << "Missing OAuth token usage flag in client settings" << Endl;
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
    } else if (!Settings.YdbDir) {
        Cerr << "Missing YDB directory in client settings" << Endl;
    } else {
        return;
    }
    exit(EXIT_FAILURE);
}

void TClientCommandRootCommon::FillConfig(TConfig& config) {
    config.UseOAuthToken = Settings.UseOAuthToken.GetRef();
    config.UseIamAuth = Settings.UseIamAuth.GetRef();
    config.UseStaticCredentials = Settings.UseStaticCredentials.GetRef();
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

    if (config.UseOAuthToken) {
        TStringBuilder tokenHelp;
        tokenHelp << "OAuth token file" << Endl
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

void TClientCommandRootCommon::ParseCaCerts(TConfig& config) {
    if (CaCertsFile.empty()) {
        auto profile = Profile;
        if (!profile) {
            profile = ProfileManager->GetActiveProfile();
        }
        if (profile && profile->Has("ca-file")) {
            CaCertsFile = profile->GetValue("ca-file").as<TString>();
        }
    }
    if (!config.EnableSsl && !CaCertsFile.empty()) {
        throw TMisuseException()
            << "\"ca-file\" option provided for a non-ssl connection. Use grpcs:// prefix for host to connect using SSL.";
    }
    if (!CaCertsFile.empty()) {
        config.CaCerts = ReadFromFile(CaCertsFile, "CA certificates");
    }
}

void TClientCommandRootCommon::ParseAddress(TConfig& config) {
    TString hostname;
    TString port = "2135";

    if (Address.empty()) {
        auto profile = Profile;
        if (!profile) {
            profile = ProfileManager->GetActiveProfile();
        }
        if (profile && profile->Has("endpoint")) {
            Address = profile->GetValue("endpoint").as<TString>();
        }
    }

    if (!Address.empty()) {
        config.EnableSsl = Settings.EnableSsl.GetRef();
        TString message;
        if (!ParseProtocol(config, message)) {
            MisuseErrors.push_back(message);
        }
        auto colon_pos = Address.find(":");
        if (colon_pos == TString::npos) {
            config.Address = Address + ":" + port;
        } else {
            if (colon_pos == Address.rfind(":")) {
                config.Address = Address;
            } else {
                MisuseErrors.push_back("Wrong format for option 'endpoint': more than one colon found.");
                return;
            }
        }
    }
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
    if (Database.empty()) {
        auto profile = Profile;
        if (!profile) {
            profile = ProfileManager->GetActiveProfile();
        }
        if (profile && profile->Has("database")) {
            Database = profile->GetValue("database").as<TString>();
        }
    }

    config.Database = Database;
}

void TClientCommandRootCommon::ParseIamEndpoint(TConfig& config) {
    if (IamEndpoint.empty()) {
        auto profile = Profile;
        if (!profile) {
            profile = ProfileManager->GetActiveProfile();
        }
        if (profile && profile->Has("iam-endpoint")) {
            IamEndpoint = profile->GetValue("iam-endpoint").as<TString>();
        }
    }
    if (!IamEndpoint.empty()) {
        config.IamEndpoint = IamEndpoint;
    }
}

void TClientCommandRootCommon::Validate(TConfig& config) {
    TClientCommandRootBase::Validate(config);
    if (!config.NeedToConnect) {
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

        throw TMisuseException() << errors;
    }

    if (Address.empty()) {
        throw TMisuseException()
            << "Missing required option 'endpoint'.";
    }

    if (Database.empty()) {
        throw TMisuseException()
            << "Missing required option 'database'.";
    } else if (!Database.StartsWith('/')) {
        throw TMisuseException() << "Path to a database \"" << Database
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

namespace {
    inline void PrintSettingFromProfile(const TString& setting, std::shared_ptr<IProfile> profile, bool explicitOption) {
        Cout << "Using " << setting << " due to configuration in" << (explicitOption ? "" : " active") << " profile \""
            << profile->GetName() << "\"" << (explicitOption ? " from explicit --profile option" : "") << Endl;
    }
}

bool TClientCommandRootCommon::GetCredentialsFromProfile(std::shared_ptr<IProfile> profile, TConfig& config, bool explicitOption) {
    if (!profile || !profile->Has("authentication")) {
        return false;
    }
    auto authValue = profile->GetValue("authentication");
    if (!authValue["method"]) {
        MisuseErrors.push_back("Configuration profile has \"authentication\" but does not has \"method\" in it");
        return false;
    }
    TString authMethod = authValue["method"].as<TString>();

    if (authMethod == "use-metadata-credentials") {
        if (IsVerbose()) {
            PrintSettingFromProfile("metadata service", profile, explicitOption);
        }
        config.UseMetadataCredentials = true;
        return true;
    }
    if (authMethod == "anonymous-auth") {
        if (IsVerbose()) {
            PrintSettingFromProfile("anonymous authentication", profile, explicitOption);
        }
        return true;
    }
    bool knownMethod = false;
    if (config.UseIamAuth) {
        knownMethod |= (authMethod == "iam-token" || authMethod == "yc-token" || authMethod == "sa-key-file" ||
                        authMethod == "token-file" || authMethod == "yc-token-file");
    }
    if (config.UseOAuthToken) {
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
        if (IsVerbose()) {
            PrintSettingFromProfile("iam token", profile, explicitOption);
        }
        config.SecurityToken = authData.as<TString>();
    } else if (authMethod == "token-file") {
        if (IsVerbose()) {
            PrintSettingFromProfile("token file", profile, explicitOption);
        }
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
        config.SecurityToken = fileContent;
    } else if (authMethod == "yc-token") {
        if (IsVerbose()) {
            PrintSettingFromProfile("Yandex.Cloud Passport token (yc-token)", profile, explicitOption);
        }
        config.YCToken = authData.as<TString>();
    } else if (authMethod == "yc-token-file") {
        if (IsVerbose()) {
            PrintSettingFromProfile("Yandex.Cloud Passport token file (yc-token-file)", profile, explicitOption);
        }
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
        config.YCToken = fileContent;
    } else if (authMethod == "sa-key-file") {
        if (IsVerbose()) {
            PrintSettingFromProfile("service account key file (sa-key-file)", profile, explicitOption);
        }
        TString filePath = authData.as<TString>();
        if (filePath.StartsWith("~")) {
            filePath = HomeDir + filePath.substr(1);
        }
        config.SaKeyFile = filePath;
    } else if (authMethod == "ydb-token") {
        if (IsVerbose()) {
            PrintSettingFromProfile("OAuth token (ydb-token)", profile, explicitOption);
        }
        config.SecurityToken = authData.as<TString>();
    } else if (authMethod == "static-credentials") {
        if (IsVerbose()) {
            PrintSettingFromProfile("user name & password", profile, explicitOption);
        }
        if (authData["user"]) {
            config.StaticCredentials.User = authData["user"].as<TString>();
        }
        if (authData["password"]) {
            config.StaticCredentials.Password = authData["password"].as<TString>();
            if (!config.StaticCredentials.Password) {
                DoNotAskForPassword = true;
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
            config.StaticCredentials.Password = fileContent;
            if (!config.StaticCredentials.Password) {
                DoNotAskForPassword = true;
            }

        }

    } else {
        return false;
    }
    return true;
}

void TClientCommandRootCommon::ParseCredentials(TConfig& config) {
    size_t explicitAuthMethodCount = (size_t)(!TokenFile.empty()) + (size_t)(!YCTokenFile.empty())
        + (size_t)UseMetadataCredentials + (size_t)(!SaKeyFile.empty())
        + (size_t)(!UserName.empty() || !PasswordFile.empty() || DoNotAskForPassword);

    switch (explicitAuthMethodCount) {
    case 0:
    {
        // Priority 2. No explicit auth methods. Checking configuration profile given via --profile option.
        if (GetCredentialsFromProfile(Profile, config, true)) {
            break;
        }

        // Priority 3. No auth methods from --profile either. Checking environment variables.
        if (config.UseIamAuth) {
            TString envIamToken = GetEnv("IAM_TOKEN");
            if (!envIamToken.empty()) {
                if (IsVerbose()) {
                    Cout << "Using iam token from IAM_TOKEN env variable" << Endl;
                }
                config.SecurityToken = envIamToken;
                break;
            }
            TString envYcToken = GetEnv("YC_TOKEN");
            if (!envYcToken.empty()) {
                if (IsVerbose()) {
                    Cout << "Using Yandex.Cloud Passport token from YC_TOKEN env variable" << Endl;
                }
                config.YCToken = envYcToken;
                break;
            }
            if (GetEnv("USE_METADATA_CREDENTIALS") == "1") {
                if (IsVerbose()) {
                    Cout << "Using metadata service due to USE_METADATA_CREDENTIALS=\"1\" env variable" << Endl;
                }
                config.UseMetadataCredentials = true;
                break;
            }
            TString envSaKeyFile = GetEnv("SA_KEY_FILE");
            if (!envSaKeyFile.empty()) {
                if (IsVerbose()) {
                    Cout << "Using service account key file from SA_KEY_FILE env variable" << Endl;
                }
                config.SaKeyFile = envSaKeyFile;
                break;
            }
        }
        if (config.UseOAuthToken) {
            TString envYdbToken = GetEnv("YDB_TOKEN");
            if (!envYdbToken.empty()) {
                if (IsVerbose()) {
                    Cout << "Using OAuth token from YDB_TOKEN env variable" << Endl;
                }
                config.SecurityToken = envYdbToken;
                break;
            }
        }
        if (config.UseStaticCredentials) {
            TString userName = GetEnv("YDB_USER");
            if (!userName.empty()) {
                if (IsVerbose()) {
                    Cout << "Using user name from YDB_USER env variable" << Endl;
                }
                config.StaticCredentials.User = userName;
            }

            TString password = GetEnv("YDB_PASSWORD");
            if (!password.empty()) {
                if (IsVerbose()) {
                    Cout << "Using user password from YDB_PASSWORD env variable" << Endl;
                }
                config.StaticCredentials.Password = password;
            }
            if (!userName.empty() || !password.empty()) {
                break;
            }
        }

        // Priority 4. No auth methods from environment variables too. Checking active configuration profile.
        // (if --profile option is not set)
        if (!Profile && GetCredentialsFromProfile(ProfileManager->GetActiveProfile(), config, false)) {
            break;
        }

        if (Settings.UseDefaultTokenFile.GetRef()) {
            // Priority 5. No auth methods from active configuration profile. Checking default token file.
            TString tokenFile = defaultTokenFile;
            if (ReadFromFileIfExists(tokenFile, "default token", config.SecurityToken)) {
                if (IsVerbose()) {
                    Cout << "Using auth token from default token file " << defaultTokenFile << Endl;
                }
            } else {
                if (IsVerbose()) {
                    Cout << "No authentication methods were found. Going without authentication" << Endl;
                }
            }
        }
        break;
    }
    case 1:
        // Priority 1. Exactly one explicit auth method. Using it.
        if (TokenFile) {
            if (IsVerbose()) {
                Cout << "Using token from file provided with explicit option" << Endl;
            }
            config.SecurityToken = ReadFromFile(TokenFile, "token");
        } else if (YCTokenFile) {
            if (IsVerbose()) {
                Cout << "Using Yandex.Cloud Passport token from file provided with --yc-token-file option" << Endl;
            }
            config.YCToken = ReadFromFile(YCTokenFile, "token");
        } else if (UseMetadataCredentials) {
            if (IsVerbose()) {
                Cout << "Using metadata service due to --use-metadata-credentials option" << Endl;
            }
            config.UseMetadataCredentials = true;
        } else if (SaKeyFile) {
            if (IsVerbose()) {
                Cout << "Using service account key file provided with --sa-key-file option" << Endl;
            }
            config.SaKeyFile = SaKeyFile;
        } else if (UserName || PasswordFile) {
            if (UserName) {
                if (IsVerbose()) {
                    Cout << "Using user name provided with --user option" << Endl;
                }
                config.StaticCredentials.User = UserName;
            }
            if (PasswordFile) {
                if (IsVerbose()) {
                    Cout << "Using user password from file provided with --password-file option" << Endl;
                }
                config.StaticCredentials.Password = ReadFromFile(PasswordFile, "password", true);
                if (!config.StaticCredentials.Password) {
                    DoNotAskForPassword = true;
                }
            }
        }
        break;
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

        MisuseErrors.push_back(TStringBuilder() << str << ". Choose exactly one of them");
        return;
    }

    if (config.UseStaticCredentials) {
        if (config.StaticCredentials.User) {
            if (!config.StaticCredentials.Password && !DoNotAskForPassword) {
                Cerr << "Enter password for user " << config.StaticCredentials.User << ": ";
                config.StaticCredentials.Password = InputPassword();
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
