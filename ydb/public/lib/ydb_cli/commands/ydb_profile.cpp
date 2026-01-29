#include "ydb_profile.h"

#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>

#include <util/folder/dirut.h>
#include <util/stream/input.h>
#include <util/string/strip.h>

#include <library/cpp/string_utils/url/url.h>

#include <cstdio>

#if defined(_win32_)
#include <io.h>
#elif defined(_unix_)
#include <unistd.h>
#endif


namespace NYdb::NConsoleClient {

const TString AuthNode = "authentication";

TCommandConfig::TCommandConfig()
    : TClientCommandTree("config", {}, "Manage YDB CLI configuration")
{
    AddCommand(std::make_unique<TCommandProfile>());
    AddCommand(std::make_unique<TCommandConnectionInfo>());
}

void TCommandConfig::Config(TConfig& config) {
    TClientCommandTree::Config(config);
}

TCommandProfile::TCommandProfile()
    : TClientCommandTree("profile", {}, "Manage configuration profiles")
{
    AddCommand(std::make_unique<TCommandCreateProfile>());
    AddCommand(std::make_unique<TCommandDeleteProfile>());
    AddCommand(std::make_unique<TCommandActivateProfile>());
    AddCommand(std::make_unique<TCommandDeactivateProfile>());
    AddCommand(std::make_unique<TCommandListProfiles>());
    AddCommand(std::make_unique<TCommandGetProfile>());
    AddCommand(std::make_unique<TCommandUpdateProfile>());
    AddCommand(std::make_unique<TCommandReplaceProfile>());
}

namespace {
    std::string ReplaceWithAsterisks(const std::string& in) {
        return std::string(in.length(), '*');
    }

    void SetupProfileName(TString& profileName, std::shared_ptr<IProfileManager> profileManager) {
        if (profileName) {
            return;
        }

        const auto profileNames = profileManager->ListProfiles();
        if (profileNames.empty()) {
            Cout << "You have no existing profiles yet." << Endl;
            auto result = RunFtxuiInput(
                "Please enter name for a new profile",
                "",
                [](const TString& input, TString& error) {
                    if (input.empty()) {
                        error = "Profile name cannot be empty";
                        return false;
                    }
                    return true;
                }
            );
            if (result) {
                profileName = *result;
            } else {
                Cout << "Cancelled." << Endl;
                exit(EXIT_SUCCESS);
            }
            return;
        }

        // Build menu options
        std::vector<TString> options;
        options.push_back("Create a new profile");

        const auto& activeProfileName = profileManager->GetActiveProfileName();
        for (const auto& name : profileNames) {
            TStringBuilder description;
            description << name;
            if (name == activeProfileName) {
                description << "\t(active)";
            }
            options.push_back(description);
        }

        auto selectedIdx = RunFtxuiMenu("Please choose profile to configure", options);
        if (!selectedIdx) {
            Cout << "Cancelled." << Endl;
            exit(EXIT_SUCCESS);
        }

        if (*selectedIdx == 0) {
            // Create a new profile
            auto result = RunFtxuiInput(
                "Please enter name for a new profile",
                "",
                [](const TString& input, TString& error) {
                    if (input.empty()) {
                        error = "Profile name cannot be empty";
                        return false;
                    }
                    return true;
                }
            );
            if (result) {
                profileName = *result;
            } else {
                Cout << "Cancelled." << Endl;
                exit(EXIT_SUCCESS);
            }
        } else {
            profileName = profileNames[*selectedIdx - 1];
        }
    }

    void PutAuthMethod(std::shared_ptr<IProfile> profile, const TString& id, const TString& value) {
        profile->RemoveValue(AuthNode);
        YAML::Node authValue;
        authValue["method"] = id;
        authValue["data"] = value;
        profile->SetValue(AuthNode, authValue);
    }

    void PutAuthStatic(std::shared_ptr<IProfile> profile, const TString& user, const TString& pass, bool file) {
        profile->RemoveValue(AuthNode);
        YAML::Node authValue;
        authValue["method"] = "static-credentials";
        auto authData = authValue["data"];
        authData["user"] = user;
        if (file) {
            authData["password-file"] = pass;
        } else {
            authData["password"] = pass;
        }
        profile->SetValue(AuthNode, authValue);
    }

    void PutAuthMethodWithoutPars(std::shared_ptr<IProfile> profile, const TString& id) {
        profile->RemoveValue(AuthNode);
        YAML::Node authValue;
        authValue["method"] = id;
        profile->SetValue(AuthNode, authValue);
    }

    void SetAuthMethod(const TString& id, const TString& fullName, std::shared_ptr<IProfile> profile, const TString& profileName) {
        TString title = TStringBuilder() << "Please enter " << fullName << " (" << id << ")";
        auto newValue = RunFtxuiInput(title);
        if (!newValue) {
            Cout << "Cancelled." << Endl;
            return;
        }
        if (*newValue) {
            PutAuthMethod(profile, id, *newValue);
            Cout << "Saved " << fullName << " for profile \"" << profileName << "\"." << Endl;
        }
    }

    void SetStaticCredentials(std::shared_ptr<IProfile> profile, const TString& profileName) {
        auto userName = RunFtxuiInput("Please enter user name");
        if (!userName) {
            Cout << "Cancelled." << Endl;
            return;
        }

        auto userPassword = RunFtxuiPasswordInput("Please enter password");
        if (!userPassword) {
            Cout << "Cancelled." << Endl;
            return;
        }

        if (*userName) {
            PutAuthStatic(profile, *userName, *userPassword, false);
            if (userPassword->empty()) {
                Cout << "Saved credentials with empty password for profile \"" << profileName << "\"." << Endl;
            } else {
                Cout << "Saved credentials for profile \"" << profileName << "\"." << Endl;
            }
        }
    }

    std::string TryBlurValue(const TString& authMethod, const TString& value) {
        if (!IsStdoutInteractive() || authMethod == "sa-key-file" || authMethod == "token-file" || authMethod == "yc-token-file" || authMethod == "oauth2-key-file") {
            return value;
        }
        if (authMethod == "password") {
            return ReplaceWithAsterisks(value);
        }
        return BlurSecret(value);
    }

    void PrintProfileContent(std::shared_ptr<IProfile> profile) {
        if (profile->Has("endpoint")) {
            Cout << "  endpoint: " << profile->GetValue("endpoint").as<TString>() << Endl;
        }
        if (profile->Has("database")) {
            Cout << "  database: " << profile->GetValue("database").as<TString>() << Endl;
        }
        if (profile->Has(AuthNode)) {
            auto authValue = profile->GetValue(AuthNode);
            TString authMethod = authValue["method"].as<TString>();
            Cout << "  " << authMethod;
            if (authMethod == "ydb-token" || authMethod == "oauth2-key-file" || authMethod == "iam-token"
                || authMethod == "yc-token" || authMethod == "sa-key-file"
                || authMethod == "token-file" || authMethod == "yc-token-file") {
                Cout << ": " << TryBlurValue(authMethod, authValue["data"].as<TString>());
            } else if (authMethod == "static-credentials") {
                auto authData = authValue["data"];
                if (authData) {
                    if (authData["user"]) {
                        Cout << Endl << "    user: " << authData["user"].as<TString>();
                    }
                    if (authData["password"]) {
                        Cout << Endl << "    password: " << TryBlurValue("password", authData["password"].as<TString>());
                    }
                    if (authData["password-file"]) {
                        Cout << Endl << "    password file: " << authData["password-file"].as<TString>();
                    }
                }
            }
            Cout << Endl;
        }
        if (profile->Has("iam-endpoint")) {
            Cout << "  iam-endpoint: " << profile->GetValue("iam-endpoint").as<TString>() << Endl;
        }
        if (profile->Has("ca-file")) {
            Cout << "  ca-file: " << profile->GetValue("ca-file").as<TString>() << Endl;
        }
        if (profile->Has("client-cert-file")) {
            Cout << "  client-cert-file: " << profile->GetValue("client-cert-file").as<TString>() << Endl;
        }
        if (profile->Has("client-cert-key-file")) {
            Cout << "  client-cert-key-file: " << profile->GetValue("client-cert-key-file").as<TString>() << Endl;
        }
        if (profile->Has("client-cert-key-password-file")) {
            Cout << "  client-cert-key-password-file: " << profile->GetValue("client-cert-key-password-file").as<TString>() << Endl;
        }
    }
} // anonymous namespace

TCommandConnectionInfo::TCommandConnectionInfo()
    : TClientCommand("info", {}, "List current connection parameters")
{}

void TCommandConnectionInfo::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsNum(0);
}

int TCommandConnectionInfo::Run(TConfig& config) {
    if (config.IsVerbose()) {
        PrintVerboseInfo(config);
    } else {
        PrintInfo(config);
    }
    return EXIT_SUCCESS;
}

void TCommandConnectionInfo::PrintInfo(TConfig& config) {
    if (config.Address) {
        Cout << "endpoint: " << config.Address << Endl;
    }
    if (config.Database) {
        Cout << "database: " << config.Database << Endl;
    }
    if (config.SecurityToken) {
        Cout << "token: " << TryBlurValue("token", config.SecurityToken) << Endl;
    }
    if (config.UseOauth2TokenExchange) {
        if (config.Oauth2KeyFile) {
            Cout << "oauth2-key-file: " << config.Oauth2KeyFile << Endl;
        }
    }
    if (config.UseIamAuth) {
        if (config.YCToken) {
            Cout << "yc-token: " << TryBlurValue("yc-token", config.YCToken) << Endl;
        }
        if (config.SaKeyFile) {
            Cout << "sa-key-file: " << config.SaKeyFile << Endl;
        }
        if (config.UseMetadataCredentials) {
            Cout << "use-metadata-credentials" << Endl;
        }
        if (config.IamEndpoint) {
            Cout << "iam-endpoint: " << config.IamEndpoint << Endl;
        }
    }
    if (config.UseStaticCredentials) {
        if (!config.StaticCredentials.User.empty()) {
            Cout << "user: " << config.StaticCredentials.User << Endl;
        }
        if (!config.StaticCredentials.Password.empty()) {
            Cout << "password: " << TryBlurValue("password", TString(config.StaticCredentials.Password)) << Endl;
        }
    }
    if (config.CaCertsFile) {
        Cout << "ca-file: " << config.CaCertsFile << Endl;
    }
    if (config.ClientCertFile) {
        Cout << "client-cert-file: " << config.ClientCertFile << Endl;
    }
    if (config.ClientCertPrivateKeyFile) {
        Cout << "client-cert-key-file: " << config.ClientCertPrivateKeyFile << Endl;
    }
    if (config.ClientCertPrivateKeyPasswordFile) {
        Cout << "client-cert-key-password-file: " << config.ClientCertPrivateKeyPasswordFile << Endl;
    }
}

void TCommandConnectionInfo::PrintVerboseInfo(TConfig& config) {
    Cout << Endl;
    PrintInfo(config);
    Cout << "current auth method: " << (config.ChosenAuthMethod ? config.ChosenAuthMethod : "no-auth") << Endl;
    for (const auto& [name, params] : config.ConnectionParams) {
        size_t cnt = 1;
        Cout << Endl << "\"" << name << "\" sources:" << Endl;
        for (const auto& [value, source] : params) {
            Cout << "  " << cnt << ". Value: " << TryBlurValue(name, value) << ". Got from: " << source << Endl;
            ++cnt;
        }
    }
}

void TCommandProfile::Config(TConfig& config) {
    TClientCommandTree::Config(config);

    config.NeedToConnect = false;
}

TCommandInit::TCommandInit()
    : TCommandProfileCommon("init", {}, "YDB CLI initialization")
{}

void TCommandInit::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;

    config.SetFreeArgsNum(0);
}

int TCommandInit::Run(TConfig& config) {
    Cout << "Welcome! This command will take you through the configuration process." << Endl;
    auto profileManager = CreateProfileManager(config.ProfileFile);
    TString profileName;
    SetupProfileName(profileName, profileManager);
    ConfigureProfile(profileName, profileManager, config, /* interactive */ true, /* cmdLine */ false);
    return EXIT_SUCCESS;
}

TCommandProfileCommon::TCommandProfileCommon(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TClientCommand(name, aliases, description)
{}

void TCommandProfileCommon::ParseUrl(const TString &url) {
    if (Endpoint) {
        throw TMisuseException() << "You entered too many \"endpoint\" options.";
    }
    TString trimmedUrl;
    Strip(url, trimmedUrl);
    TStringBuf endpoint, query, fragment;
    SeparateUrlFromQueryAndFragment(trimmedUrl, endpoint, query, fragment);
    Y_UNUSED(fragment);
    if (query.contains("database=")) {
        if (Database) {
            throw TMisuseException() << "You entered too many \"database\" options.";
        }
        Endpoint = GetSchemeHostAndPort(endpoint);
        query.remove_prefix(query.find("database=") + 9);
        Database = query.substr(0, query.find_first_of(";&"));
    } else {
        Endpoint = trimmedUrl;
    }
}

void TCommandProfileCommon::GetOptionsFromStdin() {
    TString line, trimmedLine;
    THashMap<TString, TString&> options {
        {"database", Database},
        {"token-file", TokenFile},
        {"oauth2-key-file", Oauth2KeyFile},
        {"yc-token-file", YcTokenFile},
        {"iam-token-file", IamTokenFile},
        {"sa-key-file", SaKeyFile},
        {"user", User},
        {"password-file", PasswordFile},
        {"iam-endpoint", IamEndpoint},
        {"ca-file", CaCertsFile},
        {"client-cert-file", ClientCertFile},
        {"client-cert-key-file", ClientCertPrivateKeyFile},
        {"client-cert-key-password-file", ClientCertPrivateKeyPasswordFile},
    };
    while (Cin.ReadLine(line)) {
        Strip(line, trimmedLine);
        if (trimmedLine.StartsWith("endpoint:")) {
            ParseUrl(ToString(trimmedLine.substr(9)));
            continue;
        }

        if (trimmedLine.StartsWith("use-metadata-credentials")) {
            if (UseMetadataCredentials) {
                throw TMisuseException() << "You entered too many \"use-metadata-credentials\" options.";
            }
            UseMetadataCredentials = true;
            continue;
        }
        if (trimmedLine.StartsWith("anonymous-auth")) {
            if (AnonymousAuth) {
                throw TMisuseException() << "You entered too many \"anonymous-auth\" options.";
            }
            AnonymousAuth = true;
            continue;
        }

        for (const auto& [str, ref] : options) {
            if (trimmedLine.StartsWith(str + ":")) {
                if (ref) {
                    throw TMisuseException() << "You entered too many \"" << str << "\" options.";
                }
                Strip(trimmedLine.substr(str.size() + 1), ref);
                break;
            }
        }
    }
}

void TCommandProfileCommon::ConfigureProfile(const TString& profileName, std::shared_ptr<IProfileManager> profileManager,
                      TConfig& config, bool interactive, bool cmdLine) {
    bool existingProfile = profileManager->HasProfile(profileName);
    auto profile = profileManager->GetProfile(profileName);
    if (interactive) {
        Cout << Endl << (existingProfile ? "Updating" : "Creating")
             << " profile \"" << profileName << "\"..." << Endl;
    }
    SetupProfileSetting("endpoint", Endpoint, existingProfile, profileName, profile, interactive, cmdLine, config.IsVerbose());
    SetupProfileSetting("database", Database, existingProfile, profileName, profile, interactive, cmdLine, config.IsVerbose());
    SetupProfileAuthentication(existingProfile, profileName, profile, config, interactive, cmdLine);
    if (cmdLine && CaCertsFile) {
        profile->SetValue("ca-file", CaCertsFile);
    }
    if (cmdLine && ClientCertFile) {
        profile->SetValue("client-cert-file", ClientCertFile);
    }
    if (cmdLine && ClientCertPrivateKeyFile) {
        profile->SetValue("client-cert-key-file", ClientCertPrivateKeyFile);
    }
    if (cmdLine && ClientCertPrivateKeyPasswordFile) {
        profile->SetValue("client-cert-key-password-file", ClientCertPrivateKeyPasswordFile);
    }

    if (interactive) {
        TString activeProfileName = profileManager->GetActiveProfileName();
        if (profileName != activeProfileName) {
            TStringBuilder query;
            query << "Activate profile \"" << profileName << "\" to use by default? (current active profile is ";
            TString currentActiveProfile = profileManager->GetActiveProfileName();
            if (currentActiveProfile) {
                query << "\"" << currentActiveProfile << "\"";
            } else {
                query << "not set";
            }
            query << ")";
            if (AskYesNoFtxui(query, /* defaultAnswer */ true)) {
                profileManager->SetActiveProfile(profileName);
                Cout << "Profile \"" << profileName << "\" is now active." << Endl;
            }
        }
        Cout << "Profile \"" << profileName << "\" configured successfully." << Endl;
    }
}

void TCommandProfileCommon::SetupProfileSetting(const TString& name, const TString& value, bool existingProfile, const TString& profileName,
    std::shared_ptr<IProfile> profile, bool interactive, bool cmdLine, bool /* verbose */)
{
    if (cmdLine) {
        if (value) {
            profile->SetValue(name, value);
            return;
        }
    }
    if (!interactive) {
        return;
    }

    // Build menu options
    std::vector<TString> options;
    options.push_back(TStringBuilder() << "Set a new " << name << " value");
    options.push_back(TStringBuilder() << "Don't save " << name);
    if (existingProfile && profile->Has(name)) {
        options.push_back(TStringBuilder() << "Use current " << name << " value\t" << profile->GetValue(name).as<TString>());
    }

    TString title = TStringBuilder() << "Pick desired action to configure " << name << " in profile \"" << profileName << "\"";
    auto selectedIdx = RunFtxuiMenu(title, options);
    if (!selectedIdx) {
        Cout << "Cancelled." << Endl;
        exit(EXIT_SUCCESS);
    }

    switch (*selectedIdx) {
        case 0: {
            // Set a new value
            TString inputTitle = TStringBuilder() << "Please enter new " << name << " value";
            auto input = RunFtxuiInput(inputTitle);
            if (!input) {
                Cout << "Cancelled." << Endl;
                exit(EXIT_SUCCESS);
            }
            if (*input) {
                profile->SetValue(name, *input);
                Cout << "Saved " << name << " \"" << *input << "\" for profile \"" << profileName << "\"." << Endl;
            }
            break;
        }
        case 1:
            // Don't save
            profile->RemoveValue(name);
            break;
        case 2:
            // Use current value - do nothing
            break;
    }
}

void TCommandProfileCommon::SetupProfileAuthentication(bool existingProfile, const TString& profileName, std::shared_ptr<IProfile> profile,
    TConfig& config, bool interactive, bool cmdLine)
{
    if (cmdLine) {
        if (SetAuthFromCommandLine(profile)) {
            return;
        }
    }
    if (!interactive) {
        return;
    }

    // Build menu options with corresponding actions
    std::vector<TString> options;
    std::vector<std::function<void()>> actions;

    if (config.UseStaticCredentials) {
        options.push_back("Use static credentials\t(user & password)");
        actions.push_back([&profile, &profileName]() {
            SetStaticCredentials(profile, profileName);
        });
    }
    if (config.UseIamAuth) {
        options.push_back("Use IAM token\t(iam-token) cloud.yandex.ru/docs/iam/concepts/authorization/iam-token");
        actions.push_back([&profile, &profileName]() {
            SetAuthMethod("iam-token", "IAM token", profile, profileName);
        });

        options.push_back("Use OAuth token of a Yandex Passport user\t(yc-token) cloud.yandex.ru/docs/iam/concepts/authorization/oauth-token");
        actions.push_back([&profile, &profileName]() {
            SetAuthMethod("yc-token", "OAuth token of a Yandex Passport user", profile, profileName);
        });

        options.push_back("Use OAuth 2.0 token exchange credentials\t(oauth2-key-file)");
        actions.push_back([&profile, &profileName]() {
            SetAuthMethod("oauth2-key-file", "OAuth 2.0 RFC8693 token exchange credentials parameters json file", profile, profileName);
        });

        options.push_back("Use metadata service on a virtual machine\t(use-metadata-credentials) cloud.yandex.ru/docs/compute/operations/vm-connect/auth-inside-vm");
        actions.push_back([&profile, &profileName]() {
            PutAuthMethodWithoutPars(profile, "use-metadata-credentials");
            Cout << "Metadata service authentication enabled for profile \"" << profileName << "\"." << Endl;
        });

        options.push_back("Use service account key file\t(sa-key-file) cloud.yandex.ru/docs/iam/operations/iam-token/create-for-sa");
        actions.push_back([&profile, &profileName]() {
            SetAuthMethod("sa-key-file", "Path to service account key file", profile, profileName);
        });
    }
    if (config.UseAccessToken) {
        options.push_back("Set new access token\t(ydb-token)");
        actions.push_back([&profile, &profileName]() {
            SetAuthMethod("ydb-token", "YDB token", profile, profileName);
        });
    }

    options.push_back("Set anonymous authentication");
    actions.push_back([&profile, &profileName]() {
        PutAuthMethodWithoutPars(profile, "anonymous-auth");
        Cout << "Anonymous authentication enabled for profile \"" << profileName << "\"." << Endl;
    });

    options.push_back("Don't save authentication data\t(environment variables can be used)");
    actions.push_back([&profile]() {
        profile->RemoveValue(AuthNode);
    });

    if (existingProfile && profile->Has(AuthNode)) {
        auto& authValue = profile->GetValue(AuthNode);
        if (authValue["method"]) {
            TString method = authValue["method"].as<TString>();
            TStringBuilder description;
            description << "Use current settings\t" << method;
            if (method == "iam-token" || method == "yc-token" || method == "ydb-token") {
                description << ": " << BlurSecret(authValue["data"].as<TString>());
            } else if (method == "sa-key-file" || method == "token-file" || method == "yc-token-file" || method == "oauth2-key-file") {
                description << ": " << authValue["data"].as<TString>();
            }
            options.push_back(description);
            actions.push_back([]() {});
        }
    }

    auto selectedIdx = RunFtxuiMenu("Pick desired action to configure authentication method", options);
    if (!selectedIdx) {
        Cout << "Cancelled." << Endl;
        exit(EXIT_SUCCESS);
    }

    actions[*selectedIdx]();
}

bool TCommandProfileCommon::SetAuthFromCommandLine(std::shared_ptr<IProfile> profile) {
    if (IamEndpoint) {
        profile->SetValue("iam-endpoint", IamEndpoint);
    }
    if (TokenFile) {
        PutAuthMethod(profile, "token-file", TokenFile);
    } else if (Oauth2KeyFile) {
        PutAuthMethod(profile, "oauth2-key-file", Oauth2KeyFile);
    } else if (IamTokenFile) {
        // no error here, we take the iam-token-file option as just a token-file authentication
        PutAuthMethod(profile, "token-file", IamTokenFile);
    } else if (YcTokenFile) {
        PutAuthMethod(profile, "yc-token-file", YcTokenFile);
    } else if (UseMetadataCredentials) {
        PutAuthMethodWithoutPars(profile, "use-metadata-credentials");
    } else if (SaKeyFile) {
        PutAuthMethod(profile, "sa-key-file", SaKeyFile);
    } else if (User) {
        PutAuthStatic(profile, User, PasswordFile, true);
    } else if (AnonymousAuth) {
        PutAuthMethodWithoutPars(profile, "anonymous-auth");
    } else {
        return false;
    }
    return true;
}

void TCommandProfileCommon::ValidateAuth() {
    size_t authMethodCount =
            (bool) (TokenFile) + (bool) (Oauth2KeyFile) +
            (bool) (IamTokenFile) +
            (bool) (YcTokenFile) + UseMetadataCredentials +
            (bool) (SaKeyFile) + AnonymousAuth +
            (User || PasswordFile);

    if (!User && PasswordFile) {
        throw TMisuseException() << "You cannot enter password-file without user";
    }

    if (authMethodCount > 1) {
        TStringBuilder str;
        str << authMethodCount << " authentication methods were provided via options:";
        if (TokenFile) {
            str << " TokenFile (" << TokenFile << ")";
        }
        if (Oauth2KeyFile) {
            str << " OAuth2KeyFile (" << Oauth2KeyFile << ")";
        }
        if (IamTokenFile) {
            str << " IamTokenFile (" << IamTokenFile << ")";
        }
        if (YcTokenFile) {
            str << " YCTokenFile (" << YcTokenFile << ")";
        }
        if (UseMetadataCredentials) {
            str << " Metadata credentials";
        }
        if (SaKeyFile) {
            str << " SAKeyFile (" << SaKeyFile << ")";
        }
        if (User) {
            str << " User (" << User << ")";
        }
        if (PasswordFile) {
            str << " Password file (" << PasswordFile << ")";
        }
        if (AnonymousAuth) {
            str << " Anonymous authentication";
        }

        throw TMisuseException() << str << ". Choose exactly one of them";
    }
}

bool TCommandProfileCommon::AnyProfileOptionInCommandLine() {
    return Endpoint || Database || TokenFile || Oauth2KeyFile ||
           IamTokenFile || YcTokenFile ||
           SaKeyFile || UseMetadataCredentials || User ||
           PasswordFile || IamEndpoint || AnonymousAuth || CaCertsFile ||
           ClientCertFile || ClientCertPrivateKeyFile || ClientCertPrivateKeyPasswordFile;
}

TCommandCreateProfile::TCommandCreateProfile()
    : TCommandProfileCommon("create", {}, "Create new configuration profile or re-configure existing one")
{}

void TCommandProfileCommon::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<name>", "Profile name");
    TClientCommandOptions& opts = *config.Opts;

    opts.AddLongOption('e', "endpoint", "Endpoint to save in the profile").RequiredArgument("STRING").StoreResult(&Endpoint);
    opts.AddLongOption('d', "database", "Database to save in the profile").RequiredArgument("PATH").StoreResult(&Database);

    opts.AddLongOption("token-file", "Access token file").RequiredArgument("PATH").StoreResult(&TokenFile);
    if (config.UseOauth2TokenExchange) {
        opts.AddLongOption("oauth2-key-file", "OAuth 2.0 RFC8693 token exchange credentials parameters json file").RequiredArgument("PATH").StoreResult(&Oauth2KeyFile);
    }
    opts.AddLongOption("iam-token-file", "Access token file").RequiredArgument("PATH").Hidden().StoreResult(&IamTokenFile);
    opts.AddLongOption("anonymous-auth", "Anonymous authentication").Optional().StoreTrue(&AnonymousAuth);
    if (config.UseIamAuth) {
        opts.AddLongOption("yc-token-file", "YC OAuth refresh token file").RequiredArgument("PATH").StoreResult(&YcTokenFile);
        opts.AddLongOption("use-metadata-credentials", "Metadata service authentication").Optional().StoreTrue(&UseMetadataCredentials);
        opts.AddLongOption("sa-key-file", "YC Service account key file").RequiredArgument("PATH").StoreResult(&SaKeyFile);
    }

    if (config.UseStaticCredentials) {
        opts.AddLongOption("user", "User name").RequiredArgument("STR").StoreResult(&User);
        opts.AddLongOption("password-file", "Password file").RequiredArgument("PATH").StoreResult(&PasswordFile);
    }
    if (config.UseIamAuth) {
        opts.AddLongOption("iam-endpoint", "Endpoint of IAM service to refresh token in YC OAuth or YC Service account authentication modes")
        .RequiredArgument("STR").StoreResult(&IamEndpoint);
    }
    opts.AddLongOption("ca-file",
        "File containing PEM encoded root certificates for SSL/TLS connections.")
        .RequiredArgument("PATH").StoreResult(&CaCertsFile);
    opts.AddLongOption("client-cert-file",
        "File containing client certificate for SSL/TLS connections (PKCS#12 or PEM-encoded).")
        .RequiredArgument("PATH").StoreResult(&ClientCertFile);
    opts.AddLongOption("client-cert-key-file",
        "File containing PEM encoded client certificate private key for SSL/TLS connections.")
        .RequiredArgument("PATH").StoreResult(&ClientCertPrivateKeyFile);
    opts.AddLongOption("client-cert-key-password-file",
        "File containing password for client certificate private key (if key is encrypted). If key file is encrypted, but this option is not set, password will be asked interactively.")
        .RequiredArgument("PATH").StoreResult(&ClientCertPrivateKeyPasswordFile);
    if (!IsStdinInteractive()) {
        GetOptionsFromStdin();
    }
}

void TCommandProfileCommon::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ValidateAuth();
}

void TCommandCreateProfile::Config(TConfig& config) {
    TCommandProfileCommon::Config(config);
}

void TCommandCreateProfile::Parse(TConfig& config) {
    TCommandProfileCommon::Parse(config);
    if (config.ParseResult->GetFreeArgCount()) {
        ProfileName = config.ParseResult->GetFreeArgs()[0];
    } else {
        if (!IsStdinInteractive()) {
            throw TMisuseException() << "You should enter profile name when stdin is not interactive";
        }
    }
}

int TCommandCreateProfile::Run(TConfig& config) {
    TString profileName = ProfileName;
    Interactive = (!AnyProfileOptionInCommandLine() || !profileName) && IsStdinInteractive();
    auto profileManager = CreateProfileManager(config.ProfileFile);
    if (Interactive) {
        Cout << "Welcome! This command will take you through configuration profile creation process." << Endl;
    } else {
        if (profileManager->HasProfile(ProfileName)) {
            Cerr << "Profile \"" << ProfileName << "\" already exists. Consider using update, replace or delete command." << Endl;
            return EXIT_FAILURE;
        }
        profileManager->CreateProfile(ProfileName);
    }
    if (!profileName) {
        auto result = RunFtxuiInput(
            "Please enter configuration profile name to create or re-configure",
            "",
            [](const TString& input, TString& error) {
                if (input.empty()) {
                    error = "Profile name cannot be empty";
                    return false;
                }
                return true;
            }
        );
        if (result) {
            profileName = *result;
        } else {
            Cout << "Cancelled." << Endl;
            return EXIT_SUCCESS;
        }
    }
    ConfigureProfile(profileName, profileManager, config, Interactive, true);
    return EXIT_SUCCESS;
}

TCommandDeleteProfile::TCommandDeleteProfile()
    : TClientCommand("delete", {"remove"}, "Delete specified configuration profile")
{}

void TCommandDeleteProfile::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<name>", "Profile name (case sensitive)");

    config.Opts->AddLongOption('f', "force", "Ignore nonexistent profiles, never prompt")
        .StoreTrue(&Force);
}

void TCommandDeleteProfile::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (config.ParseResult->GetFreeArgCount()) {
        ProfileName = config.ParseResult->GetFreeArgs()[0];
    }
    if (Force && !ProfileName) {
        throw TMisuseException() << "Profile name was not specified while using --force option.";
    }
}

int TCommandDeleteProfile::Run(TConfig& config) {
    auto profileManager = CreateProfileManager(config.ProfileFile);
    const auto profileNames = profileManager->ListProfiles();
    if (ProfileName) {
        if (find(profileNames.begin(), profileNames.end(), ProfileName) == profileNames.end()) {
            if (Force) {
                return EXIT_SUCCESS;
            }
            Cerr << "No existing profile \"" << ProfileName << "\". "
                << "Run \"ydb config profile list\" without arguments to see existing profiles" << Endl;
            return EXIT_FAILURE;
        }
    }
    if (!ProfileName) {
        if (profileNames.size()) {
            // Build menu options
            std::vector<TString> options;
            options.push_back("Don't remove anything, just exit");
            TString activeProfileName = profileManager->GetActiveProfileName();
            for (const auto& name : profileNames) {
                TStringBuilder description;
                description << name;
                if (name == activeProfileName) {
                    description << "\t(active)";
                }
                options.push_back(description);
            }

            auto selectedIdx = RunFtxuiMenu("Please choose profile to remove", options);
            if (!selectedIdx || *selectedIdx == 0) {
                Cout << "No changes made." << Endl;
                return EXIT_SUCCESS;
            }

            ProfileName = profileNames[*selectedIdx - 1];
        } else {
            Cerr << "You have no existing profiles yet." << Endl;
            return EXIT_FAILURE;
        }
    }
    if (Force) {
        profileManager->RemoveProfile(ProfileName);
    } else {
        TString question = TStringBuilder() << "Profile \"" << ProfileName << "\" will be permanently removed. Continue?";
        if (AskYesNoFtxui(question)) {
            if (profileManager->RemoveProfile(ProfileName)) {
                Cout << "Profile \"" << ProfileName << "\" deleted." << Endl;
            } else {
                Cerr << "Failed to delete profile \"" << ProfileName << "\"." << Endl;
                return EXIT_FAILURE;
            }
        } else {
            Cout << "No changes made." << Endl;
            return EXIT_SUCCESS;
        }
    }

    return EXIT_SUCCESS;
}

TCommandActivateProfile::TCommandActivateProfile()
    : TClientCommand("activate", {"set"}, "Activate specified configuration profile")
{}

void TCommandActivateProfile::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<name>", "Profile name (case sensitive)");
}

void TCommandActivateProfile::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (config.ParseResult->GetFreeArgCount()) {
        ProfileName = config.ParseResult->GetFreeArgs()[0];
    }
}

int TCommandActivateProfile::Run(TConfig& config) {
    auto profileManager = CreateProfileManager(config.ProfileFile);
    const auto profileNames = profileManager->ListProfiles();
    if (ProfileName) {
        if (find(profileNames.begin(), profileNames.end(), ProfileName) == profileNames.end()) {
            Cerr << "No existing profile \"" << ProfileName << "\". "
                << "Run \"ydb config profile list\" without arguments to see existing profiles" << Endl;
            return EXIT_FAILURE;
        }
    }
    TString currentActiveProfileName = profileManager->GetActiveProfileName();
    if (!ProfileName) {
        const auto profileNames = profileManager->ListProfiles();
        if (profileNames.size()) {
            // Build menu options
            std::vector<TString> options;
            options.push_back("Don't do anything, just exit");
            if (currentActiveProfileName) {
                options.push_back(TStringBuilder() << "Deactivate current active profile\t" << currentActiveProfileName);
            }
            for (const auto& name : profileNames) {
                TStringBuilder description;
                description << name;
                if (name == currentActiveProfileName) {
                    description << "\t(active)";
                }
                options.push_back(description);
            }

            auto selectedIdx = RunFtxuiMenu("Please choose profile to activate", options);
            if (!selectedIdx || *selectedIdx == 0) {
                Cout << "No changes made." << Endl;
                return EXIT_SUCCESS;
            }

            size_t optionOffset = 1; // "Don't do anything" option
            if (currentActiveProfileName) {
                if (*selectedIdx == 1) {
                    // Deactivate current profile
                    profileManager->DeactivateProfile();
                    Cout << "Profile \"" << currentActiveProfileName << "\" deactivated." << Endl;
                    return EXIT_SUCCESS;
                }
                optionOffset = 2; // "Don't do anything" + "Deactivate" options
            }

            ProfileName = profileNames[*selectedIdx - optionOffset];
        } else {
            Cerr << "You have no existing profiles yet. Run \"ydb init\" to create one." << Endl;
            return EXIT_FAILURE;
        }
    }
    if (currentActiveProfileName != ProfileName) {
        profileManager->SetActiveProfile(ProfileName);
        Cout << "Profile \"" << ProfileName << "\" is now active." << Endl;
    } else {
        Cout << "Profile \"" << ProfileName << "\" is already active." << Endl;
    }
    return EXIT_SUCCESS;
}

TCommandDeactivateProfile::TCommandDeactivateProfile()
    : TClientCommand("deactivate", {"unset"}, "Deactivate current active configuration profile")
{}

void TCommandDeactivateProfile::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(0);
}

int TCommandDeactivateProfile::Run(TConfig& config) {
    auto profileManager = CreateProfileManager(config.ProfileFile);
    TString currentActiveProfileName = profileManager->GetActiveProfileName();
    if (currentActiveProfileName) {
        profileManager->DeactivateProfile();
        Cout << "Profile \"" << currentActiveProfileName << "\" deactivated." << Endl;
    } else {
        Cout << "No active profile to deactivate." << Endl;
    }
    return EXIT_SUCCESS;
}

TCommandListProfiles::TCommandListProfiles()
    : TClientCommand("list", {}, "List configuration profiles")
{}

void TCommandListProfiles::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("with-content", "Show content in each profile.")
        .StoreTrue(&WithContent);
}

int TCommandListProfiles::Run(TConfig& config) {
    Y_UNUSED(config);
    auto profileManager = CreateProfileManager(config.ProfileFile);
    const auto profileNames = profileManager->ListProfiles();
    TString activeProfileName = profileManager->GetActiveProfileName();
    for (const auto& profileName : profileNames) {
        Cout << profileName;
        if (activeProfileName == profileName) {
            Cout << " (active)";
        }
        if (WithContent) {
            Cout << ":";
        }
        Cout << Endl;
        if (WithContent) {
            PrintProfileContent(profileManager->GetProfile(profileName));
        }
    }
    return EXIT_SUCCESS;
}

TCommandGetProfile::TCommandGetProfile()
    : TClientCommand("get", {}, "List values for specified configuration profile")
{}

void TCommandGetProfile::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<name>", "Profile name (case sensitive)");
}

void TCommandGetProfile::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (config.ParseResult->GetFreeArgCount()) {
        ProfileName = config.ParseResult->GetFreeArgs()[0];
    }
}

int TCommandGetProfile::Run(TConfig& config) {
    auto profileManager = CreateProfileManager(config.ProfileFile);
    const auto profileNames = profileManager->ListProfiles();
    if (ProfileName) {
        if (find(profileNames.begin(), profileNames.end(), ProfileName) == profileNames.end()) {
            Cerr << "No existing profile \"" << ProfileName << "\". "
                << "Run \"ydb config profile list\" without arguments to see existing profiles" << Endl;
            return EXIT_FAILURE;
        }
    }
    if (!ProfileName) {
        const auto profileNames = profileManager->ListProfiles();
        if (profileNames.size()) {
            // Build menu options
            std::vector<TString> options;
            TString activeProfileName = profileManager->GetActiveProfileName();
            for (const auto& name : profileNames) {
                TStringBuilder description;
                description << name;
                if (name == activeProfileName) {
                    description << "\t(active)";
                }
                options.push_back(description);
            }

            auto selectedIdx = RunFtxuiMenu("Please choose profile to list its values", options);
            if (!selectedIdx) {
                Cout << "Cancelled." << Endl;
                return EXIT_SUCCESS;
            }

            ProfileName = profileNames[*selectedIdx];
        } else {
            Cerr << "You have no existing profiles yet. Run \"ydb init\" to create one" << Endl;
            return EXIT_FAILURE;
        }
    }
    PrintProfileContent(profileManager->GetProfile(ProfileName));
    return EXIT_SUCCESS;
}

TCommandUpdateProfile::TCommandUpdateProfile()
    : TCommandProfileCommon("update", {}, "Update existing configuration profile")
{}

void TCommandUpdateProfile::Config(TConfig& config) {
    TCommandProfileCommon::Config(config);

    config.SetFreeArgsMin(1);
    TClientCommandOptions& opts = *config.Opts;
    opts.AddLongOption("no-endpoint", "Delete endpoint from the profile").StoreTrue(&NoEndpoint);
    opts.AddLongOption("no-database", "Delete database from the profile").StoreTrue(&NoDatabase);
    opts.AddLongOption("no-auth", "Delete authentication data from the profile").StoreTrue(&NoAuth);

    if (config.UseIamAuth) {
        opts.AddLongOption("no-iam-endpoint", "Delete endpoint of IAM service from the profile").StoreTrue(&NoIamEndpoint);
    }
    opts.AddLongOption("no-ca-file", "Delete path to file containing the PEM encoded "
        "root certificates for SSL/TLS connections from the profile").StoreTrue(&NoCaCertsFile);
    opts.AddLongOption("no-client-cert-file", "Delete path to file containing client certificate "
        "for SSL/TLS connections").StoreTrue(&NoClientCertFile);
    opts.AddLongOption("no-client-cert-key-file", "Delete path to file containing PEM encoded client "
        "certificate private key for SSL/TLS connections").StoreTrue(&NoClientCertPrivateKeyFile);
    opts.AddLongOption("no-client-cert-key-password-file", "Delete path to file containing password for "
        "client certificate private key (if key is encrypted)").StoreTrue(&NoClientCertPrivateKeyPasswordFile);
}

void TCommandUpdateProfile::ValidateNoOptions() {
    size_t authMethodCount =
            (bool) (TokenFile) + (bool) (Oauth2KeyFile) +
            (bool) (IamTokenFile) +
            (bool) (YcTokenFile) + UseMetadataCredentials +
            (bool) (SaKeyFile) + AnonymousAuth +
            (User || PasswordFile);

    if (authMethodCount > 0 && NoAuth) {
        throw TMisuseException() << "You cannot enter authentication options and the \"--no-auth\" option at the same time";
    }
    TStringBuilder str;
    auto addMutuallyExclusiveOptionError = [&](bool validationResult, TStringBuf optionName) {
        if (validationResult) {
            if (str) {
                str << ", ";
            }
            str << "\"--" << optionName << "\" and \"--no-" << optionName << "\"";
        }
    };
    addMutuallyExclusiveOptionError(Endpoint && NoEndpoint, "endpoint");
    addMutuallyExclusiveOptionError(Database && NoDatabase, "database");
    addMutuallyExclusiveOptionError(IamEndpoint && NoIamEndpoint, "iam-endpoint");
    addMutuallyExclusiveOptionError(CaCertsFile && NoCaCertsFile, "ca-file");
    addMutuallyExclusiveOptionError(ClientCertFile && NoClientCertFile, "client-cert-file");
    addMutuallyExclusiveOptionError(ClientCertPrivateKeyFile && NoClientCertPrivateKeyFile, "client-cert-key-file");
    addMutuallyExclusiveOptionError(NoClientCertPrivateKeyPasswordFile && NoClientCertPrivateKeyPasswordFile, "client-cert-key-password-file");
    if (!str.empty()) {
        throw TMisuseException() << "Options " << str << " are mutually exclusive";
    }
}

void TCommandUpdateProfile::DropNoOptions(std::shared_ptr<IProfile> profile) {
    if (NoEndpoint) {
        profile->RemoveValue("endpoint");
    }
    if (NoDatabase) {
        profile->RemoveValue("database");
    }
    if (NoIamEndpoint) {
        profile->RemoveValue("iam-endpoint");
    }
    if (NoAuth) {
        profile->RemoveValue("authentication");
    }
    if (NoCaCertsFile) {
        profile->RemoveValue("ca-file");
    }
    if (NoClientCertFile) {
        profile->RemoveValue("client-cert-file");
    }
    if (NoClientCertPrivateKeyFile) {
        profile->RemoveValue("client-cert-key-file");
    }
    if (NoClientCertPrivateKeyPasswordFile) {
        profile->RemoveValue("client-cert-key-password-file");
    }
}

void TCommandUpdateProfile::Parse(TConfig& config) {
    TCommandProfileCommon::Parse(config);
    ProfileName = config.ParseResult->GetFreeArgs()[0];
    ValidateNoOptions();
}

int TCommandUpdateProfile::Run(TConfig& config) {
    auto profileManager = CreateProfileManager(config.ProfileFile);
    if (!profileManager->HasProfile(ProfileName)) {
        Cerr << "No existing profile \"" << ProfileName << "\". "
             << "Run \"ydb config profile list\" without arguments to see existing profiles" << Endl;
        return EXIT_FAILURE;
    }
    DropNoOptions(profileManager->GetProfile(ProfileName));
    if (profileManager->GetProfile(ProfileName)->IsEmpty()) {
        profileManager->RemoveProfile(ProfileName);
        profileManager->CreateProfile(ProfileName);
    }
    ConfigureProfile(ProfileName, profileManager, config, false, true);
    return EXIT_SUCCESS;
}

TCommandReplaceProfile::TCommandReplaceProfile()
            : TCommandProfileCommon("replace", {}, "Deletes profile and creates a new one with the same name and new property values from provided options or an stdin")
{}

void TCommandReplaceProfile::Config(TConfig& config) {
    TCommandProfileCommon::Config(config);
    config.Opts->AddLongOption('f', "force", "Never prompt").StoreTrue(&Force);
    config.SetFreeArgsMin(1);
}

void TCommandReplaceProfile::Parse(TConfig& config) {
    TCommandProfileCommon::Parse(config);
    ProfileName = config.ParseResult->GetFreeArgs()[0];
}

int TCommandReplaceProfile::Run(TConfig& config) {
    auto profileManager = CreateProfileManager(config.ProfileFile);
    if (profileManager->HasProfile(ProfileName)) {
        if (!Force) {
            if (!AskYesNoFtxui("Current profile will be replaced with a new one. All current profile data will be lost. Continue?")) {
                return EXIT_FAILURE;
            }
        }
        profileManager->RemoveProfile(ProfileName);
    }
    profileManager->CreateProfile(ProfileName);
    ConfigureProfile(ProfileName, profileManager, config, false, true);
    return EXIT_SUCCESS;
}

} // NYdb::NConsoleClient
