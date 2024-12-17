#include "ydb_profile.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

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

    config.NeedToConnect = false;
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
    size_t MakeNumericChoice(const size_t optCount) {
        Cout << "Please enter your numeric choice: ";
        size_t numericChoice = 0;
        for (;;) {
            try {
                Cin >> numericChoice;
            }
            catch (yexception&) {
            }
            if (numericChoice > 0 && numericChoice <= optCount) {
                break;
            }
            Cerr << "Please enter a value between 1 and " << optCount << ": ";
        }
        return numericChoice;
    }

    class TNumericOptionsPicker {
    public:
        using TPickableAction = std::function<void()>;
        TNumericOptionsPicker() {};

        void AddOption(const TString& description, TPickableAction&& action) {
            Cout << " [" << ++OptionsCount << "] " << description << Endl;
            Options.emplace(OptionsCount, std::move(action));
        }

        void PickOptionAndDoAction() {
            size_t numericChoice = MakeNumericChoice(OptionsCount);
            auto optionIt = Options.find(numericChoice);
            if (optionIt != Options.end()) {
                // Do action
                optionIt->second();
            } else {
                Cerr << "Can't find action with index " << numericChoice << Endl;
            }
        }

    private:
        size_t OptionsCount = 0;
        THashMap<size_t, TPickableAction> Options;

    };

    TString BlurSecret(const TString& in) {
        TString out(in);
        size_t clearSymbolsCount = Min(size_t(10), out.length() / 4);
        for (size_t i = clearSymbolsCount; i < out.length() - clearSymbolsCount; ++i) {
            out[i] = '*';
        }
        return out;
    }

    TString ReplaceWithAsterisks(const TString& in) {
        return TString(in.length(), '*');
    }

    void SetupProfileName(TString& profileName, std::shared_ptr<IProfileManager> profileManager) {
        if (profileName) {
            return;
        }
        const auto profileNames = profileManager->ListProfiles();
        TString activeProfileName = profileManager->GetActiveProfileName();
        if (profileNames.size()) {
            Cout << "Please choose profile to configure:" << Endl;
            TNumericOptionsPicker picker;
            picker.AddOption(
                "Create a new profile",
                [&profileName]() {
                Cout << "Please enter name for a new profile: ";
                Cin >> profileName;
            }
            );
            for (size_t i = 0; i < profileNames.size(); ++i) {
                TStringBuilder description;
                TString currentProfile = profileNames[i];
                description << currentProfile;
                if (currentProfile == activeProfileName) {
                    description << " (active)";
                }
                picker.AddOption(
                    description,
                    [&profileName, currentProfile]() {
                    profileName = currentProfile;
                }
                );
            }
            picker.PickOptionAndDoAction();
        } else {
            Cout << "You have no existing profiles yet." << Endl;
            Cout << "Please enter name for a new profile: ";
            Cin >> profileName;
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

    void SetAuthMethod(const TString& id, const TString& fullName, std::shared_ptr<IProfile> profile,
            const TString& profileName) {
        Cout << "Please enter " << fullName << " (" << id << "): ";
        TString newValue;
        Cin >> newValue;
        if (newValue) {
            Cout << "Setting " << fullName << " for profile \"" << profileName << "\"" << Endl;
            PutAuthMethod( profile, id, newValue );
        }
    }

    void SetStaticCredentials(std::shared_ptr<IProfile> profile, const TString& profileName) {
        Cout << "Please enter user name: ";
        TString userName;
        Cin >> userName;
        Cout << "Please enter password: ";
        TString userPassword = InputPassword();
        if (userName) {
            Cout << "Setting user & password for profile \"" << profileName << "\"" << Endl;
            PutAuthStatic( profile, userName, userPassword, false );
        }
    }

    TString TryBlurValue(const TString& authMethod, const TString& value) {
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
    }
}

TCommandConnectionInfo::TCommandConnectionInfo()
    : TClientCommand("info", {}, "List current connection parameters")
{}

void TCommandConnectionInfo::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;
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
        if (config.StaticCredentials.User) {
            Cout << "user: " << config.StaticCredentials.User << Endl;
        }
        if (config.StaticCredentials.Password) {
            Cout << "password: " << TryBlurValue("password", config.StaticCredentials.Password) << Endl;
        }
    }
    if (config.CaCertsFile) {
        Cout << "ca-file: " << config.CaCertsFile << Endl;
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

TCommandInit::TCommandInit()
    : TCommandProfileCommon("init", {}, "YDB CLI initialization")
{}

void TCommandInit::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;

    config.SetFreeArgsNum(0);
}

int TCommandInit::Run(TConfig& config) {
    //Y_UNUSED(config);
    Cout << "Welcome! This command will take you through the configuration process." << Endl;
    auto profileManager = CreateProfileManager(config.ProfileFile);
    TString profileName;
    SetupProfileName(profileName, profileManager);
    ConfigureProfile(profileName, profileManager, config, true, false);
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
        {"ca-file", CaCertsFile}
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
        Cout << "Configuring " << (existingProfile ? "existing" : "new")
             << " profile \"" << profileName << "\"." << Endl;
    }
    SetupProfileSetting("endpoint", Endpoint, existingProfile, profileName, profile, interactive, cmdLine);
    SetupProfileSetting("database", Database, existingProfile, profileName, profile, interactive, cmdLine);
    SetupProfileAuthentication(existingProfile, profileName, profile, config, interactive, cmdLine);
    if (cmdLine && CaCertsFile) {
        profile->SetValue("ca-file", CaCertsFile);
    }

    if (interactive) {
        TString activeProfileName = profileManager->GetActiveProfileName();
        if (profileName != activeProfileName) {
            Cout << Endl << "Activate profile \"" << profileName << "\" to use by default? (current active profile is ";
            TString currentActiveProfile = profileManager->GetActiveProfileName();
            if (currentActiveProfile) {
                Cout << "\"" << currentActiveProfile << "\"";
            } else {
                Cout << "not set";
            }
            Cout << ") y/n: ";
            if (AskYesOrNo()) {
                profileManager->SetActiveProfile(profileName);
                Cout << "Profile \"" << profileName << "\" was set as active." << Endl;
            }
        }
        Cout << "Configuration process for profile \"" << profileName << "\" is complete." << Endl;
    }
}

void TCommandProfileCommon::SetupProfileSetting(const TString& name, const TString& value, bool existingProfile, const TString& profileName,
                         std::shared_ptr<IProfile> profile, bool interactive, bool cmdLine) {
    if (cmdLine) {
        if (value) {
            profile->SetValue(name, value);
            return;
        }
    }
    if (!interactive) {
        return;
    }

    Cout << Endl << "Pick desired action to configure " << name << " in profile \"" << profileName << "\":" << Endl;

    TNumericOptionsPicker picker;
    picker.AddOption(
            TStringBuilder() << "Set a new " << name << " value",
            [&name, &profileName, &profile]() {
                Cout << "Please enter new " << name << " value: ";
                TString newValue;
                Cin >> newValue;
                if (newValue) {
                    Cout << "Setting " << name << " value \"" << newValue << "\" for profile \"" << profileName
                         << "\"" << Endl;
                    profile->SetValue(name, newValue);
                }
            }
    );
    picker.AddOption(
            TStringBuilder() << "Don't save " << name << " for profile \"" << profileName << "\"",
            [&name, &profile]() {
                profile->RemoveValue(name);
            }
    );
    if (existingProfile && profile->Has(name)) {
        picker.AddOption(
                TStringBuilder() << "Use current " << name << " value \"" << profile->GetValue(name).as<TString>() << "\"",
                []() {}
        );
    }
    picker.PickOptionAndDoAction();
}

void TCommandProfileCommon::SetupProfileAuthentication(bool existingProfile, const TString& profileName, std::shared_ptr<IProfile> profile,
                                TConfig& config, bool interactive, bool cmdLine) {

    if (cmdLine) {
        if (SetAuthFromCommandLine(profile)) {
            return;
        }
    }
    if (!interactive) {
        return;
    }
    Cout << Endl << "Pick desired action to configure authentication method:" << Endl;
    TNumericOptionsPicker picker;
    if (config.UseStaticCredentials) {
        picker.AddOption(
                "Use static credentials (user & password)",
                [&profile, &profileName]() {
                    SetStaticCredentials(profile, profileName);
                }
        );
    }
    if (config.UseIamAuth) {
        picker.AddOption(
                "Use IAM token (iam-token) cloud.yandex.ru/docs/iam/concepts/authorization/iam-token",
                [&profile, &profileName]() {
                    SetAuthMethod("iam-token", "IAM token", profile, profileName);
                }
        );
        picker.AddOption(
                "Use OAuth token of a Yandex Passport user (yc-token). Doesn't work with federative accounts."
                " cloud.yandex.ru/docs/iam/concepts/authorization/oauth-token",
                [&profile, &profileName]() {
                    SetAuthMethod("yc-token", "OAuth token of a Yandex Passport user", profile, profileName);
                }
        );
        picker.AddOption(
                "Use OAuth 2.0 RFC8693 token exchange credentials parameters json file.",
                [&profile, &profileName]() {
                    SetAuthMethod("oauth2-key-file", "OAuth 2.0 RFC8693 token exchange credentials parameters json file", profile, profileName);
                }
        );
        picker.AddOption(
                "Use metadata service on a virtual machine (use-metadata-credentials)"
                " cloud.yandex.ru/docs/compute/operations/vm-connect/auth-inside-vm",
                [&profile, &profileName]() {
                    Cout << "Setting metadata service usage for profile \"" << profileName << "\"" << Endl;
                    PutAuthMethodWithoutPars(profile, "use-metadata-credentials");
                }
        );
        picker.AddOption(
                "Use service account key file (sa-key-file)"
                " cloud.yandex.ru/docs/iam/operations/iam-token/create-for-sa",
                [&profile, &profileName]() {
                    SetAuthMethod("sa-key-file", "Path to service account key file", profile, profileName);
                }
        );
    }
    if (config.UseAccessToken) {
        picker.AddOption(
                "Set new access token (ydb-token)",
                [&profile, &profileName]() {
                    SetAuthMethod("ydb-token", "YDB token", profile, profileName);
                }
        );
    }
    picker.AddOption(
            TStringBuilder() << "Set anonymous authentication for profile \"" << profileName << "\"",
            [&profile, &profileName]() {
                Cout << "Setting anonymous authentication method for profile \"" << profileName << "\"" << Endl;
                PutAuthMethodWithoutPars(profile, "anonymous-auth");
            }
    );
    picker.AddOption(
            TStringBuilder() << "Don't save authentication data for profile (environment variables can be used) \"" << profileName << "\"",
            [&profile]() {
                profile->RemoveValue(AuthNode);
            }
    );
    if (existingProfile && profile->Has(AuthNode)) {
        auto& authValue = profile->GetValue(AuthNode);
        if (authValue["method"]) {
            TString method = authValue["method"].as<TString>();
            TStringBuilder description;
            description << "Use current settings with method \"" << method << "\"";
            if (method == "iam-token" || method == "yc-token" || method == "ydb-token") {
                description << " and value \"" << BlurSecret(authValue["data"].as<TString>()) << "\"";
            } else if (method == "sa-key-file" || method == "token-file" || method == "yc-token-file" || method == "oauth2-key-file") {
                description << " and value \"" << authValue["data"].as<TString>() << "\"";
            }
            picker.AddOption(
                    description,
                    []() {}
            );
        }
    }

    picker.PickOptionAndDoAction();
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
           PasswordFile || IamEndpoint || AnonymousAuth || CaCertsFile;
}

TCommandCreateProfile::TCommandCreateProfile()
    : TCommandProfileCommon("create", {}, "Create new configuration profile or re-configure existing one")
{}

void TCommandProfileCommon::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<name>", "Profile name");
    NLastGetopt::TOpts& opts = *config.Opts;

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
        "Path to a file containing the PEM encoding of the server root certificates for tls connections.")
        .RequiredArgument("PATH").StoreResult(&CaCertsFile);
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
//    Y_UNUSED(config);
    TString profileName = ProfileName;
    Interactive = (!AnyProfileOptionInCommandLine() || !profileName) && IsStdinInteractive();
    auto profileManager = CreateProfileManager(config.ProfileFile);
    if (Interactive) {
        Cout << "Welcome! This command will take you through configuration profile creation process." << Endl;
    }
    else {
        if (profileManager->HasProfile(ProfileName)) {
            Cerr << "Profile \"" << ProfileName << "\" already exists. Consider using update, replace or delete command." << Endl;
            return EXIT_FAILURE;
        }
        profileManager->CreateProfile(ProfileName);
    }
    if (!profileName) {
        Cout << "Please enter configuration profile name to create or re-configure: ";
        Cin >> profileName;
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
    Y_UNUSED(config);
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
            TMaybe<int> returnCode;
            Cout << "Please choose profile to remove:" << Endl;
            TNumericOptionsPicker picker;
            picker.AddOption(
                "Don't remove anything, just exit",
                [&returnCode]() {
                    Cout << "Nothing is done." << Endl;
                    returnCode = EXIT_SUCCESS;
                }
            );
            for (size_t i = 0; i < profileNames.size(); ++i) {
                TString currentProfileName = profileNames[i];
                picker.AddOption(
                    currentProfileName,
                    [this, currentProfileName]() {
                        ProfileName = currentProfileName;
                    }
                );
            }
            picker.PickOptionAndDoAction();
            if (returnCode.Defined()) {
                return returnCode.GetRef();
            }
        } else {
            Cerr << "You have no existing profiles yet." << Endl;
            return EXIT_FAILURE;
        }
    }
    if (Force) {
        profileManager->RemoveProfile(ProfileName);
    } else {
        Cout << "Profile \"" << ProfileName << "\" will be permanently removed. Continue? (y/n): ";
        if (AskYesOrNo()) {
            if (profileManager->RemoveProfile(ProfileName)) {
                Cout << "Profile \"" << ProfileName << "\" was removed." << Endl;
            } else {
                Cerr << "Profile \"" << ProfileName << "\" was not removed." << Endl;
                return EXIT_FAILURE;
            }
        } else {
            Cout << "Nothing is done." << Endl;
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
            TMaybe<int> returnCode;
            Cout << "Please choose profile to activate:" << Endl;
            TNumericOptionsPicker picker;
            picker.AddOption(
                "Don't do anything, just exit",
                [&returnCode]() {
                    Cout << "Nothing is done." << Endl;
                    returnCode = EXIT_SUCCESS;
                }
            );
            if (currentActiveProfileName) {
                picker.AddOption(
                    TStringBuilder() << "Deactivate current active profile \"" << currentActiveProfileName << "\"",
                    [&returnCode, &profileManager, &currentActiveProfileName]() {
                        profileManager->DeactivateProfile();
                        Cout << "Current active profile \"" << currentActiveProfileName << "\" was deactivated." << Endl;
                        returnCode = EXIT_SUCCESS;
                    }
                );
            }
            for (size_t i = 0; i < profileNames.size(); ++i) {
                TString currentProfileName = profileNames[i];
                TStringBuilder description;
                description << currentProfileName;
                if (currentProfileName == currentActiveProfileName) {
                    description << " (active)";
                }
                picker.AddOption(
                    description,
                    [this, currentProfileName]() {
                        ProfileName = currentProfileName;
                    }
                );
            }
            picker.PickOptionAndDoAction();
            if (returnCode.Defined()) {
                return returnCode.GetRef();
            }
        } else {
            Cerr << "You have no existing profiles yet. Run \"ydb init\" to create one" << Endl;
            return EXIT_FAILURE;
        }
    }
    if (currentActiveProfileName != ProfileName) {
        profileManager->SetActiveProfile(ProfileName);
        Cout << "Profile \"" << ProfileName << "\" was activated." << Endl;
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
        Cout << "Profile \"" << currentActiveProfileName << "\" was deactivated." << Endl;
    } else {
        Cout << "There is no profile active. Nothing is done." << Endl;
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
    Y_UNUSED(config);
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
            Cout << "Please choose profile to list its values:" << Endl;
            TNumericOptionsPicker picker;
            TString activeProfileName = profileManager->GetActiveProfileName();
            for (size_t i = 0; i < profileNames.size(); ++i) {
                TString currentProfileName = profileNames[i];
                TStringBuilder description;
                description << currentProfileName;
                if (currentProfileName == activeProfileName) {
                    description << " (active)";
                }
                picker.AddOption(
                    description,
                    [this, currentProfileName]() {
                        ProfileName = currentProfileName;
                    }
                );
            }
            picker.PickOptionAndDoAction();
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
    NLastGetopt::TOpts& opts = *config.Opts;
    opts.AddLongOption("no-endpoint", "Delete endpoint from the profile").StoreTrue(&NoEndpoint);
    opts.AddLongOption("no-database", "Delete database from the profile").StoreTrue(&NoDatabase);
    opts.AddLongOption("no-auth", "Delete authentication data from the profile").StoreTrue(&NoAuth);

    if (config.UseIamAuth) {
        opts.AddLongOption("no-iam-endpoint", "Delete endpoint of IAM service from the profile").StoreTrue(&NoIamEndpoint);
    }
    opts.AddLongOption("no-ca-file", "Delete path to file containing the PEM encoding of the "
        "server root certificates for tls connections from the profile").StoreTrue(&NoCaCertsFile);
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
    if (Endpoint && NoEndpoint) {
        str << "\"--endpoint\" and \"--no-endpoint\"";
    } else {
        if (Database && NoDatabase) {
            str << "\"--database and \"--no-database\"";
        } else {
            if (IamEndpoint && NoIamEndpoint) {
                str << "\"--iam-endpoint\" and \"--no-iam-endpoint\"";
            } else {
                if (CaCertsFile && NoCaCertsFile) {
                    str << "\"--ca-file\" and \"--no-ca-file\"";
                }
            }
        }
    }
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
            Cout << "Current profile will be replaced with a new one. All current profile data will be lost. Continue? (y/n): ";
            if (!AskYesOrNo()) {
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
