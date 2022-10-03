#include "ydb_profile.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

#include <util/folder/dirut.h>

#if defined(_win32_)
#include <util/system/env.h>
#endif

namespace NYdb::NConsoleClient {

namespace {
#if defined(_darwin_)
    const TString HomeDir = GetHomeDir();
#elif defined(_win32_)
    const TString HomeDir = GetEnv("USERPROFILE");
#else
    const TString HomeDir = GetHomeDir();
#endif
}

const TString AuthNode = "authentication";

std::shared_ptr<IProfileManager> CreateYdbProfileManager(const TString& ydbDir) {
    return CreateProfileManager(TStringBuilder() << HomeDir << '/' << ydbDir << "/config/config.yaml");
}

TCommandConfig::TCommandConfig()
    : TClientCommandTree("config", {}, "Manage YDB CLI configuration")
{
    AddCommand(std::make_unique<TCommandProfile>());
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

    void SetupProfileSetting(const TString& name, bool existingProfile, const TString& profileName,
            std::shared_ptr<IProfile> profile, TClientCommand::TConfig& config, bool interactive, bool cmdLine) {
        
        if (cmdLine) {
            if (config.ParseResult->Has(name)) {
                profile->SetValue(name, config.ParseResult->Get(name));
                return;
            };
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

    bool SetAuthFromCommandLine( std::shared_ptr<IProfile> profile, TClientCommand::TConfig& config) {
        if (config.ParseResult->Has("iam-endpoint")) {
            profile->SetValue("iam-endpoint", config.ParseResult->Get("iam-endpoint"));
        }
        if (config.ParseResult->Has("token-file")) {
            PutAuthMethod( profile, "token-file", config.ParseResult->Get("token-file"));
        } else if (config.ParseResult->Has("iam-token-file")) {
            // no error here, we take the iam-token-file option as just a token-file authentication
            PutAuthMethod( profile, "token-file", config.ParseResult->Get("iam-token-file")); 
        }else if (config.ParseResult->Has("yc-token-file")) {
            PutAuthMethod( profile, "yc-token-file", config.ParseResult->Get("yc-token-file"));
        } else if (config.ParseResult->Has("use-metadata-credentials")) {
            PutAuthMethodWithoutPars( profile, "use-metadata-credentials");
        } else if (config.ParseResult->Has("sa-key-file")) {
            PutAuthMethod( profile, "sa-key-file", config.ParseResult->Get("sa-key-file"));
        } else if (config.ParseResult->Has("user")) {
            PutAuthStatic( profile, config.ParseResult->Get("user"), config.ParseResult->Get("password-file"), true );
        } else {
            return false;
        }
        return true;
    }

    void SetupProfileAuthentication(bool existingProfile, const TString& profileName, std::shared_ptr<IProfile> profile,
            TClientCommand::TConfig& config, bool interactive, bool cmdLine) {
        
        if (cmdLine) {
            if (SetAuthFromCommandLine( profile, config )) {
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
                "Use metadata service on a virtual machine (use-metadata-credentials)"
                " cloud.yandex.ru/docs/compute/operations/vm-connect/auth-inside-vm",
                [&profile, &profileName]() {
                    Cout << "Setting metadata service usage for profile \"" << profileName << "\"" << Endl;
                    PutAuthMethodWithoutPars( profile, "use-metadata-credentials" );
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
        if (config.UseOAuthToken) {
            picker.AddOption(
                "Set new OAuth token (ydb-token)",
                [&profile, &profileName]() {
                    SetAuthMethod("ydb-token", "OAuth YDB token", profile, profileName);
                }
            );
        }
        picker.AddOption(
            TStringBuilder() << "Don't save authentication data for profile \"" << profileName << "\"",
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
                } else if (method == "sa-key-file" || method == "token-file" || method == "yc-token-file") {
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

    void ConfigureProfile(const TString& profileName, std::shared_ptr<IProfileManager> profileManager,
            TClientCommand::TConfig& config, bool interactive, bool cmdLine ) {
        bool existingProfile = profileManager->HasProfile(profileName);
        auto profile = profileManager->GetProfile(profileName);
        if (interactive) {
            Cout << "Configuring " << (existingProfile ? "existing" : "new")
                << " profile \"" << profileName << "\"." << Endl;
        }
        SetupProfileSetting("endpoint", existingProfile, profileName, profile, config, interactive, cmdLine );
        SetupProfileSetting("database", existingProfile, profileName, profile, config, interactive, cmdLine );
        SetupProfileAuthentication(existingProfile, profileName, profile, config, interactive, cmdLine );

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
            if (authMethod == "ydb-token" ||authMethod == "iam-token"
                || authMethod == "yc-token" || authMethod == "sa-key-file" 
                || authMethod == "token-file" || authMethod == "yc-token-file")
            {
                TString authData = authValue["data"].as<TString>();
                if (authMethod == "sa-key-file" || authMethod == "token-file" || authMethod == "yc-token-file") {
                    Cout << ": " << authData;
                } else {
                    Cout << ": " << BlurSecret(authData);
                }
            } else if (authMethod == "static-credentials") {
                auto authData = authValue["data"];
                if (authData) {
                    if (authData["user"]) {
                        Cout << Endl << "    user: " << authData["user"].as<TString>();
                    }
                    if (authData["password"]) {
                        Cout << Endl << "    password: " << ReplaceWithAsterisks(authData["password"].as<TString>());
                    }
                    if (authData["password-file"]) {
                        Cout << Endl << "    password file: " << authData["password-file"].as<TString>();
                    }
                }
            }
            Cout << Endl;
        }
    }
}

TCommandInit::TCommandInit()
    : TClientCommand("init", {}, "YDB CLI initialization")
{}

void TCommandInit::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;

    config.SetFreeArgsNum(0);
}

int TCommandInit::Run(TConfig& config) {
    //Y_UNUSED(config);
    Cout << "Welcome! This command will take you through the configuration process." << Endl;
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
    TString profileName;
    SetupProfileName(profileName, profileManager);
    ConfigureProfile(profileName, profileManager, config, true, false);
    return EXIT_SUCCESS;
}

TCommandCreateProfile::TCommandCreateProfile()
    : TClientCommand("create", {}, "Create new configuration profile or re-configure existing one")
{}

void TCommandCreateProfile::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<name>", "Profile name");
    NLastGetopt::TOpts& opts = *config.Opts;

    opts.AddLongOption('e', "endpoint", "Endpoint to save in the profile").RequiredArgument("STRING");
    opts.AddLongOption('d', "database", "Database to save in the profile").RequiredArgument("PATH");

    opts.AddLongOption("token-file", "Access token file").RequiredArgument("PATH");
    opts.AddLongOption("iam-token-file", "Access token file").RequiredArgument("PATH").Hidden();
    if (config.UseIamAuth) {
        opts.AddLongOption("yc-token-file", "YC OAuth refresh token file").RequiredArgument("PATH");
        opts.AddLongOption("use-metadata-credentials", "Metadata service authentication").Optional().StoreTrue(&UseMetadataCredentials);
        opts.AddLongOption("sa-key-file", "YC Service account key file").RequiredArgument("PATH");
    }

    if (config.UseStaticCredentials) {
        opts.AddLongOption("user", "User name").RequiredArgument("STR");
        opts.AddLongOption("password-file", "Password file").RequiredArgument("PATH");
    }
    if (config.UseIamAuth) {
        opts.AddLongOption("iam-endpoint", "Endpoint of IAM service to refresh token in YC OAuth or YC Service account authentication modes").RequiredArgument("STR");
    }

}

bool TCommandCreateProfile::AnyProfileOptionInCommandLine(TConfig& config) {
    return (config.ParseResult->Has("endpoint") || config.ParseResult->Has("database") || 
      config.ParseResult->Has("token-file") || config.ParseResult->Has("iam-token-file") || config.ParseResult->Has("yc-token-file") || 
      config.ParseResult->Has("sa-key-file") || config.ParseResult->Has("use-metadata-credentials") ||
      config.ParseResult->Has("user") || config.ParseResult->Has("password-file") ||
      config.ParseResult->Has("iam-endpoint"));
}

void TCommandCreateProfile::ValidateAuth(TConfig& config) {
    size_t authMethodCount = 
        (size_t)(config.ParseResult->Has("token-file")) + 
        (size_t)(config.ParseResult->Has("iam-token-file")) + 
        (size_t)(config.ParseResult->Has("yc-token-file")) + 
        (size_t)UseMetadataCredentials + 
        (size_t)(config.ParseResult->Has("sa-key-file")) + 
        (size_t)(config.ParseResult->Has("user") || config.ParseResult->Has("password-file"));

    if (authMethodCount > 1) {
        TStringBuilder str;
        str << authMethodCount << " authentication methods were provided via options:";
        if (config.ParseResult->Has("token-file")) {
            str << " TokenFile (" << config.ParseResult->Get("token-file") << ")";
        }
        if (config.ParseResult->Has("iam-token-file")) {
            str << " IamTokenFile (" << config.ParseResult->Get("iam-token-file") << ")";
        }
        if (config.ParseResult->Has("yc-token-file")) {
            str << " YCTokenFile (" << config.ParseResult->Get("yc-token-file") << ")";
        }
        if (UseMetadataCredentials) {
            str << " Metadata credentials" << ")";
        }
        if (config.ParseResult->Has("sa-key-file")) {
            str << " SAKeyFile (" << config.ParseResult->Get("sa-key-file") << ")";
        }
        if (config.ParseResult->Has("user")) {
            str << " User (" << config.ParseResult->Get("user") << ")";
        }
        if (config.ParseResult->Has("password-file")) {
            str << " Password file (" << config.ParseResult->Get("password-file") << ")";
        }

        throw TMisuseException() << str << ". Choose exactly one of them";        
    }


}

void TCommandCreateProfile::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (config.ParseResult->GetFreeArgCount()) {
        ProfileName = config.ParseResult->GetFreeArgs()[0];
    }
    ValidateAuth(config);
}

int TCommandCreateProfile::Run(TConfig& config) {
//    Y_UNUSED(config);
    TString profileName = ProfileName;
    Interactive = !AnyProfileOptionInCommandLine(config) || !profileName;
    if (Interactive) {
        Cout << "Welcome! This command will take you through configuration profile creation process." << Endl;
    }
    if (!profileName) {
        Cout << "Please enter configuration profile name to create or re-configure: ";
        Cin >> profileName;
    }
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
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
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
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
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
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
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
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
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
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
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
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

} // NYdb::NConsoleClient
