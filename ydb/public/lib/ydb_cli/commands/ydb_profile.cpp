#include "ydb_profile.h"

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

std::shared_ptr<IProfileManager> CreateYdbProfileManager(const TString& ydbDir) {
    return CreateProfileManager(TStringBuilder() << HomeDir << '/' << ydbDir << "/config/config.yaml");
}

TCommandConfig::TCommandConfig()
    : TClientCommandTree("config", {}, "Manage YDB CLI configuration")
{
    AddCommand(std::make_unique<TCommandProfile>());
}

TCommandProfile::TCommandProfile()
    : TClientCommandTree("profile", {}, "Manage configuration profiles")
{
    AddCommand(std::make_unique<TCommandCreateProfile>());
    AddCommand(std::make_unique<TCommandDeleteProfile>());
    AddCommand(std::make_unique<TCommandActivateProfile>());
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
            std::shared_ptr<IProfile> profile) {
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

    void SetAuthMethod(const TString& id, const TString& fullName, std::shared_ptr<IProfile> profile,
            const TString& profileName) {
        Cout << "Please enter " << fullName << " (" << id << "): ";
        TString newValue;
        Cin >> newValue;
        if (newValue) {
            Cout << "Setting " << fullName << " for profile \"" << profileName << "\"" << Endl;
            profile->RemoveValue("authentication");
            YAML::Node authValue;
            authValue["method"] = id;
            authValue["data"] = newValue;
            profile->SetValue("authentication", authValue);
        }
    }

    void SetStaticCredentials(std::shared_ptr<IProfile> profile, const TString& profileName) {
        Cout << "Please enter user name: ";
        TString userName;
        Cin >> userName;
        Cout << "Please enter password: ";
        TString userPassword = InputPassword();
        if (userName && userPassword) {
            Cout << "Setting user & password for profile \"" << profileName << "\"" << Endl;
            profile->RemoveValue("authentication");
            YAML::Node authValue;
            authValue["method"] = "static-credentials";
            auto authData = authValue["data"];
            authData["user"] = userName;
            authData["password"] = userPassword;
            profile->SetValue("authentication", authValue);
        }
    }

    void SetupProfileAuthentication(bool existingProfile, const TString& profileName, std::shared_ptr<IProfile> profile,
            TClientCommand::TConfig& config) {
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
                    profile->RemoveValue("authentication");
                    YAML::Node authValue;
                    authValue["method"] = "use-metadata-credentials";
                    profile->SetValue("authentication", authValue);
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
                profile->RemoveValue("authentication");
            }
        );
        if (existingProfile && profile->Has("authentication")) {
            auto& authValue = profile->GetValue("authentication");
            if (authValue["method"]) {
                TString method = authValue["method"].as<TString>();
                TStringBuilder description;
                description << "Use current settings with method \"" << method << "\"";
                if (method == "iam-token" || method == "yc-token" || method == "ydb-token") {
                    description << " and value \"" << BlurSecret(authValue["data"].as<TString>()) << "\"";
                } else if (method == "sa-key-file") {
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

    bool AskYesOrNo() {
        TString input;
        for (;;) {
            Cin >> input;
            if (to_lower(input) == "y" || to_lower(input) == "yes") {
                return true;
            } else if (to_lower(input) == "n" || to_lower(input) == "n") {
                return false;
            } else {
                Cout << "Type \"y\" (yes) or \"n\" (no): ";
            }
        }
        return false;
    }

    void ConfigureProfile(const TString& profileName, std::shared_ptr<IProfileManager> profileManager,
            TClientCommand::TConfig& config) {
        bool existingProfile = profileManager->HasProfile(profileName);
        auto profile = profileManager->GetProfile(profileName);
        Cout << "Configuring " << (existingProfile ? "existing" : "new")
            << " profile \"" << profileName << "\"." << Endl;

        SetupProfileSetting("endpoint", existingProfile, profileName, profile);
        SetupProfileSetting("database", existingProfile, profileName, profile);
        SetupProfileAuthentication(existingProfile, profileName, profile, config);

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

    void PrintProfileContent(std::shared_ptr<IProfile> profile) {
        if (profile->Has("endpoint")) {
            Cout << "  endpoint: " << profile->GetValue("endpoint").as<TString>() << Endl;
        }
        if (profile->Has("database")) {
            Cout << "  database: " << profile->GetValue("database").as<TString>() << Endl;
        }
        if (profile->Has("authentication")) {
            auto authValue = profile->GetValue("authentication");
            TString authMethod = authValue["method"].as<TString>();
            Cout << "  " << authMethod;
            if (authMethod == "ydb-token" ||authMethod == "iam-token"
                || authMethod == "yc-token" || authMethod == "sa-key-file")
            {
                TString authData = authValue["data"].as<TString>();
                if (authMethod == "sa-key-file") {
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

    config.SetFreeArgsNum(0);
}

int TCommandInit::Run(TConfig& config) {
    Y_UNUSED(config);
    Cout << "Welcome! This command will take you through the configuration process." << Endl;
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
    TString profileName;
    SetupProfileName(profileName, profileManager);
    ConfigureProfile(profileName, profileManager, config);
    return EXIT_SUCCESS;
}

TCommandCreateProfile::TCommandCreateProfile()
    : TClientCommand("create", {}, "Create new configuration profile or re-configure existing one")
{}

void TCommandCreateProfile::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.SetFreeArgsMax(1);
    SetFreeArgTitle(0, "<name>", "Profile name");
}

void TCommandCreateProfile::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (config.ParseResult->GetFreeArgCount()) {
        ProfileName = config.ParseResult->GetFreeArgs()[0];
    }
}

int TCommandCreateProfile::Run(TConfig& config) {
    Y_UNUSED(config);
    Cout << "Welcome! This command will take you through configuration profile creation process." << Endl;
    TString profileName = ProfileName;
    if (!profileName) {
        Cout << "Please enter configuration profile name to create or re-configure: ";
        Cin >> profileName;
    }
    auto profileManager = CreateYdbProfileManager(config.YdbDir);
    ConfigureProfile(profileName, profileManager, config);
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
