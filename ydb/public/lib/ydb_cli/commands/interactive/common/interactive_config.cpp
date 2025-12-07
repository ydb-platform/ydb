#include "interactive_config.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>

#include <library/cpp/yaml/as/tstring.h>

namespace NYdb::NConsoleClient {

TInteractiveConfigurationManager::TAiProfile::TAiProfile(const TString& name, YAML::Node config, TInteractiveConfigurationManager::TPtr manager)
    : Name(name)
    , Manager(manager)
    , Config(config)
{}

const TString& TInteractiveConfigurationManager::TAiProfile::GetName() const {
    return Name;
}

TInteractiveConfigurationManager::TInteractiveConfigurationManager(const TString& configurationPath, const TInteractiveLogger& log)
    : Log(log)
    , ConfigurationPath(configurationPath)
{
    LoadProfile();
    CanonizeStructure();
}

TInteractiveConfigurationManager::~TInteractiveConfigurationManager() {
    if (ConfigChanged) {
        SaveConfig();
    }
}

TInteractiveConfigurationManager::EMode TInteractiveConfigurationManager::GetDefaultMode() const {
    if (Config["interactive_settings"] && Config["interactive_settings"]["default_mode"]) {
        const auto defaultMode = Config["interactive_settings"]["default_mode"].as<ui64>(static_cast<ui64>(EMode::YQL));
        return static_cast<EMode>(std::min(defaultMode, static_cast<ui64>(EMode::AI)));
    }
    return EMode::YQL;
}

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::InitAiModelProfile() {
    std::unordered_map<TString, TAiProfile::TPtr> existingAiProfiles;
    if (auto interactiveSettings = Config["interactive_settings"]; interactiveSettings && interactiveSettings["ai_profiles"]) {
        for (const auto& profile : interactiveSettings["ai_profiles"]) {
            const auto& name = profile.first.as<TString>("");
            if (!name) {
                Log.Warning() << "AI profile has no name";
            }
            if (!existingAiProfiles.emplace(name, std::make_shared<TAiProfile>(name, profile.second, shared_from_this())).second) {
                Log.Warning() << "AI profile \"" << name << "\" already exists";
            }
        }
    }

    auto activeAiProfile = GetActiveAiProfileName();
    if (!activeAiProfile && existingAiProfiles.size() == 1) {
        activeAiProfile = existingAiProfiles.begin()->first;
        ChangeActiveProfile(activeAiProfile);
    }

    if (activeAiProfile) {
        if (auto it = existingAiProfiles.find(activeAiProfile); it != existingAiProfiles.end()) {
            return it->second;
        }
    }

    if (!existingAiProfiles.empty()) {
        Cout << Endl << "Please choose AI profile to continue:" << Endl;

        TAiProfile::TPtr resultProfile;
        TNumericOptionsPicker picker(Log.IsVerbose());
        for (const auto& [name, profile] : existingAiProfiles) {
            picker.AddOption(name, [&resultProfile, profile]() {
                resultProfile = profile;
            });
        }

        TString newProfile;
        TString prompt = "Please enter name for a new profile: ";
        picker.AddInputOptionWithValidation(
            "Create a new profile",
            prompt,
            [&](const TString& input) {
                newProfile = input;
                prompt = "Please enter non empty name for a new profile: ";
                return !newProfile.empty();
            },
            /* exitOnError */ false
        );

        picker.AddOption("Return to YQL mode", []() {});
        picker.PickOptionAndDoAction(/* exitOnError */ false);

        if (resultProfile) {
            ChangeActiveProfile(resultProfile->GetName());
            return resultProfile;
        }

        if (newProfile) {
            return InitNewProfile(newProfile);
        }

        return nullptr;
    }

    Cout << Endl << "Welcome to AI mode! This command will take you through the configuration process." << Endl;
    Cout << "You have no existing AI profiles yet, configure new profile or return to YQL mode by using " << Colors.BoldColor() << "Ctrl+C" << Colors.OldColor() << Endl;

    TString profileName;
    TString prompt = "Please enter name for a new AI profile: ";
    if (!AskInputWithPrompt(prompt, [&](const TString& input) {
        profileName = input;
        prompt = "Please enter non empty name for a new AI profile: ";
        return !profileName.empty();
    }, Log.IsVerbose(), /* exitOnError */ false)) {
        return nullptr;
    }

    auto result = InitNewProfile(profileName);

    if (AskYesOrNo("Activate AI interactive mode by default? y/n: ", /* defaultAnswer */ false)) {
        ChangeDefaultMode(EMode::AI);
        Cout << "AI interactive mode is set by default, you can change it by using " << Colors.BoldColor() << "Ctrl+G" << Colors.OldColor() << " hotkey in AI interactive mode." << Endl;
    }

    return result;
}

void TInteractiveConfigurationManager::ChangeDefaultMode(EMode mode) {
    Config["interactive_settings"]["default_mode"] = static_cast<ui64>(mode);
    Log.Info() << "Default interactive mode was changed to " << mode;
}

void TInteractiveConfigurationManager::ChangeActiveProfile(const TString& name) {
    Config["interactive_settings"]["active_ai_profile"] = name;
    ConfigChanged = true;
    Log.Info() << "Active AI profile was changed to \"" << name << "\"";
}

TInteractiveConfigurationManager::TAiProfile::TPtr  TInteractiveConfigurationManager::InitNewProfile(const TString& name) {
    Y_DEBUG_VERIFY(name, "Profiles with empty names are not allowed");

    Cout << "Configuring new AI profile \"" << name << "\"." << Endl << Endl;
    Cout << "Configuration process for AI profile \"" << name << "\" is complete." << Endl;

    if (const auto activeProfile = GetActiveAiProfileName(); activeProfile && activeProfile != name) {
        if (AskYesOrNo(TStringBuilder() << Endl << "Activate AI profile \"" << name << "\" to use by default? (current active AI profile is \"" << activeProfile << "\") y/n: ")) {
            ChangeActiveProfile(name);
            Cout << "AI profile \"" << name << "\" was set as active." << Endl;
        }
    } else if (!activeProfile) {
        ChangeActiveProfile(name);
    }

    Config["interactive_settings"]["ai_profiles"][name] = YAML::Node();
    ConfigChanged = true;
    Log.Notice() << "AI profile \"" << name << "\" was created";

    return std::make_shared<TAiProfile>(name, Config["interactive_settings"]["ai_profiles"][name], shared_from_this());
}

TString TInteractiveConfigurationManager::GetActiveAiProfileName() const {
    if (auto interactiveSettings = Config["interactive_settings"]; interactiveSettings && interactiveSettings["active_ai_profile"]) {
        if (auto activeProfile = interactiveSettings["active_ai_profile"].as<TString>("")) {
            return activeProfile;
        }

        Log.Warning() << "Current active profile has empty name";
    }

    return "";
}

void TInteractiveConfigurationManager::LoadProfile() {
    try {
        TFsPath configFilePath(ConfigurationPath);
        configFilePath.Fix();

        if (configFilePath.Exists()) {
            Config = YAML::LoadFile(configFilePath.GetPath());
        }
    } catch (...) {
        Log.Critical() << "Couldn't load configuration from file \"" << ConfigurationPath << "\": " << CurrentExceptionMessage();
    }
}

void TInteractiveConfigurationManager::CanonizeStructure() {
    ConfigChanged = true;

    auto interactiveSettings = Config["interactive_settings"];
    if (!interactiveSettings) {
        return;
    }

    if (interactiveSettings.Type() != YAML::NodeType::Map) {
        Log.Error() << "$.interactive_settings section has unexpected type " << static_cast<ui64>(interactiveSettings.Type()) << ", changed to map and cleared";
        interactiveSettings = YAML::Node();
    }

    if (auto aiProfiles = interactiveSettings["ai_profiles"]; aiProfiles && aiProfiles.Type() != YAML::NodeType::Map) {
        Log.Error() << "$.interactive_settings.ai_profiles section has unexpected type " << static_cast<ui64>(interactiveSettings.Type()) << ", changed to map and cleared";
        aiProfiles = YAML::Node();
    }
}

void TInteractiveConfigurationManager::SaveConfig() {
    try {
        TFsPath configFilePath(ConfigurationPath);
        configFilePath.Fix();

        if (const auto& parent = configFilePath.Parent(); !parent.Exists()) {
            parent.MkDirs();
        }

        if (TFileStat(configFilePath).Mode & (S_IRGRP | S_IROTH)) {
            if (Chmod(configFilePath.GetPath().c_str(), S_IRUSR | S_IWUSR)) {
                throw yexception() << "Couldn't change permissions for the file \"" << configFilePath.GetPath() << "\"";
            }
        }

        TFileOutput resultConfigFile(TFile(configFilePath, CreateAlways | WrOnly | AWUser | ARUser));
        resultConfigFile << YAML::Dump(Config);
    } catch (...) {
        Log.Critical() << "Couldn't save configuration to file \"" << ConfigurationPath << "\": " << CurrentExceptionMessage();
    }
}

} // namespace NYdb::NConsoleClient
