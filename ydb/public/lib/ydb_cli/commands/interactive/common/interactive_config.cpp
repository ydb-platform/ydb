#include "interactive_config.h"
#include "api_utils.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>

#include <util/string/strip.h>

#include <library/cpp/yaml/as/tstring.h>

namespace NYdb::NConsoleClient {

TInteractiveConfigurationManager::TAiProfile::TAiProfile(const TString& name, YAML::Node config, TInteractiveConfigurationManager::TPtr manager, const TInteractiveLogger& log)
    : Name(name)
    , Manager(manager)
    , Log(log)
    , Config(config)
{
    Y_DEBUG_VERIFY(Config.IsNull() || Config.IsMap());
}

bool TInteractiveConfigurationManager::TAiProfile::IsValid(TString& error) const {
    if (!GetName()) {
        error = "AI profile name is empty";
        return false;
    }

    if (!GetApiType()) {
        error = "AI profile has no API type";
        return false;
    }

    if (!GetApiEndpoint()) {
        error = "AI profile has no API endpoint";
        return false;
    }

    return true;
}

const TString& TInteractiveConfigurationManager::TAiProfile::GetName() const {
    return Name;
}

std::optional<TInteractiveConfigurationManager::TAiProfile::EApiType> TInteractiveConfigurationManager::TAiProfile::GetApiType() const {
    if (auto apiTypeNode = Config["api_type"]) {
        auto apiType = apiTypeNode.as<ui64>(static_cast<ui64>(EApiType::Invalid));
        if (apiType >= static_cast<ui64>(EMode::Invalid)) {
            Log.Debug() << "AI profile \"" << Name << "\" has invalid API type: " << apiType << ", API type will be removed";
            return std::nullopt;
        }

        return static_cast<EApiType>(apiType);
    }

    Log.Debug() << "AI profile \"" << Name << "\" has no API type";
    return std::nullopt;
}

TString TInteractiveConfigurationManager::TAiProfile::GetApiEndpoint() const {
    if (auto apiEndpoint = Config["api_endpoint"]) {
        auto endpoint = apiEndpoint.as<TString>("");
        if (!endpoint) {
            Log.Debug() << "AI profile \"" << Name << "\" has invalid or empty API endpoint";
        }

        return endpoint;
    }

    Log.Debug() << "AI profile \"" << Name << "\" has no API endpoint";
    return "";
}

TString TInteractiveConfigurationManager::TAiProfile::GetApiToken() const {
    TString token;
    if (auto apiToken = Config["api_token"]) {
        token = apiToken.as<TString>("");
    }

    if (!token) {
        Log.Debug() << "AI profile \"" << Name << "\" has no API token";
    }

    return token;
}

TString TInteractiveConfigurationManager::TAiProfile::GetModelName() const {
    TString modelName;
    if (auto modelNameNode = Config["model_name"]) {
        modelName = modelNameNode.as<TString>("");
    }

    if (!modelName) {
        Log.Debug() << "AI profile \"" << Name << "\" has no model name";
    }

    return modelName;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupProfile() {
    if (!SetupApiType()) {
        Log.Notice() << "AI profile \"" << Name << "\" is not configured, no API type provided";
        return false;
    }

    Cout << Endl;
    if (!SetupApiEndpoint()) {
        Log.Notice() << "AI profile \"" << Name << "\" is not configured, no API endpoint provided";
        return false;
    }

    Cout << Endl;
    if (!SetupApiToken()) {
        Log.Notice() << "AI profile \"" << Name << "\" is not configured, no API token provided";
        return false;
    }

    Cout << Endl;
    if (!SetupModelName()) {
        Log.Notice() << "AI profile \"" << Name << "\" is not configured, no model name provided";
        return false;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupApiType() {
    TNumericOptionsPicker picker(Log.IsVerbose());
    std::optional<EApiType> apiType;

    const TString openAiDescription = TStringBuilder() << Colors.BoldColor() << EApiType::OpenAI << Colors.OldColor() << " (e. g. for models on Yandex Cloud or openai.com)";
    const auto openAiAction = [&]() { apiType = EApiType::OpenAI; };

    const TString anthropicDescription = TStringBuilder() << Colors.BoldColor() << EApiType::Anthropic << Colors.OldColor() << " (e. g. for models on anthropic.com)";
    const auto anthropicAction = [&]() { apiType = EApiType::Anthropic; };

    const auto currentApiType = GetApiType();
    if (currentApiType) {
        Cout << "Pick desired action to configure API type in AI profile \"" << Name << "\":" << Endl;

        if (*currentApiType != EApiType::OpenAI) {
            picker.AddOption(TStringBuilder() << "Change API type to " << openAiDescription << ", " << Colors.Yellow() << "all other settings will be removed" << Colors.OldColor(), openAiAction);
        }

        if (*currentApiType != EApiType::Anthropic) {
            picker.AddOption(TStringBuilder() << "Change API type to " << anthropicDescription << ", " << Colors.Yellow() << "all other settings will be removed" << Colors.OldColor(), anthropicAction);
        }

        picker.AddOption(TStringBuilder() << "Use current API type " << Colors.BoldColor() << *currentApiType << Colors.OldColor(), [&]() { apiType = *currentApiType; });
    } else {
        Cout << "Please choose desired API type for AI profile \"" << Name << "\":" << Endl;

        picker.AddOption(openAiDescription, openAiAction);
        picker.AddOption(anthropicDescription, anthropicAction);
    }

    picker.PickOptionAndDoAction(/* exitOnError */ false);
    if (!apiType) {
        return false;
    }

    if (!currentApiType || *currentApiType != *apiType) {
        if (currentApiType) {
            Cout << "API type " << Colors.BoldColor() << *currentApiType << Colors.OldColor() << " was changed to " << Colors.BoldColor() << *apiType << Colors.OldColor() << " for profile \"" << Name << "\", removing endpoint, token and model name values" << Endl;
            Config["api_endpoint"] = "";
            Config["api_token"] = "";
            Config["model_name"] = "";
        } else {
            Cout << "Setting API type " << Colors.BoldColor() << *apiType << Colors.OldColor() << " for profile \"" << Name << "\"" << Endl;
        }

        Config["api_type"] = static_cast<ui64>(*apiType);
        Manager->ConfigChanged = true;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupApiEndpoint() {
    const auto apiType = GetApiType();
    if (!apiType) {
        Log.Warning() << "Can not setup API endpoint for AI profile \"" << Name << "\", there is no API type";
        return false;
    }

    TString endpoint;
    TNumericOptionsPicker picker(Log.IsVerbose());
    Cout << "Pick desired action to configure API endpoint in AI profile \"" << Name << "\":" << Endl;

    auto defaultEndpointInfo = TStringBuilder() << "Use default endpoint for " << Colors.BoldColor() << *apiType << Colors.OldColor() << ": ";
    const TString openAiEndpoint = "https://api.openai.com";
    const TString anthropicEndpoint = "https://api.anthropic.com";
    switch (*apiType) { 
        case EApiType::OpenAI: {
            picker.AddOption(defaultEndpointInfo << openAiEndpoint, [&]() { endpoint = openAiEndpoint; });
            break;
        }
        case EApiType::Anthropic: {
            picker.AddOption(defaultEndpointInfo << anthropicEndpoint, [&]() { endpoint = anthropicEndpoint; });
            break;
        }
        case EApiType::Invalid:
            Y_DEBUG_VERIFY(false, "Invalid API type: %s", ToString(*apiType).c_str());
    }

    picker.AddOption("Set a new endpoint value", [&]() {
        TString prompt = TStringBuilder() << "Please enter API endpoint for AI profile \"" << Name << "\": ";
        AskInputWithPrompt(prompt, [&](const TString& input) {
            auto url = Strip(input);
            if (!url) {
                prompt = TStringBuilder() << "Please enter non empty API endpoint for AI profile \"" << Name << "\": ";
                return false;
            }

            if (!url.StartsWith("http://") && !url.StartsWith("https://")) {
                prompt = TStringBuilder() << "API endpoint supports only http:// or https:// schema. Please enter API endpoint for AI profile \"" << Name << "\": ";
                return false;
            }

            try {
                NAi::CreateApiUrl(url, "/");
            } catch (const std::exception& e) {
                prompt = TStringBuilder() << "Invalid API endpoint: " << e.what() << ". Please enter API endpoint for AI profile \"" << Name << "\": ";
                return false;
            }

            endpoint = url;
            return true;
        }, Log.IsVerbose(), /* exitOnError */ false);
    });

    const auto currentEndpoint = GetApiEndpoint();
    if (currentEndpoint) {
        picker.AddOption(TStringBuilder() << "Use current endpoint value \"" << currentEndpoint << "\"", [&]() {
            endpoint = currentEndpoint;
        });
    }

    picker.PickOptionAndDoAction(/* exitOnError */ false);
    if (!endpoint) {
        return false;
    }

    if (!currentEndpoint || currentEndpoint != endpoint) {
        Cout << "Setting API endpoint value \"" << endpoint << "\" for profile \"" << Name << "\"" << Endl;
        Config["api_endpoint"] = endpoint;
        Manager->ConfigChanged = true;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupApiToken() {
    TString token;
    Cout << "Pick desired action to configure API token in AI profile \"" << Name << "\":" << Endl;

    TNumericOptionsPicker picker(Log.IsVerbose());
    picker.AddInputOption("Set a new token value", "Please enter token: ", [&](const TString& input) {
        token = input;
    });

    picker.AddOption(TStringBuilder() << "Don't save token for AI profile \"" << Name << "\"", []() {});

    const auto currentToken = GetApiToken();
    if (currentToken) {
        picker.AddOption(TStringBuilder() << "Use current token value \"" << BlurSecret(currentToken) << "\"", [&]() {
            token = currentToken;
        });
    }

    picker.PickOptionAndDoAction(/* exitOnError */ false);
    if (token && (!currentToken || currentToken != token)) {
        Cout << "Setting API token value \"" << BlurSecret(token) << "\" for profile \"" << Name << "\"" << Endl;
        Config["api_token"] = token;
        Manager->ConfigChanged = true;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupModelName() {
    TString modelName;
    Cout << "Pick desired action to configure model name in AI profile \"" << Name << "\":" << Endl;

    TNumericOptionsPicker picker(Log.IsVerbose());
    picker.AddOption("Set a new model name", [&]() {
        TString prompt = TStringBuilder() << "Please enter model name for AI profile \"" << Name << "\": ";
        AskInputWithPrompt(prompt, [&](const TString& input) {
            modelName = Strip(input);
            prompt = TStringBuilder() << "Please enter non empty model name for AI profile \"" << Name << "\": ";
            return !modelName.empty();
        }, Log.IsVerbose(), /* exitOnError */ false);
    });

    picker.AddOption(TStringBuilder() << "Don't save model name for AI profile \"" << Name << "\"", []() {});

    const auto currentModelName = GetModelName();
    if (currentModelName) {
        picker.AddOption(TStringBuilder() << "Use current model name \"" << currentModelName << "\"", [&]() {
            modelName = currentModelName;
        });
    }

    picker.PickOptionAndDoAction(/* exitOnError */ false);
    if (modelName && (!currentModelName || currentModelName != modelName)) {
        Cout << "Setting model name \"" << modelName << "\" for profile \"" << Name << "\"" << Endl;
        Config["model_name"] = modelName;
        Manager->ConfigChanged = true;
    }

    return true;
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
        auto defaultMode = Config["interactive_settings"]["default_mode"].as<ui64>(static_cast<ui64>(EMode::Invalid));
        if (defaultMode >= static_cast<ui64>(EMode::Invalid)) {
            Log.Warning() << "Interactive config has invalid default mode: " << defaultMode << ", falling back to YQL mode";
            defaultMode = static_cast<ui64>(EMode::YQL);
        }

        return static_cast<EMode>(defaultMode);
    }

    return EMode::YQL;
}

TString TInteractiveConfigurationManager::GetActiveAiProfileName() const {
    if (auto interactiveSettings = Config["interactive_settings"]; interactiveSettings && interactiveSettings["active_ai_profile"]) {
        if (auto activeProfile = interactiveSettings["active_ai_profile"].as<TString>("")) {
            return activeProfile;
        }

        Log.Warning() << "Current active profile has empty name, profile disabled";
    }

    return "";
}

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::GetAiProfile(const TString& name) {
    const auto& profiles = ListAiProfiles();
    const auto it = profiles.find(name);
    return it != profiles.end() ? it->second : nullptr;
}

std::unordered_map<TString, TInteractiveConfigurationManager::TAiProfile::TPtr> TInteractiveConfigurationManager::ListAiProfiles() {
    auto interactiveSettings = Config["interactive_settings"];
    if (!interactiveSettings || !interactiveSettings["ai_profiles"]) {
        return {};
    }

    std::unordered_map<TString, TAiProfile::TPtr> existingAiProfiles;
    for (const auto& profile : interactiveSettings["ai_profiles"]) {
        const auto& name = profile.first.as<TString>("");
        if (!name) {
            Log.Warning() << "AI profile has no name, profile skipped";
            continue;
        }

        const auto& settings = profile.second;
        if (!settings.IsMap()) {
            Log.Warning() << "AI profile \"" << name << "\" has unexpected type " << static_cast<ui64>(settings.Type()) << " instead of map, profile skipped";
            continue;
        }

        auto aiProfile = std::make_shared<TAiProfile>(name, settings, shared_from_this(), Log);
        if (TString error; !aiProfile->IsValid(error)) {
            Log.Warning() << "AI profile \"" << name << "\" is invalid: " << error << ", profile skipped";
            continue;
        }

        if (!existingAiProfiles.emplace(name, std::move(aiProfile)).second) {
            Log.Warning() << "AI profile \"" << name << "\" already exists, profile skipped";
        }
    }

    return existingAiProfiles;
}

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::InitAiModelProfile() {
    const auto& existingAiProfiles = ListAiProfiles();
    auto activeAiProfile = GetActiveAiProfileName();
    if (!activeAiProfile && existingAiProfiles.size() == 1) {
        activeAiProfile = existingAiProfiles.begin()->first;
        ChangeActiveAiProfile(activeAiProfile);
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
        picker.AddOption("Create a new profile", [&]() { resultProfile = CreateNewAiModelProfile(); });
        picker.AddOption("Return to YQL mode", []() {});
        picker.PickOptionAndDoAction(/* exitOnError */ false);

        if (resultProfile) {
            ChangeActiveAiProfile(resultProfile->GetName());
        }

        return resultProfile;
    }

    Cout << Endl << "Welcome to AI mode! This command will take you through the configuration process." << Endl;
    Cout << "You have no existing AI profiles yet, configure new profile or return to YQL mode by using " << Colors.BoldColor() << "Ctrl+C" << Colors.OldColor() << Endl;

    auto result = CreateNewAiModelProfile();
    if (result && GetDefaultMode() == EMode::YQL) {
        if (AskYesOrNo("Activate AI interactive mode by default? y/n: ", /* defaultAnswer */ false)) {
            ChangeDefaultMode(EMode::AI);
            Cout << "AI interactive mode is set by default, you can change it by using " << Colors.BoldColor() << "Ctrl+G" << Colors.OldColor() << " hotkey in AI interactive mode." << Endl;
        }
    }

    return result;
}

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::CreateNewAiModelProfile() {
    TString profileName;
    TString prompt = "Please enter name for a new AI profile: ";
    if (!AskInputWithPrompt(prompt, [&](const TString& input) {
        profileName = Strip(input);
        prompt = "Please enter non empty name for a new AI profile: ";
        return !profileName.empty();
    }, Log.IsVerbose(), /* exitOnError */ false)) {
        return nullptr;
    }

    Y_DEBUG_VERIFY(profileName, "Profiles with empty names are not allowed");
    Cout << "Configuring new AI profile \"" << profileName << "\"." << Endl << Endl;

    Config["interactive_settings"]["ai_profiles"][profileName] = YAML::Node();
    ConfigChanged = true;
    auto aiProfile = std::make_shared<TAiProfile>(profileName, Config["interactive_settings"]["ai_profiles"][profileName], shared_from_this(), Log);
    if (!aiProfile->SetupProfile()) {
        return nullptr;
    }

    Cout << "Configuration process for AI profile \"" << profileName << "\" is complete." << Endl;

    if (const auto activeProfile = GetActiveAiProfileName(); activeProfile && activeProfile != profileName) {
        if (AskYesOrNo(TStringBuilder() << Endl << "Activate AI profile \"" << profileName << "\" to use by default? (current active AI profile is \"" << activeProfile << "\") y/n: ")) {
            ChangeActiveAiProfile(profileName);
            Cout << "AI profile \"" << profileName << "\" was set as active." << Endl;
        }
    } else if (!activeProfile) {
        ChangeActiveAiProfile(profileName);
    }

    Log.Notice() << "AI profile \"" << profileName << "\" was created";
    return aiProfile;
}

void TInteractiveConfigurationManager::RemoveAiModelProfile(const TString& name) {
    if (Config["interactive_settings"] && Config["interactive_settings"]["ai_profiles"]) {
        if (!Config["interactive_settings"]["ai_profiles"].remove(name)) {
            Log.Warning() << "AI profile \"" << name << "\" doesn't exist, profile removal skipped";       
        }
        ConfigChanged = true;
    }
}

void TInteractiveConfigurationManager::ChangeDefaultMode(EMode mode) {
    Config["interactive_settings"]["default_mode"] = static_cast<ui64>(mode);
    ConfigChanged = true;
    Log.Info() << "Default interactive mode was changed to " << mode;
}

void TInteractiveConfigurationManager::ChangeActiveAiProfile(const TString& name) {
    Config["interactive_settings"]["active_ai_profile"] = name;
    ConfigChanged = true;
    Log.Info() << "Active AI profile was changed to \"" << name << "\"";
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
    auto interactiveSettings = Config["interactive_settings"];
    if (!interactiveSettings) {
        return;
    }

    if (!interactiveSettings.IsMap()) {
        Log.Error() << "$.interactive_settings section has unexpected type " << static_cast<ui64>(interactiveSettings.Type()) << ", changed to map and cleared";
        Config["interactive_settings"] = YAML::Node();
        interactiveSettings = Config["interactive_settings"];
        ConfigChanged = true;
    }

    if (auto aiProfiles = interactiveSettings["ai_profiles"]; aiProfiles && !aiProfiles.IsMap()) {
        Log.Error() << "$.interactive_settings.ai_profiles section has unexpected type " << static_cast<ui64>(interactiveSettings.Type()) << ", changed to map and cleared";
        Config["interactive_settings"]["ai_profiles"] = YAML::Node();
        ConfigChanged = true;
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
