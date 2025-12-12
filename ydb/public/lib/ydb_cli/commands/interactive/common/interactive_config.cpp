#include "interactive_config.h"
#include "api_utils.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log_defs.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
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
    Y_VALIDATE(Config.IsNull() || Config.IsMap(), "Unexpected config type: " << static_cast<ui64>(Config.Type()));
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
            YDB_CLI_LOG(Debug, "AI profile \"" << Name << "\" has invalid API type: " << apiType << ", API type will be removed");
            return std::nullopt;
        }

        return static_cast<EApiType>(apiType);
    }

    YDB_CLI_LOG(Debug, "AI profile \"" << Name << "\" has no API type");
    return std::nullopt;
}

TString TInteractiveConfigurationManager::TAiProfile::GetApiEndpoint() const {
    if (auto apiEndpoint = Config["api_endpoint"]) {
        auto endpoint = apiEndpoint.as<TString>("");
        if (!endpoint) {
            YDB_CLI_LOG(Debug, "AI profile \"" << Name << "\" has invalid or empty API endpoint");
        }

        return endpoint;
    }

    YDB_CLI_LOG(Debug, "AI profile \"" << Name << "\" has no API endpoint");
    return "";
}

TString TInteractiveConfigurationManager::TAiProfile::GetApiToken() const {
    TString token;
    if (auto apiToken = Config["api_token"]) {
        token = apiToken.as<TString>("");
    }

    if (!token) {
        YDB_CLI_LOG(Debug, "AI profile \"" << Name << "\" has no API token");
    }

    return token;
}

TString TInteractiveConfigurationManager::TAiProfile::GetModelName() const {
    TString modelName;
    if (auto modelNameNode = Config["model_name"]) {
        modelName = modelNameNode.as<TString>("");
    }

    if (!modelName) {
        YDB_CLI_LOG(Debug, "AI profile \"" << Name << "\" has no model name");
    }

    return modelName;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupProfile() {
    if (!SetupApiType()) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no API type provided");
        return false;
    }

    Cout << Endl;
    if (!SetupApiEndpoint()) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no API endpoint provided");
        return false;
    }

    Cout << Endl;
    if (!SetupApiToken()) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no API token provided");
        return false;
    }

    Cout << Endl;
    if (!SetupModelName()) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no model name provided");
        return false;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupApiType() {
    std::optional<EApiType> apiType;
    std::vector<TMenuEntry> options;

    const TString openAiDescription = TStringBuilder() << EApiType::OpenAI << " (e. g. for models on Yandex Cloud or openai.com)";
    const auto openAiAction = [&]() { apiType = EApiType::OpenAI; };

    const TString anthropicDescription = TStringBuilder() << EApiType::Anthropic << " (e. g. for models on anthropic.com)";
    const auto anthropicAction = [&]() { apiType = EApiType::Anthropic; };

    const auto currentApiType = GetApiType();
    TString title;
    if (currentApiType) {
        title = TStringBuilder() << "Pick desired action to configure API type in AI profile \"" << Name << "\":";

        if (*currentApiType != EApiType::OpenAI) {
            options.push_back({TStringBuilder() << "Change API type to " << openAiDescription << ", all other settings will be removed", openAiAction});
        }

        if (*currentApiType != EApiType::Anthropic) {
            options.push_back({TStringBuilder() << "Change API type to " << anthropicDescription << ", all other settings will be removed", anthropicAction});
        }

        options.push_back({TStringBuilder() << "Use current API type " << *currentApiType, [&]() { apiType = *currentApiType; }});
    } else {
        title = TStringBuilder() << "Please choose desired API type for AI profile \"" << Name << "\":";
        options.push_back({openAiDescription, openAiAction});
        options.push_back({anthropicDescription, anthropicAction});
    }

    if (!RunFtxuiMenuWithActions(title, options) || !apiType) {
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
        YDB_CLI_LOG(Warning, "Can not setup API endpoint for AI profile \"" << Name << "\", there is no API type");
        return false;
    }

    TString endpoint;
    std::vector<TMenuEntry> options;
    const TString title = TStringBuilder() << "Pick desired action to configure API endpoint in AI profile \"" << Name << "\":";

    auto defaultEndpointInfo = TStringBuilder() << "Use default endpoint for " << *apiType << ": ";
    const TString openAiEndpoint = "https://api.openai.com";
    const TString anthropicEndpoint = "https://api.anthropic.com";
    switch (*apiType) { 
        case EApiType::OpenAI: {
            options.push_back({defaultEndpointInfo << openAiEndpoint, [&]() { endpoint = openAiEndpoint; }});
            break;
        }
        case EApiType::Anthropic: {
            options.push_back({defaultEndpointInfo << anthropicEndpoint, [&]() { endpoint = anthropicEndpoint; }});
            break;
        }
        case EApiType::Invalid:
            Y_VALIDATE(false, "Invalid API type: " << *apiType);
    }

    options.push_back({"Set a new endpoint value", [&]() {
        auto value = RunFtxuiInput(TStringBuilder() << "Please enter API endpoint for AI profile \"" << Name << "\": ", "", [&](const TString& input, TString& error) {
            auto url = Strip(input);
            if (!url) {
                error = TStringBuilder() << "Please enter non empty API endpoint for AI profile \"" << Name << "\": ";
                return false;
            }

            if (!url.StartsWith("http://") && !url.StartsWith("https://")) {
                error = TStringBuilder() << "API endpoint supports only http:// or https:// schema. Please enter API endpoint for AI profile \"" << Name << "\": ";
                return false;
            }

            try {
                NAi::CreateApiUrl(url, "/");
            } catch (const std::exception& e) {
                error = TStringBuilder() << "Invalid API endpoint: " << e.what() << ". Please enter API endpoint for AI profile \"" << Name << "\": ";
                return false;
            }

            endpoint = url;
            return true;
        });
        if (!value) {
            return;
        }
    }});

    const auto currentEndpoint = GetApiEndpoint();
    if (currentEndpoint) {
        options.push_back({TStringBuilder() << "Use current endpoint value \"" << currentEndpoint << "\"", [&]() {
            endpoint = currentEndpoint;
        }});
    }

    if (!RunFtxuiMenuWithActions(title, options) || !endpoint) {
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
    std::vector<TMenuEntry> options;
    const TString title = TStringBuilder() << "Pick desired action to configure API token in AI profile \"" << Name << "\":";

    options.push_back({"Set a new token value", [&]() {
        auto value = RunFtxuiInput("Please enter token:", "", [](const TString& input, TString& error) {
            if (input.empty()) {
                error = "Token cannot be empty";
                return false;
            }
            return true;
        });
        if (value) {
            token = *value;
        }
    }});

    options.push_back({TStringBuilder() << "Don't save token for AI profile \"" << Name << "\"", []() {}});

    const auto currentToken = GetApiToken();
    if (currentToken) {
        options.push_back({TStringBuilder() << "Use current token value \"" << BlurSecret(currentToken) << "\"", [&]() {
            token = currentToken;
        }});
    }

    if (!RunFtxuiMenuWithActions(title, options)) {
        return false;
    }

    if (token && (!currentToken || currentToken != token)) {
        Cout << "Setting API token value \"" << BlurSecret(token) << "\" for profile \"" << Name << "\"" << Endl;
        Config["api_token"] = token;
        Manager->ConfigChanged = true;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupModelName() {
    TString modelName;
    std::vector<TMenuEntry> options;
    const TString title = TStringBuilder() << "Pick desired action to configure model name in AI profile \"" << Name << "\":";

    options.push_back({"Set a new model name", [&]() {
        auto value = RunFtxuiInput(TStringBuilder() << "Please enter model name for AI profile \"" << Name << "\": ", "", [&](const TString& input, TString& error) {
            modelName = Strip(input);
            if (!modelName) {
                error = TStringBuilder() << "Please enter non empty model name for AI profile \"" << Name << "\": ";
                return false;
            }
            return true;
        });
        if (!value) {
            modelName.clear();
        }
    }});

    options.push_back({TStringBuilder() << "Don't save model name for AI profile \"" << Name << "\"", []() {}});

    const auto currentModelName = GetModelName();
    if (currentModelName) {
        options.push_back({TStringBuilder() << "Use current model name \"" << currentModelName << "\"", [&]() {
            modelName = currentModelName;
        }});
    }

    if (!RunFtxuiMenuWithActions(title, options)) {
        return false;
    }

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
            YDB_CLI_LOG(Warning, "Interactive config has invalid default mode: " << defaultMode << ", falling back to YQL mode");
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

        YDB_CLI_LOG(Warning, "Current active profile has empty name, profile disabled");
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
            YDB_CLI_LOG(Warning, "AI profile has no name, profile skipped");
            continue;
        }

        const auto& settings = profile.second;
        if (!settings.IsMap()) {
            YDB_CLI_LOG(Warning, "AI profile \"" << name << "\" has unexpected type " << static_cast<ui64>(settings.Type()) << " instead of map, profile skipped");
            continue;
        }

        auto aiProfile = std::make_shared<TAiProfile>(name, settings, shared_from_this(), Log);
        if (TString error; !aiProfile->IsValid(error)) {
            YDB_CLI_LOG(Warning, "AI profile \"" << name << "\" is invalid: " << error << ", profile skipped");
            continue;
        }

        if (!existingAiProfiles.emplace(name, std::move(aiProfile)).second) {
            YDB_CLI_LOG(Warning, "AI profile \"" << name << "\" already exists, profile skipped");
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
        TAiProfile::TPtr resultProfile;
        std::vector<TMenuEntry> options;
        options.reserve(existingAiProfiles.size() + 2);
        for (const auto& [name, profile] : existingAiProfiles) {
            options.push_back({name, [&resultProfile, profile]() {
                resultProfile = profile;
            }});
        }
        options.push_back({"Create a new profile", [&]() { resultProfile = CreateNewAiModelProfile(); }});
        options.push_back({"Return to YQL mode", []() {}});

        RunFtxuiMenuWithActions("Please choose AI profile to continue:", options);

        if (resultProfile) {
            ChangeActiveAiProfile(resultProfile->GetName());
        }

        return resultProfile;
    }

    Cout << Endl << "Welcome to AI mode! This command will take you through the configuration process." << Endl;
    Cout << "You have no existing AI profiles yet, configure new profile or return to YQL mode by using " << Colors.BoldColor() << "Ctrl+C" << Colors.OldColor() << Endl;

    auto result = CreateNewAiModelProfile();
    if (result && GetDefaultMode() == EMode::YQL) {
        if (AskYesNoFtxui("Activate AI interactive mode by default?", /* defaultAnswer */ false)) {
            ChangeDefaultMode(EMode::AI);
            Cout << "AI interactive mode is set by default, you can change it by using " << Colors.BoldColor() << "Ctrl+G" << Colors.OldColor() << " hotkey in AI interactive mode." << Endl;
        }
    }

    return result;
}

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::CreateNewAiModelProfile() {
    TString profileName;
    auto name = RunFtxuiInput("Please enter name for a new AI profile:", "", [](const TString& input, TString& error) {
        auto value = Strip(input);
        if (!value) {
            error = "Name cannot be empty";
            return false;
        }
        return true;
    });
    if (!name) {
        return nullptr;
    }
    profileName = *name;

    Y_VALIDATE(profileName, "Profiles with empty names are not allowed");
    Cout << "Configuring new AI profile \"" << profileName << "\"." << Endl << Endl;

    Config["interactive_settings"]["ai_profiles"][profileName] = YAML::Node();
    ConfigChanged = true;
    auto aiProfile = std::make_shared<TAiProfile>(profileName, Config["interactive_settings"]["ai_profiles"][profileName], shared_from_this(), Log);
    if (!aiProfile->SetupProfile()) {
        return nullptr;
    }

    Cout << "Configuration process for AI profile \"" << profileName << "\" is complete." << Endl;

    if (const auto activeProfile = GetActiveAiProfileName(); activeProfile && activeProfile != profileName) {
        if (AskYesNoFtxui(TStringBuilder() << "Activate AI profile \"" << profileName << "\" to use by default? (current active AI profile is \"" << activeProfile << "\")", /* defaultAnswer */ true)) {
            ChangeActiveAiProfile(profileName);
            Cout << "AI profile \"" << profileName << "\" was set as active." << Endl;
        }
    } else if (!activeProfile) {
        ChangeActiveAiProfile(profileName);
    }

    YDB_CLI_LOG(Notice, "AI profile \"" << profileName << "\" was created");
    return aiProfile;
}

void TInteractiveConfigurationManager::RemoveAiModelProfile(const TString& name) {
    if (Config["interactive_settings"] && Config["interactive_settings"]["ai_profiles"]) {
        if (!Config["interactive_settings"]["ai_profiles"].remove(name)) {
            YDB_CLI_LOG(Warning, "AI profile \"" << name << "\" doesn't exist, profile removal skipped");       
        }
        ConfigChanged = true;
    }
}

void TInteractiveConfigurationManager::ChangeDefaultMode(EMode mode) {
    Config["interactive_settings"]["default_mode"] = static_cast<ui64>(mode);
    ConfigChanged = true;
    YDB_CLI_LOG(Info, "Default interactive mode was changed to " << mode);
}

void TInteractiveConfigurationManager::ChangeActiveAiProfile(const TString& name) {
    Config["interactive_settings"]["active_ai_profile"] = name;
    ConfigChanged = true;
    YDB_CLI_LOG(Info, "Active AI profile was changed to \"" << name << "\"");
}

void TInteractiveConfigurationManager::LoadProfile() {
    try {
        TFsPath configFilePath(ConfigurationPath);
        configFilePath.Fix();

        if (configFilePath.Exists()) {
            Config = YAML::LoadFile(configFilePath.GetPath());
        }
    } catch (...) {
        YDB_CLI_LOG(Critical, "Couldn't load configuration from file \"" << ConfigurationPath << "\": " << CurrentExceptionMessage());
    }
}

void TInteractiveConfigurationManager::CanonizeStructure() {
    auto interactiveSettings = Config["interactive_settings"];
    if (!interactiveSettings) {
        return;
    }

    if (!interactiveSettings.IsMap()) {
        YDB_CLI_LOG(Error, "$.interactive_settings section has unexpected type " << static_cast<ui64>(interactiveSettings.Type()) << ", changed to map and cleared");
        Config["interactive_settings"] = YAML::Node();
        interactiveSettings = Config["interactive_settings"];
        ConfigChanged = true;
    }

    if (auto aiProfiles = interactiveSettings["ai_profiles"]; aiProfiles && !aiProfiles.IsMap()) {
        YDB_CLI_LOG(Error, "$.interactive_settings.ai_profiles section has unexpected type " << static_cast<ui64>(interactiveSettings.Type()) << ", changed to map and cleared");
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
        YDB_CLI_LOG(Critical, "Couldn't save configuration to file \"" << ConfigurationPath << "\": " << CurrentExceptionMessage());
    }
}

} // namespace NYdb::NConsoleClient
