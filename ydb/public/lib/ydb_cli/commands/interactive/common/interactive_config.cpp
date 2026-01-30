#include "interactive_config.h"
#if defined(YDB_CLI_AI_ENABLED)
#include "api_utils.h"
#endif
#include <ydb/public/lib/ydb_cli/common/log.h>

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>

#include <util/generic/scope.h>
#include <util/string/strip.h>

#include <library/cpp/yaml/as/tstring.h>
#include <yaml-cpp/yaml.h>

namespace NYdb::NConsoleClient {

namespace {

TInteractiveConfigurationManager::TAiPresets::TInfo CreateOpenAiPreset(ui64 idx, const TString& name, const TString& id, const TString& endpoint = "https://api.openai.com/v1") {
    return {
        .OrderIdx = idx,
        .Name = name,
        .ApiType = TInteractiveConfigurationManager::EAiApiType::OpenAI,
        .ApiEndpoint = endpoint,
        .ModelName = id
    };
}

TInteractiveConfigurationManager::TAiPresets::TInfo CreateAnthropicPreset(ui64 idx, const TString& name, const TString& id, const TString& endpoint = "https://api.anthropic.com/v1") {
    return {
        .OrderIdx = idx,
        .Name = name,
        .ApiType = TInteractiveConfigurationManager::EAiApiType::Anthropic,
        .ApiEndpoint = endpoint,
        .ModelName = id
    };
}

} // anonymous namespace

void TInteractiveConfigurationManager::TAiPresets::ClearPresets() {
    Presets.clear();
}

void TInteractiveConfigurationManager::TAiPresets::AddPreset(const TString& id, const TInfo& info) {
    Y_VALIDATE(Presets.emplace(id, std::move(info)).second, "Preset with id " << id << " already exists");
}

std::optional<TInteractiveConfigurationManager::TAiPresets::TInfo> TInteractiveConfigurationManager::TAiPresets::GetPreset(const TString& id) {
    const auto it = Presets.find(id);
    if (it == Presets.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<std::pair<TString, TInteractiveConfigurationManager::TAiPresets::TInfo>> TInteractiveConfigurationManager::TAiPresets::ListPresets() {
    std::vector<std::pair<TString, TInfo>> result;
    for (const auto& [id, info] : Presets) {
        result.emplace_back(id, info);
    }
    std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.second.OrderIdx < rhs.second.OrderIdx;
    });
    return result;
}

std::unordered_map<TString, TInteractiveConfigurationManager::TAiPresets::TInfo> TInteractiveConfigurationManager::TAiPresets::GetOssPresets() {
    return {
        {"gpt-5.2", CreateOpenAiPreset(0, "GPT 5.2", "gpt-5.2")},
        {"claude-opus-4.5", CreateAnthropicPreset(1, "Claude Opus 4.5", "claude-opus-4-5-20251101")},
        {"claude-sonnet-4.5", CreateAnthropicPreset(2, "Claude Sonnet 4.5", "claude-sonnet-4-5-20250929")},
        {"qwen2-coder-plus", CreateOpenAiPreset(3, "Qwen3 Coder Plus", "qwen3-coder-plus", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1")},
        {"gemeni-2.5-pro", CreateOpenAiPreset(4, "Gemeni 2.5 Pro", "gemini-2.0-pro", "https://generativelanguage.googleapis.com/v1beta/openai")},
        {"glm-4.5", CreateOpenAiPreset(5, "GLM 4.5", "glm-4-0520,", "https://open.bigmodel.cn/api/paas/v4")},
    };
}

TInteractiveConfigurationManager::TAiProfile::TAiProfile(const TString& name, YAML::Node config, TInteractiveConfigurationManager::TPtr manager)
    : Name(name)
    , Manager(manager)
    , Config(config)
{
    Y_VALIDATE(Config.IsNull() || Config.IsMap(), "Unexpected config type: " << static_cast<ui64>(Config.Type()));
}

TInteractiveConfigurationManager::TAiProfile::TAiProfile(TInteractiveConfigurationManager::TPtr manager)
    : Manager(manager)
{}

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

TString TInteractiveConfigurationManager::TAiProfile::GetPresetId() const {
    if (auto presetIdNode = Config["preset_id"]) {
        return presetIdNode.as<TString>("");
    }
    return "";
}

const TString& TInteractiveConfigurationManager::TAiProfile::GetName() const {
    return Name;
}

void TInteractiveConfigurationManager::TAiProfile::SetName(const TString& name) {
    Name = name;
}

std::optional<TInteractiveConfigurationManager::EAiApiType> TInteractiveConfigurationManager::TAiProfile::GetApiType() const {
    if (auto apiTypeNode = Config["api_type"]) {
        auto apiType = apiTypeNode.as<ui64>(static_cast<ui64>(EAiApiType::Invalid));
        if (apiType >= static_cast<ui64>(EMode::Invalid)) {
            YDB_CLI_LOG(Debug, "AI profile \"" << Name << "\" has invalid API type: " << apiType << ", API type will be removed");
            return std::nullopt;
        }

        return static_cast<EAiApiType>(apiType);
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

bool TInteractiveConfigurationManager::TAiProfile::SetupProfile(const TString& preset) {
    auto presetInfo = TAiPresets::GetPreset(preset);
    if (presetInfo) {
        Name = presetInfo->Name;
        Config["preset_id"] = preset;
        Manager->ConfigChanged = true;
    }

    Y_DEFER { Cout << Endl; };

    if (!SetupApiType(presetInfo)) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no API type provided");
        return false;
    }

    Cout << Endl;
    if (!SetupApiEndpoint(presetInfo)) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no API endpoint provided");
        return false;
    }

    Cout << Endl;
    if (!SetupApiToken()) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no API token provided");
        return false;
    }

    Cout << Endl;
    if (!SetupModelName(presetInfo)) {
        YDB_CLI_LOG(Notice, "AI profile \"" << Name << "\" is not configured, no model name provided");
        return false;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupApiType(const std::optional<TAiPresets::TInfo>& presetInfo) {
    if (presetInfo) {
        Config["api_type"] = static_cast<ui64>(presetInfo->ApiType);
        Manager->ConfigChanged = true;
        return true;
    }

    std::optional<EAiApiType> apiType;
    std::vector<TMenuEntry> options;

    const TString openAiDescription = TStringBuilder() << EAiApiType::OpenAI << " (e. g. for models on Yandex Cloud or openai.com)";
    const auto openAiAction = [&]() { apiType = EAiApiType::OpenAI; };

    const TString anthropicDescription = TStringBuilder() << EAiApiType::Anthropic << " (e. g. for models on anthropic.com)";
    const auto anthropicAction = [&]() { apiType = EAiApiType::Anthropic; };

    const auto currentApiType = GetApiType();
    TString title;
    if (currentApiType) {
        title = TStringBuilder() << "Pick desired action to configure API type:";

        if (*currentApiType != EAiApiType::OpenAI) {
            options.push_back({TStringBuilder() << "Change API type to " << openAiDescription << ", all other settings will be removed", openAiAction});
        }

        if (*currentApiType != EAiApiType::Anthropic) {
            options.push_back({TStringBuilder() << "Change API type to " << anthropicDescription << ", all other settings will be removed", anthropicAction});
        }

        options.push_back({TStringBuilder() << "Use current API type " << *currentApiType, [&]() { apiType = *currentApiType; }});
    } else {
        title = TStringBuilder() << "Please choose desired API type:";
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
            Cout << "Setting API type " << Colors.BoldColor() << *apiType << Colors.OldColor() << Endl;
        }

        Config["api_type"] = static_cast<ui64>(*apiType);
        Manager->ConfigChanged = true;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupApiEndpoint(const std::optional<TAiPresets::TInfo>& presetInfo) {
    if (presetInfo) {
        Config["api_endpoint"] = presetInfo->ApiEndpoint;
        Manager->ConfigChanged = true;
        return true;
    }

    const auto apiType = GetApiType();
    if (!apiType) {
        YDB_CLI_LOG(Warning, "Can not setup API endpoint, there is no API type");
        return false;
    }

    TString endpoint;
    std::vector<TMenuEntry> options;
    const TString title = TStringBuilder() << "Pick desired action to configure API endpoint:";

    auto defaultEndpointInfo = TStringBuilder() << "Use default endpoint for " << *apiType << ": ";
    const TString openAiEndpoint = "https://api.openai.com";
    const TString anthropicEndpoint = "https://api.anthropic.com";
    switch (*apiType) {
        case EAiApiType::OpenAI: {
            options.push_back({defaultEndpointInfo << openAiEndpoint, [&]() { endpoint = openAiEndpoint; }});
            break;
        }
        case EAiApiType::Anthropic: {
            options.push_back({defaultEndpointInfo << anthropicEndpoint, [&]() { endpoint = anthropicEndpoint; }});
            break;
        }
        case EAiApiType::Invalid:
            Y_VALIDATE(false, "Invalid API type: " << *apiType);
    }

    options.push_back({"Set a new endpoint value", [&]() {
        auto value = RunFtxuiInput("Please enter API endpoint:", "", [&](const TString& input, TString& error) {
            auto url = Strip(input);
            if (!url) {
                error = "API endpoint can not be empty";
                return false;
            }

            if (!url.StartsWith("http://") && !url.StartsWith("https://")) {
                error = "API endpoint supports only http:// or https:// schema";
                return false;
            }

#if defined(YDB_CLI_AI_ENABLED)
            try {
                NAi::CreateApiUrl(url, "/");
            } catch (const std::exception& e) {
                error = e.what();
                return false;
            }
#endif

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
        Cout << "Setting API endpoint value \"" << endpoint << "\"" << Endl;
        Config["api_endpoint"] = endpoint;
        Manager->ConfigChanged = true;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupApiToken() {
    TString token;
    std::vector<TMenuEntry> options;
    const TString title = TStringBuilder() << "Pick desired action to configure API token:";

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

    options.push_back({TStringBuilder() << "Don't save token", []() {}});

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
        Cout << "Setting API token value \"" << BlurSecret(token) << "\"" << Endl;
        Config["api_token"] = token;
        Manager->ConfigChanged = true;
    }

    return true;
}

bool TInteractiveConfigurationManager::TAiProfile::SetupModelName(const std::optional<TAiPresets::TInfo>& presetInfo) {
    if (presetInfo) {
        Config["model_name"] = presetInfo->ModelName;
        Manager->ConfigChanged = true;
        return true;
    }

    const TString& apiEndpoint = GetApiEndpoint();
    if (!apiEndpoint) {
        YDB_CLI_LOG(Warning, "Can not setup model name, there is no API endpoint");
        return false;
    }

    std::vector<TString> allowedModels;
#if defined(YDB_CLI_AI_ENABLED)
    try {
        allowedModels = NAi::ListModelNames(apiEndpoint, GetApiToken());
    } catch (const std::exception& e) {
        Cerr << Colors.Yellow() << "Failed to list model names, maybe model API endpoint is not correct: " << e.what() << Colors.OldColor() << Endl;
    }
#endif

    TString modelName;
    std::vector<TMenuEntry> options;
    const TString title = TStringBuilder() << "Pick desired action to configure model name:";

    options.push_back({"Set custom model name", [&]() {
        auto value = RunFtxuiInput("Please enter model name:", "", [&](const TString& input, TString& error) {
            modelName = Strip(input);
            if (!modelName) {
                error = "Model name cannot be empty";
                return false;
            }
            return true;
        });
        if (!value) {
            modelName.clear();
        }
    }});

    options.push_back({TStringBuilder() << "Do not set model name", []() {}});

    const auto currentModelName = GetModelName();
    if (currentModelName) {
        options.push_back({TStringBuilder() << "Use current model name \"" << currentModelName << "\"", [&]() {
            modelName = currentModelName;
        }});
    }

    for (const auto& allowedName : allowedModels) {
        if (allowedName == currentModelName) {
            continue;
        }

        options.push_back({TStringBuilder() << "Use model \"" << allowedName << "\"", [&modelName, allowedName]() {
            modelName = allowedName;
        }});
    }

    if (!RunFtxuiMenuWithActions(title, options)) {
        return false;
    }

    if (modelName && (!currentModelName || currentModelName != modelName)) {
        Cout << "Setting model name \"" << modelName << "\"" << Endl;
        Config["model_name"] = modelName;
        Manager->ConfigChanged = true;
    }

    return true;
}

TInteractiveConfigurationManager::TInteractiveConfigurationManager(const TString& configurationPath)
    : ConfigurationPath(configurationPath)
{
    LoadProfile();
    CanonizeStructure();
}

void TInteractiveConfigurationManager::EnsurePredefinedProfiles(const std::vector<TAiPresetConfig>& profiles, std::function<TAiTokenConfig()> tokenGetter) {
    if (profiles.empty()) {
        return;
    }

    std::optional<TAiTokenConfig> tokenInfo;
    if (tokenGetter) {
        tokenInfo = tokenGetter();
    }

    if (!tokenInfo || tokenInfo->Token.empty()) {
        return;
    }

    if (!Config["ai_profiles"]) {
        Config["ai_profiles"] = YAML::Node(YAML::NodeType::Map);
    }
    auto aiProfiles = Config["ai_profiles"];

    bool updated = false;

    // 1. Create or Update existing predefined profiles
    THashSet<TString> predefinedProfileNames;
    for (const auto& profile : profiles) {
        const TString& profileName = profile.Name;
        predefinedProfileNames.insert(profileName);

        if (!aiProfiles[profileName].IsDefined()) {
            // Create new predefined profile
            YAML::Node newProfile;
            if (profile.ApiType == "OpenAI") {
                newProfile["api_type"] = static_cast<ui64>(EAiApiType::OpenAI);
            } else if (profile.ApiType == "Anthropic") {
                newProfile["api_type"] = static_cast<ui64>(EAiApiType::Anthropic);
            } else {
                 YDB_CLI_LOG(Warning, "Unknown API type for predefined profile: " << profile.ApiType);
                 continue;
            }
            newProfile["api_endpoint"] = profile.ApiEndpoint;
            newProfile["model_name"] = profile.ModelName;
            newProfile["api_token"] = tokenInfo->Token;
            newProfile["type"] = "predefined";

            aiProfiles[profileName] = newProfile;
            YDB_CLI_LOG(Info, "Created predefined AI profile: " << profileName);
            updated = true;
        } else {
            // Update existing if predefined
            auto node = aiProfiles[profileName];
            TString type = node["type"].IsDefined() ? node["type"].as<TString>() : "custom";

            if (type == "predefined") {
                bool changed = false;

                // Check ApiType
                ui64 targetApiType = static_cast<ui64>(EAiApiType::Invalid);
                if (profile.ApiType == "OpenAI") targetApiType = static_cast<ui64>(EAiApiType::OpenAI);
                else if (profile.ApiType == "Anthropic") targetApiType = static_cast<ui64>(EAiApiType::Anthropic);

                if (!node["api_type"].IsDefined() || node["api_type"].as<ui64>() != targetApiType) {
                    node["api_type"] = targetApiType;
                    changed = true;
                }

                // Check Endpoint
                if (!node["api_endpoint"].IsDefined() || node["api_endpoint"].as<TString>() != profile.ApiEndpoint) {
                    node["api_endpoint"] = profile.ApiEndpoint;
                    changed = true;
                }

                // Check ModelName
                if (!node["model_name"].IsDefined() || node["model_name"].as<TString>() != profile.ModelName) {
                    node["model_name"] = profile.ModelName;
                    changed = true;
                }

                // Check Token
                if (tokenInfo->WasUpdated || !node["api_token"].IsDefined() || node["api_token"].as<TString>() != tokenInfo->Token) {
                    node["api_token"] = tokenInfo->Token;
                    changed = true;
                }

                if (changed) {
                    YDB_CLI_LOG(Info, "Updated predefined AI profile: " << profileName);
                    updated = true;
                }
            }
        }
    }

    // 2. Remove obsolete predefined profiles
    std::vector<TString> keysToRemove;
    for (const auto& it : aiProfiles) {
        TString name = it.first.as<TString>();
        YAML::Node node = it.second;
        TString type = node["type"].IsDefined() ? node["type"].as<TString>() : "custom";

        if (type == "predefined") {
            if (!predefinedProfileNames.contains(name)) {
                keysToRemove.push_back(name);
            }
        }
    }

    for (const auto& name : keysToRemove) {
        aiProfiles.remove(name);
        YDB_CLI_LOG(Info, "Removed obsolete predefined AI profile: " << name);
        updated = true;
    }

    // 3. Recommended profile logic
    if (!profiles.empty()) {
        const auto& recommendedProfile = profiles.front().Name;
        const auto& currentRecommendedNode = Config["recommended_ai_profile"];
        if (!currentRecommendedNode || currentRecommendedNode.as<TString>("") != recommendedProfile) {
            Config["recommended_ai_profile"] = recommendedProfile;
            updated = true;
        }
    }

    if (updated) {
        ConfigChanged = true;
        SaveConfig();
    }
}

TInteractiveConfigurationManager::~TInteractiveConfigurationManager() {
    if (ConfigChanged) {
        SaveConfig();
    }
}

TInteractiveConfigurationManager::EMode TInteractiveConfigurationManager::GetDefaultMode() const {
    if (const auto& defaultModeNode = Config["default_mode"]) {
        auto defaultMode = defaultModeNode.as<ui64>(static_cast<ui64>(EMode::Invalid));
        if (defaultMode >= static_cast<ui64>(EMode::Invalid)) {
            YDB_CLI_LOG(Warning, "Interactive config has invalid default mode: " << defaultMode << ", falling back to YQL mode");
            defaultMode = static_cast<ui64>(EMode::YQL);
        }

        return static_cast<EMode>(defaultMode);
    }

    return EMode::YQL;
}

void TInteractiveConfigurationManager::ChangeDefaultMode(EMode mode) {
    Config["default_mode"] = static_cast<ui64>(mode);
    ConfigChanged = true;
    YDB_CLI_LOG(Info, "Default interactive mode was changed to " << mode);
}

TString TInteractiveConfigurationManager::ModeToString(EMode mode) {
    return mode == EMode::YQL
        ? TStringBuilder() << Colors.Green() << "YQL" << Colors.OldColor()
        : TStringBuilder() << Colors.Cyan() << "AI" << Colors.OldColor();
}

TString TInteractiveConfigurationManager::GetActiveAiProfileName() const {
    TString activeProfile;
    if (const auto& activeAiProfileNode = Config["active_ai_profile"]) {
        activeProfile = activeAiProfileNode.as<TString>("");
    }

    if (activeProfile.empty()) {
        if (const auto& recommendedAiProfileNode = Config["recommended_ai_profile"]) {
            activeProfile = recommendedAiProfileNode.as<TString>("");
        }
    }

    YDB_CLI_LOG(Debug, "Current active profile name: " << activeProfile);
    return activeProfile;
}

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::GetAiProfile(const TString& name) {
    const auto& profiles = ListAiProfiles();
    const auto it = profiles.find(name);
    return it != profiles.end() ? it->second : nullptr;
}

std::unordered_map<TString, TInteractiveConfigurationManager::TAiProfile::TPtr> TInteractiveConfigurationManager::ListAiProfiles() {
    auto aiProfiles = Config["ai_profiles"];
    if (!aiProfiles) {
        return {};
    }

    std::unordered_map<TString, TAiProfile::TPtr> existingAiProfiles;
    for (const auto& profile : aiProfiles) {
        const auto& name = profile.first.as<TString>("");
        if (!name) {
            YDB_CLI_LOG(Warning, "AI profile has no name, profile skipped");
            continue;
        }

        const auto& settings = profile.second;
        if (!settings.IsMap() && !settings.IsNull()) {
            YDB_CLI_LOG(Warning, "AI profile \"" << name << "\" has unexpected type " << static_cast<ui64>(settings.Type()) << " instead of map or null, profile skipped");
            continue;
        }

        auto aiProfile = std::make_shared<TAiProfile>(name, settings, shared_from_this());
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

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::SelectAiModelProfile() {
    std::vector<TMenuEntry> options;

    const auto activeAiProfile = GetAiProfile(GetActiveAiProfileName());
    TString presetId;
    for (const auto& [id, preset] : TAiPresets::ListPresets()) {
        auto description = TStringBuilder() << preset.Name;
        if (activeAiProfile && activeAiProfile->GetPresetId() == id) {
            description << " (active)";
        }

        options.emplace_back(description, [&presetId, id]() {
            presetId = id;
        });
    }

    const auto& existingAiProfiles = ListAiProfiles();
    TAiProfile::TPtr result;
    std::unordered_map<TString, TAiProfile::TPtr> presets;
    for (const auto& [name, profile] : existingAiProfiles) {
        if (const auto& id = profile->GetPresetId()) {
            presets.emplace(id, profile);
            continue;
        }

        auto description = TStringBuilder() << name;
        if (activeAiProfile && activeAiProfile->GetName() == name) {
            description << " (active)";
        }

        options.emplace_back(description, [&result, profile]() {
            result = profile;
        });
    }

    options.emplace_back("Setup custom model", []() {});

    Y_DEFER { Cout << Endl; };

    if (!RunFtxuiMenuWithActions("Please choose AI model:", options)) {
        return nullptr;
    }

    if (result) {
        return result;
    }

    if (presetId) {
        if (const auto it = presets.find(presetId); it != presets.end()) {
            return it->second;
        }
    }

    return CreateNewAiModelProfile(presetId);
}

void TInteractiveConfigurationManager::ChangeActiveAiProfile(const TString& name) {
    Config["active_ai_profile"] = name;
    ConfigChanged = true;
    YDB_CLI_LOG(Info, "Active AI model was changed to \"" << name << "\"");
}

TInteractiveConfigurationManager::TAiProfile::TPtr TInteractiveConfigurationManager::CreateNewAiModelProfile(const TString& presetId) {
    auto aiProfile = std::make_shared<TAiProfile>(shared_from_this());
    if (!aiProfile->SetupProfile(presetId)) {
        YDB_CLI_LOG(Warning, "AI profile settings setup failed");
        return nullptr;
    }

    const auto& profileName = CreateAiProfileName(aiProfile->GetName(), aiProfile->GetModelName());
    if (!profileName) {
        YDB_CLI_LOG(Warning, "AI profile has no name, profile creation failed");
        return nullptr;
    }

    aiProfile->SetName(profileName);
    aiProfile->Config["type"] = "custom";
    Config["ai_profiles"][profileName] = aiProfile->Config;
    ConfigChanged = true;

    if (GetActiveAiProfileName() != profileName) {
        ChangeActiveAiProfile(profileName);
    }

    Cout << "Current AI model is \"" << profileName << "\"." << Endl;
    YDB_CLI_LOG(Notice, "AI model \"" << profileName << "\" was created");
    return aiProfile;
}

TString TInteractiveConfigurationManager::CreateAiProfileName(const TString& currentName, const TString& defaultName) {
    TStringBuilder promptBuilder;
    TString name = defaultName;

    const auto& existingAiProfiles = ListAiProfiles();
    if (currentName) {
        if (!existingAiProfiles.contains(currentName)) {
            return currentName;
        }

        promptBuilder << "Current name \"" << currentName << "\" already exists, please enter name for a new AI model";
    } else {
        promptBuilder << "Please enter name for a new AI model";
    }

    if (name) {
        if (!existingAiProfiles.contains(name)) {
            promptBuilder << " (\"" << name << "\" by default)";
        } else {
            name = "";
        }
    }

    RunFtxuiInput(promptBuilder << ":", "", [&name, &existingAiProfiles](const TString& input, TString& error) {
        const auto& newName = Strip(input);
        if (!newName) {
            if (!name) {
                error = "AI model name cannot be empty";
                return false;
            }
        } else if (existingAiProfiles.contains(newName)) {
            error = TStringBuilder() << "AI model with name \"" << newName << "\" already exists";
            return false;
        } else {
            name = newName;
        }

        return true;
    });

    return name;
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
    if (auto aiProfiles = Config["ai_profiles"]; aiProfiles && !aiProfiles.IsMap()) {
        YDB_CLI_LOG(Error, "$.ai_profiles section has unexpected type " << static_cast<ui64>(aiProfiles.Type()) << ", changed to map and cleared");
        Config["ai_profiles"] = YAML::Node();
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

        YAML::Emitter out;
        out.SetMapFormat(YAML::Block);
        out.SetSeqFormat(YAML::Block);
        out << Config;

        TFileOutput resultConfigFile(TFile(configFilePath, CreateAlways | WrOnly | AWUser | ARUser));
        resultConfigFile << out.c_str();
    } catch (...) {
        YDB_CLI_LOG(Critical, "Couldn't save configuration to file \"" << ConfigurationPath << "\": " << CurrentExceptionMessage());
    }
}

} // namespace NYdb::NConsoleClient
