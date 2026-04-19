#include "interactive_config.h"
#include "api_utils.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>

#include <util/generic/scope.h>
#include <util/string/strip.h>
#include <util/system/env.h>

#include <library/cpp/yaml/as/tstring.h>
#include <yaml-cpp/yaml.h>

namespace NYdb::NConsoleClient {

/// Presets ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

std::function<TAiPresets()> PresetsInit;
std::optional<TAiPresets> Presets;

template <typename TInfo>
std::vector<std::pair<TString, TInfo>> GetOrderedInfo(const std::unordered_map<TString, TInfo>& info) {
    std::vector<std::pair<TString, TInfo>> orderedInfo;
    orderedInfo.assign(info.begin(), info.end());

    std::sort(orderedInfo.begin(), orderedInfo.end(), [](const std::pair<TString, TInfo>& lhs, const std::pair<TString, TInfo>& rhs) {
        const auto& l = lhs.second;
        const auto& r = rhs.second;
        return std::tie(l.OrderIdx, l.Name) < std::tie(r.OrderIdx, r.Name);
    });

    return orderedInfo;
}

} // anonymous namespace

std::optional<TString> TAiPresets::TTokenProvider::GetToken() const {
    Y_VALIDATE(Provider, "Missing token provider");
    return Provider->GetToken();
}

TString TAiPresets::TTokenProvider::ToString() const {
    return TStringBuilder() << "TTokenProvider[" << OrderIdx << "]{"
        << ".Name = '" << Name << "'"
        << "}";
}

TAiPresets::TEndpoint TAiPresets::TEndpoint::AppendApiPath(const TString& apiPath) const {
    TEndpoint result = *this;
    result.ApiEndpoint.append(apiPath);
    return result;
}

TAiPresets::TEndpoint TAiPresets::TEndpoint::SetInfo(const TString& info) const {
    TEndpoint result = *this;
    result.Info = info;
    return result;
}

TAiPresets::TEndpoint TAiPresets::TEndpoint::SetApiType(EApiType apiType) const {
    TEndpoint result = *this;
    result.ApiType = apiType;
    return result;
}

TAiPresets::TEndpoint TAiPresets::TEndpoint::SetModelName(const TString& modelName) const {
    TEndpoint result = *this;
    result.ModelName = modelName;
    return result;
}

TString TAiPresets::TEndpoint::ToString() const {
    return TStringBuilder() << "TEndpoint{"
        << ".ApiEndpoint = '" << ApiEndpoint << "', "
        << ".Info = '" << Info << "', "
        << ".ApiType = " << (ApiType ? ::ToString(*ApiType) : "<null>") << ", "
        << ".ModelName = '" << (ModelName ? *ModelName : "<null>") << "', "
        << ".TokenProvider = " << (TokenProvider ? *TokenProvider : "<null>")
        << "}";
}

TString TAiPresets::TPreset::ToString() const {
    return TStringBuilder() << "TPreset[" << OrderIdx << "]{"
        << ".Name = '" << Name << "', "
        << ".Info = " << Info.ToString()
        << "}";
}

TAiPresets::TAiPresets(
    std::unordered_map<TString, TTokenProvider>&& tokenProviders,
    std::vector<TEndpoint>&& endpoints,
    std::unordered_map<TString, TPreset>&& presets,
    TMetaInfo&& metaInfo)
    : TokenProviders(std::move(tokenProviders))
    , Endpoints(std::move(endpoints))
    , Presets(std::move(presets))
    , MetaInfo(std::move(metaInfo))
{}

std::optional<TAiPresets::TTokenProvider> TAiPresets::GetTokenProvider(const TString& id) const {
    const auto it = TokenProviders.find(id);
    return it != TokenProviders.end() ? std::optional<TTokenProvider>(it->second) : std::nullopt;
}

const TAiPresets::TTokenProviders& TAiPresets::GetTokenProviders() {
    if (!TokenProvidersOrdered) {
        TokenProvidersOrdered = GetOrderedInfo(TokenProviders);
    }
    return *TokenProvidersOrdered;
}

const std::vector<TAiPresets::TEndpoint>& TAiPresets::GetEndpoints() const {
    return Endpoints;
}

std::optional<TAiPresets::TEndpoint> TAiPresets::GetPreset(const TString& id) const {
    const auto it = Presets.find(id);
    return it != Presets.end() ? std::optional<TEndpoint>(it->second.Info) : std::nullopt;
}

const TAiPresets::TPresets& TAiPresets::GetPresets() {
    if (!PresetsOrdered) {
        PresetsOrdered = GetOrderedInfo(Presets);
    }
    return *PresetsOrdered;
}

const TAiPresets::TMetaInfo& TAiPresets::GetMetaInfo() const {
    return MetaInfo;
}

void TAiPresetsBuilder::AddToken(const TString& id, const TString& name, std::shared_ptr<ITokenProvider> provider) {
    Y_VALIDATE(
        TokenProviders.emplace(id, TTokenProvider{.OrderIdx = TokenProviders.size(), .Name = name, .Provider = provider}).second,
        "Token provider with id: " << id << " already exists"
    );
}

TAiPresets::TEndpoint TAiPresetsBuilder::AddEndpoint(const TEndpoint& endpoint) {
    Endpoints.emplace_back(endpoint);
    return endpoint;
}

void TAiPresetsBuilder::AddPreset(const TString& id, const TEndpoint& info) {
    Y_VALIDATE(
        Presets.emplace(id, TPreset{.OrderIdx = Presets.size(), .Name = info.Info, .Info = info}).second,
        "Preset with id: " << id << " already exists"
    );
}

void TAiPresetsBuilder::SetMetaInfo(const TMetaInfo& metaInfo) {
    MetaInfo = metaInfo;

    if (const auto& defaultPreset = metaInfo.DefaultPreset) {
        Y_VALIDATE(Presets.contains(defaultPreset), "Default preset: " << defaultPreset << " not found");
    }
}

TAiPresets TAiPresetsBuilder::Done() {
    return TAiPresets(std::move(TokenProviders), std::move(Endpoints), std::move(Presets), std::move(MetaInfo));
}

void SetupPresetsInitializer(std::function<TAiPresets()> presetsInit) {
    Y_VALIDATE(presetsInit, "Missing presets initializer");
    PresetsInit = [init = std::move(presetsInit)]() {
        auto presets = init();
        YDB_CLI_LOG(Debug, "Setup presets initializer:\n" << [&presets]() mutable {
            TStringBuilder info;
            info << "Tokens:\n";
            for (const auto& [_, token] : presets.GetTokenProviders()) {
                info << token.ToString() << "\n";
            }
            info << "Endpoints:\n";
            for (const auto& endpoint : presets.GetEndpoints()) {
                info << endpoint.ToString() << "\n";
            }
            info << "Presets:\n";
            for (const auto& [_, preset] : presets.GetPresets()) {
                info << preset.ToString() << "\n";
            }
            return info;
        }());
        return presets;
    };
}

TAiPresets& GetAiPresets() {
    if (!Presets) {
        Presets = PresetsInit ? PresetsInit() : TAiPresets();
    }
    return *Presets;
}

/// AI model ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

TString StringFromYaml(const YAML::Node& config, const TString& key) {
    if (auto keyNode = config[key]) {
        auto keyValue = keyNode.as<TString>("");
        if (!keyValue) {
            YDB_CLI_LOG(Debug, "YAML config has invalid or empty value for key: " << key);
        }

        return keyValue;
    }

    YDB_CLI_LOG(Debug, "YAML config has no key: " << key);
    return "";
}

template <typename EEnum>
std::optional<EEnum> EnumFromYaml(const YAML::Node& config, const TString& key) {
    if (auto keyNode = config[key]) {
        auto keyValue = keyNode.as<ui64>(static_cast<ui64>(EEnum::Max));
        if (keyValue >= static_cast<ui64>(EEnum::Max)) {
            YDB_CLI_LOG(Debug, "YAML config has invalid or empty value for enum key: " << key << ", value: " << keyValue);
            return std::nullopt;
        }

        return static_cast<EEnum>(keyValue);
    }

    YDB_CLI_LOG(Debug, "YAML config has no enum key: " << key);
    return std::nullopt;
}

template <typename TValue>
TString JoinOptionDesc(const TString& info, const TValue& currentValue) {
    if (const TString valueStr = ToString(currentValue)) {
        return TStringBuilder() << info << "\t" << valueStr;
    }
    return info;
}

template <typename TValue>
TString JoinOptionDesc(const TString& info, const std::optional<TValue>& currentValue) {
    if (currentValue) {
        return JoinOptionDesc(info, *currentValue);
    }
    return info;
}

} // anonymous namespace

void TYamlConfigBase::SetString(YAML::Node& config, const TString& key, const TString& value) {
    config[key] = value;
    OnConfigChanged();
}

void TYamlConfigBase::SetInt(YAML::Node& config, const TString& key, ui64 value) {
    config[key] = value;
    OnConfigChanged();
}

TAiModelConfig::TAiModelConfig(YAML::Node config, TYamlConfigBase::TPtr baseConfig, const TString& id)
    : BaseConfig(std::move(baseConfig))
    , Id(id)
    , Config(std::move(config))
{
    Y_VALIDATE(BaseConfig, "Missing base config");
    Y_VALIDATE(Config.IsNull() || Config.IsMap(), "Unexpected config type: " << static_cast<ui64>(Config.Type()));
}

bool TAiModelConfig::IsValid(TString& error) const {
    if (!GetName()) {
        error = "AI model name is empty";
        return false;
    }

    if (!GetEndpoint()) {
        error = "AI model has no API endpoint";
        return false;
    }

    if (!GetApiType()) {
        error = "AI model has no API type";
        return false;
    }

    if (const auto& presetId = GetPresetId(); presetId && !GetAiPresets().GetPreset(presetId)) {
        error = TStringBuilder() << "Where is no preset with id: " << presetId << " in presets list";
        return false;
    }

    return true;
}

TString TAiModelConfig::GetName() const {
    return StringFromYaml(Config, NAME_PROPERTY);
}

TString TAiModelConfig::GetEndpoint() const {
    if (const auto& endpoint = StringFromYaml(Config, ENDPOINT_PROPERTY)) {
        return endpoint;
    }

    if (const auto& preset = GetAiPresets().GetPreset(GetPresetId())) {
        return preset->ApiEndpoint;
    }

    return "";
}

std::optional<TAiPresets::EApiType> TAiModelConfig::GetApiType() const {
    if (auto apiType = EnumFromYaml<TAiPresets::EApiType>(Config, API_TYPE_PROPERTY)) {
        return apiType;
    }

    if (const auto& preset = GetAiPresets().GetPreset(GetPresetId())) {
        return preset->ApiType;
    }

    return std::nullopt;
}

std::optional<TString> TAiModelConfig::GetApiToken(bool allowEnv) {
    if (auto token = StringFromYaml(Config, TOKEN_PROPERTY)) {
        YDB_CLI_LOG(Debug, "Using token from config: " << BlurSecret(token));
        return token;
    }

    TString tokenProvider = StringFromYaml(Config, TOKEN_PROVIDER_PROPERTY);
    if (const auto& preset = GetAiPresets().GetPreset(GetPresetId()); !tokenProvider && !Config[TOKEN_PROVIDER_PROPERTY] && preset && preset->TokenProvider) {
        tokenProvider = *preset->TokenProvider;
    }

    std::optional<TAiPresets::TTokenProvider> provider;
    if (tokenProvider) {
        provider = GetAiPresets().GetTokenProvider(tokenProvider);
        if (!provider) {
            YDB_CLI_LOG(Info, "No token provider configured with id: " << tokenProvider);
        }
    }

    if (!provider) {
        if (allowEnv) {
            const auto& token = GetEnv(TOKEN_ENV);
            YDB_CLI_LOG(Debug, "Fetched token from env: " << BlurSecret(token));
            return token;
        }
    
        YDB_CLI_LOG(Debug, "No token found");
        return "";
    }

    YDB_CLI_LOG(Debug, "Using token from provider: " << tokenProvider);

    try {
        const auto& token = provider->GetToken();
        YDB_CLI_LOG(Debug, "Resolved token from provider \"" << tokenProvider << "\": " << (token ? BlurSecret(*token) : "<null>"));
        return token;
    } catch (const std::exception& e) {
        YDB_CLI_LOG(Notice, "Failed to resolve token from provider \"" << tokenProvider << "\":\n" << e.what());
        if (RequestApiToken()) {
            return StringFromYaml(Config, TOKEN_PROPERTY);
        }
        return std::nullopt;
    }
}

TString TAiModelConfig::GetModelName() const {
    if (const auto& modelName = StringFromYaml(Config, MODEL_PROPERTY)) {
        return modelName;
    }

    if (const auto& preset = GetAiPresets().GetPreset(GetPresetId())) {
        return preset->ModelName.value_or("");
    }

    return "";
}

TString TAiModelConfig::GetPresetId() const {
    return StringFromYaml(Config, PRESET_ID_PROPERTY);
}

YAML::Node TAiModelConfig::GetConfig() const {
    return Config;
}

TString TAiModelConfig::GetId() const {
    return Id;
}

bool TAiModelConfig::Setup(const TString& presetId) {
    if (presetId) {
        const auto& preset = GetAiPresets().GetPreset(presetId);
        Y_VALIDATE(preset, "No preset configured with id: " << presetId);
        FillFromPreset(*preset, /* setProperties */ false, /* setName */ true);
        BaseConfig->SetString(Config, PRESET_ID_PROPERTY, presetId);
    } else {
        if (!GetEndpoint() && !SetupEndpoint()) {
            return false;
        }
        if (!GetApiType() && !SetupApiType()) {
            return false;
        }
        if (const auto& token = GetApiToken(); !token || (!*token && !SetupApiToken())) {
            return false;
        }
        if (!GetModelName() && !SetupModelName()) {
            return false;
        }
        if (!GetName() && !SetupName()) {
            return false;
        }
    }
    return true;
}

bool TAiModelConfig::Edit(bool& changed) {
    using TAction = bool (TAiModelConfig::*)();

    bool finished = false;
    const auto doAction = [this, &finished, &changed](TAction action) {
        return [this, &finished, &changed, action]() {
            if ((this->*action)()) {
                changed = true;
            } else{ 
                finished = true;
            }
        };
    };

    bool success = false;
    while (!finished) {
        std::vector<TMenuEntry> options;
        options.push_back({JoinOptionDesc("API endpoint", GetEndpoint()), doAction(&TAiModelConfig::SetupEndpoint)});
        options.push_back({JoinOptionDesc("API type", GetApiType()), doAction(&TAiModelConfig::SetupApiType)});
        options.push_back({JoinOptionDesc("Model name", GetModelName()), doAction(&TAiModelConfig::SetupModelName)});
        options.push_back({JoinOptionDesc("Token", StringFromYaml(Config, TOKEN_PROVIDER_PROPERTY)), doAction(&TAiModelConfig::SetupApiToken)});
        options.push_back({JoinOptionDesc("Profile display name", GetName()), doAction(&TAiModelConfig::SetupName)});
        options.push_back({"Finish editing", [&finished, &success]() {
            finished = true;
            success = true;
        }});

        if (!RunFtxuiMenuWithActions("Please choose setting to change or use ^C to exit:", options)) {
            return false;
        }
    }

    return success;
}

void TAiModelConfig::FillFromPreset(const TAiPresets::TEndpoint& info, bool setProperties, bool setName) {
    ResetPresetInfo();

    Y_VALIDATE(info.ApiEndpoint, "Invalid API endpoint in preset");

    if (setProperties) {
        BaseConfig->SetString(Config, ENDPOINT_PROPERTY, info.ApiEndpoint);

        if (info.ApiType) {
            BaseConfig->SetInt(Config, API_TYPE_PROPERTY, static_cast<ui64>(*info.ApiType));
        }
    
        if (info.ModelName) {
            BaseConfig->SetString(Config, MODEL_PROPERTY, *info.ModelName);
        }
    
        if (info.TokenProvider) {
            BaseConfig->SetString(Config, TOKEN_PROVIDER_PROPERTY, *info.TokenProvider);
        }
    }

    if (setName && info.Info) {
        BaseConfig->SetString(Config, NAME_PROPERTY, info.Info);
    }
}

void TAiModelConfig::ResetPresetInfo() {
    const auto& presetId = GetPresetId();
    if (!presetId) {
        return;
    }

    BaseConfig->SetString(Config, PRESET_ID_PROPERTY, "");

    if (const auto& preset = GetAiPresets().GetPreset(presetId)) {
        FillFromPreset(*preset, /* setProperties */ true, /* setName */ false);
    }
}

bool TAiModelConfig::SetupEndpoint() {
    std::optional<TAiPresets::TEndpoint> presetEndpoint;
    const auto& presetEndpoints = GetAiPresets().GetEndpoints();
    const TString& currentEndpoint = GetEndpoint();
    if (!presetEndpoints.empty()) {
        std::vector<TMenuEntry> options;
        for (const auto& endpoint : presetEndpoints) {
            Y_VALIDATE(endpoint.ApiEndpoint, "Invalid API endpoint in preset");
            if (endpoint.ApiEndpoint == currentEndpoint) {
                continue;
            }

            options.push_back({JoinOptionDesc(endpoint.ApiEndpoint, endpoint.Info), [&presetEndpoint, endpoint]() {
                presetEndpoint = endpoint;
            }});
        }

        if (!options.empty()) {
            options.push_back({
                currentEndpoint ? TStringBuilder() << "Edit current endpoint\t" << currentEndpoint : TStringBuilder() << "Setup custom endpoint",
                []() {}
            });

            if (!RunFtxuiMenuWithActions("Please choose API endpoint:", options)) {
                return false;
            }
        }
    }

    if (presetEndpoint) {
        FillFromPreset(*presetEndpoint, /* setProperties */ true, /* setName */ false);
        return true;
    }

    const auto setEndpoint = [&](const TString& endpoint) {
        ResetPresetInfo();
        BaseConfig->SetString(Config, ENDPOINT_PROPERTY, endpoint);
        return true;
    };

    while (true) {
        TString result;
        const auto hasResult = RunFtxuiInput("Please enter API endpoint:", currentEndpoint, [&result](const TString& input, TString& error) {
            auto url = Strip(input);
            if (!url) {
                error = "Endpoint can not be empty";
                return false;
            }

            if (!url.StartsWith("http://") && !url.StartsWith("https://")) {
                error = "Endpoint should have http:// or https:// schema";
                return false;
            }

            try {
                NAi::CreateApiUrl(url, "/");
            } catch (const std::exception& e) {
                error = e.what();
                return false;
            }

            result = url;
            return true;
        }, "https://api.openai.com/v1/").has_value();

        if (!hasResult) {
            return false;
        }

        if (const auto checkResult = NAi::TestConnection(result)) {
            if (*checkResult) {
                setEndpoint(result);
                return true;
            }
        } else {
            return false;
        }

        if (const auto forceContinue = AskYesNoFtxuiOptional(ftxui::hbox({
            ftxui::text("Failed to connect to API endpoint "),
            ftxui::text(result) | ftxui::color(ftxui::Color::Cyan),
            ftxui::text(". Continue anyway?"),
        }) | ftxui::bold, false, ftxui::Color::Yellow)) {
            if (*forceContinue) {
                setEndpoint(result);
                return true;
            }
        } else {
            return false;
        }
    }

    return false;
}

bool TAiModelConfig::SetupApiType() {
    std::vector<TMenuEntry> options;

    for (auto apiType : {TAiPresets::EApiType::OpenAI, TAiPresets::EApiType::Anthropic}) {
        auto prompt = TStringBuilder() << apiType;

        switch (apiType) {
            case TAiPresets::EApiType::OpenAI:
                prompt << "\te. g. for models on Yandex Cloud or openai.com";
                break;
            case TAiPresets::EApiType::Anthropic:
                prompt << "\te. g. for models on anthropic.com";
                break;
            case TAiPresets::EApiType::Max:
                break;
        }

        if (apiType == GetApiType()) {
            prompt << " (current)";
        }

        options.push_back({prompt, [this, apiType]() {
            ResetPresetInfo();
            BaseConfig->SetInt(Config, API_TYPE_PROPERTY, static_cast<ui64>(apiType));
        }});
    }

    return RunFtxuiMenuWithActions("Pick desired API type:", options);
}

bool TAiModelConfig::SetupApiToken() {
    const auto& currentToken = GetApiToken();
    if (!currentToken) {
        return false;
    }

    std::optional<TString> tokenProviderId;
    const auto& tokenProviders = GetAiPresets().GetTokenProviders();
    if (!tokenProviders.empty()) {
        std::vector<TMenuEntry> options;
        for (const auto& [id, tokenProvider] : tokenProviders) {
            auto prompt = TStringBuilder() << tokenProvider.Name;
            if (id == StringFromYaml(Config, TOKEN_PROVIDER_PROPERTY)) {
                prompt << "\tcurrent";
            }

            options.push_back({prompt, [&tokenProviderId, id]() {
                tokenProviderId = id;
            }});
        }

        options.push_back({
            *currentToken ? TStringBuilder() << "Replace current API token\t" << BlurSecret(*currentToken) : TStringBuilder() << "Setup custom token (will be stored in plain text)",
            []() {}
        });

        if (!RunFtxuiMenuWithActions("Please choose API token:", options)) {
            return false;
        }
    }

    if (tokenProviderId) {
        BaseConfig->SetString(Config, TOKEN_PROVIDER_PROPERTY, *tokenProviderId);
        BaseConfig->SetString(Config, TOKEN_PROPERTY, "");
        return true;
    }

    return RequestApiToken();
}

bool TAiModelConfig::SetupModelName() {
    const auto& apiEndpoint = GetEndpoint();
    Y_VALIDATE(apiEndpoint, "Can not setup model name, there is no API endpoint");

    const auto& currentToken = GetApiToken();
    if (!currentToken) {
        return false;
    }

    std::vector<TString> allowedModels;
    try {
        if (auto result = NAi::ListModelNames(apiEndpoint, *currentToken)) {
            allowedModels.swap(*result);
            std::sort(allowedModels.begin(), allowedModels.end());
        } else {
            return false;
        }
    } catch (const std::exception& e) {
        Cerr << Colors.Yellow() << "Failed to list model names, maybe model API endpoint is not correct: " << e.what() << Colors.OldColor() << Endl;
    }

    std::optional<TString> modelName;
    const auto& currentModelName = GetModelName();
    if (!allowedModels.empty()) {
        std::vector<TMenuEntry> options;
        for (const auto& model : allowedModels) {
            if (!model || model == currentModelName) {
                continue;
            }

            options.push_back({model, [&modelName, model]() {
                modelName = model;
            }});
        }

        if (!options.empty()) {
            options.push_back({
                currentModelName ? TStringBuilder() << "Edit current model name\t" << currentModelName : TStringBuilder() << "Setup custom model name",
                []() {}
            });

            if (!RunFtxuiMenuWithActions("Please choose model name:", options)) {
                return false;
            }
        }
    }

    const auto setModelName = [&](const TString& modelName) {
        ResetPresetInfo();
        BaseConfig->SetString(Config, MODEL_PROPERTY, modelName);
        return true;
    };

    if (modelName) {
        setModelName(*modelName);
        return true;
    }

    const auto& result = RunFtxuiInput("Please enter model name (use empty to disable model name passing):", currentModelName);
    if (!result) {
        return false;
    }

    setModelName(Strip(*result));
    return true;
}

bool TAiModelConfig::SetupName() {
    auto currentName = GetName();
    if (!currentName) {
        currentName = GetModelName();
    }

    TString result;
    const auto hasResult = RunFtxuiInput("Please enter profile name:", currentName, [&result](const TString& input, TString& error) {
        auto name = Strip(input);
        if (!name) {
            error = "Profile name can not be empty";
            return false;
        }

        result = name;
        return true;
    }).has_value();

    if (!hasResult) {
        return false;
    }

    BaseConfig->SetString(Config, NAME_PROPERTY, result);
    return true;
}

bool TAiModelConfig::RequestApiToken() {
    std::vector title = {
        ftxui::text("Please enter API token") | ftxui::bold,
        ftxui::text(TStringBuilder() << "Leave empty to use $" << TOKEN_ENV)
    };
    if (const auto& docs = GetAiPresets().GetMetaInfo().TokenDocs) {
        title.push_back(*docs);
    }

    const auto& result = RunFtxuiPasswordInput(ftxui::vbox(title));
    if (!result) {
        return false;
    }

    BaseConfig->SetString(Config, TOKEN_PROVIDER_PROPERTY, "");
    BaseConfig->SetString(Config, TOKEN_PROPERTY, *result);
    return true;
}

/// Config manager /////////////////////////////////////////////////////////////////////////////////////////////////////////////

TInteractiveConfigurationManager::TInteractiveConfigurationManager(const TString& configurationPath, bool readOnly)
    : ConfigurationPath(configurationPath)
    , ReadOnly(readOnly)
{
    LoadProfile();
}

TInteractiveConfigurationManager::~TInteractiveConfigurationManager() {
    Flush();
}

TString TInteractiveConfigurationManager::GetActiveAiProfileId() const {
    return StringFromYaml(Config, CURRENT_PROFILE_PROPERTY);
}

TInteractiveConfigurationManager::EMode TInteractiveConfigurationManager::GetInteractiveMode() const {
    return EnumFromYaml<EMode>(Config, INTERACTIVE_MODE_PROPERTY).value_or(EMode::YQL);
}

std::unordered_map<TString, TAiModelConfig::TPtr> TInteractiveConfigurationManager::ListAiProfiles() {
    auto aiProfilesNode = Config[AI_PROFILES_PROPERTY];
    if (!aiProfilesNode) {
        return {};
    }

    std::unordered_map<TString, TAiModelConfig::TPtr> aiProfiles;
    for (const auto& profile : aiProfilesNode) {
        const auto& id = profile.first.as<TString>("");
        if (!id) {
            YDB_CLI_LOG(Warning, "AI profile has no id, profile skipped");
            continue;
        }

        const auto& settings = profile.second;
        if (!settings.IsMap() && !settings.IsNull()) {
            YDB_CLI_LOG(Warning, "AI profile \"" << id << "\" has unexpected type " << static_cast<ui64>(settings.Type()) << " instead of map or null, profile skipped");
            continue;
        }

        auto aiProfile = std::make_shared<TAiModelConfig>(settings, shared_from_this(), id);
        if (TString error; !aiProfile->IsValid(error)) {
            YDB_CLI_LOG(Info, "AI profile \"" << id << "\" is invalid: " << error << ", profile skipped");
            continue;
        }

        if (!aiProfiles.emplace(id, std::move(aiProfile)).second) {
            YDB_CLI_LOG(Warning, "AI profile \"" << id << "\" already exists, profile skipped");
        }
    }

    return aiProfiles;
}

void TInteractiveConfigurationManager::SetInteractiveMode(EMode mode) {
    SetInt(Config, INTERACTIVE_MODE_PROPERTY, static_cast<ui64>(mode));
    Flush();
}

TAiModelConfig::TPtr TInteractiveConfigurationManager::ActivateAiProfile(const TString& id, bool printWelcomeMessage) {
    const auto& existingAiProfiles = ListAiProfiles();

    if (id) {
        const auto it = existingAiProfiles.find(id);
        if (it == existingAiProfiles.end()) {
            YDB_CLI_LOG(Info, "AI profile \"" << id << "\" not found");
            return nullptr;
        }

        TString error;
        Y_VALIDATE(it->second->IsValid(error), "AI profile was listed but is invalid: \"" << id << "\": " << error);
        ChangeActiveAiProfile(id);
        return it->second;
    }

    if (const auto it = existingAiProfiles.find(StringFromYaml(Config, CURRENT_PROFILE_PROPERTY)); it != existingAiProfiles.end()) {
        TString error;
        Y_VALIDATE(it->second->IsValid(error), "AI profile was listed but is invalid: \"" << id << "\": " << error);
        return it->second;
    }

    const auto welcomeMessagePrinter = [&](const TString& info) {
        if (WelcomeMessagePrinted) {
            return;
        }

        WelcomeMessagePrinted = true;

        if (printWelcomeMessage) {
            Cout << Endl << "Welcome to YDB CLI " << ModeToString(EMode::AI) << " interactive mode!" << Endl;
        }
        Cout << info << Endl;
    };

    if (const auto& defaultPreset = GetAiPresets().GetMetaInfo().DefaultPreset) {
        welcomeMessagePrinter(TStringBuilder() << "Using model: " << TLogger::EntityName(GetAiPresets().GetPreset(defaultPreset)->Info));
        return CreateAiProfile(defaultPreset);
    }

    welcomeMessagePrinter("Please setup your first model to continue.");
    return SelectAiProfile();
}

TAiModelConfig::TPtr TInteractiveConfigurationManager::SelectAiProfile() {
    TAiModelConfig::TPtr existingProfile;
    std::vector<TMenuEntry> options;
    std::unordered_set<TString> usedPresets;
    std::unordered_set<TString> usedNames;
    for (const auto& [id, profile] : ListAiProfiles()) {
        usedPresets.emplace(profile->GetPresetId());

        TString prompt = profile->GetName();
        usedNames.emplace(prompt);
        if (profile->GetId() == GetActiveAiProfileId()) {
            prompt += "\tactive";
        }

        options.emplace_back(prompt, [profile, &existingProfile]() {
            existingProfile = profile;
        });
    }

    TString presetId;
    for (const auto& [id, preset] : GetAiPresets().GetPresets()) {
        if (usedPresets.contains(id)) {
            continue;
        }

        TString prompt = preset.Name;
        if (usedNames.contains(prompt)) {
            prompt += " [preset]";
        }

        options.emplace_back(prompt, [id, &presetId]() {
            presetId = id;
        });
    }

    if (options.empty()) {
        return CreateAiProfile();
    }

    options.emplace_back("Setup custom model", []() {});

    if (!RunFtxuiMenuWithActions("Please choose AI model:", options)) {
        return nullptr;
    }

    if (existingProfile) {
        ChangeActiveAiProfile(existingProfile->GetId());
        return existingProfile;
    }

    return CreateAiProfile(presetId);
}

void TInteractiveConfigurationManager::RemoveAiProfile(const TString& id) {
    Config[AI_PROFILES_PROPERTY].remove(id);
    OnConfigChanged();
    Flush();
}

void TInteractiveConfigurationManager::Flush() {
    if (!ConfigChanged || ReadOnly) {
        return;
    }

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
        ConfigChanged = false;
    } catch (...) {
        YDB_CLI_LOG(Error, "Couldn't save configuration to file \"" << ConfigurationPath << "\": " << CurrentExceptionMessage());
    }
}

TString TInteractiveConfigurationManager::ModeToString(EMode mode) {
    return mode == EMode::YQL
        ? TStringBuilder() << Colors.Green() << "YQL" << Colors.OldColor()
        : TStringBuilder() << Colors.Cyan() << "AI" << Colors.OldColor();
}

void TInteractiveConfigurationManager::OnConfigChanged() {
    ConfigChanged = true;
}

void TInteractiveConfigurationManager::ChangeActiveAiProfile(const TString& id) {
    SetString(Config, CURRENT_PROFILE_PROPERTY, id);
    Flush();
}

TAiModelConfig::TPtr TInteractiveConfigurationManager::CreateAiProfile(const TString& presetId) {
    TAiModelConfig profile(YAML::Node(), shared_from_this(), "");
    if (!profile.Setup(presetId)) {
        return nullptr;
    }

    const auto& existingAiProfiles = ListAiProfiles();
    auto id = profile.GetName();
    TString suffix;
    for (ui64 i = 0; existingAiProfiles.find(id + suffix) != existingAiProfiles.end(); ++i) {
        suffix = TStringBuilder() << "_" << i;
    }
    id += suffix;

    Config[AI_PROFILES_PROPERTY][id] = profile.GetConfig();
    ChangeActiveAiProfile(id);

    return std::make_shared<TAiModelConfig>(Config[AI_PROFILES_PROPERTY][id], shared_from_this(), id);
}

void TInteractiveConfigurationManager::LoadProfile() {
    try {
        TFsPath configFilePath(ConfigurationPath);
        configFilePath.Fix();

        if (configFilePath.Exists()) {
            Config = YAML::LoadFile(configFilePath.GetPath());
        }
    } catch (...) {
        if (ReadOnly) {
            return;
        }

        YDB_CLI_LOG(Error, "Couldn't load configuration from file \"" << ConfigurationPath << "\": " << CurrentExceptionMessage());
    }

    if (auto aiProfiles = Config[AI_PROFILES_PROPERTY]; aiProfiles && !aiProfiles.IsMap()) {
        YDB_CLI_LOG(Notice, "$.ai_profiles section has unexpected type " << static_cast<ui64>(aiProfiles.Type()) << ", changed to map and cleared");
        Config[AI_PROFILES_PROPERTY] = YAML::Node();
        OnConfigChanged();
    }
}

} // namespace NYdb::NConsoleClient
