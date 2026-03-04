#pragma once

#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <yaml-cpp/node/node.h>

#include <util/generic/fwd.h>
#include <util/stream/output.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TAiPresets {
public:
    // Token presets
    class ITokenProvider {
    public:
        virtual ~ITokenProvider() = default;

        // Returns std::nullopt if interrupted
        virtual std::optional<TString> GetToken() = 0;
    };

    struct TTokenProvider {
        ui64 OrderIdx = 0;
        TString Name;
        std::shared_ptr<ITokenProvider> Provider;

        std::optional<TString> GetToken() const;

        TString ToString() const;
    };

    using TTokenProviders = std::vector<std::pair<TString, TTokenProvider>>;

    // Endpoint presets
    enum class EApiType {
        OpenAI,
        Anthropic,
        Max, // Last value, do not use
    };

    struct TEndpoint {
        // This endpoint will be suggested
        TString ApiEndpoint;
        TString Info;

        // Information that inferred from endpoint
        std::optional<EApiType> ApiType;
        std::optional<TString> ModelName;
        std::optional<TString> TokenProvider;

        TEndpoint AppendApiPath(const TString& apiPath) const;

        TEndpoint SetInfo(const TString& info) const;

        TEndpoint SetApiType(EApiType apiType) const;

        TEndpoint SetModelName(const TString& modelName) const;

        TString ToString() const;
    };

    // Full presets
    struct TPreset {
        ui64 OrderIdx = 0;
        TString Name;
        TEndpoint Info;

        TString ToString() const;
    };

    // Common information
    struct TMetaInfo {
        TString DefaultPreset;
        TString ModelsDocs;
        TString EndpointDocs;
        TString TokenDocs;
    };

    using TPresets = std::vector<std::pair<TString, TPreset>>;

    TAiPresets() = default;

    TAiPresets(
        std::unordered_map<TString, TTokenProvider>&& tokenProviders,
        std::vector<TEndpoint>&& endpoints,
        std::unordered_map<TString, TPreset>&& presets,
        TMetaInfo&& metaInfo
    );

    std::optional<TTokenProvider> GetTokenProvider(const TString& id) const;

    const TTokenProviders& GetTokenProviders();

    const std::vector<TEndpoint>& GetEndpoints() const;

    std::optional<TEndpoint> GetPreset(const TString& id) const;

    const TPresets& GetPresets();

    const TMetaInfo& GetMetaInfo() const;

protected:
    std::unordered_map<TString, TTokenProvider> TokenProviders; // Provider id -> Provider info
    std::vector<TEndpoint> Endpoints;
    std::unordered_map<TString, TPreset> Presets; // Preset id -> Preset info
    TMetaInfo MetaInfo;

private:
    std::optional<TTokenProviders> TokenProvidersOrdered;
    std::optional<TPresets> TPresetsOrdered;
};

class TAiPresetsBuilder final : private TAiPresets {
public:
    void AddToken(const TString& id, const TString& name, std::shared_ptr<ITokenProvider> provider);

    TEndpoint AddEndpoint(const TEndpoint& endpoint);

    void AddPreset(const TString& id, const TEndpoint& info);

    void SetMetaInfo(const TMetaInfo& metaInfo);

    TAiPresets Done();
};

void SetupPresetsInitializer(std::function<TAiPresets()> presetsInit);

TAiPresets& GetAiPresets();

// AI model config:
// name: openai [required]
// endpoint: https://api.openai.com/v1 [required]
// api_type: 1 [required]
// model: gpt-3.5
// token: XXX
// token_provider: XXX
// preset_id: gpt-preset-v1

class TYamlConfigBase {
public:
    using TPtr = std::shared_ptr<TYamlConfigBase>;

    virtual ~TYamlConfigBase() = default;

    void SetString(YAML::Node& config, const TString& key, const TString& value);

    void SetInt(YAML::Node& config, const TString& key, ui64 value);

protected:
    virtual void OnConfigChanged() = 0;
};

class TAiModelConfig {
    inline static const NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

    static constexpr char NAME_PROPERTY[] = "name";
    static constexpr char ENDPOINT_PROPERTY[] = "endpoint";
    static constexpr char API_TYPE_PROPERTY[] = "api_type";
    static constexpr char TOKEN_PROPERTY[] = "token";
    static constexpr char TOKEN_PROVIDER_PROPERTY[] = "token_provider";
    static constexpr char MODEL_PROPERTY[] = "model";
    static constexpr char PRESET_ID_PROPERTY[] = "preset_id";

    static constexpr char TOKEN_ENV[] = "YDB_CLI_AI_TOKEN";

public:
    using TPtr = std::shared_ptr<TAiModelConfig>;

    TAiModelConfig(YAML::Node config, TYamlConfigBase::TPtr baseConfig, const TString& id);

    bool IsValid(TString& error) const;

    YAML::Node GetConfig() const;

    TString GetId() const;

    TString GetName() const;

    TString GetEndpoint() const;

    std::optional<TAiPresets::EApiType> GetApiType() const;

    std::optional<TString> GetApiToken(bool allowEnv = true) const; // std::nullopt if request interrupted, empty token <=> do not pass token header

    TString GetModelName() const;

    TString GetPresetId() const;

    bool Setup(const TString& presetId); // Empty preset id for custom setup

    bool Edit();

private:
    void FillFromPreset(const TAiPresets::TEndpoint& info);

    bool SetupEndpoint();

    bool SetupApiType();

    bool SetupApiToken();

    bool SetupModelName();

    bool SetupName();

    const TYamlConfigBase::TPtr BaseConfig;
    const TString Id;
    YAML::Node Config;
};

// Interactive config:
// current_profile: openai
// interactive_mode: 1
// ai_profiles:
// - ... AI model config ...

class TInteractiveConfigurationManager final : public std::enable_shared_from_this<TInteractiveConfigurationManager>, public TYamlConfigBase {
    inline static const NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

    static constexpr char CURRENT_PROFILE_PROPERTY[] = "current_profile";
    static constexpr char INTERACTIVE_MODE_PROPERTY[] = "interactive_mode";
    static constexpr char AI_PROFILES_PROPERTY[] = "ai_profiles";

public:
    using TPtr = std::shared_ptr<TInteractiveConfigurationManager>;

    enum class EMode {
        YQL,
        AI,
        Max, // Last value, do not use
    };

    explicit TInteractiveConfigurationManager(const TString& configurationPath);

    ~TInteractiveConfigurationManager();

    TString GetActiveAiProfileId() const;

    EMode GetInteractiveMode() const;

    std::unordered_map<TString, TAiModelConfig::TPtr> ListAiProfiles();

    void SetInteractiveMode(EMode mode);

    TAiModelConfig::TPtr ActivateAiProfile(const TString& id = ""); // Empty for default profile

    TAiModelConfig::TPtr SelectAiProfile();

    void RemoveAiProfile(const TString& id);

    void Flush();

    static TString ModeToString(EMode mode);

protected:
    void OnConfigChanged() final;

private:
    void ChangeActiveAiProfile(const TString& id);

    TAiModelConfig::TPtr CreateAiProfile(const TString& presetId = ""); // Empty for empty preset

    void LoadProfile();

    const TString ConfigurationPath;
    YAML::Node Config;
    bool ConfigChanged = false;
    bool WelcomeMessagePrinted = false;
};

} // namespace NYdb::NConsoleClient
