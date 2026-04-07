#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <ydb/public/lib/ydb_cli/common/colors.h>

#include <yaml-cpp/node/node.h>

#include <util/generic/fwd.h>
#include <util/stream/output.h>

#include <functional>

namespace NYdb::NConsoleClient {

class TInteractiveConfigurationManager : public std::enable_shared_from_this<TInteractiveConfigurationManager> {
    inline static const NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

public:
    using TPtr = std::shared_ptr<TInteractiveConfigurationManager>;

    enum class EAiApiType {
        OpenAI,
        Anthropic,
        Invalid,
    };

    class TAiPresets {
    public:
        struct TInfo {
            ui64 OrderIdx = 0;
            TString Name;
            EAiApiType ApiType;
            TString ApiEndpoint;
            TString ModelName;
        };

        static void ClearPresets();

        static void AddPreset(const TString& id, const TInfo& info);

        static std::optional<TInfo> GetPreset(const TString& id);

        static std::vector<std::pair<TString, TInfo>> ListPresets();

        static std::unordered_map<TString, TInfo> GetOssPresets();

    private:
        inline static std::unordered_map<TString, TInfo> Presets;
    };

    class TAiProfile {
        friend class TInteractiveConfigurationManager;

    public:
        using TPtr = std::shared_ptr<TAiProfile>;

        TAiProfile(const TString& name, YAML::Node config, TInteractiveConfigurationManager::TPtr manager);

        explicit TAiProfile(TInteractiveConfigurationManager::TPtr manager);

        bool IsValid(TString& error) const;

        TString GetPresetId() const;

        const TString& GetName() const;

        void SetName(const TString& name);

        std::optional<EAiApiType> GetApiType() const;

        TString GetApiEndpoint() const;

        TString GetApiToken() const;

        TString GetModelName() const;

        bool SetupProfile(const TString& preset = "");

    private:
        bool SetupApiType(const std::optional<TAiPresets::TInfo>& presetInfo);

        bool SetupApiEndpoint(const std::optional<TAiPresets::TInfo>& presetInfo);

        bool SetupApiToken();

        bool SetupModelName(const std::optional<TAiPresets::TInfo>& presetInfo);

    private:
        TString Name;
        const TInteractiveConfigurationManager::TPtr Manager;
        YAML::Node Config;
    };

public:
    explicit TInteractiveConfigurationManager(const TString& configurationPath);

    ~TInteractiveConfigurationManager();

    void EnsurePredefinedProfiles(const std::vector<TAiPresetConfig>& profiles, std::function<TAiTokenConfig()> tokenGetter = {});

    enum class EMode {
        YQL,
        AI,
        Invalid,
    };

    EMode GetDefaultMode() const;

    void ChangeDefaultMode(EMode mode);

    static TString ModeToString(EMode mode);

    TString GetActiveAiProfileName() const;

    TAiProfile::TPtr GetAiProfile(const TString& name);

    std::unordered_map<TString, TAiProfile::TPtr> ListAiProfiles();

    TAiProfile::TPtr SelectAiModelProfile();

    void ChangeActiveAiProfile(const TString& name);

private:
    TAiProfile::TPtr CreateNewAiModelProfile(const TString& preset = "");

    TString CreateAiProfileName(const TString& currentName, const TString& defaultName);

    void LoadProfile();

    void CanonizeStructure();

    void SaveConfig();

private:
    const TString ConfigurationPath;
    YAML::Node Config;
    bool ConfigChanged = false;
};

} // namespace NYdb::NConsoleClient
