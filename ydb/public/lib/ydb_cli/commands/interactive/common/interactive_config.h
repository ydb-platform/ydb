#pragma once

#include "interactive_log.h"

#include <util/generic/fwd.h>

#include <yaml-cpp/node/node.h>

namespace NYdb::NConsoleClient {

class TInteractiveConfigurationManager : public std::enable_shared_from_this<TInteractiveConfigurationManager> {
    inline static const NColorizer::TColors Colors = NColorizer::AutoColors(Cout);

public:
    using TPtr = std::shared_ptr<TInteractiveConfigurationManager>;

    class TAiProfile {
    public:
        using TPtr = std::shared_ptr<TAiProfile>;

        enum class EApiType {
            OpenAI,
            Anthropic,
            Invalid,
        };

        TAiProfile(const TString& name, YAML::Node config, TInteractiveConfigurationManager::TPtr manager, const TInteractiveLogger& log);

        bool IsValid(TString& error) const;

        const TString& GetName() const;

        std::optional<EApiType> GetApiType() const;

        TString GetApiEndpoint() const;

        TString GetApiToken() const;

        TString GetModelName() const;

        bool SetupProfile();

    private:
        bool SetupApiType();

        bool SetupApiEndpoint();

        bool SetupApiToken();

        bool SetupModelName();

    private:
        const TString Name;
        const TInteractiveConfigurationManager::TPtr Manager;
        const TInteractiveLogger Log;
        YAML::Node Config;
    };

public:
    TInteractiveConfigurationManager(const TString& configurationPath, const TInteractiveLogger& log);

    ~TInteractiveConfigurationManager();

    enum class EMode {
        YQL,
        AI,
        Invalid,
    };

    EMode GetDefaultMode() const;

    TString GetActiveAiProfileName() const;

    TAiProfile::TPtr GetAiProfile(const TString& name);

    std::unordered_map<TString, TAiProfile::TPtr> ListAiProfiles();

    TAiProfile::TPtr InitAiModelProfile();

    TAiProfile::TPtr CreateNewAiModelProfile();

    void RemoveAiModelProfile(const TString& name);

    void ChangeDefaultMode(EMode mode);

    void ChangeActiveAiProfile(const TString& name);

private:
    void LoadProfile();

    void CanonizeStructure();

    void SaveConfig();

private:
    const TInteractiveLogger Log;
    const TString ConfigurationPath;
    YAML::Node Config;
    bool ConfigChanged = false;
};

} // namespace NYdb::NConsoleClient
