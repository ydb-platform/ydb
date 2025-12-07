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

        TAiProfile(const TString& name, YAML::Node config, TInteractiveConfigurationManager::TPtr manager);

        const TString& GetName() const;

    private:
        const TString Name;
        const TInteractiveConfigurationManager::TPtr Manager;
        YAML::Node Config;
    };

public:
    TInteractiveConfigurationManager(const TString& configurationPath, const TInteractiveLogger& log);

    ~TInteractiveConfigurationManager();

    enum class EMode {
        YQL,
        AI,
    };

    EMode GetDefaultMode() const;

    TAiProfile::TPtr InitAiModelProfile();

private:
    void ChangeDefaultMode(EMode mode);

    void ChangeActiveProfile(const TString& name);

    TAiProfile::TPtr InitNewProfile(const TString& name);

    TString GetActiveAiProfileName() const;

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
