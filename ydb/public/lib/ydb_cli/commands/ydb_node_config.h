#pragma once

#include "ydb_command.h"

namespace NYdb::NConsoleClient::NNodeConfig {

class TCommandNodeConfig : public TClientCommandTree {
public:
    TCommandNodeConfig();
};

class TCommandNodeConfigInit : public TYdbCommand {
public:
    TCommandNodeConfigInit();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
    bool SaveConfig(const TString& config, const TString& configName, const TString& configDirPath);
private:
    TString ConfigYamlPath;
    TString ConfigDirPath;
    TString SeedNodeEndpoint;
};

} // namespace NYdb::NConsoleClient::NNodeConfig
