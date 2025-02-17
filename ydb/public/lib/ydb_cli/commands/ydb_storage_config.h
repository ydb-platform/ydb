#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <util/generic/set.h>

namespace NYdb::NConsoleClient::NStorageConfig {

class TCommandStorageConfig : public TClientCommandTree {
public:
    TCommandStorageConfig(std::optional<bool> overrideOnlyExplicitProfile = std::nullopt);
    void PropagateFlags(const TCommandFlags& flags) override;
private:
    std::optional<bool> OverrideOnlyExplicitProfile;
};

class TCommandStorageConfigReplace : public TYdbCommand {
public:
    TCommandStorageConfigReplace();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Filename;
    TString ClusterYamlPath;
    TString StorageYamlPath;
    bool EnableDedicatedStorageSection = false;
    bool DisableDedicatedStorageSection = false;

    std::optional<TString> ClusterYaml;
    std::optional<TString> StorageYaml;
    std::optional<bool> SwitchDedicatedStorageSection;
    bool DedicatedConfigMode = false;
};

class TCommandStorageConfigFetch : public TYdbCommand {
public:
    TCommandStorageConfigFetch();
    void Config(TConfig&) override;
    void Parse(TConfig&) override;
    int Run(TConfig& config) override;

public:
    bool DedicatedStorageSection = false;
    bool DedicatedClusterSection = false;
};
}
