#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <util/generic/set.h>

namespace NYdb::NConsoleClient::NStorageConfig {

class TCommandStorageConfig : public TClientCommandTree {
public:
    TCommandStorageConfig();
};

class TCommandStorageConfigReplace : public TYdbCommand {
public:
    TCommandStorageConfigReplace();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString StorageConfig;
    TString Filename;
};

class TCommandStorageConfigFetch : public TYdbCommand {
public:
    TCommandStorageConfigFetch();
    void Config(TConfig&) override;
    void Parse(TConfig&) override;
    int Run(TConfig& config) override;
};
}
