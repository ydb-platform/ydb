#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

class TCommandTestShard : public TClientCommandTree {
public:
    TCommandTestShard();
};

class TCommandTestShardCreate : public TYdbCommand {
public:
    TCommandTestShardCreate();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    uint64_t OwnerIdx = 0;
    TVector<TString> Channels;
    uint32_t Count = 1;
    TString ConfigFile;
    TString ConfigYaml;
};

class TCommandTestShardDelete : public TYdbCommand {
public:
    TCommandTestShardDelete();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    uint64_t OwnerIdx = 0;
    uint32_t Count = 1;
};

} // namespace NYdb::NConsoleClient
