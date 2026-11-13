#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/format.h>

#include <util/generic/set.h>

namespace NYdb::NConsoleClient::NCluster {

class TCommandCluster : public TClientCommandTree {
public:
    TCommandCluster();
};

class TCommandClusterBootstrap : public TYdbCommand {
    TString SelfAssemblyUUID;
    bool AllowUnknownFields = false;

public:
    TCommandClusterBootstrap();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandClusterDump : public TYdbReadOnlyCommand {
public:
    TCommandClusterDump();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString FilePath;
};

class TCommandClusterRestore : public TYdbCommand {
public:
    TCommandClusterRestore();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString FilePath;
    TDuration WaitNodesDuration;
};

class TCommandClusterReassignTablets : public TYdbCommand, public TCommandWithOutput {
public:
    TCommandClusterReassignTablets();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString StoragePool;
    ui32 GroupId = 0;
    ui32 MaxInFlight = 1;
    ui32 Percent = 100;
    bool OnRestart = false;
};

} // namespace NYdb::NConsoleClient::NCluster
