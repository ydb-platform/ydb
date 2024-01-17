#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <util/generic/set.h>

namespace NYdb::NConsoleClient::NDynamicConfig {

class TCommandConfig : public TClientCommandTree {
public:
    TCommandConfig();
};

class TCommandConfigReplace : public TYdbCommand {
public:
    TCommandConfigReplace();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool IgnoreCheck = false;
    bool Force = false;
    bool DryRun = false;
    bool AllowUnknownFields = false;
    TString DynamicConfig;
    TString Filename;
};

class TCommandConfigFetch : public TYdbCommand {
public:
    TCommandConfigFetch();
    void Config(TConfig&) override;
    void Parse(TConfig&) override;
    int Run(TConfig& config) override;

private:
    bool All = false;
    bool StripMetadata = false;
    TString OutDir;
};

class TCommandConfigResolve : public TYdbCommand {
public:
    TCommandConfigResolve();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TMap<TString, TString> Labels;
    bool All = false;
    TString Filename;
    TString Dir;
    TString OutDir;
    bool FromCluster = false;
    bool RemoteResolve = false;
    bool SkipVolatile = false;
    ui64 NodeId;
};

class TCommandVolatileConfig : public TClientCommandTree {
public:
    TCommandVolatileConfig();
};

class TCommandConfigVolatileAdd : public TYdbCommand {
public:
    TCommandConfigVolatileAdd();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool IgnoreCheck = false;
    bool DryRun = false;
    TString Filename;
};

class TCommandConfigVolatileDrop : public TYdbCommand {
public:
    TCommandConfigVolatileDrop();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    ui64 Version;
    TString Cluster;
    THashSet<ui64> Ids;
    TString Dir;
    TString Filename;
    bool All = false;
    bool Force = false;
};

class TCommandConfigVolatileFetch : public TYdbCommand {
public:
    TCommandConfigVolatileFetch();
    void Config(TConfig&) override;
    void Parse(TConfig&) override;
    int Run(TConfig& config) override;

private:
    THashSet<ui64> Ids;
    bool All = false;
    TString OutDir;
    bool StripMetadata = false;
};

} // namespace NYdb::NConsoleClient::NDynamicConfig
