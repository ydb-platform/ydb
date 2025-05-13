#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <util/generic/set.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

namespace NYdb::NConsoleClient::NDynamicConfig {

struct TCommandFlagsOverrides {
    std::optional<bool> Dangerous;
    std::optional<bool> OnlyExplicitProfile;
};

class TCommandConfig : public TClientCommandTree {
public:
    TCommandConfig(
        bool useLegacyApi,
        TCommandFlagsOverrides commandFlagsOverrides = {},
        bool allowEmptyDatabase = false);

    TCommandConfig(
        bool useLegacyApi,
        bool allowEmptyDatabase);

    void PropagateFlags(const TCommandFlags& flags) override;
private:
    TCommandFlagsOverrides CommandFlagsOverrides;
};

class TCommandConfigReplace : public TYdbCommand {
public:
    TCommandConfigReplace(
        bool useLegacyApi,
        bool allowEmptyDatabase);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool UseLegacyApi = false;
    bool IgnoreCheck = false;
    bool Force = false;
    bool DryRun = false;
    bool AllowUnknownFields = false;
    TString DynamicConfig;
    TString Filename;
    bool AllowEmptyDatabase = false;
};

class TCommandConfigFetch : public TYdbReadOnlyCommand {
public:
    TCommandConfigFetch(
        bool useLegacyApi,
        bool allowEmptyDatabase);
    void Config(TConfig&) override;
    void Parse(TConfig&) override;
    int Run(TConfig& config) override;

private:
    bool UseLegacyApi = false;
    bool StripMetadata = false;
    TString OutDir;
    bool AllowEmptyDatabase = false;
    bool DedicatedStorageSection = false;
    bool DedicatedClusterSection = false;
    bool FetchInternalState = false;
    bool FetchExplicitSections = false;
};

class TCommandConfigResolve : public TYdbReadOnlyCommand {
public:
    TCommandConfigResolve();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    std::map<std::string, std::string> Labels;
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
    ui64 Version = 0;
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

class TCommandGenerateDynamicConfig : public TYdbReadOnlyCommand {
public:
    TCommandGenerateDynamicConfig(bool allowEmptyDatabase);
    void Config(TConfig&) override;
    int Run(TConfig&) override;
private:
    bool AllowEmptyDatabase = false;
};

class TCommandVersionDynamicConfig : public TYdbReadOnlyCommand, public TCommandWithOutput {
public:
    TCommandVersionDynamicConfig(bool allowEmptyDatabase);
    void Config(TConfig&) override;
    void Parse(TConfig&) override;
    int Run(TConfig&) override;
private:
    bool ListNodes = false;
    bool AllowEmptyDatabase = false;
};

} // namespace NYdb::NConsoleClient::NDynamicConfig
