#pragma once

#include "ydb_command.h"
#include "ydb_common.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <util/generic/string.h>
#include <vector>

namespace NYdb::NConsoleClient {

class TCommandBridge : public TClientCommandTree {
public:
    TCommandBridge(bool allowEmptyDatabase);
};

class TCommandBridgeUpdate : public TYdbCommand {
public:
    TCommandBridgeUpdate(bool allowEmptyDatabase);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool AllowEmptyDatabase = false;

    TVector<TString> PileStateUpdates;
    TString FilePath;

    std::vector<NYdb::NBridge::TPileStateUpdate> Updates;
    std::vector<TString> QuorumPiles;
};

class TCommandBridgeList : public TYdbReadOnlyCommand, public TCommandWithOutput {
public:
    TCommandBridgeList(bool allowEmptyDatabase);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool AllowEmptyDatabase;
    bool Detailed = false;
};

class TCommandBridgeSwitchover : public TYdbCommand {
public:
    TCommandBridgeSwitchover(bool allowEmptyDatabase);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool AllowEmptyDatabase = false;
    TString NewPrimaryPile;
};

class TCommandBridgeFailover : public TYdbCommand {
public:
    TCommandBridgeFailover(bool allowEmptyDatabase);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool AllowEmptyDatabase = false;
    TString DownPile;
    TString NewPrimaryPile;
};


class TCommandBridgeTakedown : public TYdbCommand {
public:
    TCommandBridgeTakedown(bool allowEmptyDatabase);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool AllowEmptyDatabase = false;
    TString DownPile;
    TString NewPrimaryPile;
};

class TCommandBridgeRejoin : public TYdbCommand {
public:
    TCommandBridgeRejoin(bool allowEmptyDatabase);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool AllowEmptyDatabase = false;
    TString Pile;
};

}
