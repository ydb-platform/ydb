#pragma once

#include "ydb_command.h"
#include <ydb/public/lib/ydb_cli/common/format.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandAdmin : public TClientCommandTree {
public:
    TCommandAdmin();
protected:
    virtual void Config(TConfig& config) override;
};

class TCommandDatabaseDump : public TYdbReadOnlyCommand {
public:
    TCommandDatabaseDump();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString FilePath;
};

class TCommandDatabaseRestore : public TYdbCommand {
public:
    TCommandDatabaseRestore();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString FilePath;
    TDuration WaitNodesDuration;
};

class TCommandDatabaseReassignTablets : public TYdbCommand, public TCommandWithOutput {
public:
    TCommandDatabaseReassignTablets();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString StoragePool;
    ui32 Group = 0;
    ui32 MaxInFlight = 1;
    ui32 Percent = 100;
    bool Async = false;
};

}
}
