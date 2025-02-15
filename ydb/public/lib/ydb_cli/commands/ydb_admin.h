#pragma once

#include "ydb_command.h"

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

}
}
