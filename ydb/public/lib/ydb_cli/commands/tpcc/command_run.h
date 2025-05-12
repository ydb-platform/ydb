#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb::NConsoleClient {

class TCommandTPCCRun
    : public TYdbCommand
{
public:
    TCommandTPCCRun();
    ~TCommandTPCCRun();

    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int WarehouseCount;
    int WarmupSeconds;
    int RunSeconds;
};

} // namespace NYdb::NConsoleClient
