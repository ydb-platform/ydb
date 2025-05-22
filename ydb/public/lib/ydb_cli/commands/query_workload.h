#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandQueryWorkload : public TClientCommandTree {
public:
    TCommandQueryWorkload();
};

class TCommandQueryWorkloadRun : public TYdbSimpleCommand, public TInterruptibleCommand
{
public:
    TCommandQueryWorkloadRun();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    virtual void Parse(TConfig& config) override;

    TString Query;
    TString QueryFile;
    TString PlanFileName;
    size_t Threads = 1;
    size_t IntervalSeconds = 1;
};

}
}
