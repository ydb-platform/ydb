#pragma once

#include "ydb_command.h"
#include "ydb_common.h"
#include "ydb_root_common.h"

#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandBenchmark : public TYdbOperationCommand, public TInterruptibleCommand
{
public:
    TCommandBenchmark();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int RunCommand(TConfig& config, const TString &script);
    bool PrintResponse(NScripting::TYqlResultPartIterator& result);

    TString Query;
    size_t Threads = 1;
    size_t IntervalSeconds = 1;
};

}
}
