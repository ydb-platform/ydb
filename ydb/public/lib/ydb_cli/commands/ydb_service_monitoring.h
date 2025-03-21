#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/monitoring/monitoring.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandMonitoring : public TClientCommandTree {
public:
    TCommandMonitoring();
};

class TCommandSelfCheck : public TYdbSimpleCommand, public TCommandWithOutput {
public:
    TCommandSelfCheck();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    int PrintResponse(NMonitoring::TSelfCheckResult& result);

    bool Verbose = false;
};

}
}
