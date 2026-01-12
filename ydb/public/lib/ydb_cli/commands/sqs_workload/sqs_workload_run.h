#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb::NConsoleClient {

    class TCommandWorkloadSqsRun: public TClientCommandTree {
    public:
        TCommandWorkloadSqsRun();
    };

} // namespace NYdb::NConsoleClient
