#pragma once

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TSqsWorkloadWriteScenario: public TSqsWorkloadScenario {
    public:
        int Run(const TClientCommand::TConfig&);

    private:
        int RunScenario();
    };

} // namespace NYdb::NConsoleClient
