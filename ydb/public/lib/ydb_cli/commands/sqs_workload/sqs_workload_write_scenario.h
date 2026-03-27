#pragma once

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TSqsWorkloadWriteScenario: public TSqsWorkloadScenario {
    public:
        int Run(const TClientCommand::TConfig& config);

    private:
        int RunScenario(const TClientCommand::TConfig& config);
    };

} // namespace NYdb::NConsoleClient
