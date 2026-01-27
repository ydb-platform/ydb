#pragma once

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TSqsWorkloadReadScenario: public TSqsWorkloadScenario {
    public:
        int Run(const TClientCommand::TConfig&);

        TMaybe<ui32> ErrorMessagesRate;
        TString ErrorMessagesDestiny;
        ui64 VisibilityTimeoutMs;
        ui64 HandleMessageDelayMs;

    private:
        int RunScenario();
    };

} // namespace NYdb::NConsoleClient
