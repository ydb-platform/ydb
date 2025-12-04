#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>

namespace NYdb::NConsoleClient {

class TSqsWorkloadCleanScenario : public TSqsWorkloadScenario {
public:
    int Run(TClientCommand::TConfig&);

    TString QueueName;
};

} // namespace NYdb::NConsoleClient

