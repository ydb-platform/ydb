#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>

namespace NYdb::NConsoleClient {

class TSqsWorkloadInitScenario : public TSqsWorkloadScenario {
public:
    int Run(const TClientCommand::TConfig&);

    TString QueueName;
    bool Fifo;
    bool DeduplicationOn;
    TMaybe<TString> DlqQueueName;
    ui32 MaxReceiveCount;

};

} // namespace NYdb::NConsoleClient