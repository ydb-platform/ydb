#pragma once

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TSqsWorkloadInitScenario: public TSqsWorkloadScenario {
    public:
        int Run(TClientCommand::TConfig&);

        TString TopicPath;
        TString QueueName;
        bool KeepMessagesOrder;
        bool DeduplicationOn; // ?
        TMaybe<TDuration> DefaultProcessingTimeout;
        TMaybe<TString> DlqQueueName;
        ui32 MaxReceiveCount;
    };

} // namespace NYdb::NConsoleClient
