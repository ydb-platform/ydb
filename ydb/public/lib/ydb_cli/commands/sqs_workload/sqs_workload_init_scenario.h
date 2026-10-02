#pragma once

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TSqsWorkloadInitScenario: public TSqsWorkloadScenario {
    private:
        void AddCreateTopicSettings(NTopic::TCreateTopicSettings& createTopicSettings);
        void AddCreateConsumerSettings(NTopic::TCreateTopicSettings& createTopicSettings);

    public:
        int Run(TClientCommand::TConfig&);

        TString TopicPath;
        TString Consumer;
        bool KeepMessagesOrder;
        TMaybe<TDuration> DefaultProcessingTimeout;
        TMaybe<TString> DlqQueueName;
        ui32 MaxReceiveCount;
        ui32 TopicPartitionCount;
    };

} // namespace NYdb::NConsoleClient
