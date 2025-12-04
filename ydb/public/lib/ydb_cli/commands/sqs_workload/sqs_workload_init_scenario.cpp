#include "sqs_workload_init_scenario.h"

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>

#include <fmt/format.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

namespace NYdb::NConsoleClient {

int TSqsWorkloadInitScenario::Run(TClientCommand::TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(
        TYdbCommand::CreateDriver(
            config,
            std::unique_ptr<TLogBackend>(CreateLogBackend("cerr",
                TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)
            ).Release())
        )
    );

    NTopic::TTopicClient client(*driver);

    NTopic::TAlterTopicSettings settings;
    settings.BeginAddConsumer().
        ConsumerType(NTopic::EConsumerType::Shared).
        KeepMessagesOrder(KeepMessagesOrder).
        DefaultProcessingTimeout(TDuration::Seconds(DefaultProcessingTimeout)).
        DeadLetterPolicy(NTopic::TDeadLetterPolicySettings().Action(NTopic::EDeadLetterAction::Move).
        DeadLetterQueue(DlqQueueName.Defined() ? std::make_optional(*DlqQueueName) : std::nullopt)).
        EndAddConsumer();

    auto status = client.AlterTopic(TopicPath, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(status);

    return EXIT_SUCCESS;
}

}  // namespace NYdb::NConsoleClient
