#include "sqs_workload_init_scenario.h"
#include "consts.h"

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>

#include <fmt/format.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

namespace NYdb::NConsoleClient {

    int TSqsWorkloadInitScenario::Run(TClientCommand::TConfig& config) {
        TDriver driver(TYdbCommand::CreateDriver(
            config, std::unique_ptr<TLogBackend>(
                        CreateLogBackend(
                            SQS_WORKLOAD_LOG_FILE_NAME,
                            TClientCommand::TConfig::VerbosityLevelToELogPriority(
                                config.VerbosityLevel))
                            .Release())));

        NTopic::TTopicClient client(driver);

        if (DlqQueueName.Defined()) {
            auto status =
                client
                    .AlterTopic(
                        TopicPath,
                        NTopic::TAlterTopicSettings()
                            .BeginAddConsumer(QueueName)
                            .ConsumerType(NTopic::EConsumerType::Shared)
                            .KeepMessagesOrder(KeepMessagesOrder)
                            .DefaultProcessingTimeout(
                                DefaultProcessingTimeout.Defined()
                                    ? std::make_optional(*DefaultProcessingTimeout)
                                    : std::nullopt)
                            .BeginDeadLetterPolicy()
                            .Enable()
                            .BeginCondition()
                            .MaxProcessingAttempts(MaxReceiveCount)
                            .EndCondition()
                            .MoveAction(*DlqQueueName)
                            .EndDeadLetterPolicy()
                            .EndAddConsumer())
                    .GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        } else if (MaxReceiveCount > 0) {
            auto status =
                client
                    .AlterTopic(
                        TopicPath,
                        NTopic::TAlterTopicSettings()
                            .BeginAddConsumer(QueueName)
                            .ConsumerType(NTopic::EConsumerType::Shared)
                            .KeepMessagesOrder(KeepMessagesOrder)
                            .DefaultProcessingTimeout(
                                DefaultProcessingTimeout.Defined()
                                    ? std::make_optional(*DefaultProcessingTimeout)
                                    : std::nullopt)
                            .BeginDeadLetterPolicy()
                            .Enable()
                            .BeginCondition()
                            .MaxProcessingAttempts(MaxReceiveCount)
                            .EndCondition()
                            .DeleteAction()
                            .EndDeadLetterPolicy()
                            .EndAddConsumer())
                    .GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        } else {
            auto status =
                client
                    .AlterTopic(
                        TopicPath,
                        NTopic::TAlterTopicSettings()
                            .BeginAddConsumer(QueueName)
                            .ConsumerType(NTopic::EConsumerType::Shared)
                            .DefaultProcessingTimeout(
                                DefaultProcessingTimeout.Defined()
                                    ? std::make_optional(*DefaultProcessingTimeout)
                                    : std::nullopt)
                            .KeepMessagesOrder(KeepMessagesOrder)
                            .EndAddConsumer())
                    .GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        }

        return 0;
    }

} // namespace NYdb::NConsoleClient
