#include "sqs_workload_init_scenario.h"

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
                            "cerr",
                            TClientCommand::TConfig::VerbosityLevelToELogPriority(
                                config.VerbosityLevel))
                            .Release())));

        NTopic::TTopicClient client(driver);

        NTopic::TCreateTopicSettings createQueueSettings;
        AddCreateTopicSettings(createQueueSettings);
        AddCreateConsumerSettings(createQueueSettings);

        auto status = client.CreateTopic(TopicPath, createQueueSettings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(status);

        return EXIT_SUCCESS;
    }

    void TSqsWorkloadInitScenario::AddCreateTopicSettings(NTopic::TCreateTopicSettings& createTopicSettings) {
        createTopicSettings.BeginConfigurePartitioningSettings()
            .MinActivePartitions(TopicPartitionCount)
            .EndConfigurePartitioningSettings();
    }

    void TSqsWorkloadInitScenario::AddCreateConsumerSettings(NTopic::TCreateTopicSettings& createTopicSettings) {
        auto& createConsumerSettings = createTopicSettings
            .BeginAddSharedConsumer(Consumer)
            .KeepMessagesOrder(KeepMessagesOrder)
            .DefaultProcessingTimeout(
                DefaultProcessingTimeout.Defined()
                    ? std::make_optional(*DefaultProcessingTimeout)
                    : std::nullopt);

        if (MaxReceiveCount > 0) {
            auto& dlqSettings = createConsumerSettings.BeginDeadLetterPolicy()
                .Enable()
                .BeginCondition()
                .MaxProcessingAttempts(MaxReceiveCount)
                .EndCondition();

            if (DlqQueueName.Defined()) {
                dlqSettings.MoveAction(*DlqQueueName);
            } else {
                dlqSettings.DeleteAction();
            }

            dlqSettings.EndDeadLetterPolicy();
        }

        createConsumerSettings.EndAddConsumer();
    }

} // namespace NYdb::NConsoleClient
