#include "sqs_workload_clean_scenario.h"
#include "consts.h"

#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>

namespace NYdb::NConsoleClient {

    int TSqsWorkloadCleanScenario::Run(TClientCommand::TConfig& config) {
        auto driver = std::make_unique<NYdb::TDriver>(TYdbCommand::CreateDriver(
            config, std::unique_ptr<TLogBackend>(
                        CreateLogBackend(
                            SQS_WORKLOAD_LOG_FILE_NAME,
                            TClientCommand::TConfig::VerbosityLevelToELogPriority(
                                config.VerbosityLevel))
                            .Release())));

        NTopic::TTopicClient client(*driver);
        NTopic::TAlterTopicSettings settings;

        auto status =
            client.AlterTopic(TopicPath, settings.AppendDropConsumers(QueueName))
                .GetValueSync();

        NStatusHelpers::ThrowOnError(status);

        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
