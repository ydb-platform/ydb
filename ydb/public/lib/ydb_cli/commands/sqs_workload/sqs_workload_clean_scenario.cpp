#include "sqs_workload_clean_scenario.h"

#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>

namespace NYdb::NConsoleClient {

    int TSqsWorkloadCleanScenario::Run(TClientCommand::TConfig& config) {
        auto driver = std::make_unique<NYdb::TDriver>(TYdbCommand::CreateDriver(
            config, std::unique_ptr<TLogBackend>(
                        CreateLogBackend(
                            "cerr",
                            TClientCommand::TConfig::VerbosityLevelToELogPriority(
                                config.VerbosityLevel))
                            .Release())));

        NTopic::TTopicClient client(*driver);
        NTopic::TAlterTopicSettings settings;

        auto status = client.DropTopic(TopicPath).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(status);

        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
