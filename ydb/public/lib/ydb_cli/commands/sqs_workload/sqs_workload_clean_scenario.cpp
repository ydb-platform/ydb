#include "sqs_workload_clean_scenario.h"

#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>

namespace NYdb::NConsoleClient {

int TSqsWorkloadCleanScenario::Run(const TClientCommand::TConfig&) {
    InitSqsClient();

    Aws::SQS::Model::GetQueueUrlRequest getQueueUrlRequest;
    getQueueUrlRequest.SetQueueName(QueueName.c_str());
    
    auto getQueueUrlOutcome = SqsClient->GetQueueUrl(getQueueUrlRequest);
    if (!getQueueUrlOutcome.IsSuccess()) {
        Log->Write(
            ELogPriority::TLOG_ERR,
            TStringBuilder() << "Error getting queue URL: "
                             << getQueueUrlOutcome.GetError().GetMessage()
        );
        ErrorFlag->store(true);
    } else {
        Aws::String queueUrl = getQueueUrlOutcome.GetResult().GetQueueUrl();
        
        Aws::SQS::Model::DeleteQueueRequest deleteQueueRequest;
        deleteQueueRequest.SetQueueUrl(queueUrl);
        
        auto deleteQueueOutcome = SqsClient->DeleteQueue(deleteQueueRequest);
        if (!deleteQueueOutcome.IsSuccess()) {
            Log->Write(
                ELogPriority::TLOG_ERR,
                TStringBuilder() << "Error deleting queue: "
                                 << deleteQueueOutcome.GetError().GetMessage()
            );
            ErrorFlag->store(true);
        } else {
            Log->Write(
                ELogPriority::TLOG_INFO,
                TStringBuilder() << "Queue deleted successfully: " << QueueName
            );
        }
    }

    DestroySqsClient();

    if (AnyErrors()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}  // namespace NYdb::NConsoleClient

