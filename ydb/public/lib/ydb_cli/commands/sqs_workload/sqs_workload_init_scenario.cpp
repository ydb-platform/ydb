#include "sqs_workload_init_scenario.h"

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <fmt/format.h>

namespace NYdb::NConsoleClient {

int TSqsWorkloadInitScenario::Run(const TClientCommand::TConfig&) {
    InitSqsClient();

    Aws::SQS::Model::CreateQueueRequest createQueueRequest;
    createQueueRequest.SetQueueName(QueueName.c_str());
    
    Aws::Map<Aws::SQS::Model::QueueAttributeName, Aws::String> attributes;
    if (Fifo) {
        attributes[Aws::SQS::Model::QueueAttributeName::FifoQueue] = "true";

        if (DeduplicationOn) {
            attributes[Aws::SQS::Model::QueueAttributeName::ContentBasedDeduplication] = "true";
        }
    }
    
    if (DlqQueueName) {
        Aws::SQS::Model::GetQueueUrlRequest getQueueUrlRequest;
        getQueueUrlRequest.SetQueueName((*DlqQueueName).c_str());
        
        auto getQueueUrlOutcome = SqsClient->GetQueueUrl(getQueueUrlRequest);
        if (!getQueueUrlOutcome.IsSuccess()) {
            Log->Write(
                ELogPriority::TLOG_ERR,
                TStringBuilder() << "Error getting DLQ queue URL: "
                                 << getQueueUrlOutcome.GetError().GetMessage()
            );
            ErrorFlag->store(true);
        } else {
            Aws::SQS::Model::GetQueueAttributesRequest getQueueAttributesRequest;
            getQueueAttributesRequest.SetQueueUrl(getQueueUrlOutcome.GetResult().GetQueueUrl());
            getQueueAttributesRequest.AddAttributeNames(Aws::SQS::Model::QueueAttributeName::QueueArn);
            
            auto getQueueAttributesOutcome = SqsClient->GetQueueAttributes(getQueueAttributesRequest);
            if (!getQueueAttributesOutcome.IsSuccess()) {
                Log->Write(
                    ELogPriority::TLOG_ERR,
                    TStringBuilder() << "Error getting DLQ queue ARN: "
                                     << getQueueAttributesOutcome.GetError().GetMessage()
                );
                ErrorFlag->store(true);
            } else {
                auto queueAttributes = getQueueAttributesOutcome.GetResult().GetAttributes();
                auto queueArnIt = queueAttributes.find(Aws::SQS::Model::QueueAttributeName::QueueArn);
                if (queueArnIt != queueAttributes.end()) {
                    Aws::String deadLetterTargetArn = queueArnIt->second;
                    
                    TString redrivePolicyStr = fmt::format(
                        R"({{"deadLetterTargetArn":"{}","maxReceiveCount":{}}})",
                        deadLetterTargetArn.c_str(),
                        MaxReceiveCount
                    );
                    Aws::String redrivePolicy(redrivePolicyStr.c_str(), redrivePolicyStr.size());
                    
                    attributes[Aws::SQS::Model::QueueAttributeName::RedrivePolicy] = redrivePolicy;
                } else {
                    Log->Write(
                        ELogPriority::TLOG_ERR,
                        TStringBuilder() << "QueueArn not found in DLQ queue attributes"
                    );
                    ErrorFlag->store(true);
                }
            }
        }
    }
    
    if (!ErrorFlag->load()) {
        if (!attributes.empty()) {
            createQueueRequest.SetAttributes(attributes);
        }

        auto createQueueOutcome = SqsClient->CreateQueue(createQueueRequest);
        if (!createQueueOutcome.IsSuccess()) {
            Log->Write(
                ELogPriority::TLOG_ERR,
                TStringBuilder() << "Error creating queue: "
                                << createQueueOutcome.GetError().GetMessage()
            );
            ErrorFlag->store(true);
        }
    }

    DestroySqsClient();

    if (AnyErrors()) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}  // namespace NYdb::NConsoleClient
