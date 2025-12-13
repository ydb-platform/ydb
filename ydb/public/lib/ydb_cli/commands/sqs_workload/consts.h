#pragma once

namespace NYdb::NConsoleClient {

    constexpr const char* kSQSWorkloadActionHeader = "X-SQS-Workload-Action";
    constexpr const char* kSQSTargetReceiveMessage = "AmazonSQS.ReceiveMessage";
    constexpr const char* kSQSTargetSendMessageBatch = "AmazonSQS.SendMessageBatch";
    constexpr const char* kSQSTargetDeleteMessageBatch =
        "AmazonSQS.DeleteMessageBatch";

    constexpr const char kSQSMessageStartTimeSeparator = '_';
    constexpr const char* kAmzTargetHeader = "x-amz-target";
    constexpr const char* kYacloudSubjectTokenHeader = "x-yacloud-subjecttoken";
    constexpr const char* kAuthorizationHeader = "Authorization";
    constexpr const char* kBearerPrefix = "Bearer ";
    constexpr const char* kSQSWorkloadLogFileName = "sqs_workload.log";

} // namespace NYdb::NConsoleClient
