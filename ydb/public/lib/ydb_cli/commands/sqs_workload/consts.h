#pragma once

namespace NYdb::NConsoleClient {

    constexpr const char* SQS_TARGET_RECEIVE_MESSAGE = "AmazonSQS.ReceiveMessage";
    constexpr const char* SQS_TARGET_SEND_MESSAGE_BATCH = "AmazonSQS.SendMessageBatch";
    constexpr const char* SQS_TARGET_DELETE_MESSAGE_BATCH = "AmazonSQS.DeleteMessageBatch";
    constexpr const char* SQS_TARGET_GET_QUEUE_URL = "AmazonSQS.GetQueueUrl";

    constexpr const char SQS_MESSAGE_START_TIME_SEPARATOR = '_';
    constexpr const char* AMZ_TARGET_HEADER = "x-amz-target";
    constexpr const char* YACLOUD_SUBJECT_TOKEN_HEADER = "x-yacloud-subjecttoken";
    constexpr const char* AUTHORIZATION_HEADER = "Authorization";
    constexpr const char* BEARER_PREFIX = "Bearer ";

} // namespace NYdb::NConsoleClient
