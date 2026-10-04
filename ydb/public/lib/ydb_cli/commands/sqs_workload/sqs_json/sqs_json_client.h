#pragma once

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/sqs/SQSClient.h>
#include <aws/core/utils/json/JsonSerializer.h>

namespace NYdb::NConsoleClient {
    using namespace Aws::SQS::Model;

    class TSQSJsonClient: public Aws::SQS::SQSClient {
    public:
        explicit TSQSJsonClient(
            const Aws::Auth::AWSCredentials& credentials,
            const Aws::Client::ClientConfiguration& clientConfiguration,
            const Aws::String& cloudIamToken);
        ~TSQSJsonClient() = default;

        SendMessageBatchOutcome SendMessageBatch(
            const SendMessageBatchRequest& request) const override;
        ReceiveMessageOutcome ReceiveMessage(
            const ReceiveMessageRequest& request) const override;
        DeleteMessageBatchOutcome DeleteMessageBatch(
            const DeleteMessageBatchRequest& request)
            const override;
        GetQueueUrlOutcome GetQueueUrl(
            const GetQueueUrlRequest& request) const override;

        SendMessageOutcome SendMessage(
            const SendMessageRequest& request) const override;
        DeleteMessageOutcome DeleteMessage(
            const DeleteMessageRequest& request) const override;
        ChangeMessageVisibilityOutcome ChangeMessageVisibility(
            const ChangeMessageVisibilityRequest& request) const override;
        ChangeMessageVisibilityBatchOutcome ChangeMessageVisibilityBatch(
            const ChangeMessageVisibilityBatchRequest& request) const override;
        GetQueueAttributesOutcome GetQueueAttributes(
            const GetQueueAttributesRequest& request) const override;
        ListQueuesOutcome ListQueues(
            const ListQueuesRequest& request) const override;
        PurgeQueueOutcome PurgeQueue(
            const PurgeQueueRequest& request) const override;
        CreateQueueOutcome CreateQueue(
            const CreateQueueRequest& request) const override;
        DeleteQueueOutcome DeleteQueue(
            const DeleteQueueRequest& request) const override;
        SetQueueAttributesOutcome SetQueueAttributes(
            const SetQueueAttributesRequest& request) const override;


    private:
        using TJsonOutcome = Aws::Utils::Outcome<Aws::Utils::Json::JsonValue, Aws::SQS::SQSError>;

        TJsonOutcome ExecuteJsonRequest(
            const char* operation,
            const Aws::Utils::Json::JsonValue& payload,
            const Aws::Http::HeaderValueCollection& headers,
            const Aws::String& queueUrl) const;

        std::shared_ptr<Aws::Http::HttpClient> HttpClient;
        std::shared_ptr<Aws::Client::AWSAuthV4Signer> Signer;
        Aws::String EndpointOverride;
        Aws::String CloudIamToken;

        void AddHeaders(const Aws::Http::HeaderValueCollection&,
                        std::shared_ptr<Aws::Http::HttpRequest>&) const;
        Aws::Utils::Json::JsonValue
        ReadResponseBody(const Aws::Http::HttpResponse& response) const;
        std::shared_ptr<Aws::Http::HttpRequest>
        CreateBaseRequest(const Aws::String& queueUrl) const;
    };

} // namespace NYdb::NConsoleClient
