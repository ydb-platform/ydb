#pragma once

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/sqs/SQSClient.h>

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


    private:
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
