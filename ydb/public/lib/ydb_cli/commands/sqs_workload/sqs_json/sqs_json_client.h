#pragma once

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/sqs/SQSClient.h>

namespace NYdb::NConsoleClient {

    class TSQSJsonClient: public Aws::SQS::SQSClient {
    public:
        explicit TSQSJsonClient(
            const Aws::Auth::AWSCredentials& credentials,
            const Aws::Client::ClientConfiguration& clientConfiguration);
        ~TSQSJsonClient() = default;

        Aws::SQS::Model::SendMessageBatchOutcome SendMessageBatch(
            const Aws::SQS::Model::SendMessageBatchRequest& request) const override;
        Aws::SQS::Model::ReceiveMessageOutcome ReceiveMessage(
            const Aws::SQS::Model::ReceiveMessageRequest& request) const override;
        Aws::SQS::Model::DeleteMessageBatchOutcome DeleteMessageBatch(
            const Aws::SQS::Model::DeleteMessageBatchRequest& request)
            const override;

    private:
        std::shared_ptr<Aws::Http::HttpClient> HttpClient;
        std::shared_ptr<Aws::Client::AWSAuthV4Signer> Signer;
        Aws::String EndpointOverride;

        void AddHeaders(const Aws::Http::HeaderValueCollection&,
                        std::shared_ptr<Aws::Http::HttpRequest>&) const;
        Aws::Utils::Json::JsonValue
        ReadResponseBody(const Aws::Http::HttpResponse& response) const;
        std::shared_ptr<Aws::Http::HttpRequest>
        CreateBaseRequest(const Aws::String& queueUrl) const;
    };

} // namespace NYdb::NConsoleClient
