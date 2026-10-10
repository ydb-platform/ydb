#include "sqs_compatibility_helpers.h"

#include <aws/core/http/URI.h>

namespace {

using Aws::SQS::SQSClient;
using namespace Aws::SQS::Model;

class TXmlSqsClient : public SQSClient {
public:
    TXmlSqsClient(const Aws::Auth::AWSCredentials& credentials,
                         const Aws::Client::ClientConfiguration& config)
        : SQSClient(credentials, config)
        , Endpoint(config.endpointOverride)
    {
    }

    ChangeMessageVisibilityOutcome ChangeMessageVisibility(const ChangeMessageVisibilityRequest& request) const override {
        return ChangeMessageVisibilityOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    ChangeMessageVisibilityBatchOutcome ChangeMessageVisibilityBatch(const ChangeMessageVisibilityBatchRequest& request) const override {
        return ChangeMessageVisibilityBatchOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    DeleteMessageOutcome DeleteMessage(const DeleteMessageRequest& request) const override {
        return DeleteMessageOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    DeleteMessageBatchOutcome DeleteMessageBatch(const DeleteMessageBatchRequest& request) const override {
        return DeleteMessageBatchOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    DeleteQueueOutcome DeleteQueue(const DeleteQueueRequest& request) const override {
        return DeleteQueueOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    GetQueueAttributesOutcome GetQueueAttributes(const GetQueueAttributesRequest& request) const override {
        return GetQueueAttributesOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    PurgeQueueOutcome PurgeQueue(const PurgeQueueRequest& request) const override {
        return PurgeQueueOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    ReceiveMessageOutcome ReceiveMessage(const ReceiveMessageRequest& request) const override {
        return ReceiveMessageOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    SendMessageOutcome SendMessage(const SendMessageRequest& request) const override {
        return SendMessageOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    SendMessageBatchOutcome SendMessageBatch(const SendMessageBatchRequest& request) const override {
        return SendMessageBatchOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

    SetQueueAttributesOutcome SetQueueAttributes(const SetQueueAttributesRequest& request) const override {
        return SetQueueAttributesOutcome(MakeRequest(Endpoint, request, Aws::Http::HttpMethod::HTTP_POST));
    }

private:
    const Aws::Http::URI Endpoint;
};

std::unique_ptr<SQSClient> MakeClient(const Aws::Client::ClientConfiguration& config) {
    return std::make_unique<TXmlSqsClient>(
        Aws::Auth::AWSCredentials("unused", "unused", "root@builtin"), config);
}

} // namespace

Y_UNIT_TEST_SUITE(SqsXmlFederationCompatibilityTests) {
    Y_UNIT_TEST(SqsBatchWriteAndReadOnFederation) {
        NFederationSqsTests::SqsBatchWriteAndReadOnFederation(MakeClient);
    }

    Y_UNIT_TEST(SqsSingleWriteReadAndDeleteOnFederation) {
        NFederationSqsTests::SqsSingleWriteReadAndDeleteOnFederation(MakeClient);
    }

    Y_UNIT_TEST(SqsChangeMessageVisibilityOnFederation) {
        NFederationSqsTests::SqsChangeMessageVisibilityOnFederation(MakeClient);
    }

    Y_UNIT_TEST(SqsChangeMessageVisibilityBatchOnFederation) {
        NFederationSqsTests::SqsChangeMessageVisibilityBatchOnFederation(MakeClient);
    }

    Y_UNIT_TEST(SqsGetQueueAttributesOnFederation) {
        NFederationSqsTests::SqsGetQueueAttributesOnFederation(MakeClient);
    }

    Y_UNIT_TEST(SqsListQueuesOnFederation) {
        NFederationSqsTests::SqsListQueuesOnFederation(MakeClient);
    }

    Y_UNIT_TEST(SqsPurgeQueueOnFederation) {
        NFederationSqsTests::SqsPurgeQueueOnFederation(MakeClient);
    }
}
