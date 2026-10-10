#include "sqs_compatibility_helpers.h"

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_json/sqs_json_client.h>

namespace {

std::unique_ptr<Aws::SQS::SQSClient> MakeClient(const Aws::Client::ClientConfiguration& config) {
    return std::make_unique<NYdb::NConsoleClient::TSQSJsonClient>(
        Aws::Auth::AWSCredentials("unused", "unused", "root@builtin"), config, "");
}

} // namespace

Y_UNIT_TEST_SUITE(SqsJsonFederationCompatibilityTests) {
    Y_UNIT_TEST(ReportsMissingQueue) {
        NFederationSqsTests::TAwsRuntime runtime;
        for (const auto& cluster : {TString("cluster_a"), TString("cluster_b")}) {
            const auto client = MakeClient(NFederationSqsTests::MakeSqsConfig(cluster));
            Aws::SQS::Model::GetQueueUrlRequest request;
            request.SetQueueName(NFederationSqsTests::ToAws("missing-" + CreateGuidAsString()));
            const auto outcome = client->GetQueueUrl(request);
            UNIT_ASSERT(!outcome.IsSuccess());
            UNIT_ASSERT_C(outcome.GetError().GetErrorType() == Aws::SQS::SQSErrors::QUEUE_DOES_NOT_EXIST,
                outcome.GetError().GetMessage());
            UNIT_ASSERT(!outcome.GetError().GetMessage().empty());
        }
    }

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
