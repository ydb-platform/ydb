#include "common_functions.h"

#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_json/sqs_json_client.h>

#include <aws/core/Aws.h>
#include <aws/sqs/model/DeleteMessageBatchRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageBatchRequest.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>
#include <util/system/env.h>

#include <set>

namespace {

using namespace NFederationTests;
using NYdb::NConsoleClient::TSQSJsonClient;

const TString CMDatabase = "/logbroker-federation/prod";
const TString Database = "/Root/logbroker-federation/prod";
const TString SqsConsumer = "sqs-consumer";
const TString StreamingConsumer = "sdk-consumer";
// Legacy PQ consumer names include the federation account.
const TString SqsConsumerPath = "prod/" + SqsConsumer;
const TString StreamingConsumerPath = "prod/" + StreamingConsumer;

// Keep the AWS runtime alive until all clients have been destroyed.
struct TAwsRuntime {
    Aws::SDKOptions Options;

    TAwsRuntime() {
        Aws::InitAPI(Options);
    }

    ~TAwsRuntime() {
        Aws::ShutdownAPI(Options);
    }
};

Aws::String ToAws(const TString& value) {
    return Aws::String(value.data(), value.size());
}

template <class TOutcome>
void AssertSuccess(const TOutcome& outcome) {
    UNIT_ASSERT_C(outcome.IsSuccess(), outcome.GetError().GetMessage());
}

Aws::Client::ClientConfiguration MakeSqsConfig(const TString& cluster) {
    const TString port = GetEnv(cluster + "_http_proxy_port");
    UNIT_ASSERT_C(!port.empty(), cluster + "_http_proxy_port is not set");
    Aws::Client::ClientConfiguration config;
    config.region = "ru-central1";
    config.endpointOverride = ToAws("http://localhost:" + port + Database);
    config.connectTimeoutMs = 5000;
    config.requestTimeoutMs = 10000;
    return config;
}

class TFederationTopic {
public:
    const TClusterEndpoints Endpoints;
    const TString Name = "sqs-" + CreateGuidAsString();

    TFederationTopic() {
        auto driver = MakeDriver(Endpoints.EndpointCM, CMDatabase);
        TTopicClient client(driver);
        const auto result = client.CreateTopic(Name,
            TCreateTopicSettings()
                .PartitioningSettings(1, 1)
                .BeginAddConsumer(CMDatabase + "/" + SqsConsumer)
                    .ConsumerType(EConsumerType::Shared)
                    .KeepMessagesOrder(false)
                    .DefaultProcessingTimeout(TDuration::Seconds(30))
                .EndAddConsumer()
                .BeginAddConsumer(CMDatabase + "/" + StreamingConsumer)
                .EndAddConsumer()
        ).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // CM applies the topic and consumer configuration asynchronously.
        WaitForTopic(Endpoints.EndpointA);
        WaitForTopic(Endpoints.EndpointB);
    }

    ~TFederationTopic() {
        try {
            auto driver = MakeDriver(Endpoints.EndpointCM, CMDatabase);
            const auto result = TTopicClient(driver).DropTopic(Name).GetValueSync();
            if (!result.IsSuccess()) {
                Cerr << "DropTopic(" << Name << "): " << result.GetIssues().ToString() << Endl;
            }
        } catch (...) {
            // Cleanup must not hide the original test failure.
            Cerr << "Failed to drop federation topic " << Name << Endl;
        }
    }

    Aws::String GetQueueUrl(const TSQSJsonClient& client) const {
        Aws::SQS::Model::GetQueueUrlRequest request;
        request.SetQueueName(ToAws(Name + "@" + SqsConsumerPath));
        const auto outcome = client.GetQueueUrl(request);
        AssertSuccess(outcome);
        UNIT_ASSERT(!outcome.GetResult().GetQueueUrl().empty());
        return outcome.GetResult().GetQueueUrl();
    }

private:
    void WaitForTopic(const TString& endpoint) const {
        auto driver = MakeDriver(endpoint, Database);
        TTopicClient client(driver);
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        TString lastIssues;
        do {
            const auto result = client.DescribeTopic(Name).GetValueSync();
            if (result.IsSuccess()) {
                bool shared = false;
                bool streaming = false;
                TStringBuilder consumers;
                for (const auto& consumer : result.GetTopicDescription().GetConsumers()) {
                    const auto& name = consumer.GetConsumerName();
                    const auto type = consumer.GetConsumerType();
                    shared |= name == SqsConsumerPath && type == EConsumerType::Shared;
                    streaming |= name == StreamingConsumerPath && type == EConsumerType::Streaming;
                    consumers << " [name=" << name << ", type=" << static_cast<int>(type) << "]";
                }
                lastIssues = TStringBuilder() << "Expected shared consumer " << SqsConsumerPath
                    << " and streaming consumer " << StreamingConsumerPath << "; actual consumers:" << consumers;
                Cerr << "Topic description is successful. Consumers count=" << result.GetTopicDescription().GetConsumers().size() << ". Shared=" << shared << ", streaming=" << streaming << "." << consumers << Endl;
                if (shared && streaming) {
                    return;
                }
            } else {
                lastIssues = result.GetIssues().ToString();
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_FAIL("Federation topic " << Name << " is not ready on " << endpoint << ": " << lastIssues);
    }
};

void SendMessages(const TSQSJsonClient& client, const Aws::String& queueUrl,
                  const std::vector<TString>& bodies) {
    Aws::SQS::Model::SendMessageBatchRequest request;
    request.SetQueueUrl(queueUrl);
    request.SetAdditionalCustomHeaderValue("x-amz-target", "AmazonSQS.SendMessageBatch");
    for (size_t i = 0; i < bodies.size(); ++i) {
        Aws::SQS::Model::SendMessageBatchRequestEntry entry;
        entry.SetId(ToAws(ToString(i)));
        entry.SetMessageBody(ToAws(bodies[i]));
        request.AddEntries(std::move(entry));
    }
    const auto outcome = client.SendMessageBatch(request);
    AssertSuccess(outcome);
    UNIT_ASSERT_VALUES_EQUAL(outcome.GetResult().GetFailed().size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(outcome.GetResult().GetSuccessful().size(), bodies.size());
    for (const auto& entry : outcome.GetResult().GetSuccessful()) {
        UNIT_ASSERT(!entry.GetMessageId().empty());
    }
}

void ReceiveMessages(const TSQSJsonClient& client, const Aws::String& queueUrl,
                     const std::vector<TString>& expected) {
    std::multiset<TString> received;
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    while (received.size() < expected.size() && TInstant::Now() < deadline) {
        Aws::SQS::Model::ReceiveMessageRequest request;
        request.SetQueueUrl(queueUrl);
        request.SetAdditionalCustomHeaderValue("x-amz-target", "AmazonSQS.ReceiveMessage");
        request.SetMaxNumberOfMessages(10);
        request.SetWaitTimeSeconds(1);
        request.SetVisibilityTimeout(30);
        const auto outcome = client.ReceiveMessage(request);
        AssertSuccess(outcome);

        Aws::SQS::Model::DeleteMessageBatchRequest deleteRequest;
        deleteRequest.SetQueueUrl(queueUrl);
        deleteRequest.SetAdditionalCustomHeaderValue("x-amz-target", "AmazonSQS.DeleteMessageBatch");
        for (const auto& message : outcome.GetResult().GetMessages()) {
            UNIT_ASSERT(!message.GetMessageId().empty());
            UNIT_ASSERT(!message.GetReceiptHandle().empty());
            received.emplace(message.GetBody().data(), message.GetBody().size());
            Aws::SQS::Model::DeleteMessageBatchRequestEntry entry;
            entry.SetId(ToAws(ToString(deleteRequest.GetEntries().size())));
            entry.SetReceiptHandle(message.GetReceiptHandle());
            deleteRequest.AddEntries(std::move(entry));
        }
        if (!deleteRequest.GetEntries().empty()) {
            const auto deleted = client.DeleteMessageBatch(deleteRequest);
            AssertSuccess(deleted);
            UNIT_ASSERT_VALUES_EQUAL(deleted.GetResult().GetFailed().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(deleted.GetResult().GetSuccessful().size(), deleteRequest.GetEntries().size());
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(received.size(), expected.size());
    UNIT_ASSERT(received == std::multiset<TString>(expected.begin(), expected.end()));
}

enum class EScenario {
    SqsToTopic,
    TopicToSqs,
    SqsToSqs,
};

void CheckCompatibility(EScenario scenario) {
    TAwsRuntime runtime;
    TFederationTopic topic;
    for (const auto& cluster : {TString("cluster_a"), TString("cluster_b")}) {
        const TString endpoint = cluster == "cluster_a" ? topic.Endpoints.EndpointA : topic.Endpoints.EndpointB;
        TSQSJsonClient sqs(Aws::Auth::AWSCredentials("unused", "unused", "root@builtin"),
                          MakeSqsConfig(cluster), "");
        const auto queueUrl = topic.GetQueueUrl(sqs);
        const std::vector<TString> bodies = {cluster + "-message-0", cluster + "-message-1", cluster + "-message-2"};

        if (scenario == EScenario::TopicToSqs) {
            auto driver = MakeDriver(endpoint, Database);
            TTopicClient client(driver);
            auto session = client.CreateSimpleBlockingWriteSession(
                TWriteSessionSettings().Path(topic.Name).MessageGroupId("sdk-producer").Codec(ECodec::RAW));
            for (const auto& body : bodies) {
                UNIT_ASSERT(session->Write(body));
            }
            UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
        } else {
            SendMessages(sqs, queueUrl, bodies);
        }

        if (scenario == EScenario::SqsToTopic) {
            auto driver = MakeDriver(endpoint, Database);
            TTopicClient client(driver);
            auto session = client.CreateReadSession(
                TReadSessionSettings().ConsumerName(StreamingConsumerPath).AppendTopics(TTopicReadSettings(topic.Name)));
            const auto messages = ReadMessages(session, bodies.size());
            session->Close(TDuration::Seconds(5));
            std::multiset<TString> received;
            for (const auto& [offset, body] : messages) {
                received.insert(body);
            }
            UNIT_ASSERT_VALUES_EQUAL(received.size(), bodies.size());
            UNIT_ASSERT(received == std::multiset<TString>(bodies.begin(), bodies.end()));
        } else {
            ReceiveMessages(sqs, queueUrl, bodies);
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(SqsFederationCompatibilityTests) {
    Y_UNIT_TEST(SqsWriteIsReadableThroughTopicSdk) {
        CheckCompatibility(EScenario::SqsToTopic);
    }

    Y_UNIT_TEST(TopicSdkWriteIsReadableThroughSqs) {
        CheckCompatibility(EScenario::TopicToSqs);
    }

    Y_UNIT_TEST(SqsBatchWriteAndReadOnFederation) {
        CheckCompatibility(EScenario::SqsToSqs);
    }
}
