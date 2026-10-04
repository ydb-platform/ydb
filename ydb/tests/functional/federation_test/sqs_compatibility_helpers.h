#pragma once

#include "common_functions.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/ChangeMessageVisibilityBatchRequest.h>
#include <aws/sqs/model/ChangeMessageVisibilityRequest.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageBatchRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/ListQueuesRequest.h>
#include <aws/sqs/model/PurgeQueueRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageBatchRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/SetQueueAttributesRequest.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>
#include <util/system/env.h>

#include <algorithm>
#include <set>
#include <vector>

namespace NFederationSqsTests {

using namespace NFederationTests;
using Aws::SQS::SQSClient;
using TClientFactory = std::function<std::unique_ptr<SQSClient>(const Aws::Client::ClientConfiguration&)>;
using namespace Aws::SQS::Model;

inline const TString CMDatabase = "/logbroker-federation/prod";
inline const TString Database = "/Root/logbroker-federation/prod";
inline const TString SqsConsumer = "sqs-consumer";
inline const TString SqsConsumerPath = "prod/" + SqsConsumer;

struct TAwsRuntime {
    Aws::SDKOptions Options;

    TAwsRuntime() {
        Aws::InitAPI(Options);
    }

    ~TAwsRuntime() {
        Aws::ShutdownAPI(Options);
    }
};

inline Aws::String ToAws(const TString& value) {
    return Aws::String(value.data(), value.size());
}

template <class TOutcome>
inline void AssertSuccess(const TOutcome& outcome) {
    UNIT_ASSERT_C(outcome.IsSuccess(), outcome.GetError().GetMessage());
}

inline Aws::Client::ClientConfiguration MakeSqsConfig(const TString& cluster) {
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

    Aws::String GetQueueUrl(const SQSClient& client) const {
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
                TStringBuilder consumers;
                for (const auto& consumer : result.GetTopicDescription().GetConsumers()) {
                    const auto& name = consumer.GetConsumerName();
                    const auto type = consumer.GetConsumerType();
                    shared |= name == SqsConsumerPath && type == EConsumerType::Shared;
                    consumers << " [name=" << name << ", type=" << static_cast<int>(type) << "]";
                }
                lastIssues = TStringBuilder() << "Expected shared consumer " << SqsConsumerPath
                    << "; actual consumers:" << consumers;
                if (shared) {
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

inline void SendMessages(const SQSClient& client, const Aws::String& queueUrl,
                  const std::vector<TString>& bodies) {
    Aws::SQS::Model::SendMessageBatchRequest request;
    request.SetQueueUrl(queueUrl);
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
    std::set<Aws::String> ids;
    for (const auto& entry : outcome.GetResult().GetSuccessful()) {
        UNIT_ASSERT(!entry.GetMessageId().empty());
        ids.insert(entry.GetId());
    }
    UNIT_ASSERT_VALUES_EQUAL(ids.size(), bodies.size());
    for (size_t i = 0; i < bodies.size(); ++i) {
        UNIT_ASSERT(ids.contains(ToAws(ToString(i))));
    }
}

inline void ReceiveMessages(const SQSClient& client, const Aws::String& queueUrl,
                     const std::vector<TString>& expected) {
    std::multiset<TString> received;
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    while (received.size() < expected.size() && TInstant::Now() < deadline) {
        Aws::SQS::Model::ReceiveMessageRequest request;
        request.SetQueueUrl(queueUrl);
        request.SetMaxNumberOfMessages(10);
        request.SetWaitTimeSeconds(1);
        request.SetVisibilityTimeout(30);
        const auto outcome = client.ReceiveMessage(request);
        AssertSuccess(outcome);

        Aws::SQS::Model::DeleteMessageBatchRequest deleteRequest;
        deleteRequest.SetQueueUrl(queueUrl);
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

template <class TCheck>
inline void CheckSqsMethod(const TClientFactory& makeClient, TCheck check) {
    TAwsRuntime runtime;
    TFederationTopic topic;
    for (const auto& cluster : {TString("cluster_a"), TString("cluster_b")}) {
        const auto sqs = makeClient(MakeSqsConfig(cluster));
        const auto queueUrl = topic.GetQueueUrl(*sqs);
        check(*sqs, queueUrl, cluster, topic);
    }
}

inline Aws::Vector<Message> Receive(const SQSClient& client, const Aws::String& queueUrl, size_t count, int visibilityTimeout = 60) {
    Aws::Vector<Message> messages;
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    while (messages.size() < count && TInstant::Now() < deadline) {
        ReceiveMessageRequest request;
        request.SetQueueUrl(queueUrl);
        request.SetMaxNumberOfMessages(count - messages.size());
        request.SetWaitTimeSeconds(1);
        request.SetVisibilityTimeout(visibilityTimeout);
        const auto outcome = client.ReceiveMessage(request);
        AssertSuccess(outcome);
        for (const auto& message : outcome.GetResult().GetMessages()) {
            UNIT_ASSERT(!message.GetMessageId().empty());
            UNIT_ASSERT(!message.GetReceiptHandle().empty());
            messages.push_back(message);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), count);
    return messages;
}

inline void AssertEmpty(const SQSClient& client, const Aws::String& queueUrl) {
    ReceiveMessageRequest request;
    request.SetQueueUrl(queueUrl);
    request.SetWaitTimeSeconds(1);
    const auto outcome = client.ReceiveMessage(request);
    AssertSuccess(outcome);
    UNIT_ASSERT(outcome.GetResult().GetMessages().empty());
}

inline void Delete(const SQSClient& client, const Aws::String& queueUrl, const Message& message) {
    DeleteMessageRequest request;
    request.SetQueueUrl(queueUrl);
    request.SetReceiptHandle(message.GetReceiptHandle());
    AssertSuccess(client.DeleteMessage(request));
}

inline void CheckVisibility(const TClientFactory& makeClient, bool batch) {
    CheckSqsMethod(makeClient, [=](const SQSClient& client, const Aws::String& url, const TString& cluster, const TFederationTopic&) {
        const std::vector<TString> bodies = batch
            ? std::vector<TString>{cluster + "-first", cluster + "-second"}
            : std::vector<TString>{cluster + "-single"};
        SendMessages(client, url, bodies);
        const auto messages = Receive(client, url, bodies.size());
        AssertEmpty(client, url);
        if (batch) {
            ChangeMessageVisibilityBatchRequest request;
            request.SetQueueUrl(url);
            for (size_t i = 0; i < messages.size(); ++i) {
                request.AddEntries(ChangeMessageVisibilityBatchRequestEntry()
                    .WithId(ToAws(ToString(i)))
                    .WithReceiptHandle(messages[i].GetReceiptHandle())
                    .WithVisibilityTimeout(0));
            }
            const auto outcome = client.ChangeMessageVisibilityBatch(request);
            AssertSuccess(outcome);
            UNIT_ASSERT(outcome.GetResult().GetFailed().empty());
            std::set<Aws::String> ids;
            for (const auto& entry : outcome.GetResult().GetSuccessful()) {
                ids.insert(entry.GetId());
            }
            UNIT_ASSERT_VALUES_EQUAL(ids.size(), messages.size());
            for (size_t i = 0; i < messages.size(); ++i) {
                UNIT_ASSERT(ids.contains(ToAws(ToString(i))));
            }
        } else {
            ChangeMessageVisibilityRequest request;
            request.SetQueueUrl(url);
            request.SetReceiptHandle(messages.front().GetReceiptHandle());
            request.SetVisibilityTimeout(0);
            AssertSuccess(client.ChangeMessageVisibility(request));
        }
        // Resetting visibility must make the same messages available again.
        const auto redelivered = Receive(client, url, bodies.size());
        std::set<Aws::String> originalIds;
        std::set<Aws::String> redeliveredIds;
        std::multiset<TString> actualBodies;
        for (const auto& message : messages) {
            originalIds.insert(message.GetMessageId());
        }
        for (const auto& message : redelivered) {
            redeliveredIds.insert(message.GetMessageId());
            actualBodies.emplace(message.GetBody().data(), message.GetBody().size());
            Delete(client, url, message);
        }
        UNIT_ASSERT(originalIds == redeliveredIds);
        UNIT_ASSERT(actualBodies == std::multiset<TString>(bodies.begin(), bodies.end()));
        AssertEmpty(client, url);
    });
}


inline void SqsBatchWriteAndReadOnFederation(const TClientFactory& makeClient) {
    CheckSqsMethod(makeClient, [](const SQSClient& client, const Aws::String& url, const TString& cluster, const TFederationTopic&) {
        const std::vector<TString> bodies = {cluster + "-0", cluster + "-1", cluster + "-2"};
        SendMessages(client, url, bodies);
        ReceiveMessages(client, url, bodies);
        AssertEmpty(client, url);
    });
}

inline void SqsSingleWriteReadAndDeleteOnFederation(const TClientFactory& makeClient) {
    CheckSqsMethod(makeClient, [](const SQSClient& client, const Aws::String& url, const TString& cluster, const TFederationTopic&) {
        SendMessageRequest request;
        request.SetQueueUrl(url);
        request.SetMessageBody(ToAws(cluster + "-single"));
        const auto sent = client.SendMessage(request);
        AssertSuccess(sent);
        UNIT_ASSERT(!sent.GetResult().GetMessageId().empty());
        const auto messages = Receive(client, url, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front().GetBody(), request.GetMessageBody());
        UNIT_ASSERT_VALUES_EQUAL(messages.front().GetMessageId(), sent.GetResult().GetMessageId());
        Delete(client, url, messages.front());
        Sleep(TDuration::Seconds(2));
        AssertEmpty(client, url);
    });
}

inline void SqsChangeMessageVisibilityOnFederation(const TClientFactory& makeClient) {
    CheckVisibility(makeClient, false);
}

inline void SqsChangeMessageVisibilityBatchOnFederation(const TClientFactory& makeClient) {
    CheckVisibility(makeClient, true);
}

inline void SqsGetQueueAttributesOnFederation(const TClientFactory& makeClient) {
    CheckSqsMethod(makeClient, [](const SQSClient& client, const Aws::String& url, const TString&, const TFederationTopic&) {
        GetQueueAttributesRequest request;
        request.SetQueueUrl(url);
        request.AddAttributeNames(QueueAttributeName::VisibilityTimeout);
        const auto outcome = client.GetQueueAttributes(request);
        AssertSuccess(outcome);
        const auto& attributes = outcome.GetResult().GetAttributes();
        UNIT_ASSERT(attributes.contains(QueueAttributeName::VisibilityTimeout));
        UNIT_ASSERT_VALUES_EQUAL(attributes.at(QueueAttributeName::VisibilityTimeout), "30");
    });
}

inline void SqsListQueuesOnFederation(const TClientFactory& makeClient) {
    CheckSqsMethod(makeClient, [](const SQSClient& client, const Aws::String& url, const TString&, const TFederationTopic& topic) {
        ListQueuesRequest request;
        request.SetQueueNamePrefix(ToAws(topic.Name));
        const auto outcome = client.ListQueues(request);
        AssertSuccess(outcome);
        const auto& urls = outcome.GetResult().GetQueueUrls();
        UNIT_ASSERT(std::find(urls.begin(), urls.end(), url) != urls.end());
    });
}

inline void SqsPurgeQueueOnFederation(const TClientFactory& makeClient) {
    CheckSqsMethod(makeClient, [](const SQSClient& client, const Aws::String& url, const TString& cluster, const TFederationTopic&) {
        SendMessages(client, url, {cluster + "-purged-0", cluster + "-purged-1"});
        // Purge must remove both available and in-flight messages.
        Receive(client, url, 1, 1);
        PurgeQueueRequest request;
        request.SetQueueUrl(url);
        AssertSuccess(client.PurgeQueue(request));
        Sleep(TDuration::Seconds(2));
        AssertEmpty(client, url);
        SendMessages(client, url, {cluster + "-after-purge"});
        ReceiveMessages(client, url, {cluster + "-after-purge"});
    });
}
} // namespace NFederationSqsTests
