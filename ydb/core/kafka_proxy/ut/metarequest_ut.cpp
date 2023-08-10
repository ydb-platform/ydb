#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/actors/kafka_metadata_actor.h>


namespace NKafka::NTests {

Y_UNIT_TEST_SUITE(TMetadataActorTests) {
    THolder<TMetadataRequestData> GetMetadataRequest(const TVector<TString>& topics) {
        auto res = MakeHolder<TMetadataRequestData>();
        for (const auto& t : topics) {
            TMetadataRequestData::TMetadataRequestTopic topic;
            topic.Name = t;
            res->Topics.push_back(topic);
        }
        return res;
    }

    auto GetEvent(NPersQueue::TTestServer& server, const TActorId& edgeActor, const TVector<TString>& topics) {
        NKikimrConfig::TKafkaProxyConfig Config;
        NACLib::TUserToken userToken("root@builtin", {});

        auto* runtime = server.CleverServer->GetRuntime();
        auto request = GetMetadataRequest(topics);
        auto actorId = runtime->Register(new TKafkaMetadataActor(edgeActor, &userToken, 1, request.Get(), Config));
        runtime->EnableScheduleForActor(actorId);
        runtime->DispatchEvents();
        Cerr << "Wait for response for topics: '";
        for (const auto& t : topics) {
            Cerr << t << "', ";
        }
        Cerr << Endl;
        return runtime->GrabEdgeEvent<TEvKafka::TEvResponse>();
    }

    Y_UNIT_TEST(TopicMetadataGoodAndBad) {
        NPersQueue::TTestServer server;
        TString topicName = "rt3.dc1--topic";
        TString topicName2 = "rt3.dc1--topic2";
        TString topicPath = TString("/Root/PQ/") + topicName;
        TString topicPath2 = TString("/Root/PQ/") + topicName2;
        ui32 totalPartitions = 5;
        server.AnnoyingClient->CreateTopic(topicName, totalPartitions);
        server.AnnoyingClient->CreateTopic(topicName2, totalPartitions * 2);

        auto edgeId = server.CleverServer->GetRuntime()->AllocateEdgeActor();
        auto event = GetEvent(server, edgeId, {topicPath});
        auto response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[0].Partitions.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[0].Partitions[0].ReplicaNodes.size(), 1);
        UNIT_ASSERT(response->Topics[0].TopicId > 0);

        event = GetEvent(server, edgeId, {topicPath, topicPath2});
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 2);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[1].Partitions.size(), totalPartitions * 2);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[1].Partitions[5].ReplicaNodes.size(), 1);

        event = GetEvent(server, edgeId, {topicPath, ""});
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 2);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT(response->Topics[1].ErrorCode == EKafkaErrors::INVALID_TOPIC_EXCEPTION);

        event = GetEvent(server, edgeId, {"/Root/bad-topic", topicPath});
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 2);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
        UNIT_ASSERT(response->Topics[1].ErrorCode == EKafkaErrors::NONE_ERROR);

        event = GetEvent(server, edgeId, {"/Root/bad-topic"});
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);

        event = GetEvent(server, edgeId, {});
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 0);
    }
};

} // namespace NKafka::NTests TEvKafka::TEvMetadataResponse