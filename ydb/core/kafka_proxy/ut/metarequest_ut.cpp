#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/actors/kafka_metadata_actor.h>


namespace NKafka::NTests {

Y_UNIT_TEST_SUITE(TMetadataActorTests) {
    TMetadataRequestData::TPtr GetMetadataRequest(const TVector<TString>& topics) {
        auto res = std::make_shared<TMetadataRequestData>();
        for (const auto& t : topics) {
            TMetadataRequestData::TMetadataRequestTopic topic;
            topic.Name = t;
            res->Topics.push_back(topic);
        }
        return res;
    }

    auto GetEvent(NPersQueue::TTestServer& server, const TActorId& edgeActor, const TVector<TString>& topics, const TString& proxyHost = "") {
        NKikimrConfig::TKafkaProxyConfig Config;
        if (proxyHost) {
            Config.MutableProxy()->SetHostname(proxyHost);
            Config.MutableProxy()->SetPort(9097);
        }
        Config.SetAutoCreateTopicsEnable(false);
        Config.SetAutoCreateConsumersEnable(false);
        auto* runtime = server.CleverServer->GetRuntime();
        auto request = GetMetadataRequest(topics);

        auto context = std::make_shared<TContext>(Config);
        context->ConnectionId = edgeActor;
        context->DatabasePath = "/Root";
        context->ResourceDatabasePath = "/Root";
        context->UserToken = new NACLib::TUserToken("root@builtin", {});

        auto actorId = runtime->Register(new TKafkaMetadataActor(context, 1, TMessagePtr<TMetadataRequestData>(std::make_shared<TBuffer>(), request),
                                                                 NKafka::MakeKafkaDiscoveryCacheID()));
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
        auto pm = MakeSimpleShared<TPortManager>();
        ui16 kafkaPort = pm->GetPort();
        auto serverSettings = NKikimr::NPersQueueTests::PQSettings(0).SetDomainName("Root").SetNodeCount(1);
        serverSettings.AppConfig->MutableKafkaProxyConfig()->SetEnableKafkaProxy(true);
        serverSettings.AppConfig->MutableKafkaProxyConfig()->SetListeningPort(kafkaPort);
        const TString DbRoot = "/Root/LbAccount";
        const TString Account = "account";
        const TString DbPath = DbRoot + "/" + Account;
        TString topicName1 = "topic";
        TString topicName2 = "topic2";
        const TString fullTopicName1 = DbPath + "/" + topicName1;
        const TString shorttopicName1 = Account + "/" + topicName1;
        const TString fullTopicName2 = DbPath + "/" + topicName2;
        const TString shorttopicName2 = Account + "/" + topicName2;
        serverSettings.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(DbRoot);
        serverSettings.PQConfig.SetTestDatabaseRoot(DbRoot);
        serverSettings.PQConfig.SetTopicsAreFirstClassCitizen(false);
        NPersQueue::TTestServer server(serverSettings, true, {}, NActors::NLog::PRI_INFO, pm);
        ui32 totalPartitions = 5;
        server.AnnoyingClient->MkDir("/Root", "LbAccount");
        server.AnnoyingClient->MkDir("/Root/LbAccount", "account");
        server.AnnoyingClient->CreateTopicNoLegacy(fullTopicName1, totalPartitions, true, true, "dc1", {"user", "test-consumer"}, "account");
        server.AnnoyingClient->CreateTopicNoLegacy(fullTopicName2, totalPartitions * 2, true, true, "dc1", {"user", "test-consumer"}, "account");
        server.WaitInit(shorttopicName1);

        auto edgeId = server.CleverServer->GetRuntime()->AllocateEdgeActor();
        auto event = GetEvent(server, edgeId, {fullTopicName1});
        auto response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[0].Partitions.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[0].Partitions[0].ReplicaNodes.size(), 1);

        event = GetEvent(server, edgeId, {fullTopicName1, fullTopicName2});
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 2);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[1].Partitions.size(), totalPartitions * 2);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics[1].Partitions[5].ReplicaNodes.size(), 1);

        event = GetEvent(server, edgeId, {fullTopicName1, ""});
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 2);
        UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::NONE_ERROR);
        UNIT_ASSERT(response->Topics[1].ErrorCode == EKafkaErrors::INVALID_TOPIC_EXCEPTION);

        event = GetEvent(server, edgeId, {"/Root/bad-topic", fullTopicName1});
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
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 2);

        event = GetEvent(server, edgeId, {fullTopicName1}, "proxy-host");
        response = dynamic_cast<TMetadataResponseData*>(event->Response.get());
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Brokers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Brokers[0].NodeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Brokers[0].Host, "proxy-host");
        UNIT_ASSERT_VALUES_EQUAL(response->Brokers[0].Port, 9097);

        for(auto& t : response->Topics) {
            for(auto& p : t.Partitions) {
                UNIT_ASSERT_VALUES_EQUAL(p.LeaderId, 1);
            }
        }
    }
};

} // namespace NKafka::NTests TEvKafka::TEvMetadataResponse
