#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/client/server/ic_nodes_cache_service.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/services/persqueue_v1/ut/test_utils.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/core/client/server/ic_nodes_cache_service.h>


namespace NKikimr::NPersQueueTests {

using namespace NKikimr::NGRpcProxy::V1;
using namespace NIcNodeCache;

const static TString topicName = "rt3.dc1--topic-x";
const static TString topicPath = "/Root/PQ/" + topicName;

class TDescribeTestServer {
public: 
    NPersQueue::TTestServer Server;
    bool UseBadTopic = false;

    TDescribeTestServer(ui32 partsCount = 15)
        : Server()
        , PartsCount(partsCount)
    {
        Server.EnableLogs({ NKikimrServices::KQP_PROXY }, NActors::NLog::PRI_EMERG);
        Server.EnableLogs({ NKikimrServices::PERSQUEUE, NKikimrServices::PQ_METACACHE }, NActors::NLog::PRI_INFO);
        Server.EnableLogs({ NKikimrServices::PERSQUEUE_CLUSTER_TRACKER }, NActors::NLog::PRI_INFO);

        Server.AnnoyingClient->CreateTopicNoLegacy(topicName, partsCount);
        Channel = grpc::CreateChannel(
                "localhost:" + ToString(Server.GrpcPort), grpc::InsecureChannelCredentials()
        );
        Stub = Ydb::Topic::V1::TopicService::NewStub(Channel);

    }

    TTestActorRuntime* GetRuntime() const {
        return Server.CleverServer->GetRuntime();
    }
    bool DescribePartition(ui32 partId, bool askLocation, bool askStats, bool mayFail = false) {
        grpc::ClientContext rcontext;
        Ydb::Topic::DescribePartitionRequest request;
        Ydb::Topic::DescribePartitionResponse response;
        Ydb::Topic::DescribePartitionResult result;
        request.set_path(JoinPath({"/Root/PQ/", UseBadTopic ? "bad-topic" : topicName}));
        request.set_partition_id(partId);
        if (askLocation)
            request.set_include_location(true);
        if (askStats)
            request.set_include_stats(true);
        
        Stub->DescribePartition(&rcontext, request, &response);
        Cerr << "Got response: " << response.DebugString() << Endl;
        if (UseBadTopic) {
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SCHEME_ERROR);
            return true;
        }
        if (partId < PartsCount) {
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
        } else {
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::BAD_REQUEST);
            NYql::TIssues opIssues;
            NYql::IssuesFromMessage(response.operation().issues(), opIssues);
            TString expectedError= TStringBuilder() << "No partition " << partId << " in topic"; 
            UNIT_ASSERT(opIssues.ToOneLineString().Contains(expectedError));
            return true;
        }

        auto unpackOk = response.operation().result().UnpackTo(&result);
        UNIT_ASSERT(unpackOk);
        
        if (askStats) {
            UNIT_ASSERT(result.partition().has_partition_stats());
        }
        UNIT_ASSERT_VALUES_EQUAL(result.partition().partition_id(), partId);
        if (askLocation) {
            UNIT_ASSERT(result.partition().has_partition_location());
            UNIT_ASSERT(result.partition().partition_location().node_id() > 0);
            auto genComparizon = (result.partition().partition_location().generation() > 0);
            if (mayFail) {
                return genComparizon;
            } else {
                UNIT_ASSERT_C(genComparizon, response.DebugString().c_str());
            }
        }
        return true;
    }
    void DescribeTopic(bool askStats, bool askLocation) {
        grpc::ClientContext rcontext;
        Ydb::Topic::DescribeTopicRequest request;
        Ydb::Topic::DescribeTopicResponse response;
        Ydb::Topic::DescribeTopicResult result;
        request.set_path(JoinPath({"/Root/PQ/", UseBadTopic ? "bad-topic" : topicName}));
        if (askStats)
            request.set_include_stats(true);
        if (askLocation)
            request.set_include_location(true);
        
        Stub->DescribeTopic(&rcontext, request, &response);
        Cerr << "Got response: " << response.DebugString() << Endl;

        if (UseBadTopic) {
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SCHEME_ERROR);
            return;
        }
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);

        auto unpackOk = response.operation().result().UnpackTo(&result);
        UNIT_ASSERT(unpackOk);
        if (askStats) {
            UNIT_ASSERT(result.has_topic_stats());
        } else {
            UNIT_ASSERT(!result.has_topic_stats());
        }
        UNIT_ASSERT_VALUES_EQUAL(result.partitions_size(), PartsCount);
        for (const auto& p : result.partitions()) {
            UNIT_ASSERT(askStats == p.has_partition_stats());
            UNIT_ASSERT(askLocation == p.has_partition_location());
            if (askLocation) {
                UNIT_ASSERT(p.partition_location().node_id() > 0);
            }
        }
    }
    void DescribeConsumer(const TString& consumerName, bool askStats, bool askLocation) {
        grpc::ClientContext rcontext;
        Ydb::Topic::DescribeConsumerRequest request;
        Ydb::Topic::DescribeConsumerResponse response;
        Ydb::Topic::DescribeConsumerResult result;
        request.set_path(JoinPath({"/Root/PQ/", UseBadTopic ? "bad-topic" : topicName}));
        if (askStats)
            request.set_include_stats(true);
        if (askLocation)
            request.set_include_location(true);
        request.set_consumer(consumerName);
        Stub->DescribeConsumer(&rcontext, request, &response);
        Cerr << "Got response: " << response.DebugString() << Endl;

        if (UseBadTopic) {
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SCHEME_ERROR);
            return;
        }
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);

        auto unpackOk = response.operation().result().UnpackTo(&result);
        UNIT_ASSERT(unpackOk);
        UNIT_ASSERT_VALUES_EQUAL(result.partitions_size(), PartsCount);
        for (const auto& p : result.partitions()) {
            UNIT_ASSERT(askStats == p.has_partition_stats());
            UNIT_ASSERT(askStats == p.has_partition_consumer_stats());
            UNIT_ASSERT(askLocation == p.has_partition_location());
            if (askLocation) {
                UNIT_ASSERT(p.partition_location().node_id() > 0);
            }
        }
    }
    void AddConsumer(const TString& consumer) {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(TStringBuilder() << "/Root/PQ/" << topicName);

        auto addConsumer = request.add_add_consumers();
        addConsumer->set_name(consumer);
        addConsumer->set_important(true);
        grpc::ClientContext rcontext;
        Ydb::Topic::AlterTopicResponse response;
        Stub->AlterTopic(&rcontext, request, &response);
        UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
    }

private:
    std::shared_ptr<grpc::Channel> Channel;
    std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> Stub;
    ui32 PartsCount;
};

Y_UNIT_TEST_SUITE(TTopicApiDescribes) {

    Y_UNIT_TEST(GetLocalDescribe) {
        TDescribeTestServer server{};
        auto* runtime = server.GetRuntime();

        runtime->GetAppData().FeatureFlags.SetEnableIcNodeCache(true);
        runtime->SetUseRealInterconnect();
        const auto edge = runtime->AllocateEdgeActor();

        TString currentTopicName = topicName;
        auto getDescribe = [&] (const TVector<ui32>& parts, bool fails = false) {
            auto partitionActor = runtime->Register(new TPartitionsLocationActor(
                TGetPartitionsLocationRequest{TString("/Root/PQ/") + currentTopicName, "", "", parts}, edge
            ));
            runtime->EnableScheduleForActor(partitionActor);
            runtime->DispatchEvents();
            auto ev = runtime->GrabEdgeEvent<TEvPQProxy::TEvPartitionLocationResponse>();
            if (currentTopicName != topicName) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SCHEME_ERROR);
                return ev;
            }
            if (fails) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::BAD_REQUEST);
                return ev;
            }
            UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), parts ? parts.size() : 15);
            return ev;

        };

        auto ev = getDescribe({});

        THashSet<ui64> allParts;
        for (const auto& p : ev->Partitions) {
            UNIT_ASSERT(p.NodeId > 0);
//            UNIT_ASSERT(p.IncGeneration > 0);
            UNIT_ASSERT(p.PartitionId < 15);
            allParts.insert(p.PartitionId);
        }
        UNIT_ASSERT_VALUES_EQUAL(allParts.size(), 15);

        ev = getDescribe({1, 3, 5});
        allParts.clear();
        for (const auto& p : ev->Partitions) {
            auto res = allParts.insert(p.PartitionId);
            UNIT_ASSERT(res.second);
            UNIT_ASSERT(p.PartitionId == 1 || p.PartitionId == 3 || p.PartitionId == 5);
        }

        getDescribe({1000}, true);
        currentTopicName = "bad-topic";
        getDescribe({}, true);
    }
    Y_UNIT_TEST(GetPartitionDescribe) {
        ui32 partsCount = 15;
        TDescribeTestServer server(partsCount);
        auto* runtime = server.GetRuntime();
        runtime->SetRegistrationObserverFunc(
                [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                    runtime.EnableScheduleForActor(parentId);
                    runtime.EnableScheduleForActor(actorId);
                }
        );

        server.DescribePartition(150, true, false);
        server.DescribePartition(2, false, true);
        server.DescribePartition(0, false, false);
        server.Server.KillTopicPqTablets(topicPath);
        bool checkRes = server.DescribePartition(1, true, false, true);
        
        if (!checkRes) {
            server.Server.KillTopicPqTablets(topicPath);
            server.DescribePartition(1, true, false);
        }
        server.DescribePartition(3, true, true);
        server.UseBadTopic = true;
        server.DescribePartition(0, true, true);

    }
    Y_UNIT_TEST(DescribeTopic) {
        auto server = TDescribeTestServer();
        Cerr << "Describe topic with stats and location\n";
        server.DescribeTopic(true, true);
        server.Server.KillTopicPqTablets(topicPath);
        Cerr << "Describe topic with stats\n";
        server.DescribeTopic(true, false);
        Cerr << "Describe topic with location\n";
        server.DescribeTopic(false, true);
        Cerr << "Describe topic with no stats or location\n";
        server.DescribeTopic(false, false);
        server.UseBadTopic = true;
        Cerr << "Describe bad topic\n";
        server.DescribeTopic(true, true);
    }
    Y_UNIT_TEST(DescribeConsumer) {
        auto server = TDescribeTestServer();
        server.AddConsumer("my-consumer");
        server.DescribeConsumer("my-consumer", true, true);
        server.Server.KillTopicPqTablets(topicPath);
        server.DescribeConsumer("my-consumer",true, false);
        server.DescribeConsumer("my-consumer",false, true);
        server.DescribeConsumer("my-consumer",false, false);
        server.UseBadTopic = true;
        server.DescribeConsumer("my-consumer",true, true);
    }
}

} // namespace
