#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

using namespace NYdb;
using namespace NYdb::NPersQueue;


namespace NKikimr::NPersQueueTests {

class TPQv1CompatTestBase {
private:
    THolder<::NPersQueue::TTestServer> Server;
    THolder<TDriver> Driver;
    THolder<TPersQueueClient> PQClient;
    TString DbRoot;
    TString DbPath;
public:
    TPQv1CompatTestBase()
    {
        Server = MakeHolder<::NPersQueue::TTestServer>(false);
        DbRoot = "/Root/LbCommunal";
        DbPath = "/Root/LbCommunal/account";
        Server->ServerSettings.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(DbRoot);
        Server->ServerSettings.PQConfig.SetTestDatabaseRoot(DbRoot);
        Server->StartServer();
        //Server->EnableLogs()
        Server->EnableLogs({ NKikimrServices::KQP_PROXY }, NActors::NLog::PRI_EMERG);
        Server->EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_WRITE_PROXY });

        Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc2--account--topic1", 1, false);
        Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--account--topic1", 1, true);

        Server->WaitInit("account/topic1");
        Server->AnnoyingClient->MkDir("/Root", "LbCommunal");
        Server->AnnoyingClient->MkDir("/Root/LbCommunal", "account");
        Server->AnnoyingClient->CreateTopicNoLegacy(
                "/Root/LbCommunal/account/topic2", 1, false, true, {}, {}, "account"
        );

        Server->AnnoyingClient->CreateTopicNoLegacy(
                "/Root/LbCommunal/account/topic2-mirrored-from-dc2", 1, true, false, {}, {}, "account"
        );
        Server->AnnoyingClient->CreateConsumer("test-consumer");
        InitPQLib();
    }

    void InitPQLib() {
        TDriverConfig driverConfig(TStringBuilder() << "localhost:" << Server->GrpcPort);
        driverConfig.SetDatabase("/Root/");
        Driver.Reset(new TDriver(driverConfig));
        PQClient.Reset(new TPersQueueClient(*Driver));
    };

    void CreateTopic(const TString& path, const TString& account, bool canWrite, bool expectFail) {
        Server->AnnoyingClient->CreateTopicNoLegacy(
                path, 1, true, canWrite, {}, {}, account, expectFail
        );
    }

    std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(const TString& path) {
        Y_VERIFY(PQClient);
        TWriteSessionSettings settings;
        settings.Path(path).MessageGroupId("test-src");
        return PQClient->CreateSimpleBlockingWriteSession(settings);
    }

    std::shared_ptr<IReadSession> CreateReadSession(const TVector<TString>& paths) {
        Y_VERIFY(PQClient);


        TReadSessionSettings settings;
        settings.ConsumerName("test-consumer").ReadMirrored("dc1");
        for (const auto& path : paths) {
            settings.AppendTopics(TTopicReadSettings{path});
        }
        return PQClient->CreateReadSession(settings);
    }
};

Y_UNIT_TEST_SUITE(TPQCompatTest) {
    Y_UNIT_TEST(DiscoverTopics) {
        TPQv1CompatTestBase testServer;
        Cerr << "Create write session\n";
        auto ws = testServer.CreateWriteSession("account/topic2");
        Cerr << "Wait init seqNo\n";
        auto seqNo = ws->GetInitSeqNo();

        ws = testServer.CreateWriteSession("account/topic1");
        Cerr << "Wait init seqNo(2)\n";
        seqNo = ws->GetInitSeqNo();
        Y_UNUSED(seqNo);
    }

    void GetLocks(const THashSet<TString>& paths, std::shared_ptr<IReadSession>& readSession) {
        THashMap<TString, THashSet<TString>> clustersFound;
        for (auto i = 0u; i < paths.size() * 2 /* 2 locks from 2 clusters for each topic */; i++) {
            auto ev = readSession->GetEvent(true);
            Y_VERIFY(ev.Defined());
            auto* lockEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*ev);
            if (!lockEvent) {
                if (auto* otherEv = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*ev)) {
                    Cerr << "Got destroy event";
                } else if (auto* otherEv = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent>(&*ev)) {
                    Cerr << "Got part closed event\n";
                } else if (auto* otherEv = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*ev)) {
                    Cerr << "Got session closed event" << otherEv->DebugString() << Endl;
                }
            }
            UNIT_ASSERT(lockEvent);
            Cerr << "Got lock event: " << lockEvent->DebugString() << Endl;
            const auto& path = lockEvent->GetPartitionStream()->GetTopicPath();
            const auto& cluster = lockEvent->GetPartitionStream()->GetCluster();
            UNIT_ASSERT(paths.contains(path));
            auto& clusters = clustersFound[path];
            UNIT_ASSERT(!clusters.contains(cluster));
            clusters.insert(cluster);
            //lockEvent->Confirm();
        }
        UNIT_ASSERT_VALUES_EQUAL(clustersFound.size(), paths.size());
        for (const auto& [path, clusters] : clustersFound) {
            UNIT_ASSERT_VALUES_EQUAL(clusters.size(), 2);
            UNIT_ASSERT(clusters.contains("dc1"));
            UNIT_ASSERT(clusters.contains("dc2"));
        }

    }

    Y_UNIT_TEST(SetupLockSession) {
        TPQv1CompatTestBase testServer;
        {
            auto rs = testServer.CreateReadSession({"account/topic2"});
            GetLocks({"account/topic2"}, rs);
            rs->Close();
        }
        {
            auto rs = testServer.CreateReadSession({"account/topic1"});
            GetLocks({"account/topic1"}, rs);
            rs->Close();
        }
        {
            auto rs = testServer.CreateReadSession({"account/topic1", "account/topic2"});
            GetLocks({"account/topic1", "account/topic2"}, rs);
            rs->Close();
        }
    }

    Y_UNIT_TEST(BadTopics) {
        TPQv1CompatTestBase testServer;
        testServer.CreateTopic("/Root/PQ/some-topic", "", true, true);
        testServer.CreateTopic("/Root/PQ/some-topic-mirrored-from-dc2", "", false, true);
        testServer.CreateTopic("/Root/PQ/.some-topic/mirrored-from-dc2", "", false, true);

        //Bad dc
        testServer.CreateTopic("/Root/PQ/rt3.bad--account--topic", "", true, true);
        testServer.CreateTopic("/Root/PQ/rt3.bad--account--topic", "", false, true);

        // Local, writes disabled
        testServer.CreateTopic("/Root/PQ/rt3.dc1--account--topic", "", false, true);

        // Non-local, writes enabled
        testServer.CreateTopic("/Root/PQ/rt3.dc2--account--topic", "", true, true);

        // Local topic with client write disabled
        testServer.CreateTopic("/Root/LbCommunal/some-topic", "account", false, true);
        // Non-local topic with client write enabled
        testServer.CreateTopic("/Root/LbCommunal/some-topic-mirrored-from-dc2", "account", true, true);
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/mirrored-from-dc2", "account", true, true);
        // No account
        testServer.CreateTopic("/Root/LbCommunal/some-topic", "", true, true);
        // Mirrored-from local
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/mirrored-from-dc1", "account", false, true);
        testServer.CreateTopic("/Root/LbCommunal/some-topic-mirrored-from-dc1", "account", false, true);
        // Bad mirrored names
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/some-topic", "account", false, true);
        // Mirrored-from non-existing
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/mirrored-from-dc777", "account", false, true);
        testServer.CreateTopic("/Root/LbCommunal/some-topic-mirrored-from-dc777", "account", false, true);

        // Just to verify it even creates anything at all
        testServer.CreateTopic("/Root/LbCommunal/account/some-topic-mirrored-from-dc2", "account", false, false);

    }
}

} // namespace NKikimr::NPersQueueTests

