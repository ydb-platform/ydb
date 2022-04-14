#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_pqlib.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/data_writer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>

namespace NKikimr::NPersQueueTests {

using namespace NPersQueue::NTests;

class TPQv0CompatTestBase {
public:
    THolder<TTestPQLib> PQLib;
    THolder<::NPersQueue::TTestServer> Server;
    TString OriginalLegacyName1;
    TString OriginalModernName1;
    TString MirroredLegacyName1;
    TString MirroredModernName1;
    TString ShortName1;

    TString OriginalLegacyName2;
    TString OriginalModernName2;
    TString MirroredLegacyName2;
    TString MirroredModernName2;
    TString ShortName2;

public:
    TPQv0CompatTestBase()
    {
        Server = MakeHolder<::NPersQueue::TTestServer>(false);
        Server->ServerSettings.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/LB");
        Server->ServerSettings.PQConfig.SetCheckACL(false);
        Server->StartServer();
        Server->EnableLogs({ NKikimrServices::KQP_PROXY }, NActors::NLog::PRI_EMERG);
        Server->EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);
        Server->EnableLogs({ NKikimrServices::PQ_METACACHE }, NActors::NLog::PRI_DEBUG);
        OriginalLegacyName1 = "rt3.dc1--account--topic1";
        MirroredLegacyName1 = "rt3.dc2--account--topic1";
        OriginalModernName1 = "/Root/LB/account/topic1";
        MirroredModernName1 = "/Root/LB/account/.topic2/mirrored-from-dc2";
        ShortName1 = "account/topic1";

        OriginalLegacyName2 = "rt3.dc1--account--topic2";
        MirroredLegacyName2 = "rt3.dc2--account--topic2";
        OriginalModernName2 = "/Root/LB/account/topic2";
        MirroredModernName2 = "/Root/LB/account/.topic2/mirrored-from-dc2";
        ShortName2 = "account/topic2";

        Server->AnnoyingClient->CreateTopicNoLegacy(OriginalLegacyName1, 1, false);
        Server->AnnoyingClient->CreateTopicNoLegacy(MirroredLegacyName1, 1, false);
        Server->AnnoyingClient->CreateTopicNoLegacy(OriginalModernName2, 1, true, true, "dc1");
        Server->AnnoyingClient->CreateTopicNoLegacy(MirroredModernName2, 1, true, true, "dc2");
        Server->AnnoyingClient->CreateConsumer("test-consumer");
        InitPQLib();
    }
    void InitPQLib() {
        PQLib = MakeHolder<TTestPQLib>(*Server);
        TPQDataWriter writer{OriginalLegacyName1, ShortName1, "test", *Server};
        writer.WaitWritePQServiceInitialization();
    };
};

Y_UNIT_TEST_SUITE(TPQCompatTest) {
    Y_UNIT_TEST(DiscoverTopics) {
        TPQv0CompatTestBase testServer;
        Cerr << "Create producer\n";
        {
            auto [producer, res] = testServer.PQLib->CreateProducer(testServer.ShortName2, "123", {}, ::NPersQueue::ECodec::RAW);
            Cerr << "Got response: " << res.Response.ShortDebugString() << Endl;
            UNIT_ASSERT(res.Response.HasInit());
        }
        Cerr << "Create producer(2)\n";
        {
            auto [producer, res] = testServer.PQLib->CreateProducer(testServer.ShortName1, "123", {}, ::NPersQueue::ECodec::RAW);
            UNIT_ASSERT(res.Response.HasInit());
        }
    }

    Y_UNIT_TEST(SetupLockSession) {
        TPQv0CompatTestBase server{};
        auto [consumer, startResult] = server.PQLib->CreateConsumer({server.ShortName1}, "test-consumer", 1, true);
        Cerr << startResult.Response << "\n";
        for (ui32 i = 0; i < 2; ++i) {
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << "Response: " << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Response.HasLock());
            UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == server.OriginalLegacyName1
                        || msg.GetValue().Response.GetLock().GetTopic() == server.MirroredLegacyName1);
            UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
        }
        auto msg = consumer->GetNextMessage();
        UNIT_ASSERT(!msg.Wait(TDuration::Seconds(1)));
        server.Server->AnnoyingClient->AlterTopic(server.MirroredLegacyName1, 2);
        msg.Wait();
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == server.MirroredLegacyName1);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 1);
    }

    Y_UNIT_TEST(LegacyRequests) {
        TPQv0CompatTestBase server{};
        server.Server->AnnoyingClient->GetPartOffset(
                {
                    {server.OriginalLegacyName1, {0}},
                    {server.MirroredLegacyName1, {0}},
                    {server.OriginalLegacyName2, {0}},
                    {server.MirroredLegacyName2, {0}},
                },
                4, 0, true
        );
        server.Server->AnnoyingClient->SetClientOffsetPQ(server.OriginalLegacyName2, 0, 5);
        server.Server->AnnoyingClient->SetClientOffsetPQ(server.MirroredLegacyName2, 0, 5);

            server.Server->AnnoyingClient->GetPartOffset(
                    {
                            {server.OriginalLegacyName2, {0}},
                            {server.MirroredLegacyName2, {0}},
                    },
                    2, 2, true
            );
    }
}
} //namespace NKikimr::NPersQueueTests;
