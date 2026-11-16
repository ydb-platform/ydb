#include "private_events.h"
#include "dst_alterer.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>
#include <util/datetime/base.h>
#include <atomic>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;

class TTopicMirroringFixture {
    static TFeatureFlags Flags() {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableReplicationControllerTopicMirroring(true);
        flags.FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
        return flags;
    }

    struct TSourceEnv: TEnv<> {
        TSourceEnv(): TEnv<>(Flags()) {
            UNIT_ASSERT_VALUES_EQUAL(CreateTopic("/Root", *MakeTopicDescription({.Name = "topic"})), NMsgBusProxy::MSTATUS_OK);
        }
    };

public:
    TSourceEnv Source;
    TEnv<> Destination;
    const ui64 SchemeShard = Destination.GetPathId("/Root").OwnerId;

    explicit TTopicMirroringFixture(const TString& sourceHost = "localhost", bool stopBeforeCreateResult = false)
        : Destination(Flags(), true, stopBeforeCreateResult ? ERuntimeMode::Simulated : ERuntimeMode::RealThreads)
    {
        auto& app = Destination.GetRuntime().GetAppData(0);
        app.PersQueueMirrorReaderFactory->Initialize(
            Destination.GetRuntime().GetAnyNodeActorSystem(), app.PQConfig.GetMirrorConfig().GetPQLibSettings());
        std::atomic<bool> createResultBlocked = false;
        auto observer = Destination.GetRuntime().AddObserver<TEvPrivate::TEvCreateDstResult>(
            [&](TEvPrivate::TEvCreateDstResult::TPtr& ev) {
                if (stopBeforeCreateResult && ev->Get()->IsSuccess()) {
                    ev.Reset();
                    createResultBlocked = true;
                }
            });
        const auto endpoint = sourceHost + Source.GetEndpoint().substr(Source.GetEndpoint().rfind(':'));
        Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/topic` AS `/Root/topic`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", endpoint.c_str()));
        if (stopBeforeCreateResult) {
            Wait([&] { return createResultBlocked.load(); });
            return;
        }
        WaitMirror(true);
        UNIT_ASSERT(Config().GetPartitionConfig().GetMirrorFrom().GetMirrorConsumerOffsets());
    }

    void Exec(const TString& query) {
        NYdb::NTable::TTableClient client(Destination.GetDriver());
        auto session = Destination.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        const auto result = Destination.RunCall([&] { return session.ExecuteSchemeQuery(query).GetValueSync(); });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    NKikimrPQ::TPQTabletConfig Config() {
        const auto result = Destination.Send<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(SchemeShard,
            new NSchemeShard::TEvSchemeShard::TEvDescribeScheme("/Root/topic"));
        return result->Get()->GetRecord().GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
    }

    template <typename TCondition>
    void Wait(const TCondition& ready) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        while (!ready()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for topic mirroring");
            if (Destination.GetRuntime().IsRealThreads()) {
                Sleep(TDuration::MilliSeconds(10));
            } else {
                Destination.GetRuntime().SimulateSleep(TDuration::MilliSeconds(10));
            }
        }
    }

    void WaitMirror(bool enabled) {
        Wait([&] { return Config().GetPartitionConfig().HasMirrorFrom() == enabled; });
    }

    void Done() {
        Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');");
        Wait([&] { return Destination.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetState().HasDone(); });
    }

    void CheckMissingDestination() {
        Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        WaitMirror(false);
        const auto pathId = Destination.GetPathId("/Root/topic");
        NYdb::NTopic::TTopicClient client(Destination.GetDriver());
        const auto dropped = client.DropTopic("/Root/topic").GetValueSync();
        UNIT_ASSERT_C(dropped.IsSuccess(), dropped.GetIssues().ToString());
        for (const auto state : {TReplication::EState::Ready, TReplication::EState::Done}) {
            Destination.GetRuntime().Register(CreateDstAlterer(Destination.GetSender(), SchemeShard,
                1, 1, TReplication::ETargetKind::Topic, pathId, state));
            const auto result = Destination.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvAlterDstResult>(Destination.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, state == TReplication::EState::Ready
                ? NKikimrScheme::StatusPathDoesNotExist : NKikimrScheme::StatusSuccess);
        }
    }

    void WriteAndWait(const TString& payload, ui64 endOffset) {
        NYdb::NTopic::TTopicClient sourceClient(Source.GetDriver()), destinationClient(Destination.GetDriver());
        auto writer = sourceClient.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
            .Path("/Root/topic").ProducerId("producer").MessageGroupId("producer").Codec(NYdb::NTopic::ECodec::RAW));
        UNIT_ASSERT(writer->Write(payload));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
        Wait([&] {
            const auto result = destinationClient.DescribeTopic("/Root/topic",
                NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& stats = result.GetTopicDescription().GetPartitions().front().GetPartitionStats();
            return stats && stats->GetEndOffset() == endOffset;
        });
    }
};

Y_UNIT_TEST_SUITE(TopicMirroring) {
    Y_UNIT_TEST(DoneBeforeCreateResultDoesNotStartMirroring) {
        TTopicMirroringFixture test("localhost", true);
        test.Done();
        UNIT_ASSERT(!test.Config().GetPartitionConfig().HasMirrorFrom());
    }

    Y_UNIT_TEST(MissingDestinationCannotResume) {
        TTopicMirroringFixture test;
        test.CheckMissingDestination();
    }

    Y_UNIT_TEST(CopiesMessagesAcrossPauseAndResume) {
        TTopicMirroringFixture test("[::1]");
        test.WriteAndWait("first", 1);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        test.WaitMirror(false);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'STANDBY');");
        test.WaitMirror(true);
        test.WriteAndWait("second", 2);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');");
        test.WaitMirror(false);
        test.Exec("DROP ASYNC REPLICATION `/Root/replication`;");
    }
}
}
}
