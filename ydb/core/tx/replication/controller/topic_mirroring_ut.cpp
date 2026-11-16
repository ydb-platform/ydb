#include "private_events.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>
#include <util/datetime/base.h>

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

public:
    TEnv<> Source{Flags()};
    TEnv<> Destination{Flags()};
    const ui64 SchemeShard = Destination.GetPathId("/Root").OwnerId;

    TTopicMirroringFixture() {
        auto& app = Destination.GetRuntime().GetAppData(0);
        app.PersQueueMirrorReaderFactory->Initialize(
            Destination.GetRuntime().GetAnyNodeActorSystem(), app.PQConfig.GetMirrorConfig().GetPQLibSettings());
        UNIT_ASSERT_VALUES_EQUAL(Source.CreateTopic("/Root", *MakeTopicDescription({.Name = "topic"})), NMsgBusProxy::MSTATUS_OK);
        Exec(Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/topic` AS `/Root/topic`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", Source.GetEndpoint().c_str()));
        WaitMirror(true);
        UNIT_ASSERT(Config().GetPartitionConfig().GetMirrorFrom().GetMirrorConsumerOffsets());
    }

    void Exec(const TString& query) {
        NYdb::NTable::TTableClient client(Destination.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
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
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    void WaitMirror(bool enabled) {
        Wait([&] { return Config().GetPartitionConfig().HasMirrorFrom() == enabled; });
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
    Y_UNIT_TEST(CopiesMessagesAcrossPauseAndResume) {
        TTopicMirroringFixture test;
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
