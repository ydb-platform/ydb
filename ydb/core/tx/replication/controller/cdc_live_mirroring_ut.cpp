#include "cdc_trim_verifier.h"
#include "private_events.h"
#include "public_events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_topic.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;

class TLiveCdcMirror {
    static constexpr const char* Consumer = "application";
    static constexpr const char* SourceFeed = "/Root/source/feed";
    static constexpr const char* DestinationFeed = "/Root/replica/feed";

    static NYdb::NTable::TSession Session(TEnv<>& env) {
        NYdb::NTable::TTableClient client(env.GetDriver());
        const auto result = client.CreateSession().GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result.GetSession();
    }

    static void Scheme(TEnv<>& env, const TString& query) {
        const auto result = Session(env).ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    static auto Execute(TEnv<>& env, const TString& query) {
        auto result = Session(env).ExecuteDataQuery(query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result;
    }

    void WaitForState(NKikimrReplication::TReplicationState::StateCase state) {
        const auto description = Destination.GetDescription("/Root/replication").GetPathDescription();
        const auto controllerId = description.GetReplicationDescription().GetControllerId();
        const auto pathId = Destination.GetPathId("/Root/replication");

        WaitForReplication(Destination, [&] {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            pathId.ToProto(request->Record.MutablePathId());
            const auto result = Destination.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
            const auto& record = result->Get()->Record;
            UNIT_ASSERT(record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
            UNIT_ASSERT_C(!record.GetState().HasError(), record.GetState().ShortDebugString());
            return record.GetState().GetStateCase() == state;
        }, "replication controller state");
    }

    void CheckOffsets(ui64 end, ui64 start = 0) {
        WaitForReplication(Destination, [&] {
            const auto offsets = GetTopicOffsets(Destination, DestinationFeed, Consumer);
            return offsets && offsets->Start == start && offsets->End == end && offsets->Committed == 1;
        }, "destination data and consumer offsets");
    }

public:
    TLiveCdcMirror()
        : Source(TopicReplicationFlags())
        , Destination(TopicReplicationFlags())
    {
        auto& runtime = Destination.GetRuntime();
        runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), runtime.Register(CreateReplicationService()));
        Scheme(Source, "CREATE TABLE `/Root/source` (key Uint32, value Uint32, PRIMARY KEY (key));");
        Scheme(Source, "ALTER TABLE `/Root/source` ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES');");
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        NYdb::NTopic::TAlterTopicSettings consumers;
        consumers.BeginAddConsumer(Consumer);
        const auto added = client.AlterTopic(SourceFeed, consumers).GetValueSync();
        UNIT_ASSERT_C(added.IsSuccess(), added.GetIssues().ToString());
    }

    void InitialScanAndMirror() {
        Execute(Source, "UPSERT INTO `/Root/source` (key, value) VALUES (1u, 10u);");
        const auto first = ReadTopic(Source, SourceFeed, 1).front();
        UNIT_ASSERT_VALUES_EQUAL(first.Data, R"({"update":{"value":10},"key":[1]})");
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        const auto commit = client.CommitOffset(SourceFeed, 0, Consumer, 1).GetValueSync();
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());

        Scheme(Destination, Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", Source.GetEndpoint().c_str()));
        WaitForReplication(Destination, [&] {
            return Destination.GetDescription(DestinationFeed).GetSchemeStatus() == NKikimrScheme::StatusSuccess;
        }, "automatic destination CDC creation");
        WaitForReplication(Destination, [&] {
            return Execute(Destination, "SELECT key FROM `/Root/replica` WHERE key = 1u AND value = 10u;")
                .GetResultSet(0).RowsCount() == 1;
        }, "initially scanned table row");
        const auto mirrored = ReadTopic(Destination, DestinationFeed, 1).front();
        UNIT_ASSERT_VALUES_EQUAL(mirrored.Data, first.Data);
        UNIT_ASSERT_VALUES_EQUAL(mirrored.Offset, first.Offset);
        UNIT_ASSERT_VALUES_EQUAL(mirrored.Producer, first.Producer);
        UNIT_ASSERT_VALUES_EQUAL(mirrored.SeqNo, first.SeqNo);
        CheckOffsets(1);

        Execute(Source, "UPSERT INTO `/Root/source` (key, value) VALUES (2u, 20u);");
        WaitForReplication(Destination, [&] {
            return Execute(Destination, "SELECT key FROM `/Root/replica` WHERE key = 2u AND value = 20u;")
                .GetResultSet(0).RowsCount() == 1;
        }, "live replicated table row");
        const auto source = ReadTopic(Source, SourceFeed, 2);
        const auto destination = ReadTopic(Destination, DestinationFeed, 2);
        UNIT_ASSERT_VALUES_EQUAL(source[1].Data, R"({"update":{"value":20},"key":[2]})");
        for (size_t i = 0; i < source.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(destination[i].Data, source[i].Data);
            UNIT_ASSERT_VALUES_EQUAL(destination[i].Offset, i);
            UNIT_ASSERT_VALUES_EQUAL(destination[i].Producer, source[i].Producer);
            UNIT_ASSERT_VALUES_EQUAL(destination[i].SeqNo, source[i].SeqNo);
        }
        CheckOffsets(2); // Initial scan must not add another CDC record for the existing row.
    }

    void ForceDoneTrimsHistory() {
        const auto feedId = Destination.GetPathId(DestinationFeed);
        const TString topicPath = TString(DestinationFeed) + "/streamImpl";
        const auto topicId = Destination.GetPathId(topicPath);
        Scheme(Destination, "ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        WaitForState(NKikimrReplication::TReplicationState::kPaused);
        UNIT_ASSERT(!Destination.GetDescription(topicPath).GetPathDescription()
            .GetPersQueueGroup().GetPQTabletConfig().GetPartitionConfig().HasMirrorFrom());

        Execute(Source, "UPSERT INTO `/Root/source` (key, value) VALUES (3u, 30u);");
        UNIT_ASSERT_VALUES_EQUAL(ReadTopic(Source, SourceFeed, 3).back().Offset, 2);
        NYdb::NTopic::TTopicClient client(Source.GetDriver());
        const auto commit = client.CommitOffset(SourceFeed, 0, Consumer, 2).GetValueSync();
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        WaitForReplication(Source, [&] {
            const auto offsets = GetTopicOffsets(Source, SourceFeed, Consumer);
            return offsets && offsets->End == 3 && offsets->Committed == 2;
        }, "source history and consumer commit ahead of standby");
        CheckOffsets(2);

        const auto result = Session(Destination).ExecuteSchemeQuery(
            "ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE');").GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "FORCE");
        WaitForState(NKikimrReplication::TReplicationState::kPaused);

        Scheme(Destination, "ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');");
        WaitForState(NKikimrReplication::TReplicationState::kDone);
        CheckOffsets(2, 2);
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetPathId(DestinationFeed), feedId);
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetPathId(topicPath), topicId);

        Execute(Destination, "UPSERT INTO `/Root/replica` (key, value) VALUES (4u, 40u);");
        // Explicitly request offset zero: trimmed records must not be readable again.
        const auto native = ReadTopic(Destination, DestinationFeed, 1, 0).front();
        UNIT_ASSERT_VALUES_EQUAL(native.Offset, 2);
        UNIT_ASSERT_VALUES_EQUAL(native.Data, R"({"update":{"value":40},"key":[4]})");
        CheckOffsets(3, 2); // The application consumer still has its pre-trim committed offset 1.
    }

private:
    TEnv<> Source;
    TEnv<> Destination;
};

Y_UNIT_TEST_SUITE(CdcLiveMirroring) {
    Y_UNIT_TEST_TWIN(TrimVerifierPollsUntilCommitted, ConflictingId) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto parent = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakePipePerNodeCacheID(false), parent);

        NKikimrSchemeOp::TPersQueueGroupDescription topic;
        topic.SetTotalGroupCount(1);
        auto* partition = topic.AddPartitions();
        partition->SetTabletId(1);
        partition->SetPartitionId(0);
        const ui64 expected = 10;
        const auto verifier = runtime.Register(CreateCdcTrimVerifier(parent, TPathId(1, 2), expected, topic));
        runtime.EnableScheduleForActor(verifier);

        for (const ui64 committed : {ui64{0}, ConflictingId ? expected + 1 : expected}) {
            runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(parent);
            auto response = MakeHolder<TEvPersQueue::TEvStatusResponse>();
            response->Record.SetTabletId(1);
            auto* part = response->Record.AddPartResult();
            part->SetPartition(0);
            part->SetCdcReplicationTrimTxId(committed);
            runtime.Send(verifier, parent, response.Release());
        }

        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCdcTrimVerified>(parent);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Success, !ConflictingId);
    }

    Y_UNIT_TEST(InitialScanMirrorsCdcHistoryAndConsumerOffset) {
        TLiveCdcMirror test;
        test.InitialScanAndMirror();
        test.ForceDoneTrimsHistory();
    }
}
}
}
