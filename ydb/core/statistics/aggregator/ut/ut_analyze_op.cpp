#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

template<>
void Out<Ydb::Table::AnalyzeState_State>(IOutputStream& o, Ydb::Table::AnalyzeState_State s) {
    o << static_cast<int>(s);
}

namespace NKikimr {
namespace NStat {

namespace {

void PrepareTable(TTestEnv& env, const TString& tableName) {
    CreateUniformTable(env, "Database", tableName);
}

} // namespace

Y_UNIT_TEST_SUITE(AnalyzeOpList) {

    Y_UNIT_TEST(ListEmpty) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");

        ui64 saTabletId;
        ResolvePathId(runtime, "/Root/Database", nullptr, &saTabletId);

        auto result = TestListAnalyzeOps(runtime, saTabletId, "/Root/Database");
        UNIT_ASSERT_VALUES_EQUAL(result.EntriesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.GetNextPageToken(), "");
    }

    Y_UNIT_TEST(ListAfterAnalyze) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        const TString opId = "op1";
        TAnalyzedTable analyzedTable(pathId);
        auto req = MakeAnalyzeRequest({analyzedTable}, opId, "/Root/Database");
        // Set path in the table proto
        req->Record.MutableTables(0)->SetPath("/Root/Database/Table");
        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(saTabletId, sender, req.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        auto result = TestListAnalyzeOps(runtime, saTabletId, "/Root/Database");
        UNIT_ASSERT_GE(result.EntriesSize(), 1);
        bool found = false;
        for (int i = 0; i < (int)result.EntriesSize(); ++i) {
            const auto& entry = result.GetEntries(i);
            if (entry.GetOperationId() == opId) {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(entry.GetTablesTotal(), 1);
                UNIT_ASSERT_GE(entry.PathsSize(), 1);
                break;
            }
        }
        UNIT_ASSERT_C(found, "Operation not found in list");
    }

    Y_UNIT_TEST(ListUnknownDb) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");

        ui64 saTabletId;
        ResolvePathId(runtime, "/Root/Database", nullptr, &saTabletId);

        // An empty database name returns BAD_REQUEST
        TestListAnalyzeOps(runtime, saTabletId, "", 100, {}, Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(FeatureFlagOff) {
        const TString opId = "opOff";
        TTestEnv env(1, 1, /*useRealThreads=*/false,
            [](Tests::TServerSettings& settings) {
                settings.FeatureFlags.SetEnableAnalyzeLongRunningOperation(false);
            });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{pathId}}, opId, "/Root/Database");

        TestListAnalyzeOps(runtime, saTabletId, "/Root/Database", 100, {},
            Ydb::StatusIds::UNSUPPORTED);
        TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", opId,
            Ydb::StatusIds::UNSUPPORTED);
        TestCancelAnalyzeOp(runtime, saTabletId, "/Root/Database", opId,
            Ydb::StatusIds::UNSUPPORTED);
        TestForgetAnalyzeOp(runtime, saTabletId, "/Root/Database", opId,
            Ydb::StatusIds::UNSUPPORTED);

        // Turn the flag back on so the Get handler proceeds to the lookup, then verify
        // that the completed operation was deleted rather than
        // retained as terminal history.
        for (ui32 nodeIdx = 0; nodeIdx < runtime.GetNodeCount(); ++nodeIdx) {
            runtime.GetAppData(nodeIdx).FeatureFlags.SetEnableAnalyzeLongRunningOperation(true);
        }
        TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", opId,
            Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(GetNotFound) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");

        ui64 saTabletId;
        ResolvePathId(runtime, "/Root/Database", nullptr, &saTabletId);

        TString unknownId("1234567890123456"); // 16 bytes, but no such op
        TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", unknownId,
            Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(ListTerminalHistory) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        // Run a full analyze and wait for completion
        Analyze(runtime, saTabletId, {{pathId}}, "opHistory", "/Root/Database");

        // Should still be listed as DONE
        auto result = TestListAnalyzeOps(runtime, saTabletId, "/Root/Database");
        bool foundDone = false;
        for (int i = 0; i < (int)result.EntriesSize(); ++i) {
            const auto& entry = result.GetEntries(i);
            if (entry.GetOperationId() == "opHistory") {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    entry.GetState(), Ydb::Table::AnalyzeState::STATE_DONE,
                    "Expected STATE_DONE after completion");
                UNIT_ASSERT_DOUBLES_EQUAL(entry.GetProgress(), 100.0f, 0.01f);
                foundDone = true;
                break;
            }
        }
        UNIT_ASSERT_C(foundDone, "DONE operation not found in history");
    }

    Y_UNIT_TEST(ProgressAfterDone) {
        // After analyze completes, the operation is visible as DONE with progress = 100.
        // Note: TEvAnalyzeActorProgress is a local event (not serializable over pipe),
        // so progress field testing is done via the full end-to-end analyze flow here.
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{pathId}}, "opProgress", "/Root/Database");

        auto result = TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", "opProgress");
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), Ydb::StatusIds::SUCCESS,
            result.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.GetAnalyzeOperation().GetState(),
            Ydb::Table::AnalyzeState::STATE_DONE,
            "Expected STATE_DONE");
        UNIT_ASSERT_DOUBLES_EQUAL(result.GetAnalyzeOperation().GetProgress(), 100.0f, 0.01f);
    }

    Y_UNIT_TEST(ProgressIntermediateColumnShards) {
        // For a column table with several shards the AnalyzeActor reports progress
        // to the Statistics Aggregator as each shard's scan completes. Verify that
        // intermediate progress (0 < ShardsDone < ShardsTotal) is reflected by
        // GetAnalyzeOperation while the analyze is still in flight.
        constexpr int kShardCount = 4;
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        auto tableInfo = PrepareColumnTable(env, "Database", "Table", kShardCount);

        const ui64 saTabletId = tableInfo.SaTabletId;
        const TPathId pathId = tableInfo.PathId;
        const TString opId = "opInter";

        // Keep the operation in IN_PROGRESS by stalling per-table results delivered
        // from the AnalyzeActor to the Statistics Aggregator.
        TBlockEvents<TEvStatistics::TEvAnalyzeActorResult> blockResult(runtime);

        // Allow progress events with ShardsDone in {0, 1, 2}; block ShardsDone >= 3.
        // Once a (4, >=3) event is observed (blocked), the SA has already processed
        // the prior (4, 0), (4, 1), (4, 2) events from the AnalyzeActor's mailbox
        // and stored ShardsDone = 2.
        TBlockEvents<TEvStatistics::TEvAnalyzeActorProgress> blockHigh(runtime,
            [](auto& ev) {
                return ev->Get()->ShardsDone >= 3;
            });

        TAnalyzedTable analyzedTable(pathId);
        auto req = MakeAnalyzeRequest({analyzedTable}, opId, "/Root/Database");
        req->Record.MutableTables(0)->SetPath(tableInfo.Path);
        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(saTabletId, sender, req.release());

        // Wait until the AnalyzeActor has tried to report ShardsDone >= 3.
        runtime.WaitFor("intermediate progress reported",
            [&]{ return !blockHigh.empty(); });

        auto result = TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", opId);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), Ydb::StatusIds::SUCCESS,
            result.ShortDebugString());

        const auto& op = result.GetAnalyzeOperation();
        UNIT_ASSERT_VALUES_EQUAL_C(op.GetState(),
            Ydb::Table::AnalyzeState::STATE_IN_PROGRESS,
            "Expected STATE_IN_PROGRESS while analyze is mid-flight");
        UNIT_ASSERT_VALUES_EQUAL(op.GetShardsTotal(), kShardCount);
        UNIT_ASSERT_VALUES_EQUAL(op.GetShardsDone(), 2);
        // 2 / 4 = 50%
        UNIT_ASSERT_DOUBLES_EQUAL(op.GetProgress(), 50.0f, 0.01f);
    }

    Y_UNIT_TEST(ForgetTerminal) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{pathId}}, "opForget", "/Root/Database");

        // Should be listed
        {
            auto result = TestListAnalyzeOps(runtime, saTabletId, "/Root/Database");
            bool found = false;
            for (int i = 0; i < (int)result.EntriesSize(); ++i) {
                if (result.GetEntries(i).GetOperationId() == "opForget") {
                    found = true;
                    break;
                }
            }
            UNIT_ASSERT_C(found, "Op not found before forget");
        }

        // Forget it
        TestForgetAnalyzeOp(runtime, saTabletId, "/Root/Database", "opForget");

        // Should be gone
        TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", "opForget",
            Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(CancelDone) {
        // Cancelling an already-done operation is idempotent (returns SUCCESS).
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{pathId}}, "opCancel", "/Root/Database");

        // Operation should be DONE; cancelling it returns SUCCESS (idempotent)
        TestCancelAnalyzeOp(runtime, saTabletId, "/Root/Database", "opCancel",
            Ydb::StatusIds::SUCCESS);

        // State should still be DONE
        auto result = TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", "opCancel");
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.GetAnalyzeOperation().GetState(),
            Ydb::Table::AnalyzeState::STATE_DONE,
            "Cancel of DONE op should not change state");
    }

} // Y_UNIT_TEST_SUITE(AnalyzeOpList)

} // NStat
} // NKikimr
