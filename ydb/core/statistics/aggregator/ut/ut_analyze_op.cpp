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
                UNIT_ASSERT_VALUES_EQUAL(entry.PathsSize(), 1);
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

    Y_UNIT_TEST(AnalyzeSameOpIdDifferentDb) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        const TString sharedOpId = "opSame";
        const TString firstDb = "/Root/Database";
        const TString secondDb = "/Root/OtherTenant";

        // First analyze runs to completion → STATE_DONE in firstDb history.
        Analyze(runtime, saTabletId, {{pathId}}, sharedOpId, firstDb);
        {
            auto result = TestGetAnalyzeOp(runtime, saTabletId, firstDb, sharedOpId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.GetAnalyzeOperation().GetState(),
                Ydb::Table::AnalyzeState::STATE_DONE,
                "Expected first opId entry to be DONE");
        }

        // Second Analyze with same opId but a different database. The SA's existing
        // entry collides on opId but lives under a different database; TxAnalyze
        // must drop the older entry rather than replaying its cached terminal
        // response (which would belong to firstDb, not secondDb).
        TAnalyzedTable analyzedTable(pathId);
        auto req = MakeAnalyzeRequest({analyzedTable}, sharedOpId, secondDb);
        req->Record.MutableTables(0)->SetPath("/Root/Database/Table");
        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(saTabletId, sender, req.release());
        // Drain enough simulated time for the TxAnalyze to commit (the second
        // operation may or may not run a full traversal — we only care about the
        // collision resolution, not the analysis itself).
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        // The first entry (firstDb) was replaced: Get under firstDb is now NOT_FOUND.
        TestGetAnalyzeOp(runtime, saTabletId, firstDb, sharedOpId,
            Ydb::StatusIds::NOT_FOUND);

        // The new entry is present under secondDb (state is whatever the active
        // schedule produced — at minimum it must not be the cached DONE response
        // from the firstDb operation; we'd see that as STATE_DONE here if the
        // replay path had fired).
        auto result = TestGetAnalyzeOp(runtime, saTabletId, secondDb, sharedOpId);
        const auto state = result.GetAnalyzeOperation().GetState();
        UNIT_ASSERT_C(
            state == Ydb::Table::AnalyzeState::STATE_ENQUEUED ||
            state == Ydb::Table::AnalyzeState::STATE_IN_PROGRESS ||
            state == Ydb::Table::AnalyzeState::STATE_DONE ||
            state == Ydb::Table::AnalyzeState::STATE_FAILED,
            "Expected an actual entry for secondDb (any non-UNSPECIFIED state), got "
                << (int)state);
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
                UNIT_ASSERT_VALUES_EQUAL(entry.InProgressPathsSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL_C(entry.DonePathsSize(), entry.PathsSize(),
                    "DonePaths should equal Paths after completion");
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
        UNIT_ASSERT_VALUES_EQUAL(result.GetAnalyzeOperation().InProgressPathsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.GetAnalyzeOperation().DonePathsSize(),
            result.GetAnalyzeOperation().PathsSize(),
            "DonePaths should equal Paths after completion");
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
        // Internally the SA stored shardsDone=2 out of shardsTotal=4 → 50% progress.
        // The shard counters are not part of the public API; only progress is.
        UNIT_ASSERT_DOUBLES_EQUAL(op.GetProgress(), 50.0f, 0.01f);
        // The active table appears in InProgressPaths while traversal is running.
        UNIT_ASSERT_VALUES_EQUAL(op.InProgressPathsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(op.GetInProgressPaths(0), tableInfo.Path);
        UNIT_ASSERT_VALUES_EQUAL(op.DonePathsSize(), 0);
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

    Y_UNIT_TEST(CancelAndForgetNonTerminal) {
        //   * One active op (IN_PROGRESS) held mid-flight by blocking the SA-internal
        //     per-table result.
        //   * One queued op (ENQUEUED) that can't start while the active one runs.
        //   Then:
        //     1. Forget on the IN_PROGRESS op  -> PRECONDITION_FAILED.
        //     2. Forget on the ENQUEUED op     -> PRECONDITION_FAILED.
        //     3. Cancel the ENQUEUED op        -> SUCCESS, immediately STATE_CANCELLED
        //        (TxAnalyzeOpCancel::Execute: not active -> marks finished inline).
        //        The active op must remain IN_PROGRESS.
        //     4. Cancel the IN_PROGRESS op     -> SUCCESS synchronously; the state
        //        flip is deferred via DispatchFinishTraversalTx which marks the op
        //        STATE_CANCELLED and poisons the SA-internal AnalyzeActor via
        //        ResetTraversalState. Verified by polling Get until STATE_CANCELLED.
        constexpr int kShardCount = 4;
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        auto tableInfo = PrepareColumnTable(env, "Database", "Table", kShardCount);

        const ui64 saTabletId = tableInfo.SaTabletId;
        const TPathId pathId = tableInfo.PathId;
        const TString activeOpId = "opActive";
        const TString queuedOpId = "opQueued";

        TBlockEvents<TEvStatistics::TEvAnalyzeActorResult> blockResult(runtime);

        TAnalyzedTable analyzedTable(pathId);

        // Submit op1 and wait until it becomes the active force traversal.
        {
            auto req = MakeAnalyzeRequest({analyzedTable}, activeOpId, "/Root/Database");
            req->Record.MutableTables(0)->SetPath(tableInfo.Path);
            runtime.SendToPipe(saTabletId, runtime.AllocateEdgeActor(), req.release());
        }
        runtime.WaitFor("active AnalyzeActor running",
            [&]{ return !blockResult.empty(); });

        // Submit op2 — TxScheduleTraversal is a no-op because TraversalPathId is
        // already set for op1, so op2 sits in ForceTraversals as STATE_ENQUEUED.
        {
            auto req = MakeAnalyzeRequest({analyzedTable}, queuedOpId, "/Root/Database");
            req->Record.MutableTables(0)->SetPath(tableInfo.Path);
            runtime.SendToPipe(saTabletId, runtime.AllocateEdgeActor(), req.release());
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        // Sanity: states match the configuration.
        {
            auto active = TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", activeOpId);
            UNIT_ASSERT_VALUES_EQUAL_C(active.GetAnalyzeOperation().GetState(),
                Ydb::Table::AnalyzeState::STATE_IN_PROGRESS,
                "active op should be IN_PROGRESS");
            auto queued = TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", queuedOpId);
            UNIT_ASSERT_VALUES_EQUAL_C(queued.GetAnalyzeOperation().GetState(),
                Ydb::Table::AnalyzeState::STATE_ENQUEUED,
                "queued op should be ENQUEUED before active finishes");
        }

        // (1) Forget on IN_PROGRESS -> PRECONDITION_FAILED, state unchanged.
        auto activeForgetResp = TestForgetAnalyzeOp(runtime, saTabletId, "/Root/Database", activeOpId,
            Ydb::StatusIds::PRECONDITION_FAILED);
        // The error issue must carry S_ERROR severity (consistent with other handlers).
        UNIT_ASSERT_VALUES_EQUAL(activeForgetResp.IssuesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(activeForgetResp.GetIssues(0).severity(),
            static_cast<ui32>(NYql::TSeverityIds::S_ERROR));
        UNIT_ASSERT_VALUES_EQUAL_C(
            TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", activeOpId)
                .GetAnalyzeOperation().GetState(),
            Ydb::Table::AnalyzeState::STATE_IN_PROGRESS,
            "rejected Forget must not change IN_PROGRESS state");

        // (2) Forget on ENQUEUED -> PRECONDITION_FAILED, state unchanged.
        auto queuedForgetResp = TestForgetAnalyzeOp(runtime, saTabletId, "/Root/Database", queuedOpId,
            Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT_VALUES_EQUAL(queuedForgetResp.IssuesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queuedForgetResp.GetIssues(0).severity(),
            static_cast<ui32>(NYql::TSeverityIds::S_ERROR));
        UNIT_ASSERT_VALUES_EQUAL_C(
            TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", queuedOpId)
                .GetAnalyzeOperation().GetState(),
            Ydb::Table::AnalyzeState::STATE_ENQUEUED,
            "rejected Forget must not change ENQUEUED state");

        // (3) Cancel queued op: synchronous transition to STATE_CANCELLED;
        // active op untouched.
        TestCancelAnalyzeOp(runtime, saTabletId, "/Root/Database", queuedOpId,
            Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL_C(
            TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", queuedOpId)
                .GetAnalyzeOperation().GetState(),
            Ydb::Table::AnalyzeState::STATE_CANCELLED,
            "queued op should be CANCELLED right after cancel");
        UNIT_ASSERT_VALUES_EQUAL_C(
            TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", activeOpId)
                .GetAnalyzeOperation().GetState(),
            Ydb::Table::AnalyzeState::STATE_IN_PROGRESS,
            "active op must remain IN_PROGRESS after a queued sibling is cancelled");

        // (4) Cancel active op: SUCCESS synchronously, but the state flip is
        // deferred via DispatchFinishTraversalTx. Unblock the analyze result so
        // the poisoned AnalyzeActor can finish tearing down, then poll until
        // STATE_CANCELLED is observed. DONE must never be observed.
        TestCancelAnalyzeOp(runtime, saTabletId, "/Root/Database", activeOpId,
            Ydb::StatusIds::SUCCESS);
        blockResult.Unblock();
        blockResult.Stop();

        bool foundCancelled = false;
        for (int i = 0; i < 60 && !foundCancelled; ++i) {
            const auto state =
                TestGetAnalyzeOp(runtime, saTabletId, "/Root/Database", activeOpId)
                    .GetAnalyzeOperation().GetState();
            UNIT_ASSERT_VALUES_UNEQUAL_C(
                state, Ydb::Table::AnalyzeState::STATE_DONE,
                "cancelled active op must not transition to DONE");
            if (state == Ydb::Table::AnalyzeState::STATE_CANCELLED) {
                foundCancelled = true;
                break;
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(foundCancelled,
            "cancelled active op did not transition to STATE_CANCELLED");
    }

} // Y_UNIT_TEST_SUITE(AnalyzeOpList)

} // NStat
} // NKikimr
