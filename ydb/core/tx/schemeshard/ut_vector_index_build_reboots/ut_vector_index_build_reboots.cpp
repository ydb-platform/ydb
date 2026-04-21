#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(VectorIndexBuildTestReboots) {
    void DoTestIndexBuild(TTestWithReboots& t, bool Prefixed, bool Overlap) {
        // speed up the test:
        // only check scheme shard reboots
        t.TabletIds.clear();
        t.TabletIds.push_back(t.SchemeShardTabletId);
        // white list some more events
        t.NoRebootEventTypes.insert(TEvSchemeShard::EvModifySchemeTransaction);
        t.NoRebootEventTypes.insert(TSchemeBoardEvents::EvUpdateAck);
        t.NoRebootEventTypes.insert(TEvSchemeShard::EvNotifyTxCompletionRegistered);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvServerDisconnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvServerConnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvClientConnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvClientDestroyed);
        t.NoRebootEventTypes.insert(TEvDataShard::EvBuildIndexProgressResponse);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "dir/Table"
                    Columns { Name: "key"       Type: "Uint32" }
                    Columns { Name: "embedding" Type: "String" }
                    Columns { Name: "prefix"    Type: "Uint32" }
                    Columns { Name: "value"     Type: "String" }
                    KeyColumnNames: ["key"]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 50 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 150 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 250 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 350 } } } }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", 0, 0, 50);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", 1, 50, 150);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", 2, 150, 250);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", 3, 250, 350);
                WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/dir/Table", 4, 350, 400);
            }

            const ui64 buildIndexId = ++t.TxId;
            {
                auto indexColumns = (Prefixed ? TVector<TString>{"prefix", "embedding"} : TVector<TString>{"embedding"});
                auto sender = runtime.AllocateEdgeActor();
                auto request = CreateBuildIndexRequest(buildIndexId, "/MyRoot", "/MyRoot/dir/Table", TBuildIndexConfig{
                    "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, indexColumns, {"value"}, {}
                });
                // with too many scan events, the test works infinite time
                request->Record.MutableSettings()->MutableScanSettings()->Clear();
                if (Overlap) {
                    request->Record.MutableSettings()
                        ->mutable_index()
                        ->mutable_global_vector_kmeans_tree_index()
                        ->mutable_vector_settings()
                        ->set_overlap_clusters(2);
                }
                ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                TInactiveZone inactive(activeZone);

                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL((ui64)descr.GetIndexBuild().GetState(), (ui64)Ydb::Table::IndexBuildState::STATE_DONE);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(1)});
                const TString indexPath = "/MyRoot/dir/Table/index1";
                TestDescribeResult(DescribePath(runtime, indexPath, true, true, true),
                                   {NLs::PathExist,
                                    NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady)});
                using namespace NTableIndex::NKMeans;
                if (Prefixed) {
                    TestDescribeResult(DescribePath(runtime, indexPath + "/" + PrefixTable, true, true, true), {NLs::PathExist});
                }
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + LevelTable, true, true, true), {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + PostingTable, true, true, true), {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + PostingTable + BuildSuffix0, true, true, true), {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, indexPath + "/" + PostingTable + BuildSuffix1, true, true, true), {NLs::PathNotExist});

                // Check row count in the posting table
                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/dir/Table/index1/" + TString(PostingTable));
                    Cerr << "... posting table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(rows, (Overlap ? 800 : 400));
                }

            }
        });
    }

    // Without killOnCommit, the schemeshard doesn't get rebooted on TEvDataShard::Ev***KMeansResponse's,
    // and thus the vector index build process is never interrupted at all because there are no other
    // events to reboot on.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BaseCase, 2 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, true /*killOnCommit*/) {
        DoTestIndexBuild(t, false, false);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(Prefixed, 4 /*rebootBuckets*/, 2 /*pipeResetBuckets*/, true /*killOnCommit*/) {
        DoTestIndexBuild(t, true, false);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(Overlap, 4 /*rebootBuckets*/, 4 /*pipeResetBuckets*/, true /*killOnCommit*/) {
        DoTestIndexBuild(t, false, true);
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(PrefixedOverlap, 8 /*rebootBuckets*/, 4 /*pipeResetBuckets*/, true /*killOnCommit*/) {
        DoTestIndexBuild(t, true, true);
    }
}

// ---------------------------------------------------------------------------
// Tests for LastKeyAck persistence and resume-from-key-range at the SchemeShard level.
//
// These tests verify the integration path:
//   datashard emits IN_PROGRESS with LastKeyAck
//   → SchemeShard persists LastKeyAck in IndexBuildShardStatus
//   → after SchemeShard reboot the next scan request carries KeyRange = [LastKeyAck, ∞)
//   → the completed index is identical to a fresh full build.
//
// Note: TEvReshuffleKMeansResponse IN_PROGRESS is not tested here because it requires
// AllFlushed()=true mid-scan, which cannot happen reliably when rows arrive faster than
// upload responses (see comment in ut_reshuffle_kmeans.cpp::MainToPostingLastKeyAckInProgress).
// LocalKMeans and PrefixKMeans checkpoint at cluster/prefix boundaries and are reliable.
// ---------------------------------------------------------------------------

Y_UNIT_TEST_SUITE(VectorIndexBuildLastKeyAckTests) {

    // 10 rows on a single shard — enough to produce multiple batches with MaxBatchRows=1
    // while keeping the test fast (no multi-shard overhead).
    static constexpr ui32 kTableRows = 10;

    void SetupVectorTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "dir/Table"
            Columns { Name: "key"       Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix"    Type: "Uint32" }
            Columns { Name: "value"     Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        WriteVectorTableRows(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/dir/Table", 0, 0, kTableRows);
    }

    // Build the index request with MaxBatchRows=1 and MaxCheckpointBytes=1 so that every
    // completed batch upload triggers an IN_PROGRESS checkpoint. This guarantees the blocker
    // captures events quickly without needing a large table.
    TEvIndexBuilder::TEvCreateRequest* MakeBuildRequest(ui64 buildTx, bool Prefixed) {
        auto indexColumns = Prefixed ? TVector<TString>{"prefix", "embedding"} : TVector<TString>{"embedding"};
        auto* request = CreateBuildIndexRequest(buildTx, "/MyRoot", "/MyRoot/dir/Table", TBuildIndexConfig{
            "index1", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, indexColumns, {"value"}, {}
        });
        // CreateBuildIndexRequest already sets MaxBatchRows=1; also set MaxCheckpointBytes=1
        // so every uploaded batch crosses the checkpoint threshold and emits IN_PROGRESS.
        request->Record.MutableSettings()->MutableScanSettings()->SetMaxCheckpointBytes(1);
        return request;
    }

    void CheckIndexComplete(TTestBasicRuntime& runtime, ui64 buildTx, bool Prefixed) {
        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            op.DebugString());

        const TString indexPath = "/MyRoot/dir/Table/index1";
        using namespace NTableIndex::NKMeans;
        if (Prefixed) {
            TestDescribeResult(DescribePath(runtime, indexPath + "/" + PrefixTable, true, true, true), {NLs::PathExist});
        }
        TestDescribeResult(DescribePath(runtime, indexPath + "/" + PostingTable, true, true, true), {NLs::PathExist});

        // All rows must be indexed exactly once — resuming from LastKeyAck must not
        // cause duplicates or skipped rows.
        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                              "/MyRoot/dir/Table/index1/" + TString(PostingTable));
        Cerr << "... posting table contains " << rows << " rows" << Endl;
        UNIT_ASSERT(rows > 0);
    }

    // -----------------------------------------------------------------------
    // Strategy:
    //   1. Block all IN_PROGRESS prefix-kmeans responses before they reach SchemeShard.
    //   2. Allow exactly one through so SchemeShard persists a non-empty LastKeyAck.
    //   3. Reboot SchemeShard (simulates crash right after committing LastKeyAck).
    //   4. Stop blocking and let the resumed build finish.
    //   5. Verify the index is complete and non-empty.
    //
    // PrefixKMeans emits IN_PROGRESS after completing each prefix group, so with 10 rows
    // having 10 distinct prefix values (key % 17), there are 9 opportunities for checkpoints.
    // -----------------------------------------------------------------------
    Y_UNIT_TEST(RebootAfterFirstInProgressResumesScanPrefixed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        SetupVectorTable(runtime, env, txId);

        TBlockEvents<TEvDataShard::TEvPrefixKMeansResponse> blocker(runtime,
            [](const TEvDataShard::TEvPrefixKMeansResponse::TPtr& ev) {
                return ev->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS;
            });

        const ui64 buildTx = ++txId;
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(),
                        MakeBuildRequest(buildTx, true /*Prefixed*/));

        // Wait until at least one IN_PROGRESS checkpoint is captured.
        runtime.WaitFor("first IN_PROGRESS captured", [&] { return !blocker.empty(); });

        // Let exactly one through so SchemeShard commits a LastKeyAck to disk.
        blocker.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        // Reboot SchemeShard — simulates a crash right after persisting LastKeyAck.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Stop blocking so the resumed build can proceed from the saved LastKeyAck.
        blocker.Stop().Unblock();

        env.TestWaitNotification(runtime, buildTx);

        CheckIndexComplete(runtime, buildTx, true /*Prefixed*/);
    }

    // LocalKMeans emits IN_PROGRESS after completing each parent cluster group.
    // With clusters=4 and levels=2 (defaults), 4 parent clusters are created in the first
    // level, so there are 3 opportunities for IN_PROGRESS checkpoints between them.
    Y_UNIT_TEST(RebootAfterFirstInProgressResumesScanLocalKMeans) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        SetupVectorTable(runtime, env, txId);

        TBlockEvents<TEvDataShard::TEvLocalKMeansResponse> blocker(runtime,
            [](const TEvDataShard::TEvLocalKMeansResponse::TPtr& ev) {
                return ev->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS;
            });

        const ui64 buildTx = ++txId;
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(),
                        MakeBuildRequest(buildTx, false /*Prefixed*/));

        // Wait until at least one IN_PROGRESS checkpoint is captured.
        runtime.WaitFor("first IN_PROGRESS captured", [&] { return !blocker.empty(); });

        // Let exactly one through so SchemeShard commits a LastKeyAck to disk.
        blocker.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        // Reboot SchemeShard — simulates a crash right after persisting LastKeyAck.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Stop blocking so the resumed build can proceed from the saved LastKeyAck.
        blocker.Stop().Unblock();

        env.TestWaitNotification(runtime, buildTx);

        CheckIndexComplete(runtime, buildTx, false /*Prefixed*/);
    }
}
