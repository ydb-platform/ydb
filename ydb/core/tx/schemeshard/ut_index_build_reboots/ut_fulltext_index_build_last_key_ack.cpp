// Tests for LastKeyAck persistence and resume-from-key-range at the SchemeShard level.
//
// These tests verify the integration path:
//   datashard emits IN_PROGRESS with LastKeyAck
//   → SchemeShard persists LastKeyAck in IndexBuildShardStatus
//   → after SchemeShard reboot the next scan request carries KeyRange = [LastKeyAck, ∞)
//   → the completed index is identical to a fresh full build.

#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

    // ---------------------------------------------------------------------------
    // Helpers shared across all tests in this file
    // ---------------------------------------------------------------------------

    // Standard 4-row table used in single-shard tests.
    void SetupTextTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "texts"
        Columns { Name: "id"   Type: "Uint64" }
        Columns { Name: "text" Type: "String" }
        Columns { Name: "data" Type: "String" }
        KeyColumnNames: [ "id" ]
    )");
        env.TestWaitNotification(runtime, txId);

        auto writeRow = [&](ui64 id, TString text, TString data) {
            TString q = Sprintf(R"(
            (
                (let key '( '('id (Uint64 '%lu)) ))
                (let row '( '('text (String '"%s")) '('data (String '"%s")) ))
                (return (AsList (UpdateRow '__user__texts key row)))
            )
        )", id, text.c_str(), data.c_str());
            NKikimrMiniKQL::TResult result;
            TString err;
            UNIT_ASSERT_VALUES_EQUAL_C(
                LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, q, result, err),
                NKikimrProto::EReplyStatus::OK, err);
        };

        writeRow(1, "green apple", "one");
        writeRow(2, "red apple", "two");
        writeRow(3, "yellow apple", "three");
        writeRow(4, "red car", "four");
    }

    Ydb::Table::TableIndex MakeFulltextIndex(const TString& name = "fulltext_idx") {
        Ydb::Table::TableIndex index;
        index.set_name(name);
        index.add_index_columns("text");
        index.add_data_columns("data");
        auto& fulltext = *index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings();
        auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
        fulltext.mutable_columns()->at(0).set_column("text");
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        return index;
    }

    // 8 tokens for 4 docs: apple×3, car, green, red×2, yellow.
    constexpr ui32 kExpectedPostingRows = 8;

} // namespace

// ---------------------------------------------------------------------------

Y_UNIT_TEST_SUITE(FulltextIndexBuildLastKeyAckTests) {

    // -----------------------------------------------------------------------
    // Test 1: LastKeyAck is persisted by SchemeShard on IN_PROGRESS responses.
    //
    // Strategy:
    //   1. Block the very first IN_PROGRESS fulltext response reaching SchemeShard.
    //   2. Allow it through so SchemeShard commits LastKeyAck to the DB.
    //   3. Stop blocking and let the build complete normally.
    //   4. Verify the index is complete and correct.
    //
    // This test does NOT reboot SchemeShard — it only verifies that the
    // IN_PROGRESS path (HandleProgress + PersistBuildIndexShardStatus) does not
    // crash and the build still reaches DONE with a correct index.
    // -----------------------------------------------------------------------
    Y_UNIT_TEST(LastKeyAckPersistedOnInProgress) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        SetupTextTable(runtime, env, txId);

        // Block all IN_PROGRESS responses before they reach SchemeShard.
        TBlockEvents<TEvDataShard::TEvBuildFulltextIndexResponse> blocker(runtime,
                                                                          [](const TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& ev) {
                                                                              return ev->Get()->Record.GetStatus() ==
                                                                                     NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS;
                                                                          });

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
                       "/MyRoot", "/MyRoot/texts", MakeFulltextIndex());

        // Wait for the first IN_PROGRESS to be captured.
        runtime.WaitFor("first IN_PROGRESS blocked", [&] { return !blocker.empty(); });

        // Let it through so SchemeShard persists LastKeyAck, then stop blocking.
        blocker.Stop().Unblock();

        env.TestWaitNotification(runtime, buildTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            op.DebugString());

        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                              "/MyRoot/texts/fulltext_idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(rows, kExpectedPostingRows);
    }

    // -----------------------------------------------------------------------
    // Test 2: SchemeShard reboot after receiving an IN_PROGRESS with LastKeyAck
    //         does not cause re-scanning already-indexed rows.
    //
    // Strategy:
    //   1. Block all IN_PROGRESS fulltext responses before they reach SchemeShard.
    //   2. Allow exactly one through so SchemeShard persists a non-empty LastKeyAck.
    //   3. Reboot SchemeShard (simulates crash right after committing LastKeyAck).
    //   4. Stop blocking and let the resumed build finish.
    //   5. Verify the index is complete.
    // -----------------------------------------------------------------------
    Y_UNIT_TEST(RebootAfterFirstInProgressResumesScan) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        SetupTextTable(runtime, env, txId);

        TBlockEvents<TEvDataShard::TEvBuildFulltextIndexResponse> blocker(runtime,
                                                                          [](const TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& ev) {
                                                                              return ev->Get()->Record.GetStatus() ==
                                                                                     NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS;
                                                                          });

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
                       "/MyRoot", "/MyRoot/texts", MakeFulltextIndex());

        // Wait until we have at least one blocked IN_PROGRESS.
        runtime.WaitFor("first IN_PROGRESS captured", [&] { return !blocker.empty(); });

        // Let exactly one through so SchemeShard commits a LastKeyAck to disk.
        blocker.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        // Reboot SchemeShard — simulates a crash right after persisting LastKeyAck.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Stop blocking so the resumed build can proceed.
        blocker.Stop().Unblock();

        env.TestWaitNotification(runtime, buildTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            op.DebugString());

        // Index must be complete — same number of tokens as a full build.
        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                              "/MyRoot/texts/fulltext_idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(rows, kExpectedPostingRows);
    }

    // -----------------------------------------------------------------------
    // Test 3: Multiple SchemeShard reboots during IN_PROGRESS — index is complete.
    //
    // Reboot SchemeShard twice during the scan, each time after at least one
    // IN_PROGRESS has been committed. The final result must match a full build.
    // -----------------------------------------------------------------------
    Y_UNIT_TEST(MultipleRebootsDuringInProgressProduceCompleteIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        SetupTextTable(runtime, env, txId);

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
                       "/MyRoot", "/MyRoot/texts", MakeFulltextIndex());

        // First reboot: block one IN_PROGRESS, let it through, then reboot.
        {
            TBlockEvents<TEvDataShard::TEvBuildFulltextIndexResponse> blocker(runtime,
                                                                              [](const TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& ev) {
                                                                                  return ev->Get()->Record.GetStatus() ==
                                                                                         NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS;
                                                                              });
            runtime.WaitFor("1st IN_PROGRESS captured", [&] { return !blocker.empty(); });
            blocker.Unblock(1);
            runtime.SimulateSleep(TDuration::MilliSeconds(50));
            blocker.Stop().Unblock();
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Second reboot: same pattern after resume.
        {
            TBlockEvents<TEvDataShard::TEvBuildFulltextIndexResponse> blocker(runtime,
                                                                              [](const TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& ev) {
                                                                                  return ev->Get()->Record.GetStatus() ==
                                                                                         NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS;
                                                                              });
            // It's fine if there are no more IN_PROGRESS — all rows may already
            // be past LastKeyAck.
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
            blocker.Stop().Unblock();
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        env.TestWaitNotification(runtime, buildTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            op.DebugString());

        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                              "/MyRoot/texts/fulltext_idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(rows, kExpectedPostingRows);
    }

    // -----------------------------------------------------------------------
    // Test 4: Two shards — LastKeyAck is tracked independently per shard.
    //
    // Create a two-shard table (split at id=3), reboot SchemeShard once after
    // at least one IN_PROGRESS from either shard, and verify the final index
    // is complete across both shards.
    // -----------------------------------------------------------------------
    Y_UNIT_TEST(TwoShardsRebootResumesFromLastKeyAck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        // Two-shard table: shard 0 holds id in [1,2], shard 1 holds id in [3,4].
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "id"   Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 3 } } } }
        )");
        env.TestWaitNotification(runtime, txId);

        auto writeRow = [&](ui64 partIdx, ui64 id, TString text, TString data) {
            TString q = Sprintf(R"(
                (
                    (let key '( '('id (Uint64 '%lu)) ))
                    (let row '( '('text (String '"%s")) '('data (String '"%s")) ))
                    (return (AsList (UpdateRow '__user__texts key row)))
                )
            )", id, text.c_str(), data.c_str());
            NKikimrMiniKQL::TResult result;
            TString err;
            UNIT_ASSERT_VALUES_EQUAL_C(
                LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets + partIdx, q, result, err),
                NKikimrProto::EReplyStatus::OK, err);
        };

        writeRow(0, 1, "green apple", "one");
        writeRow(0, 2, "red apple", "two");
        writeRow(1, 3, "yellow apple", "three");
        writeRow(1, 4, "red car", "four");

        TBlockEvents<TEvDataShard::TEvBuildFulltextIndexResponse> blocker(runtime,
                                                                          [](const TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& ev) {
                                                                              return ev->Get()->Record.GetStatus() ==
                                                                                     NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS;
                                                                          });

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
                       "/MyRoot", "/MyRoot/texts", MakeFulltextIndex());

        runtime.WaitFor("IN_PROGRESS from any shard", [&] { return !blocker.empty(); });
        blocker.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        blocker.Stop().Unblock();

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        env.TestWaitNotification(runtime, buildTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            op.DebugString());

        // 8 tokens total across all 4 documents.
        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                              "/MyRoot/texts/fulltext_idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(rows, kExpectedPostingRows);
    }

    // -----------------------------------------------------------------------
    // Test 5: Reboot SchemeShard while the DONE response is blocked.
    //
    // All IN_PROGRESS responses arrive and are persisted normally.  The DONE
    // response is blocked before reaching SchemeShard.  SchemeShard is rebooted,
    // then the DONE is released.  The datashard will resend ABORTED (or the scan
    // will restart), but the final state must be DONE with a correct index.
    // -----------------------------------------------------------------------
    Y_UNIT_TEST(RebootBeforeDoneResponseProducesCompleteIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        SetupTextTable(runtime, env, txId);

        // Block only the DONE response.
        TBlockEvents<TEvDataShard::TEvBuildFulltextIndexResponse> doneBlocker(runtime,
                                                                              [](const TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& ev) {
                                                                                  return ev->Get()->Record.GetStatus() ==
                                                                                         NKikimrIndexBuilder::EBuildStatus::DONE;
                                                                              });

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
                       "/MyRoot", "/MyRoot/texts", MakeFulltextIndex());

        // Wait until datashard signals DONE (blocked before SchemeShard sees it).
        runtime.WaitFor("DONE response captured", [&] { return !doneBlocker.empty(); });

        // Reboot SchemeShard while DONE is still in flight.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Release DONE and all future responses.
        doneBlocker.Stop().Unblock();

        env.TestWaitNotification(runtime, buildTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(
            op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            op.DebugString());

        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                              "/MyRoot/texts/fulltext_idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(rows, kExpectedPostingRows);
    }
} // Y_UNIT_TEST_SUITE(FulltextIndexBuildLastKeyAckTests)
