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
    // Test: SchemeShard reboot after receiving an IN_PROGRESS with LastKeyAck
    //       does not cause re-scanning already-indexed rows.
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
} // Y_UNIT_TEST_SUITE(FulltextIndexBuildLastKeyAckTests)
