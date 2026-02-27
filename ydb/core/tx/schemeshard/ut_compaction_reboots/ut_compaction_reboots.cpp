#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(SchemeshardForcedCompactionTestReboots) {

    void CreateTable(TTestActorRuntime& runtime, ui64& txId, const TString& tableName) {
        TestCreateTable(runtime, ++txId, "/MyRoot", TStringBuilder() << R"__(
            Name: ")__" << tableName << R"__("
            Columns { Name: "key"  Type: "Uint64"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 50 } } } }
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 2
                    MaxPartitionsCount: 2
                }
            }
        )__");
    }

    void WriteData(TTestActorRuntime& runtime, const TString& tableName, const TString& valueString) {
        TString tablePath = TStringBuilder() << "/MyRoot/" << tableName;
        for (ui64 part = 0; part < 2; ++part) {
            for (ui64 key = 0; key < 100; ++key) {
                UploadRow(runtime, tablePath, part, {1}, {2}, {TCell::Make(key)}, {TCell(valueString)});
            }
        }
    }
    
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(Test, 4, 4, false) {
        t.EnvOpts.EnableBackgroundCompaction(false);
        t.NoRebootEventTypes.insert(TEvForcedCompaction::EvCreateRequest);
        t.NoRebootEventTypes.insert(TEvForcedCompaction::EvGetRequest);
        t.NoRebootEventTypes.insert(TEvForcedCompaction::EvCancelRequest);
        t.NoRebootEventTypes.insert(TEvForcedCompaction::EvForgetRequest);
        t.NoRebootEventTypes.insert(TEvForcedCompaction::EvListRequest);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 txId = 100;

            {
                TInactiveZone inactive(activeZone);

                runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
                runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
                runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

                CreateTable(runtime, txId, "Table1");
                t.TestEnv->TestWaitNotification(runtime, txId);
                WriteData(runtime, "Table1", TString(1000, 'A'));

                CreateTable(runtime, txId, "Table2");
                t.TestEnv->TestWaitNotification(runtime, txId);
                WriteData(runtime, "Table2", TString(1000, 'B'));
            }

            AsyncCompact(runtime, ++txId, "/MyRoot", "/MyRoot/Table1");
            auto compaction1Id = txId;
            AsyncCompact(runtime, ++txId, "/MyRoot", "/MyRoot/Table2");
            auto compaction2Id = txId;

            t.TestEnv->TestWaitNotification(runtime, compaction1Id);
            t.TestEnv->TestWaitNotification(runtime, compaction2Id);

            {
                auto response1 = TestGetCompaction(runtime, compaction1Id, "/MyRoot");
                UNIT_ASSERT_VALUES_EQUAL(response1.GetForcedCompaction().GetState(), Ydb::Table::CompactState::STATE_DONE);
                auto response2 = TestGetCompaction(runtime, compaction2Id, "/MyRoot");
                UNIT_ASSERT_VALUES_EQUAL(response2.GetForcedCompaction().GetState(), Ydb::Table::CompactState::STATE_DONE);
            }

            TestForgetCompaction(runtime, ++txId, "/MyRoot", compaction1Id);
            TestForgetCompaction(runtime, ++txId, "/MyRoot", compaction2Id);

            TestGetCompaction(runtime, compaction1Id, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
            TestGetCompaction(runtime, compaction2Id, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        });
    }
}
