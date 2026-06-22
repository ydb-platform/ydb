#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(SetNotNullReboots) {

    Y_UNIT_TEST_WITH_REBOOTS(SetNotNullOnSingleColumn) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);


            const TString root = "/MyRoot";
            const TString tablePath = "/MyRoot/Table";

            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key"   Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8"   }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            const ui64 setConstraintTxId = ++t.TxId;

            TestSetColumnConstraintWithoutResponse(
                runtime, setConstraintTxId,
                TTestTxConfig::SchemeShard,
                root,
                tablePath,
                {"value"});

            t.TestEnv->TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

            {
                TInactiveZone inactive(activeZone);
                TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(SetNotNullShouldBeFailed) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);


            const TString root = "/MyRoot";
            const TString tablePath = "/MyRoot/Table";

            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key"   Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8"   }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                {
                    TVector<TCell> cells = {
                        TCell::Make((ui32)1), TCell()
                    };

                    WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, tablePath,
                        0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                        {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
                }
            }

            const ui64 setConstraintTxId = ++t.TxId;

            TestSetColumnConstraintWithoutResponse(
                runtime, setConstraintTxId,
                TTestTxConfig::SchemeShard,
                root,
                tablePath,
                {"value"});

            t.TestEnv->TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

            {
                TInactiveZone inactive(activeZone);
                TestCheckColumnsNotNull(runtime, tablePath, {{"value", false}});

                {
                    TVector<TCell> cells = {
                        TCell::Make((ui32)2), TCell()
                    };

                    WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, tablePath,
                        0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                        {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);
                }
            }
        });
    }

} // Y_UNIT_TEST_SUITE(SetNotNullReboots)
