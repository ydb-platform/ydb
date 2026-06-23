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

            AsyncSetColumnConstraintWithoutResponse(
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

            AsyncSetColumnConstraintWithoutResponse(
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

    Y_UNIT_TEST_WITH_REBOOTS(SetNotNullOnManyColumnsWithManyShards) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);


            const TString root = "/MyRoot";
            const TString tablePath = "/MyRoot/Table";

            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key"    Type: "Uint32" }
                    Columns { Name: "value"  Type: "Utf8"   }
                    Columns { Name: "value2" Type: "Utf8"   }
                    KeyColumnNames: ["key"]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 100 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 200 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 300 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 400 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 500 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 600 } } } }
                )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                {
                    TVector<TCell> cells = {
                        TCell::Make((ui32)001), TCell(TStringBuf("test_value_001")), TCell(TStringBuf("test_value2_001")),
                        TCell::Make((ui32)101), TCell(TStringBuf("test_value_101")), TCell(TStringBuf("test_value2_101")),
                        TCell::Make((ui32)201), TCell(TStringBuf("test_value_201")), TCell(TStringBuf("test_value2_201")),
                        TCell::Make((ui32)301), TCell(TStringBuf("test_value_301")), TCell(TStringBuf("test_value2_301")),
                        TCell::Make((ui32)401), TCell(TStringBuf("test_value_401")), TCell(TStringBuf("test_value2_401")),
                        TCell::Make((ui32)501), TCell(TStringBuf("test_value_501")), TCell(TStringBuf("test_value2_501")),
                        TCell::Make((ui32)601), TCell(TStringBuf("test_value_601")), TCell(TStringBuf("test_value2_601"))
                    };

                    WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, tablePath,
                        0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                        {1, 2, 3}, TSerializedCellMatrix(cells, 7, 3), true);
                }
            }

            const ui64 setConstraintTxId = ++t.TxId;

            AsyncSetColumnConstraintWithoutResponse(
                runtime, setConstraintTxId,
                TTestTxConfig::SchemeShard,
                root,
                tablePath,
                {"value", "value2"});

            t.TestEnv->TestWaitNotification(runtime, setConstraintTxId, TTestTxConfig::SchemeShard);

            {
                TInactiveZone inactive(activeZone);
                TestCheckColumnsNotNull(runtime, tablePath, {{"value", true}, {"value2", true}});
            }
        });
    }
} // Y_UNIT_TEST_SUITE(SetNotNullReboots)
