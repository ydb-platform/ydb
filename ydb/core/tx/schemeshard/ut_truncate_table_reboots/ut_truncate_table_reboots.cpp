#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(TruncateTableReboots) {
    Y_UNIT_TEST_WITH_REBOOTS(Simple) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                
                runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "TestTable"
                    Columns { Name: "id" Type: "Uint64" }
                    Columns { Name: "text" Type: "String" }
                    Columns { Name: "data" Type: "String" }
                    KeyColumnNames: [ "id" ]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TVector<TCell> cells = {
                    TCell::Make((ui64)1), TCell(TStringBuf("row one")), TCell(TStringBuf("data one")),
                    TCell::Make((ui64)2), TCell(TStringBuf("row two")), TCell(TStringBuf("data two")),
                    TCell::Make((ui64)3), TCell(TStringBuf("row three")), TCell(TStringBuf("data three")),
                    TCell::Make((ui64)4), TCell(TStringBuf("row four")), TCell(TStringBuf("data four")),
                    TCell::Make((ui64)5), TCell(TStringBuf("row five")), TCell(TStringBuf("data five")),
                };
                WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/TestTable",
                    0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                    {1, 2, 3}, TSerializedCellMatrix(cells, 5, 3), true);

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
                    UNIT_ASSERT_VALUES_EQUAL(rows, 5);
                }
            }

            const ui64 truncateTxId = ++t.TxId;
            t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot", "TestTable", TTestTxConfig::SchemeShard, {}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, truncateTxId);

            {
                TInactiveZone inactive(activeZone);
                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
                    UNIT_ASSERT_VALUES_EQUAL(rows, 0);
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(WithSplit) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "PartitionedTable"
                    Columns { Name: "id" Type: "Uint64" }
                    Columns { Name: "text" Type: "String" }
                    Columns { Name: "data" Type: "String" }
                    KeyColumnNames: [ "id" ]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } }}}
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } }}}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TVector<TCell> cells = {
                    TCell::Make((ui64)1), TCell(TStringBuf("row one")), TCell(TStringBuf("data one")),
                    TCell::Make((ui64)10), TCell(TStringBuf("row ten")), TCell(TStringBuf("data ten")),
                    TCell::Make((ui64)25), TCell(TStringBuf("row twenty five")), TCell(TStringBuf("data twenty five")),

                    TCell::Make((ui64)100), TCell(TStringBuf("row hundred")), TCell(TStringBuf("data hundred")),
                    TCell::Make((ui64)125), TCell(TStringBuf("row hundred twenty five")), TCell(TStringBuf("data hundred twenty five")),
                    TCell::Make((ui64)150), TCell(TStringBuf("row hundred fifty")), TCell(TStringBuf("data hundred fifty")),

                    TCell::Make((ui64)200), TCell(TStringBuf("row two hundred")), TCell(TStringBuf("data two hundred")),
                    TCell::Make((ui64)250), TCell(TStringBuf("row two fifty")), TCell(TStringBuf("data two fifty")),
                    TCell::Make((ui64)300), TCell(TStringBuf("row three hundred")), TCell(TStringBuf("data three hundred")),
                };
                WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/PartitionedTable",
                    0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                    {1, 2, 3}, TSerializedCellMatrix(cells, 9, 3), true);

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/PartitionedTable");
                    UNIT_ASSERT_VALUES_EQUAL(rows, 9);
                }
            }

            const ui64 truncateTxId = ++t.TxId;
            t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot", "PartitionedTable", TTestTxConfig::SchemeShard, {}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            
            const ui64 firstDatashard = TTestTxConfig::TxTablet0;

            const ui64 splitTxId = ++t.TxId;
            AsyncSplitTable(runtime, splitTxId, "/MyRoot/PartitionedTable", TStringBuilder() << R"(
                            SourceTabletId: )" << firstDatashard << R"(
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple {
                                        Optional {
                                            Uint64: 50
                                        }
                                    }
                                }
                            }
                            )");

            t.TestEnv->TestWaitNotification(runtime, {truncateTxId, splitTxId});

            {
                TInactiveZone inactive(activeZone);
                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/PartitionedTable");
                    UNIT_ASSERT_VALUES_EQUAL(rows, 0);
                }
            }
        });
    }
}
