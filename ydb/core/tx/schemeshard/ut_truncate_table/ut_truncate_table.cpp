#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(TruncateTable) {
    Y_UNIT_TEST_WITH_REBOOTS(TruncateTableWithReboot) {
        T t(true /*killOnCommit*/);

        // speed up the test: only check scheme shard reboots
        t.TabletIds.clear();
        t.TabletIds.push_back(t.SchemeShardTabletId);

        t.NoRebootEventTypes.insert(TSchemeBoardEvents::EvUpdateAck);
        t.NoRebootEventTypes.insert(TEvSchemeShard::EvNotifyTxCompletionRegistered);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvServerDisconnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvServerConnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvClientConnected);
        t.NoRebootEventTypes.insert(TEvTabletPipe::EvClientDestroyed);

        t.NoRebootEventTypes.insert(TEvSchemeShard::EvModifySchemeTransaction);

        t.NoRebootEventTypes.insert(TEvDataShard::EvPeriodicTableStats);
        t.NoRebootEventTypes.insert(TEvDataShard::EvGetTableStatsResult);
        t.NoRebootEventTypes.insert(TEvDataShard::EvGetTableStats);
        t.NoRebootEventTypes.insert(TEvDataShard::EvProposeTransaction);
        t.NoRebootEventTypes.insert(TEvDataShard::EvProposeTransactionResult);
        t.NoRebootEventTypes.insert(TEvDataShard::EvSchemaChanged);
        t.NoRebootEventTypes.insert(TEvPrivate::EvPersistTableStats);
        t.NoRebootEventTypes.insert(TEvTxProxySchemeCache::EvNavigateKeySetResult);
        t.NoRebootEventTypes.insert(TEvTxProxySchemeCache::EvResolveKeySetResult);
        
        int cntRun = 0;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            Cerr << "RUN " << cntRun << " BEGINS" << Endl;
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
            Cerr << "TestTruncateTable(runtime, truncateId..) finished" << Endl;
            t.TestEnv->TestWaitNotification(runtime, truncateTxId);

            {
                TInactiveZone inactive(activeZone);
                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
                    UNIT_ASSERT_VALUES_EQUAL(rows, 0);
                }
            }

            Cerr << "RUN " << cntRun++ << " ENDS" << Endl;
        });
    }
}
