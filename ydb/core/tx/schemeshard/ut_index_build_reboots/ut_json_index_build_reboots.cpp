#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(JsonIndexBuildTestReboots) {
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BaseCase, /* rebootBuckets */ 4, /* pipeResetBuckets */ 4, /* killOnCommit */ true) {
        t.TabletIds.clear();
        t.TabletIds.push_back(t.SchemeShardTabletId);
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
                    Name: "table"
                    Columns { Name: "id" Type: "Uint64" }
                    Columns { Name: "data" Type: "Json" }
                    KeyColumnNames: [ "id" ]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 6 } } } }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TVector<TCell> cells = {
                    TCell::Make((ui64)1), TCell(TStringBuf(R"({"a": 1})")),
                    TCell::Make((ui64)2), TCell(TStringBuf(R"({"b": 2})")),
                    TCell::Make((ui64)3), TCell(TStringBuf(R"({"a": 1, "b": 2})")),
                    TCell::Make((ui64)4), TCell(TStringBuf(R"({"c": "hello"})")),
                    TCell::Make((ui64)5), TCell(TStringBuf(R"({"a": 3, "c": "world"})")),
                };

                WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/table",
                    0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                    {1, 2}, TSerializedCellMatrix(cells, 5, 2), true);

                cells = {
                    TCell::Make((ui64)6),  TCell(TStringBuf(R"({"x": 1})")),
                    TCell::Make((ui64)7),  TCell(TStringBuf(R"({"y": 2})")),
                    TCell::Make((ui64)8),  TCell(TStringBuf(R"({"x": 1, "y": 2})")),
                    TCell::Make((ui64)9),  TCell(TStringBuf(R"({"z": "foo"})")),
                    TCell::Make((ui64)10), TCell(TStringBuf(R"({"x": 3, "z": "bar"})")),
                };

                WriteOp(runtime, TTestTxConfig::SchemeShard, ++t.TxId, "/MyRoot/table",
                    1, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                    {1, 2}, TSerializedCellMatrix(cells, 5, 2), true);
            }

            const ui64 buildIndexId = ++t.TxId;

            {
                auto sender = runtime.AllocateEdgeActor();
                Ydb::Table::TableIndex index;
                index.set_name("json_idx");
                index.add_index_columns("data");
                index.mutable_global_json_index();
                auto request = CreateBuildIndexRequest(buildIndexId, "/MyRoot", "/MyRoot/table", index);
                request->Record.MutableSettings()->MutableScanSettings()->Clear();
                ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                TInactiveZone inactive(activeZone);

                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL(
                    (ui64)descr.GetIndexBuild().GetState(),
                    (ui64)Ydb::Table::IndexBuildState::STATE_DONE
                );

                TestDescribeResult(DescribePath(runtime, "/MyRoot/table"), {
                    NLs::PathExist,
                    NLs::IndexesCount(1),
                });

                const TString indexPath = "/MyRoot/table/json_idx";
                TestDescribeResult(DescribePath(runtime, indexPath, true, true, true), {
                    NLs::PathExist,
                    NLs::IndexState(NKikimrSchemeOp::EIndexState::EIndexStateReady),
                });

                TestDescribeResult(DescribePath(runtime, indexPath + "/" + NTableIndex::ImplTable, true, true, true), {
                    NLs::PathExist,
                });

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, indexPath + "/" + TString(NTableIndex::ImplTable));
                    Cerr << "... impl table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_C(rows > 0, "indexImplTable must be non-empty after building");
                }
            }
        });
    }
}
