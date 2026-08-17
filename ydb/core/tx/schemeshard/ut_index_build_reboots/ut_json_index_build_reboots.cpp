#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

void EnableJsonRowIdFlags(TTestActorRuntime& runtime) {
    auto& appData = runtime.GetAppData();
    appData.FeatureFlags.SetEnableJsonIndex(true);
    appData.FeatureFlags.SetEnableFulltextIndex(true);
    appData.FeatureFlags.SetEnableAddUniqueIndex(true);
    appData.FeatureFlags.SetEnableUniqConstraint(true);
}

// Check that the index at `indexPath` has UseRowIdAsDocId=true in its persisted description
// and that its impl-table is keyed by [__ydb_token, __ydb_row_id].
void CheckRowIdJsonIndex(TTestActorRuntime& runtime, const TString& indexPath) {
    {
        const auto d = DescribePrivatePath(runtime, indexPath);
        const auto& tableIndex = d.GetPathDescription().GetTableIndex();
        UNIT_ASSERT_C(tableIndex.HasFulltextIndexDescription(),
            indexPath << ": FulltextIndexDescription must be set for rowid-mode JSON index");
        UNIT_ASSERT_C(tableIndex.GetFulltextIndexDescription().GetUseRowIdAsDocId(),
            indexPath << ": UseRowIdAsDocId must be true after persistence through schemeshard reboots");
    }

    TestDescribeResult(DescribePrivatePath(runtime, indexPath + "/" + TString(NTableIndex::ImplTable)), {
        NLs::PathExist,
        NLs::CheckColumns(TString(NTableIndex::ImplTable),
            { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
            {},
            { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
            /*strictCount=*/ true),
    });
}

// Common reboot event filter for JSON index build tests.
void SetupRebootFilter(TTestWithReboots& t) {
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
}

} // namespace

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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RowIdAutoProvision, /* rebootBuckets */ 4, /* pipeResetBuckets */ 4, /* killOnCommit */ true) {
        SetupRebootFilter(t);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            EnableJsonRowIdFlags(runtime);

            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "texts"
                    Columns { Name: "pk"   Type: "Utf8" NotNull: true }
                    Columns { Name: "data" Type: "Json" }
                    KeyColumnNames: ["pk"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                const struct { const char* Pk; const char* Json; } rows[] = {
                    {"aaa", R"({"k": 1})"},
                    {"bbb", R"({"k": 2})"},
                    {"ccc", R"({"k": 1, "m": 3})"},
                };
                for (const auto& row : rows) {
                    TString pk(row.Pk), json(row.Json);
                    UploadRow(runtime, "/MyRoot/texts", 0,
                        /*keyTags=*/ {1}, /*valueTags=*/ {2},
                        /*keys=*/ {TCell(pk.data(), pk.size())},
                        /*values=*/ {TCell(json.data(), json.size())});
                }
            }

            const ui64 buildIndexId = ++t.TxId;
            {
                auto sender = runtime.AllocateEdgeActor();
                Ydb::Table::TableIndex index;
                index.set_name("json_idx");
                index.add_index_columns("data");
                index.mutable_global_json_index();
                auto request = CreateBuildIndexRequest(buildIndexId, "/MyRoot", "/MyRoot/texts", index);
                // Disable scan batching to keep the test fast under reboots.
                request->Record.MutableSettings()->MutableScanSettings()->Clear();
                ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                TInactiveZone inactive(activeZone);

                auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                    op.DebugString());

                TestDescribeResult(DescribePrivatePath(runtime,
                    TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
                    NLs::PathExist,
                    NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
                    NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                });

                CheckRowIdJsonIndex(runtime, "/MyRoot/texts/json_idx");

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                        "/MyRoot/texts/json_idx/" + TString(NTableIndex::ImplTable));
                    Cerr << "... json_idx impl-table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_C(rows > 0, "json_idx impl-table must be non-empty after building");
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RowIdManualInfra, /* rebootBuckets */ 4, /* pipeResetBuckets */ 4, /* killOnCommit */ true) {
        SetupRebootFilter(t);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            EnableJsonRowIdFlags(runtime);

            {
                TInactiveZone inactive(activeZone);

                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                    TableDescription {
                        Name: "texts"
                        Columns { Name: "pk"   Type: "Utf8"   NotNull: true }
                        Columns { Name: "data" Type: "Json" }
                        Columns { Name: "%s"   Type: "Uint64" NotNull: true }
                        KeyColumnNames: ["pk"]
                    }
                    IndexDescription {
                        Name: "uniq_rowid"
                        KeyColumnNames: ["%s"]
                        Type: EIndexTypeGlobalUnique
                    }
                )", NTableIndex::NFulltext::RowIdColumn, NTableIndex::NFulltext::RowIdColumn));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                const struct { const char* Pk; const char* Json; ui64 RowId; } rows[] = {
                    {"aaa", R"({"k": 1})",       100},
                    {"bbb", R"({"k": 2})",       200},
                    {"ccc", R"({"k": 1, "m": 3})", 300},
                };
                for (const auto& row : rows) {
                    TString pk(row.Pk), json(row.Json);
                    UploadRow(runtime, "/MyRoot/texts", 0,
                        /*keyTags=*/ {1}, /*valueTags=*/ {2, 3},
                        /*keys=*/ {TCell(pk.data(), pk.size())},
                        /*values=*/ {TCell(json.data(), json.size()), TCell::Make(row.RowId)});
                }
            }

            const ui64 buildIndexId = ++t.TxId;
            {
                auto sender = runtime.AllocateEdgeActor();
                Ydb::Table::TableIndex index;
                index.set_name("json_idx");
                index.add_index_columns("data");
                index.mutable_global_json_index();
                auto request = CreateBuildIndexRequest(buildIndexId, "/MyRoot", "/MyRoot/texts", index);
                request->Record.MutableSettings()->MutableScanSettings()->Clear();
                ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request);
            }

            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                TInactiveZone inactive(activeZone);

                auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                    op.DebugString());

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/uniq_rowid"), {
                    NLs::PathExist,
                    NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
                    NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                });

                TestDescribeResult(DescribePrivatePath(runtime,
                    TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
                    NLs::PathNotExist,
                });

                CheckRowIdJsonIndex(runtime, "/MyRoot/texts/json_idx");

                {
                    auto rows = CountRows(runtime, TTestTxConfig::SchemeShard,
                        "/MyRoot/texts/json_idx/" + TString(NTableIndex::ImplTable));
                    Cerr << "... json_idx impl-table contains " << rows << " rows" << Endl;
                    UNIT_ASSERT_C(rows > 0, "json_idx impl-table must be non-empty after building");
                }
            }
        });
    }
}
