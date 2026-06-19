#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/olap_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/data_events/payload_helper.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

#define Y_UNIT_TEST_WITH_REBOOTS_TWIN(N, OPT) Y_UNIT_TEST_WITH_REBOOTS_BUCKETS_TWIN(N, 1, 1, false, OPT)

static const TString defaultRowTableSchema = R"(
    Name: "TestTable"
    Columns { Name: "id" Type: "Uint64" }
    Columns { Name: "text" Type: "String" }
    Columns { Name: "data" Type: "String" }
    KeyColumnNames: [ "id" ]
)";

static const TString defaultColumnTableSchema = R"(
    Name: "TestTable"
    ColumnShardCount: 1
    Schema {
        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
        Columns { Name: "data" Type: "Utf8" }
        KeyColumnNames: "timestamp"
    }
)";

static const TString defaultStoreSchema = R"(
    Name: "OlapStore"
    ColumnShardCount: 1
    SchemaPresets {
        Name: "default"
        Schema {
            Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
            Columns { Name: "data" Type: "Utf8" }
            KeyColumnNames: "timestamp"
        }
    }
)";

// Column table schema definition for Arrow data construction
static const std::vector<NArrow::NTest::TTestColumn>& GetColumnTableTestSchema() {
    static const std::vector<NArrow::NTest::TTestColumn> schema = {
        NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)),
        NArrow::NTest::TTestColumn("data", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
    };
    return schema;
}

// Helpers to abstract column/row table differences in TWIN tests

static void EnableTruncateFeatureFlags(TTestActorRuntime& runtime) {
    runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);
    runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
}

template <bool IsColumnTable>
void CreateTestTable(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath) {
    if constexpr (IsColumnTable) {
        TestCreateColumnTable(runtime, txId, parentPath, defaultColumnTableSchema);
    } else {
        TestCreateTable(runtime, txId, parentPath, defaultRowTableSchema);
    }
}

// Writes test data and verifies it was written for both table types.
template <bool IsColumnTable>
void WriteAndVerifyTestData(TTestActorRuntime& runtime, ui64& txId, const TString& tablePath, ui32 expectedRows) {
    if constexpr (IsColumnTable) {
        const auto& schema = GetColumnTableTestSchema();

        const ui64 pathId = GetLocalPathId(runtime, tablePath);

        auto shardIds = GetColumnShardTabletIds(runtime, tablePath);
        UNIT_ASSERT(!shardIds.empty());
        const ui64 shardId = shardIds[0];

        const TString blobData = NTxUT::MakeTestBlob({0, expectedRows}, schema, {}, {"timestamp"});
        UNIT_ASSERT(!blobData.empty());

        auto& basicRuntime = static_cast<TTestBasicRuntime&>(runtime);
        auto sender = basicRuntime.AllocateEdgeActor();

        // Write under a lock and commit via a planned transaction so the data is pinned to a known
        // (planStep, txId) snapshot. An immediate (lock-free) write commits at a mediator-time-based
        // snapshot that is not reported back in the result, leaving the data unreadable from the test.
        const ui64 writeId = ++txId;
        const ui64 lockId = ++txId;
        std::vector<ui64> writeIds;
        UNIT_ASSERT(NTxUT::WriteData(basicRuntime, sender, shardId, writeId, pathId, blobData, schema, &writeIds,
            NEvWrite::EModificationType::Replace, lockId));

        const ui64 commitTxId = ++txId;
        const auto planStep = NTxUT::ProposeCommit(basicRuntime, sender, shardId, commitTxId, writeIds, lockId);
        NTxUT::PlanCommit(basicRuntime, sender, shardId, planStep, TSet<ui64>{commitTxId});

        NTxUT::TShardReader reader(basicRuntime, shardId, pathId, NOlap::TSnapshot(planStep, commitTxId));
        reader.SetReplyColumnIds({1, 2});  // timestamp + data columns
        auto rb = reader.ReadAll();
        UNIT_ASSERT(rb);
        UNIT_ASSERT_VALUES_EQUAL((ui32)rb->num_rows(), expectedRows);
    } else {
        TVector<TCell> cells = {
            TCell::Make((ui64)1), TCell(TStringBuf("row one")), TCell(TStringBuf("data one")),
            TCell::Make((ui64)2), TCell(TStringBuf("row two")), TCell(TStringBuf("data two")),
            TCell::Make((ui64)3), TCell(TStringBuf("row three")), TCell(TStringBuf("data three")),
            TCell::Make((ui64)4), TCell(TStringBuf("row four")), TCell(TStringBuf("data four")),
            TCell::Make((ui64)5), TCell(TStringBuf("row five")), TCell(TStringBuf("data five")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(cells, expectedRows, 3), true);

        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(rows, expectedRows);
    }
}

// Verifies the table exists and has expected row count after truncation.
template <bool IsColumnTable>
void VerifyTableTruncated(TTestActorRuntime& runtime, const TString& tablePath) {
    TestLs(runtime, tablePath, false, NLs::PathExist);
    if constexpr (IsColumnTable) {
        const ui64 pathId = GetLocalPathId(runtime, tablePath);
        auto shardIds = GetColumnShardTabletIds(runtime, tablePath);
        UNIT_ASSERT(!shardIds.empty());

        NTxUT::TShardReader reader(static_cast<TTestBasicRuntime&>(runtime), shardIds[0], pathId, NOlap::TSnapshot(1000000, 1000000));
        reader.SetReplyColumnIds({1});  // timestamp column
        auto rb = reader.ReadAll();
        UNIT_ASSERT(!rb);  // no data after truncation
    } else {
        auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(rows, 0);
    }
}

template <bool IsColumnTable>
TEvTx* DropTestTableRequest(ui64 txId, const TString& parentPath, const TString& tableName) {
    if constexpr (IsColumnTable) {
        return DropColumnTableRequest(txId, parentPath, tableName);
    } else {
        return DropTableRequest(txId, parentPath, tableName);
    }
}

Y_UNIT_TEST_SUITE(TruncateTableReboots) {

    // Parameterized: create table, write data, truncate, verify
    Y_UNIT_TEST_WITH_REBOOTS_TWIN(Simple, IsColumnTable) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                EnableTruncateFeatureFlags(runtime);
                CreateTestTable<IsColumnTable>(runtime, ++t.TxId, "/MyRoot");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WriteAndVerifyTestData<IsColumnTable>(runtime, t.TxId, "/MyRoot/TestTable", 5);
            }

            const ui64 truncateTxId = ++t.TxId;
            t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot", "TestTable", TTestTxConfig::SchemeShard, {}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, truncateTxId);

            {
                TInactiveZone inactive(activeZone);
                VerifyTableTruncated<IsColumnTable>(runtime, "/MyRoot/TestTable");
            }
        });
    }

    // Row-table-specific: truncation with concurrent datashard split
    Y_UNIT_TEST_WITH_REBOOTS(WithSplit) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                EnableTruncateFeatureFlags(runtime);

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

    // Parameterized: truncate then drop for both table types
    Y_UNIT_TEST_WITH_REBOOTS_TWIN(TruncateThenDrop, IsColumnTable) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                EnableTruncateFeatureFlags(runtime);
                CreateTestTable<IsColumnTable>(runtime, ++t.TxId, "/MyRoot");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            const ui64 truncateTxId = ++t.TxId;
            t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot", "TestTable", TTestTxConfig::SchemeShard, {}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, truncateTxId);

            t.TestEnv->ReliablePropose(runtime, DropTestTableRequest<IsColumnTable>(++t.TxId, "/MyRoot", "TestTable"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/TestTable", false, NLs::PathNotExist);
            }
        });
    }

    // Parameterized: multiple sequential truncates for both table types
    Y_UNIT_TEST_WITH_REBOOTS_TWIN(MultipleTruncates, IsColumnTable) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                EnableTruncateFeatureFlags(runtime);
                CreateTestTable<IsColumnTable>(runtime, ++t.TxId, "/MyRoot");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // First truncate
            {
                const ui64 truncateTxId = ++t.TxId;
                t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot", "TestTable", TTestTxConfig::SchemeShard, {}),
                                           {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
                t.TestEnv->TestWaitNotification(runtime, truncateTxId);
            }

            // Second truncate
            {
                const ui64 truncateTxId = ++t.TxId;
                t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot", "TestTable", TTestTxConfig::SchemeShard, {}),
                                           {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
                t.TestEnv->TestWaitNotification(runtime, truncateTxId);
            }

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/TestTable", false, NLs::PathExist);
            }
        });
    }

    // Column-table-specific: truncation of column table inside OLAP store
    Y_UNIT_TEST_WITH_REBOOTS(SimpleInStore) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                EnableTruncateFeatureFlags(runtime);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable"
                    ColumnShardCount: 1
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::PathExist);
            }

            const ui64 truncateTxId = ++t.TxId;
            t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot/OlapStore", "ColumnTable", TTestTxConfig::SchemeShard, {}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, truncateTxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::PathExist);
            }
        });
    }

    // Column-table-specific: truncation then drop of column table inside OLAP store
    Y_UNIT_TEST_WITH_REBOOTS(TruncateInStoreThenDrop) {
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                EnableTruncateFeatureFlags(runtime);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable"
                    ColumnShardCount: 1
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            const ui64 truncateTxId = ++t.TxId;
            t.TestEnv->ReliablePropose(runtime, TruncateTableRequest(truncateTxId, "/MyRoot/OlapStore", "ColumnTable", TTestTxConfig::SchemeShard, {}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, truncateTxId);

            t.TestEnv->ReliablePropose(runtime, DropColumnTableRequest(++t.TxId, "/MyRoot/OlapStore", "ColumnTable"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::PathNotExist);
            }
        });
    }
}
