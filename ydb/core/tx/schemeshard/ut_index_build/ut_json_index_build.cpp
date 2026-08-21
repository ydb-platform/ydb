#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

void DoCreateJsonTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "table"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "data" Type: "Json" }
            KeyColumnNames: [ "id" ]
        )");
    env.TestWaitNotification(runtime, txId);
}

void DoWriteJsonRows(TTestBasicRuntime& runtime, const TVector<std::pair<ui64, TString>>& rows) {
    for (const auto& [id, jsonStr] : rows) {
        TVector<TCell> keys = {TCell::Make(id)};
        TVector<TCell> values = {TCell(jsonStr.data(), jsonStr.size())};
        UploadRow(runtime, "/MyRoot/table", 0, {1}, {2}, keys, values);
    }
}

Ydb::Table::TableIndex JsonIndexConfig(const TString& name = "json_idx") {
    Ydb::Table::TableIndex index;
    index.set_name(name);
    index.add_index_columns("data");
    index.mutable_global_json_index();
    return index;
}

void DoCreateJsonTableWithRowId(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
    const TString& rowIdType = "Uint64", bool rowIdNotNull = true, bool createUniqueIndex = true,
    const TString& uniqueIndexKey = NTableIndex::NFulltext::RowIdColumn)
{
    const TString tableColumns = Sprintf(R"(
            Columns { Name: "pk" Type: "Utf8" NotNull: true }
            Columns { Name: "data" Type: "Json" }
            Columns { Name: "%s" Type: "%s" %s }
            KeyColumnNames: ["pk"]
    )", NTableIndex::NFulltext::RowIdColumn, rowIdType.c_str(),
        rowIdNotNull ? "NotNull: true" : "");

    if (!createUniqueIndex) {
        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "texts"
            %s
        )", tableColumns.c_str()));
    } else {
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "texts"
                %s
            }
            IndexDescription {
                Name: "uniq_rowid"
                KeyColumnNames: ["%s"]
                Type: EIndexTypeGlobalUnique
            }
        )", tableColumns.c_str(), uniqueIndexKey.c_str()));
    }
    env.TestWaitNotification(runtime, txId);
}

void DoCreateCustomPkJsonTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "texts"
        Columns { Name: "pk" Type: "Utf8" NotNull: true }
        Columns { Name: "data" Type: "Json" }
        KeyColumnNames: ["pk"]
    )");
    env.TestWaitNotification(runtime, txId);
}

void DoWriteJsonTextRows(TTestBasicRuntime& runtime, bool withRowId) {
    struct TRow { TString Pk; TString Json; ui64 RowId; };
    const TVector<TRow> rows = {
        {"pone",   R"({"a": 1})",          1},
        {"ptwo",   R"({"a": 1, "b": 2})",  2},
        {"pthree", R"({"b": 2})",          3},
        {"pfour",  R"({"c": 3})",          4},
    };
    for (const auto& row : rows) {
        TVector<TCell> keys = {TCell(row.Pk.data(), row.Pk.size())};
        TVector<TCell> values = {TCell(row.Json.data(), row.Json.size())};
        TVector<ui32> valueTags = {2};
        if (withRowId) {
            values.push_back(TCell::Make(row.RowId));
            valueTags.push_back(3);
        }
        UploadRow(runtime, "/MyRoot/texts", 0, {1}, valueTags, keys, values);
    }
}

void EnableJsonRowIdFlags(TTestActorRuntime& runtime) {
    auto& appData = runtime.GetAppData();
    appData.FeatureFlags.SetEnableJsonIndex(true);
    appData.FeatureFlags.SetEnableFulltextIndex(true);
    appData.FeatureFlags.SetEnableAddUniqueIndex(true);
    appData.FeatureFlags.SetEnableUniqConstraint(true);
}

// Same as EnableJsonRowIdFlags plus the compact-index flag so a JSON build proto is materialized as a
// compact (rowid-mode) index. The schemeshard caches EnableCompactFulltextIndex at activation (it read
// appData before this runs), so reboot it to pick up the updated value.
void EnableJsonCompactRowIdFlags(TTestActorRuntime& runtime) {
    EnableJsonRowIdFlags(runtime);
    runtime.GetAppData().FeatureFlags.SetEnableCompactFulltextIndex(true);
    RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
}

void RebootJsonTableShardsAndAssertPartitions(TTestBasicRuntime& runtime, const TString& path,
    ui32 expectedPartitions)
{
    const auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard,
        path, /*returnPartitioning=*/ true, /*returnBoundaries=*/ true, /*showPrivate=*/ true);
    const auto& partitions = describe.GetPathDescription().GetTablePartitions();
    UNIT_ASSERT_VALUES_EQUAL_C(partitions.size(), expectedPartitions, path);
    for (const auto& partition : partitions) {
        RebootTablet(runtime, partition.GetDatashardId(), runtime.AllocateEdgeActor());
    }
}

TString RowIdSrcTablePath(const TString& indexPath) {
    return TStringBuilder() << indexPath << "/"
        << NTableIndex::ImplTable << NTableIndex::NFulltext::RowIdSrcBuildSuffix;
}

} // namespace

Y_UNIT_TEST_SUITE(JsonIndexBuildTest) {
    Y_UNIT_TEST_FLAG(Basic, Compact) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableCompactFulltextIndex(Compact));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateJsonTable(runtime, env, txId);

        DoWriteJsonRows(runtime, {
            {1, R"({"a": 1})"},
            {2, R"({"b": 2})"},
            {3, R"({"a": 1, "b": 2})"},
        });

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                op.DebugString()
            );
        }

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/json_idx"), {
            NLs::PathExist,
            NLs::IndexType(runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()
                ? NKikimrSchemeOp::EIndexTypeGlobalJsonCompact
                : NKikimrSchemeOp::EIndexTypeGlobalJson),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"data"}),
            NLs::ChildrenCount(1),
        });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/json_idx/" + TString(NTableIndex::ImplTable)), {
            NLs::PathExist,
        });

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/table/json_idx/" + TString(NTableIndex::ImplTable));
            Cerr << "... impl table contains " << rows << " rows" << Endl;
            UNIT_ASSERT_C(rows > 0, "indexImplTable must be non-empty after building");
        }
    }

    Y_UNIT_TEST_FLAG(Drop, Compact) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableCompactFulltextIndex(Compact));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateJsonTable(runtime, env, txId);

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                op.DebugString()
            );
        }

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/json_idx"), {
            NLs::PathExist,
            NLs::IndexType(runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()
                ? NKikimrSchemeOp::EIndexTypeGlobalJsonCompact
                : NKikimrSchemeOp::EIndexTypeGlobalJson),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"data"}),
            NLs::ChildrenCount(1),
        });

        TestDropTableIndex(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", R"(
            TableName: "table"
            IndexName: "json_idx"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/json_idx"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST_FLAG(DropTableWithJsonIndex, Compact) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableCompactFulltextIndex(Compact));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateJsonTable(runtime, env, txId);

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/json_idx"), {
            NLs::PathExist,
            NLs::IndexType(runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()
                ? NKikimrSchemeOp::EIndexTypeGlobalJsonCompact
                : NKikimrSchemeOp::EIndexTypeGlobalJson),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexKeys({"data"}),
            NLs::ChildrenCount(1),
        });

        TestDropTable(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", "table");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST_FLAG(Limit, Compact) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true).EnableCompactFulltextIndex(Compact));
        ui64 txId = 100;

        DoCreateJsonTable(runtime, env, txId);

        auto describe = DescribePath(runtime, "/MyRoot/table");
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NKikimrScheme::StatusSuccess, describe.GetStatus());
        auto curShards = describe.GetPathDescription().GetDomainDescription().GetShardsInside();

        // JSON index creates 2 or 3 new paths (index + indexImplTable + __ydb_generation sequence) and 1 or 2 new shards
        const ui32 requiredPaths = runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex() ? 3 : 2;
        const ui32 requiredShards = runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex() ? 2 : 1;
        Ydb::Table::TableIndex index = JsonIndexConfig();

        TSchemeLimits lowLimits;

        // Not enough paths: /MyRoot/table is 1 path inside domain; need 2 more (index + implTable) = 3 total
        lowLimits.MaxPaths = requiredPaths;
        lowLimits.MaxShards = curShards + requiredShards;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        // Not enough shards
        lowLimits.MaxPaths = 1 + requiredPaths;
        lowLimits.MaxShards = curShards + requiredShards - 1;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        // Enough paths and shards
        lowLimits.MaxPaths = 1 + requiredPaths;
        lowLimits.MaxShards = curShards + requiredShards;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", index, Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST_TWIN(ImportExport, Materialized) {
        NKikimr::InitAwsAPI();

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        NWrappers::NTestHelpers::TS3Mock s3Mock({}, NWrappers::NTestHelpers::TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableIndexMaterialization(Materialized));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateJsonTable(runtime, env, txId);
        DoWriteJsonRows(runtime, {
            {1, R"({"a": 1})"},
            {2, R"({"b": 2})"},
        });

        {
            const ui64 buildIndexTx = ++txId;
            TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", JsonIndexConfig());
            env.TestWaitNotification(runtime, buildIndexTx);
        }

        auto checkIndex = [&](const TString& path) {
            const auto d = DescribePath(runtime, path, true, true);
            bool found = false;
            for (const auto& idx : d.GetPathDescription().GetTable().GetTableIndexes()) {
                if (idx.GetName() == "json_idx") {
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()
                        ? NKikimrSchemeOp::EIndexTypeGlobalJsonCompact
                        : NKikimrSchemeOp::EIndexTypeGlobalJson);
                    found = true;
                }
            }
            UNIT_ASSERT_C(found, "json_idx missing on " << path);
        };

        checkIndex("/MyRoot/table");

        const ui64 exportTxId = ++txId;
        TestExport(runtime, exportTxId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                    source_path: "/MyRoot/table"
                    destination_prefix: "test"
                }
                %s
            }
        )", port, Materialized ? "include_index_data: true" : ""));
        env.TestWaitNotification(runtime, exportTxId);
        TestGetExport(runtime, exportTxId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        const ui64 importId = ++txId;
        const TString popMode = Materialized
            ? "index_population_mode: " + Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT)
            : "";
        TestImport(runtime, importId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                    source_prefix: "test"
                    destination_path: "/MyRoot/table_imported"
                }
                %s
            }
        )", port, popMode.c_str()));
        env.TestWaitNotification(runtime, importId);
        TestGetImport(runtime, importId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        checkIndex("/MyRoot/table_imported");

        NKikimr::ShutdownAwsAPI();
    }

    Y_UNIT_TEST(RowIdOptIn_BuildsAndKeysByRowId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateJsonTableWithRowId(runtime, env, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/uniq_rowid"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        // No data is written here: the assertion below is purely on the impl-table schema (its key columns),
        // which holds regardless of contents. Bulk upload cannot target a table that already has the unique
        // secondary index, and the custom-PK auto-provision tests cover the data-backfill path instead.

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                op.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                op.DebugString());
        }

        if (!runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            // The JSON posting impl-table must be keyed by [__ydb_token, __ydb_row_id], not by [__ydb_token, pk].
            // But with the compact index, the posting table doesn't differ.
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_idx/" + TString(NTableIndex::ImplTable)), {
                NLs::PathExist,
                NLs::CheckColumns(TString(NTableIndex::ImplTable),
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    {},
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    /*strictCount=*/ true),
            });
        }
    }

    Y_UNIT_TEST(RowIdOptIn_RejectsIfRowIdWrongType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        DoCreateJsonTableWithRowId(runtime, env, txId,
            /*rowIdType=*/ "Uint32",
            /*rowIdNotNull=*/ true,
            /*createUniqueIndex=*/ false);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig(),
            Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(RowIdOptIn_RejectsIfRowIdNullable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        DoCreateJsonTableWithRowId(runtime, env, txId,
            /*rowIdType=*/ "Uint64",
            /*rowIdNotNull=*/ false,
            /*createUniqueIndex=*/ false);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig(),
            Ydb::StatusIds::BAD_REQUEST);
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(RowIdOptIn_AutoProvisionsMissingUniqueIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // __ydb_row_id is well-formed (Uint64 NOT NULL) but has no unique index yet - auto-provision it.
        DoCreateJsonTableWithRowId(runtime, env, txId,
            /*rowIdType=*/ "Uint64",
            /*rowIdNotNull=*/ true,
            /*createUniqueIndex=*/ false);

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
        UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());

        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
    }

    Y_UNIT_TEST(RowIdOptIn_AutoProvisionsRowIdAndUniqueIndexForCustomPk) {
        // A custom (non single integer) PK without __ydb_row_id is auto-provisioned: the build adds the
        // __ydb_row_id column and a unique index over it.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateCustomPkJsonTable(runtime, env, txId);
        DoWriteJsonTextRows(runtime, /*withRowId=*/ false);

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // Both the __ydb_row_id column and its unique index were auto-provisioned; the unique index is Ready.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        if (!runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            // The JSON posting impl-table is keyed by [__ydb_token, __ydb_row_id].
            // But with the compact index, it doesn't differ.
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_idx/" + TString(NTableIndex::ImplTable)), {
                NLs::PathExist,
                NLs::CheckColumns(TString(NTableIndex::ImplTable),
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    {},
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    /*strictCount=*/ true),
            });
        }
    }

    Y_UNIT_TEST(RowIdOptIn_CompactBuildsOverCustomPkAndDropsRowIdSrc) {
        // Compact rowid-mode JSON build over a custom (Utf8) PK: rowid mode must activate for the compact
        // JSON type (EIndexTypeGlobalJsonCompact) exactly as it does for plain JSON. The build runs the
        // row-id source prepass, auto-provisions __ydb_row_id + its unique index, builds the compact
        // posting impl-table and, on completion, drops the transient "rowidsrc" build table.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableCompactFulltextIndex(true));
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateCustomPkJsonTable(runtime, env, txId);
        DoWriteJsonTextRows(runtime, /*withRowId=*/ false);

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // The auto-provisioned unique index over __ydb_row_id exists and is Ready.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        // The compact posting impl-table is keyed by [__ydb_token, __ydb_max_id, __ydb_generation] and
        // stores the delta-encoded __ydb_segment (this is what distinguishes a compact index from a plain
        // one, whose impl-table is keyed by [__ydb_token, __ydb_row_id] and has no segment column).
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_idx/" + TString(NTableIndex::ImplTable)), {
            NLs::PathExist,
            NLs::CheckColumns(TString(NTableIndex::ImplTable),
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn, NTableIndex::NFulltext::AddedColumn,
                  NTableIndex::NFulltext::SegmentColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn },
                /*strictCount=*/ true),
        });

        // The transient row-id source build table was dropped on completion.
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/json_idx")), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(RowIdOptIn_CompactTopologyImplSplitMainMergeRebootAndRebuild) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableJsonCompactRowIdFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkJsonTable(runtime, env, txId);
        DoWriteJsonTextRows(runtime, /*withRowId=*/ false);

        const ui64 initialBuildTx = ++txId;
        TestBuildIndex(runtime, initialBuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        env.TestWaitNotification(runtime, initialBuildTx);
        const auto initialBuild = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot", initialBuildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(initialBuild.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, initialBuild.DebugString());

        const TString compactImpl = "/MyRoot/texts/json_idx/indexImplTable";
        const TString rowIdImpl = TStringBuilder() << "/MyRoot/texts/"
            << NTableIndex::NFulltext::RowIdUniqueIndexName << "/" << NTableIndex::ImplTable;
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts"), {
            NLs::CheckColumns("texts",
                {"pk", "data", NTableIndex::NFulltext::RowIdColumn},
                {}, {"pk"}, /*strictCount=*/ true),
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime, compactImpl), {
            NLs::CheckColumns("indexImplTable",
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn, NTableIndex::NFulltext::AddedColumn,
                  NTableIndex::NFulltext::SegmentColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::MaxIdColumn,
                  NTableIndex::NFulltext::GenColumn },
                /*strictCount=*/ true),
        });
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        // Distinct JSON tokens: root plus key/value pairs for a=1, b=2 and c=3.
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, compactImpl), 7u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rowIdImpl), 4u);
        const TString mainRowsBeforeTopology = ReadShards(
            runtime, TTestTxConfig::SchemeShard, "/MyRoot/texts").at(0);

        auto split = [&](const TString& path, const TString& boundary) {
            const auto before = DescribePath(runtime, TTestTxConfig::SchemeShard,
                path, /*returnPartitioning=*/ true, /*returnBoundaries=*/ true, /*showPrivate=*/ true);
            const auto& partitions = before.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT_VALUES_EQUAL_C(partitions.size(), 1u, path);
            TestSplitTable(runtime, TTestTxConfig::SchemeShard, ++txId, path,
                Sprintf(R"(
                    SourceTabletId: %lu
                    SplitBoundary { KeyPrefix { %s } }
                )", partitions[0].GetDatashardId(), boundary.c_str()));
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, TTestTxConfig::SchemeShard,
                path, true, true, true), {
                NLs::PathExist,
                NLs::PartitionCount(2),
            });
        };

        split("/MyRoot/texts", R"(Tuple { Optional { Text: "ptwo" } })");
        // JSON tokens are binary strings, but a String key-prefix remains a supported deterministic
        // split boundary even when one side happens to be empty for a particular small corpus.
        split(compactImpl, R"(Tuple { Optional { Bytes: "m" } })");
        split(rowIdImpl, R"(Tuple { Optional { Uint64: 9223372036854775808 } })");

        RebootJsonTableShardsAndAssertPartitions(runtime, "/MyRoot/texts", 2);
        RebootJsonTableShardsAndAssertPartitions(runtime, compactImpl, 2);
        RebootJsonTableShardsAndAssertPartitions(runtime, rowIdImpl, 2);

        const auto mainAfterReboot = DescribePath(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/texts", /*returnPartitioning=*/ true, /*returnBoundaries=*/ true,
            /*showPrivate=*/ true);
        const auto& mainChildren = mainAfterReboot.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT_VALUES_EQUAL(mainChildren.size(), 2u);
        const auto& mainSelf = mainAfterReboot.GetPathDescription().GetSelf();
        const TTableId mainTableId(mainSelf.GetSchemeshardId(), mainSelf.GetPathId());
        for (const auto& child : mainChildren) {
            const auto result = CompactTable(runtime, child.GetDatashardId(), mainTableId,
                /*compactBorrowed=*/ true, /*compactSinglePartedShards=*/ true);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(),
                NKikimrTxDataShard::TEvCompactTableResult::OK, result.DebugString());
        }

        // Compact and generated unique implementation tables currently cannot be merged after a split:
        // DataShard's borrow logic rejects back-borrowed parts. Preserve their supported split/reboot
        // coverage and exercise the merge transition only on the main table.
        const auto compactRowsAfterSplit = ReadShards(runtime, TTestTxConfig::SchemeShard, compactImpl);
        const auto rowIdRowsAfterSplit = ReadShards(runtime, TTestTxConfig::SchemeShard, rowIdImpl);
        TestSplitTable(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/texts",
            Sprintf(R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", mainChildren[0].GetDatashardId(), mainChildren[1].GetDatashardId()));
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/texts", true, true, true), {
            NLs::PathExist,
            NLs::PartitionCount(1),
        });

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, compactImpl), 7u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rowIdImpl), 4u);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard,
            "/MyRoot/texts").at(0), mainRowsBeforeTopology);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard,
            compactImpl), compactRowsAfterSplit);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard,
            rowIdImpl), rowIdRowsAfterSplit);

        // Update an existing first-range row, preserving its generated row id. As in the fulltext build
        // harness, UploadRow is intentionally a raw main-table write, so a second build is the oracle that
        // the post-topology scan observes this DML without relying on asynchronous index writes.
        const TString pk = "pone";
        const TString json = R"({"topology": 9})";
        UploadRow(runtime, "/MyRoot/texts", 0, {1}, {2},
            {TCell(pk.data(), pk.size())}, {TCell(json.data(), json.size())});

        const ui64 rebuildTx = ++txId;
        TestBuildIndex(runtime, rebuildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", JsonIndexConfig("json_after_topology"));
        env.TestWaitNotification(runtime, rebuildTx);
        const auto rebuild = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", rebuildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(rebuild.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, rebuild.DebugString());

        const TString rebuiltImpl = "/MyRoot/texts/json_after_topology/indexImplTable";
        // a=1 remains in ptwo; replacing pone adds the new topology key and value token: 7 + 2.
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rebuiltImpl), 9u);
        TString physicalRows;
        for (const auto& shard : ReadShards(runtime, TTestTxConfig::SchemeShard, rebuiltImpl)) {
            physicalRows += shard;
        }
        UNIT_ASSERT_C(physicalRows.Contains("topology"), physicalRows);
    }

    Y_UNIT_TEST(RowIdOptIn_CancelCompactPrepassThenRestartAndRetryReusesInfra) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableJsonCompactRowIdFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkJsonTable(runtime, env, txId);
        DoWriteJsonTextRows(runtime, /*withRowId=*/ false);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> prepassBlocker(runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTargetName().EndsWith(NTableIndex::NFulltext::RowIdSrcBuildSuffix);
        });

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        runtime.WaitFor("JSON row-id source prepass scan request", [&]{ return prepassBlocker.size() > 0; });

        const auto running = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(running.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA, running.DebugString());

        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        prepassBlocker.Stop().Unblock();
        env.TestWaitNotification(runtime, buildTx);

        const auto cancelled = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
        UNIT_ASSERT_VALUES_EQUAL_C(cancelled.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_CANCELLED, cancelled.DebugString());
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_idx"), {NLs::PathNotExist});
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/json_idx")), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdSequenceName), {
            NLs::PathExist,
        });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        const ui64 retryTx = ++txId;
        TestBuildIndex(runtime, retryTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        env.TestWaitNotification(runtime, retryTx);
        const auto retry = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", retryTx);
        UNIT_ASSERT_VALUES_EQUAL_C(retry.GetIndexBuild().GetState(),
            Ydb::Table::IndexBuildState::STATE_DONE, retry.DebugString());

        const TString rowIdImpl = TStringBuilder() << "/MyRoot/texts/"
            << NTableIndex::NFulltext::RowIdUniqueIndexName << "/" << NTableIndex::ImplTable;
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts/json_idx/indexImplTable"), 7u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rowIdImpl), 4u);
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/json_idx")), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(RowIdOptIn_CompactPrepassSurvivesSchemeShardRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableJsonCompactRowIdFlags(runtime);
        ui64 txId = 100;

        DoCreateCustomPkJsonTable(runtime, env, txId);
        DoWriteJsonTextRows(runtime, /*withRowId=*/ false);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> prepassBlocker(runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTargetName().EndsWith(NTableIndex::NFulltext::RowIdSrcBuildSuffix);
        });

        const ui64 buildTx = ++txId;
        TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard,
            "/MyRoot", "/MyRoot/texts", JsonIndexConfig());
        runtime.WaitFor("JSON row-id source prepass scan request", [&]{ return prepassBlocker.size() > 0; });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        prepassBlocker.Stop().Unblock();

        Ydb::Table::IndexBuildState::State state = Ydb::Table::IndexBuildState::STATE_UNSPECIFIED;
        for (int i = 0; i < 100; ++i) {
            const auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
            state = op.GetIndexBuild().GetState();
            if (state == Ydb::Table::IndexBuildState::STATE_DONE ||
                state == Ydb::Table::IndexBuildState::STATE_REJECTED ||
                state == Ydb::Table::IndexBuildState::STATE_CANCELLED) {
                break;
            }
            env.SimulateSleep(runtime, TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(state, Ydb::Table::IndexBuildState::STATE_DONE,
            "compact JSON build did not finish after SchemeShard restart");

        const TString rowIdImpl = TStringBuilder() << "/MyRoot/texts/"
            << NTableIndex::NFulltext::RowIdUniqueIndexName << "/" << NTableIndex::ImplTable;
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalUnique),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts"), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/texts/json_idx/indexImplTable"), 7u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, rowIdImpl), 4u);
        TestDescribeResult(DescribePrivatePath(runtime, RowIdSrcTablePath("/MyRoot/texts/json_idx")), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(AutoProvision_SecondJsonBuildReusesInfra) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        DoCreateCustomPkJsonTable(runtime, env, txId);
        DoWriteJsonTextRows(runtime, /*withRowId=*/ false);

        // First JSON index provisions __ydb_row_id + the unique index.
        {
            const ui64 buildTx = ++txId;
            TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig("json_one"));
            env.TestWaitNotification(runtime, buildTx);
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // Second JSON index reuses the existing __ydb_row_id + unique index (no duplicates).
        {
            const ui64 buildTx = ++txId;
            TestBuildIndex(runtime, buildTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", JsonIndexConfig("json_two"));
            env.TestWaitNotification(runtime, buildTx);
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/texts/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathExist,
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
        });

        if (!runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_two/" + TString(NTableIndex::ImplTable)), {
                NLs::PathExist,
                NLs::CheckColumns(TString(NTableIndex::ImplTable),
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    {},
                    { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                    /*strictCount=*/ true),
            });
        }
    }

    Y_UNIT_TEST(AutoProvision_SingleIntegerPkUnaffected) {
        // A single integer PK keeps the legacy doc_id=PK behaviour: no __ydb_row_id / unique index added.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        DoCreateJsonTable(runtime, env, txId);

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        {
            auto op = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(op.GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE, op.DebugString());
        }

        // No auto unique index was created.
        TestDescribeResult(DescribePrivatePath(runtime,
            TStringBuilder() << "/MyRoot/table/" << NTableIndex::NFulltext::RowIdUniqueIndexName), {
            NLs::PathNotExist,
        });

        if (!runtime.GetAppData().FeatureFlags.GetEnableCompactFulltextIndex()) {
            // The JSON impl-table is keyed by [__ydb_token, id] (the integer PK), not __ydb_row_id.
            TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/json_idx/" + TString(NTableIndex::ImplTable)), {
                NLs::PathExist,
                NLs::CheckColumns(TString(NTableIndex::ImplTable),
                    { NTableIndex::NFulltext::TokenColumn, "id" },
                    {},
                    { NTableIndex::NFulltext::TokenColumn, "id" },
                    /*strictCount=*/ true),
            });
        }
    }
}
