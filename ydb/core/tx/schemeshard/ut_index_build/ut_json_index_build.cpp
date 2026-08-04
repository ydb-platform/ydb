#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
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

TString RowIdSrcTablePath(const TString& indexPath) {
    return TStringBuilder() << indexPath << "/"
        << NTableIndex::ImplTable << NTableIndex::NFulltext::RowIdSrcBuildSuffix;
}

} // namespace

Y_UNIT_TEST_SUITE(JsonIndexBuildTest) {
    Y_UNIT_TEST(Basic) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
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

    Y_UNIT_TEST(Drop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
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

    Y_UNIT_TEST(DropTableWithJsonIndex) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateJsonTable(runtime, env, txId);

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", JsonIndexConfig());
        env.TestWaitNotification(runtime, buildIndexTx);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/table/json_idx"), {
            NLs::PathExist,
            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobalJson),
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

    Y_UNIT_TEST(Limit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        DoCreateJsonTable(runtime, env, txId);

        auto describe = DescribePath(runtime, "/MyRoot/table");
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NKikimrScheme::StatusSuccess, describe.GetStatus());
        auto curShards = describe.GetPathDescription().GetDomainDescription().GetShardsInside();

        // JSON index creates 2 new paths (index + indexImplTable) and 1 new shard
        Ydb::Table::TableIndex index = JsonIndexConfig();

        TSchemeLimits lowLimits;

        // Not enough paths: /MyRoot/table is 1 path inside domain; need 2 more (index + implTable) = 3 total
        lowLimits.MaxPaths = 2;
        lowLimits.MaxShards = curShards + 1;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        // Not enough shards
        lowLimits.MaxPaths = 3;
        lowLimits.MaxShards = curShards;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        // Enough paths and shards
        lowLimits.MaxPaths = 3;
        lowLimits.MaxShards = curShards + 1;
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
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), NKikimrSchemeOp::EIndexTypeGlobalJson);
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
        EnableJsonRowIdFlags(runtime);
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

        // The JSON posting impl-table must be keyed by [__ydb_token, __ydb_row_id], not by [__ydb_token, pk].
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_idx/" + TString(NTableIndex::ImplTable)), {
            NLs::PathExist,
            NLs::CheckColumns(TString(NTableIndex::ImplTable),
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                /*strictCount=*/ true),
        });
    }

    Y_UNIT_TEST(RowIdOptIn_RejectsIfRowIdWrongType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableJsonRowIdFlags(runtime);
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
        EnableJsonRowIdFlags(runtime);
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
        EnableJsonRowIdFlags(runtime);
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
        EnableJsonRowIdFlags(runtime);
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

        // The JSON posting impl-table is keyed by [__ydb_token, __ydb_row_id].
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_idx/" + TString(NTableIndex::ImplTable)), {
            NLs::PathExist,
            NLs::CheckColumns(TString(NTableIndex::ImplTable),
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                /*strictCount=*/ true),
        });
    }

    Y_UNIT_TEST(RowIdOptIn_CompactBuildsOverCustomPkAndDropsRowIdSrc) {
        // Compact rowid-mode JSON build over a custom (Utf8) PK: rowid mode must activate for the compact
        // JSON type (EIndexTypeGlobalJsonCompact) exactly as it does for plain JSON. The build runs the
        // row-id source prepass, auto-provisions __ydb_row_id + its unique index, builds the compact
        // posting impl-table and, on completion, drops the transient "rowidsrc" build table.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableJsonCompactRowIdFlags(runtime);
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

    Y_UNIT_TEST(AutoProvision_SecondJsonBuildReusesInfra) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableJsonRowIdFlags(runtime);
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
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/texts/json_two/" + TString(NTableIndex::ImplTable)), {
            NLs::PathExist,
            NLs::CheckColumns(TString(NTableIndex::ImplTable),
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                {},
                { NTableIndex::NFulltext::TokenColumn, NTableIndex::NFulltext::RowIdColumn },
                /*strictCount=*/ true),
        });
    }

    Y_UNIT_TEST(AutoProvision_SingleIntegerPkUnaffected) {
        // A single integer PK keeps the legacy doc_id=PK behaviour: no __ydb_row_id / unique index added.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableJsonRowIdFlags(runtime);
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
