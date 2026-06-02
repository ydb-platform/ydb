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

Ydb::Table::TableIndex JsonIndexConfig() {
    Ydb::Table::TableIndex index;
    index.set_name("json_idx");
    index.add_index_columns("data");
    index.mutable_global_json_index();
    return index;
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
}
