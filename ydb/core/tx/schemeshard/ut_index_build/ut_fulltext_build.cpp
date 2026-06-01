#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/metering/metering.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(FulltextIndexBuildTest) {

    void DoCreateTextTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "texts"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Ydb::Table::TableIndex FulltextIndexConfig(bool relevance) {
        Ydb::Table::TableIndex index;
        index.set_name("fulltext_idx");
        index.add_index_columns("text");
        if (relevance) {
            auto& fulltext = *index.mutable_global_fulltext_relevance_index()->mutable_fulltext_settings();
            auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
            fulltext.mutable_columns()->at(0).set_column("text");
            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        } else {
            auto& fulltext = *index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings();
            auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
            fulltext.mutable_columns()->at(0).set_column("text");
            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        }
        return index;
    }

    void DoCreateTextTableAndIndex(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
        bool relevance, std::function<void(Ydb::Table::TableIndex&)> cfg) {
        DoCreateTextTable(runtime, env, txId);

        auto fnWriteRow = [&] (ui64 id, TString text, TString data) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('id   (Uint64 '%u) ) ) )
                    (let row   '( '('text (String '"%s") )  '('data (String '"%s") ) ) )
                    (return (AsList (UpdateRow '__user__texts key row) ))
                )
            )", id, text.c_str(), data.c_str());

            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        };

        fnWriteRow(1, "green apple", "one");
        fnWriteRow(2, "red apple and blue apple", "two");
        fnWriteRow(3, "yellow apple", "three");
        fnWriteRow(4, "red car", "four");

        auto index = FulltextIndexConfig(relevance);
        if (cfg) {
            cfg(index);
        }

        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);
    }

    void DoCheckPlainIndexTable(TTestBasicRuntime& runtime, const TString& index) {
        auto rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplTable").at(0);
        Cerr << index << "/indexImplTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["and";["two"];["2"]];)"
            R"(["apple";["one"];["1"]];)"
            R"(["apple";["two"];["2"]];)"
            R"(["apple";["three"];["3"]];)"
            R"(["blue";["two"];["2"]];)"
            R"(["car";["four"];["4"]];)"
            R"(["green";["one"];["1"]];)"
            R"(["red";["two"];["2"]];)"
            R"(["red";["four"];["4"]];)"
            R"(["yellow";["three"];["3"]]];)"
        "%false]]]", rows);
    }

    void DoCheckRelevanceIndexTables(TTestBasicRuntime& runtime, const TString& index) {
        auto rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplTable").at(0);
        Cerr << index << "/indexImplTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["1";"and";["2"]];)"
            R"(["1";"apple";["1"]];)"
            R"(["2";"apple";["2"]];)"
            R"(["1";"apple";["3"]];)"
            R"(["1";"blue";["2"]];)"
            R"(["1";"car";["4"]];)"
            R"(["1";"green";["1"]];)"
            R"(["1";"red";["2"]];)"
            R"(["1";"red";["4"]];)"
            R"(["1";"yellow";["3"]]];)"
        "%false]]]", rows);

        rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplDictTable").at(0);
        Cerr << index << "/indexImplDictTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["1";"and"];)"
            R"(["3";"apple"];)"
            R"(["1";"blue"];)"
            R"(["1";"car"];)"
            R"(["1";"green"];)"
            R"(["2";"red"];)"
            R"(["1";"yellow"]];)"
        "%false]]]", rows);

        rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplDocsTable").at(0);
        Cerr << index << "/indexImplDocsTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["2";["one"];["1"]];)"
            R"(["5";["two"];["2"]];)"
            R"(["2";["three"];["3"]];)"
            R"(["2";["four"];["4"]]];)"
        "%false]]]", rows);

        rows = ReadShards(runtime, TTestTxConfig::SchemeShard, index+"/indexImplStatsTable").at(0);
        Cerr << index << "/indexImplStatsTable rows: " << rows << "\n";
        UNIT_ASSERT_VALUES_EQUAL("[[[["
            R"(["4";"0";"11"]];)"
        "%false]]]", rows);
    }

    Y_UNIT_TEST(Basic) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTableAndIndex(runtime, env, txId, false, [&](Ydb::Table::TableIndex& index) {
            index.add_data_columns("data");
        });

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", txId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
        }

        DoCheckPlainIndexTable(runtime, "/MyRoot/texts/fulltext_idx");
    }

    Y_UNIT_TEST(FlatRelevance) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTableAndIndex(runtime, env, txId, true, [&](Ydb::Table::TableIndex& index) {
            index.add_data_columns("data");
        });
        const ui64 buildIndexTx = txId;

        {
            auto buildIndexOperation = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexTx);
            UNIT_ASSERT_VALUES_EQUAL_C(
                buildIndexOperation.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
                buildIndexOperation.DebugString()
            );
        }

        DoCheckRelevanceIndexTables(runtime, "/MyRoot/texts/fulltext_idx");

        // Check that the index is successfully dropped
        TestDropTableIndex(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", R"(
            TableName: "texts"
            IndexName: "fulltext_idx"
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DropTableWithFlatRelevance) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        DoCreateTextTable(runtime, env, txId);

        Ydb::Table::TableIndex index = FulltextIndexConfig(true);
        const ui64 buildIndexTx = ++txId;
        TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
        env.TestWaitNotification(runtime, buildIndexTx);

        // Check that the table with index is successfully dropped
        TestDropTable(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", "texts");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(FlatRelevanceLimit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        DoCreateTextTable(runtime, env, txId);

        auto describe = DescribePath(runtime, "/MyRoot/texts");
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NKikimrScheme::StatusSuccess, "Unexpected status: " << describe.GetStatus());
        auto curShards = describe.GetPathDescription().GetDomainDescription().GetShardsInside();

        Ydb::Table::TableIndex index = FulltextIndexConfig(true);

        TSchemeLimits lowLimits;

        lowLimits.MaxPaths = 6;
        lowLimits.MaxShards = curShards + 3;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 5;
        lowLimits.MaxShards = curShards + 4;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::PRECONDITION_FAILED);
        env.TestWaitNotification(runtime, txId);

        lowLimits.MaxPaths = 6;
        lowLimits.MaxShards = curShards + 4;
        SetSchemeshardSchemaLimits(runtime, lowLimits);
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index, Ydb::StatusIds::SUCCESS);
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

        DoCreateTextTableAndIndex(runtime, env, txId, false, [&](Ydb::Table::TableIndex& index) {
            index.add_data_columns("data");
        });

        {
            auto index = FulltextIndexConfig(true);
            index.set_name("fulltext_rel_idx");
            index.add_data_columns("data");
            const ui64 buildIndexTx = ++txId;
            TestBuildIndex(runtime, buildIndexTx, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/texts", index);
            env.TestWaitNotification(runtime, buildIndexTx);
        }

        auto checkIndexes = [&](const TString& path) {
            const auto d = DescribePath(runtime, path, true, true);
            THashSet<TString> found;
            for (const auto& idx: d.GetPathDescription().GetTable().GetTableIndexes()) {
                found.insert(idx.GetName());
                if (idx.GetName() == "fulltext_idx") {
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain);
                } else if (idx.GetName() == "fulltext_rel_idx") {
                    UNIT_ASSERT_VALUES_EQUAL(idx.GetType(), NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance);
                }
            }
            UNIT_ASSERT_C(found.contains("fulltext_idx"), "missing fulltext_idx on " << path);
            UNIT_ASSERT_C(found.contains("fulltext_rel_idx"), "missing fulltext_rel_idx on " << path);
        };

        checkIndexes("/MyRoot/texts");

        const ui64 exportTxId = ++txId;
        TestExport(runtime, exportTxId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                    source_path: "/MyRoot/texts"
                    destination_prefix: "test"
                }
                %s
            }
        )", port, Materialized ? "include_index_data: true" : ""));
        env.TestWaitNotification(runtime, exportTxId);
        TestGetExport(runtime, exportTxId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        const ui64 importId = ++txId;
        const TString popMode = Materialized
            ? "index_population_mode: "+Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT)
            : "";
        TestImport(runtime, importId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                    source_prefix: "test"
                    destination_path: "/MyRoot/texts_imported"
                }
                %s
            }
        )", port, popMode.c_str()));
        env.TestWaitNotification(runtime, importId);
        TestGetImport(runtime, importId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        checkIndexes("/MyRoot/texts_imported");
        DoCheckPlainIndexTable(runtime, "/MyRoot/texts_imported/fulltext_idx");
        DoCheckRelevanceIndexTables(runtime, "/MyRoot/texts_imported/fulltext_rel_idx");

        NKikimr::ShutdownAwsAPI();
    }
}
