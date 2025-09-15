#include "ut_helpers.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
using namespace Tests;
using Ydb::Table::FulltextIndexSettings;
using namespace NTableIndex::NFulltext;

static std::atomic<ui64> sId = 1;
static const TString kMainTable = "/Root/table-main";
static const TString kIndexTable = "/Root/table-index";

Y_UNIT_TEST_SUITE(TTxDataShardBuildFulltextIndexScan) {

    void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest,
        const TString& expectedError, bool expectedErrorSubstring = false, NKikimrIndexBuilder::EBuildStatus expectedStatus = NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        UNIT_ASSERT(datashards.size() == 1);

        auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
        auto& request = ev->Record;
        request.SetId(1);
        request.SetSeqNoGeneration(id);
        request.SetSeqNoRound(1);

        request.SetTabletId(datashards[0]);
        tableId.PathId.ToProto(request.MutablePathId());

        request.SetSnapshotTxId(snapshot.TxId);
        request.SetSnapshotStep(snapshot.Step);

        FulltextIndexSettings settings;
        settings.set_layout(FulltextIndexSettings::FLAT);
        *request.MutableSettings() = settings;

        request.SetIndexName(kIndexTable);

        setupRequest(request);

        NKikimr::DoBadRequest<TEvDataShard::TEvBuildFulltextIndexResponse>(server, sender, std::move(ev), datashards[0], expectedError, expectedErrorSubstring, expectedStatus);
    }

    TString DoBuild(Tests::TServer::TPtr server, TActorId sender, NKikimrTxDataShard::TEvBuildFulltextIndexRequest request) {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        UNIT_ASSERT(datashards.size() == 1);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        request.SetId(1);
        request.SetSeqNoGeneration(id);
        request.SetSeqNoRound(1);

        request.SetTabletId(datashards[0]);
        tableId.PathId.ToProto(request.MutablePathId());

        request.SetSnapshotTxId(snapshot.TxId);
        request.SetSnapshotStep(snapshot.Step);

        request.AddKeyColumns("text");
        request.AddKeyColumns("key");
        request.AddDataColumns("data");

        request.SetIndexName(kIndexTable);

        auto ev1 = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
        ev1->Record.CopyFrom(request);
        
        auto ev2 = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
        ev1->Record.CopyFrom(request);

        runtime.SendToPipe(datashards[0], sender, ev1.release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(datashards[0], sender, ev2.release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(handle);

        UNIT_ASSERT_EQUAL_C(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE, reply->Record.ShortDebugString());

        auto index = ReadShardedTable(server, kIndexTable);
        Cerr << "Index:" << Endl;
        Cerr << index << Endl;
        return std::move(index);
    }

    void CreateMainTable(Tests::TServer::TPtr server, TActorId sender) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        options.AllowSystemColumnNames(false);
        options.Columns({
            {"key", "Uint32", true, true},
            {"text", "String", false, false},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-main", options);
    }

    void FillMainTable(Tests::TServer::TPtr server, TActorId sender) {
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-main` (key, text, data) VALUES
                (1, "green apple", "one"),
                (2, "red apple", "two"),
                (3, "yellow apple", "three"),
                (4, "red car", "four")
        )"); 
    }

    void CreateIndexTable(Tests::TServer::TPtr server, TActorId sender) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        options.AllowSystemColumnNames(true);
        options.Columns({
            {TokenColumn, NTableIndex::NFulltext::TokenTypeName, true, true},
            {"key", "Uint32", true, true},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-index", options);
    }

    void Setup(Tests::TServer::TPtr server, TActorId sender) {
        server->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateMainTable(server, sender);
        FillMainTable(server, sender);
        CreateIndexTable(server, sender);
    }

    Y_UNIT_TEST(BadRequest) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        Setup(server, sender);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kMainTable)[0] << " }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     TPathId(0, 0).ToProto(request.MutablePathId());
        // }, "{ <main>: Error: Unknown table id: 0 }");

        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetSnapshotStep(request.GetSnapshotStep() + 1);
        // }, "Error: Unknown snapshot", true);
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
        // }, "Error: Unknown snapshot", true);

        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.MutableSettings()->set_vector_type(FulltextIndexSettings::VECTOR_TYPE_UNSPECIFIED);
        // }, "{ <main>: Error: vector_type should be set }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.MutableSettings()->set_vector_type(FulltextIndexSettings::VECTOR_TYPE_BIT);
        // }, "{ <main>: Error: Unsupported vector_type: VECTOR_TYPE_BIT }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.MutableSettings()->set_metric(FulltextIndexSettings::METRIC_UNSPECIFIED);
        // }, "{ <main>: Error: either distance or similarity should be set }");

        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetUpload(NKikimrTxDataShard::UNSPECIFIED);
        // }, "{ <main>: Error: Wrong upload }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetUpload(NKikimrTxDataShard::SAMPLE);
        // }, "{ <main>: Error: Wrong upload }");

        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetK(0);
        // }, "{ <main>: Error: Should be requested partition on at least two rows }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetK(1);
        // }, "{ <main>: Error: Should be requested partition on at least two rows }");

        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetParentFrom(100);
        //     request.SetParentTo(99);
        // }, "{ <main>: Error: Parent from 100 should be less or equal to parent to 99 }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetParentFrom(0);
        //     request.SetParentTo(0);
        //     request.SetUpload(NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING);
        // }, "{ <main>: Error: Wrong upload for zero parent }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetParentFrom(100);
        //     request.SetParentTo(200);
        //     request.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD);
        // }, "{ <main>: Error: Wrong upload for non-zero parent }");

        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.ClearLevelName();
        // }, "{ <main>: Error: Empty level table name }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.ClearOutputName();
        // }, "{ <main>: Error: Empty output table name }");

        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetEmbeddingColumn("some");
        // }, "{ <main>: Error: Unknown embedding column: some }");
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.AddDataColumns("some");
        // }, "{ <main>: Error: Unknown data column: some }");

        // // test multiple issues:
        // DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
        //     request.SetK(1);
        //     request.SetEmbeddingColumn("some");
        // }, "[ { <main>: Error: Should be requested partition on at least two rows } { <main>: Error: Unknown embedding column: some } ]");
    }

    // Y_UNIT_TEST(MainToPosting) {
    //     TPortManager pm;
    //     TServerSettings serverSettings(pm.GetPort(2134));
    //     serverSettings.SetDomainName("Root");

    //     Tests::TServer::TPtr server = new TServer(serverSettings);
    //     auto& runtime = *server->GetRuntime();
    //     auto sender = runtime.AllocateEdgeActor();

    //     runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
    //     runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

    //     InitRoot(server, sender);

    //     TShardedTableOptions options;
    //     options.EnableOutOfOrder(true);
    //     options.Shards(1);

    //     CreateMainTable(server, sender, options);
    //     // Upsert some initial values
    //     ExecSQL(server, sender,
    //             R"(
    //     UPSERT INTO `/Root/table-main`
    //         (key, embedding, data)
    //     VALUES )"
    //             "(1, \"\x30\x30\3\", \"one\"),"
    //             "(2, \"\x31\x31\3\", \"two\"),"
    //             "(3, \"\x32\x32\3\", \"three\"),"
    //             "(4, \"\x65\x65\3\", \"four\"),"
    //             "(5, \"\x75\x75\3\", \"five\");");

    //     auto create = [&] {
    //         CreateLevelTable(server, sender, options);
    //         CreatePostingTable(server, sender, options);
    //     };
    //     create();
    //     auto recreate = [&] {
    //         DropTable(server, sender, "table-level");
    //         DropTable(server, sender, "table-posting");
    //         create();
    //     };

    //     ui64 seed, k;
    //     k = 2;

    //     seed = 0;
    //     for (auto distance : {FulltextIndexSettings::DISTANCE_MANHATTAN, FulltextIndexSettings::DISTANCE_EUCLIDEAN}) {
    //         auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
    //                                               NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
    //                                               FulltextIndexSettings::VECTOR_TYPE_UINT8, distance);
    //         UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = mm\3\n"
    //                                         "__ydb_parent = 0, __ydb_id = 9223372036854775810, __ydb_centroid = 11\3\n");
    //         UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
    //                                           "__ydb_parent = 9223372036854775809, key = 5, data = five\n"
    //                                           "__ydb_parent = 9223372036854775810, key = 1, data = one\n"
    //                                           "__ydb_parent = 9223372036854775810, key = 2, data = two\n"
    //                                           "__ydb_parent = 9223372036854775810, key = 3, data = three\n");
    //         recreate();
    //     }

    //     seed = 111;
    //     for (auto distance : {FulltextIndexSettings::DISTANCE_MANHATTAN, FulltextIndexSettings::DISTANCE_EUCLIDEAN}) {
    //         auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
    //                                               NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
    //                                               FulltextIndexSettings::VECTOR_TYPE_UINT8, distance);
    //         UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = 11\3\n"
    //                                         "__ydb_parent = 0, __ydb_id = 9223372036854775810, __ydb_centroid = mm\3\n");
    //         UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
    //                                           "__ydb_parent = 9223372036854775809, key = 2, data = two\n"
    //                                           "__ydb_parent = 9223372036854775809, key = 3, data = three\n"
    //                                           "__ydb_parent = 9223372036854775810, key = 4, data = four\n"
    //                                           "__ydb_parent = 9223372036854775810, key = 5, data = five\n");
    //         recreate();
    //     }
    //     seed = 32;
    //     for (auto similarity : {FulltextIndexSettings::SIMILARITY_INNER_PRODUCT, FulltextIndexSettings::SIMILARITY_COSINE,
    //                             FulltextIndexSettings::DISTANCE_COSINE})
    //     {
    //         auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
    //                                               NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
    //                                               FulltextIndexSettings::VECTOR_TYPE_UINT8, similarity);
    //         UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = II\3\n");
    //         UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
    //                                           "__ydb_parent = 9223372036854775809, key = 2, data = two\n"
    //                                           "__ydb_parent = 9223372036854775809, key = 3, data = three\n"
    //                                           "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
    //                                           "__ydb_parent = 9223372036854775809, key = 5, data = five\n");
    //         recreate();
    //     }
    // }
}

}
