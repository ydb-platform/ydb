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
using Ydb::Table::VectorIndexSettings;
using namespace NTableIndex::NTableVectorKmeansTreeIndex;

static std::atomic<ui64> sId = 1;
static const TString kMainTable = "/Root/table-main";
static const TString kLevelTable = "/Root/table-level";
static const TString kPostingTable = "/Root/table-posting";

Y_UNIT_TEST_SUITE(TTxDataShardLocalKMeansScan) {

    static void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvLocalKMeansRequest&)> setupRequest,
        const TString& expectedError, bool expectedErrorSubstring = false, NKikimrIndexBuilder::EBuildStatus expectedStatus = NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        TStringBuilder data;
        TString err;
        UNIT_ASSERT(datashards.size() == 1);

        auto ev = std::make_unique<TEvDataShard::TEvLocalKMeansRequest>();
        auto& rec = ev->Record;
        rec.SetId(1);

        rec.SetSeqNoGeneration(id);
        rec.SetSeqNoRound(1);

        rec.SetTabletId(datashards[0]);
        tableId.PathId.ToProto(rec.MutablePathId());

        rec.SetSnapshotTxId(snapshot.TxId);
        rec.SetSnapshotStep(snapshot.Step);

        VectorIndexSettings settings;
        settings.set_vector_dimension(2);
        settings.set_vector_type(VectorIndexSettings::VECTOR_TYPE_UINT8);
        settings.set_metric(VectorIndexSettings::DISTANCE_COSINE);
        *rec.MutableSettings() = settings;

        rec.SetK(2);
        rec.SetSeed(1337);

        rec.SetUpload(NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING);

        rec.SetNeedsRounds(3);

        rec.SetParentFrom(0);
        rec.SetParentTo(0);
        rec.SetChild(1);

        rec.SetEmbeddingColumn("embedding");

        rec.SetLevelName(kLevelTable);
        rec.SetOutputName(kPostingTable);

        setupRequest(rec);

        NKikimr::DoBadRequest<TEvDataShard::TEvLocalKMeansResponse>(server, sender, std::move(ev), datashards[0], expectedError, expectedErrorSubstring, expectedStatus);
    }

    static std::tuple<TString, TString> DoLocalKMeans(
        Tests::TServer::TPtr server, TActorId sender, NTableIndex::TClusterId parentFrom, NTableIndex::TClusterId parentTo, ui64 seed, ui64 k,
        NKikimrTxDataShard::EKMeansState upload, VectorIndexSettings::VectorType type,
        VectorIndexSettings::Metric metric, ui32 maxBatchRows = 50000)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        TString err;

        for (auto tid : datashards) {
            auto ev1 = std::make_unique<TEvDataShard::TEvLocalKMeansRequest>();
            auto ev2 = std::make_unique<TEvDataShard::TEvLocalKMeansRequest>();
            auto fill = [&](std::unique_ptr<TEvDataShard::TEvLocalKMeansRequest>& ev) {
                auto& rec = ev->Record;
                rec.SetId(1);

                rec.SetSeqNoGeneration(id);
                rec.SetSeqNoRound(1);

                rec.SetTabletId(tid);
                tableId.PathId.ToProto(rec.MutablePathId());

                rec.SetSnapshotTxId(snapshot.TxId);
                rec.SetSnapshotStep(snapshot.Step);

                VectorIndexSettings settings;
                settings.set_vector_dimension(2);
                settings.set_vector_type(type);
                settings.set_metric(metric);
                *rec.MutableSettings() = settings;

                rec.SetK(k);
                rec.SetSeed(seed);

                rec.SetUpload(upload);

                rec.SetNeedsRounds(300);

                rec.SetParentFrom(parentFrom);
                rec.SetParentTo(parentTo);
                rec.SetChild(parentTo + 1);

                rec.SetEmbeddingColumn("embedding");
                rec.AddDataColumns("data");

                rec.SetLevelName(kLevelTable);
                rec.SetOutputName(kPostingTable);

                rec.MutableScanSettings()->SetMaxBatchRows(maxBatchRows);
            };
            fill(ev1);
            fill(ev2);

            runtime.SendToPipe(tid, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tid, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvLocalKMeansResponse>(handle);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
            UNIT_ASSERT_EQUAL_C(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE,
                                issues.ToOneLineString());
        }

        auto level = ReadShardedTable(server, kLevelTable);
        auto posting = ReadShardedTable(server, kPostingTable);
        Cerr << "Level:" << Endl;
        Cerr << level << Endl;
        Cerr << "Posting:" << Endl;
        Cerr << posting << Endl;
        return {std::move(level), std::move(posting)};
    }

    static void DropTable(Tests::TServer::TPtr server, TActorId sender, const TString& name)
    {
        ui64 txId = AsyncDropTable(server, sender, "/Root", name);
        WaitTxNotification(server, sender, txId);
    }

    static void CreateMainTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
    {
        options.AllowSystemColumnNames(false);
        options.Columns({
            {"key", "Uint32", true, true},
            {"embedding", "String", false, false},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-main", options);
    }

    static void CreateLevelTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
    {
        options.AllowSystemColumnNames(true);
        options.Columns({
            {ParentColumn, NTableIndex::ClusterIdTypeName, true, true},
            {IdColumn, NTableIndex::ClusterIdTypeName, true, true},
            {CentroidColumn, "String", false, true},
        });
        CreateShardedTable(server, sender, "/Root", "table-level", options);
    }

    static void CreatePostingTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
    {
        options.AllowSystemColumnNames(true);
        options.Columns({
            {ParentColumn, NTableIndex::ClusterIdTypeName, true, true},
            {"key", "Uint32", true, true},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-posting", options);
    }

    static void CreateBuildTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options, const TString& name)
    {
        options.AllowSystemColumnNames(true);
        options.Columns({
            {ParentColumn, NTableIndex::ClusterIdTypeName, true, true},
            {"key", "Uint32", true, true},
            {"embedding", "String", false, false},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", name, options);
    }

    Y_UNIT_TEST (BadRequest) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?
        options.Shards(1);
        CreateMainTable(server, sender, options);
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (key, embedding, data)
        VALUES )"
                "(1, \"\x30\x30\3\", \"one\"),"
                "(2, \"\x31\x31\3\", \"two\"),"
                "(3, \"\x32\x32\3\", \"three\"),"
                "(4, \"\x65\x65\3\", \"four\"),"
                "(5, \"\x75\x75\3\", \"five\");");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kMainTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetSnapshotStep(request.GetSnapshotStep() + 1);
        }, "Error: Unknown snapshot", true);
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
        }, "Error: Unknown snapshot", true);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED);
        }, "{ <main>: Error: Wrong vector type }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_BIT);
        }, "{ <main>: Error: TODO(mbkkt) bit vector type is not supported }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.MutableSettings()->set_metric(VectorIndexSettings::METRIC_UNSPECIFIED);
        }, "{ <main>: Error: Wrong similarity }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::UNSPECIFIED);
        }, "{ <main>: Error: Wrong upload }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::SAMPLE);
        }, "{ <main>: Error: Wrong upload }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetK(0);
        }, "{ <main>: Error: Should be requested partition on at least two rows }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetK(1);
        }, "{ <main>: Error: Should be requested partition on at least two rows }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetParentFrom(100);
            request.SetParentTo(99);
        }, "{ <main>: Error: Parent from 100 should be less or equal to parent to 99 }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetParentFrom(0);
            request.SetParentTo(0);
            request.SetUpload(NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING);
        }, "{ <main>: Error: Wrong upload for zero parent }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetParentFrom(100);
            request.SetParentTo(200);
            request.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD);
        }, "{ <main>: Error: Wrong upload for non-zero parent }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.ClearLevelName();
        }, "{ <main>: Error: Empty level table name }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.ClearOutputName();
        }, "{ <main>: Error: Empty output table name }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetEmbeddingColumn("some");
        }, "{ <main>: Error: Unknown embedding column: some }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.AddDataColumns("some");
        }, "{ <main>: Error: Unknown data column: some }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetK(1);
            request.SetEmbeddingColumn("some");
        }, "[ { <main>: Error: Should be requested partition on at least two rows } { <main>: Error: Unknown embedding column: some } ]");
    }

    Y_UNIT_TEST (TooManyClusters) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        CreateMainTable(server, sender, options);
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (key, embedding, data)
        VALUES )"
                "(1, \"\x30\x30\3\", \"one\"),"
                "(2, \"\x31\x31\3\", \"two\"),"
                "(3, \"\x32\x32\3\", \"three\"),"
                "(4, \"\x65\x65\3\", \"four\"),"
                "(5, \"\x75\x75\3\", \"five\");");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetChild(Max<ui64>() - 100);
        }, "Condition violated: `(parent & PostingParentFlag) == 0'", true, NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
    }

    Y_UNIT_TEST (MainToPosting) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?
        options.Shards(1);

        CreateMainTable(server, sender, options);
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (key, embedding, data)
        VALUES )"
                "(1, \"\x30\x30\3\", \"one\"),"
                "(2, \"\x31\x31\3\", \"two\"),"
                "(3, \"\x32\x32\3\", \"three\"),"
                "(4, \"\x65\x65\3\", \"four\"),"
                "(5, \"\x75\x75\3\", \"five\");");

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreatePostingTable(server, sender, options);
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-level");
            DropTable(server, sender, "table-posting");
            create();
        };

        ui64 seed, k;
        k = 2;

        seed = 0;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = mm\3\n"
                                            "__ydb_parent = 0, __ydb_id = 9223372036854775810, __ydb_centroid = 11\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
                                              "__ydb_parent = 9223372036854775809, key = 5, data = five\n"
                                              "__ydb_parent = 9223372036854775810, key = 1, data = one\n"
                                              "__ydb_parent = 9223372036854775810, key = 2, data = two\n"
                                              "__ydb_parent = 9223372036854775810, key = 3, data = three\n");
            recreate();
        }

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = 11\3\n"
                                            "__ydb_parent = 0, __ydb_id = 9223372036854775810, __ydb_centroid = mm\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
                                              "__ydb_parent = 9223372036854775809, key = 2, data = two\n"
                                              "__ydb_parent = 9223372036854775809, key = 3, data = three\n"
                                              "__ydb_parent = 9223372036854775810, key = 4, data = four\n"
                                              "__ydb_parent = 9223372036854775810, key = 5, data = five\n");
            recreate();
        }
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = II\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
                                              "__ydb_parent = 9223372036854775809, key = 2, data = two\n"
                                              "__ydb_parent = 9223372036854775809, key = 3, data = three\n"
                                              "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
                                              "__ydb_parent = 9223372036854775809, key = 5, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST (MainToBuild) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?
        options.Shards(1);

        CreateMainTable(server, sender, options);
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (key, embedding, data)
        VALUES )"
                "(1, \"\x30\x30\3\", \"one\"),"
                "(2, \"\x31\x31\3\", \"two\"),"
                "(3, \"\x32\x32\3\", \"three\"),"
                "(4, \"\x65\x65\3\", \"four\"),"
                "(5, \"\x75\x75\3\", \"five\");");

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreateBuildTable(server, sender, options, "table-posting");
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-level");
            DropTable(server, sender, "table-posting");
            create();
        };

        ui64 seed, k;
        k = 2;

        seed = 0;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 1, __ydb_centroid = mm\3\n"
                                            "__ydb_parent = 0, __ydb_id = 2, __ydb_centroid = 11\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 1, key = 5, embedding = \x75\x75\3, data = five\n"
                                              "__ydb_parent = 2, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 2, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 2, key = 3, embedding = \x32\x32\3, data = three\n");
            recreate();
        }

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 1, __ydb_centroid = 11\3\n"
                                            "__ydb_parent = 0, __ydb_id = 2, __ydb_centroid = mm\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 1, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 1, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 2, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 2, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 1, __ydb_centroid = II\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 1, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 1, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 1, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 1, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST (BuildToPosting) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?
        options.Shards(1);

        CreateBuildTable(server, sender, options, "table-main");
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (__ydb_parent, key, embedding, data)
        VALUES )"
                "(39, 1, \"\x30\x30\3\", \"one\"),"
                "(40, 1, \"\x30\x30\3\", \"one\"),"
                "(40, 2, \"\x31\x31\3\", \"two\"),"
                "(40, 3, \"\x32\x32\3\", \"three\"),"
                "(40, 4, \"\x65\x65\3\", \"four\"),"
                "(40, 5, \"\x75\x75\3\", \"five\"),"
                "(41, 5, \"\x75\x75\3\", \"five\");");

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreatePostingTable(server, sender, options);
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-level");
            DropTable(server, sender, "table-posting");
            create();
        };

        ui64 seed, k;
        k = 2;

        seed = 0;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                                                VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = mm\3\n"
                                            "__ydb_parent = 40, __ydb_id = 9223372036854775850, __ydb_centroid = 11\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775849, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775849, key = 5, data = five\n"
                                            "__ydb_parent = 9223372036854775850, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775850, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775850, key = 3, data = three\n");
            recreate();
        }

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                                                VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = 11\3\n"
                                            "__ydb_parent = 40, __ydb_id = 9223372036854775850, __ydb_centroid = mm\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775849, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775849, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775849, key = 3, data = three\n"
                                            "__ydb_parent = 9223372036854775850, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775850, key = 5, data = five\n");
            recreate();
        }
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                                                VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = II\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775849, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775849, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775849, key = 3, data = three\n"
                                            "__ydb_parent = 9223372036854775849, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775849, key = 5, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST (BuildToBuild) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?
        options.Shards(1);

        CreateBuildTable(server, sender, options, "table-main");
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (__ydb_parent, key, embedding, data)
        VALUES )"
                "(39, 1, \"\x30\x30\3\", \"one\"),"
                "(40, 1, \"\x30\x30\3\", \"one\"),"
                "(40, 2, \"\x31\x31\3\", \"two\"),"
                "(40, 3, \"\x32\x32\3\", \"three\"),"
                "(40, 4, \"\x65\x65\3\", \"four\"),"
                "(40, 5, \"\x75\x75\3\", \"five\"),"
                "(41, 5, \"\x75\x75\3\", \"five\");");

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreateBuildTable(server, sender, options, "table-posting");
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-level");
            DropTable(server, sender, "table-posting");
            create();
        };

        ui64 seed, k;
        k = 2;

        seed = 0;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = mm\3\n"
                                            "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = 11\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 41, key = 5, embedding = \x75\x75\3, data = five\n"
                                              "__ydb_parent = 42, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 42, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 42, key = 3, embedding = \x32\x32\3, data = three\n");
            recreate();
        }

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = 11\3\n"
                                            "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = mm\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 41, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 41, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 42, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 42, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = II\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 41, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 41, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 41, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 41, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST (BuildToBuild_Ranges) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?
        options.Shards(1);

        CreateBuildTable(server, sender, options, "table-main");
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (__ydb_parent, key, embedding, data)
        VALUES )"
                "(39, 1, \"\x30\x30\3\", \"one\"),"
                "(39, 2, \"\x32\x32\3\", \"two\"),"
                "(40, 1, \"\x30\x30\3\", \"one\"),"
                "(40, 2, \"\x31\x31\3\", \"two\"),"
                "(40, 3, \"\x32\x32\3\", \"three\"),"
                "(40, 4, \"\x65\x65\3\", \"four\"),"
                "(40, 5, \"\x75\x75\3\", \"five\"),"
                "(41, 5, \"\x75\x75\3\", \"five2\"),"
                "(41, 6, \"\x76\x76\3\", \"six\");");

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreateBuildTable(server, sender, options, "table-posting");
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-level");
            DropTable(server, sender, "table-posting");
            create();
        };

        { // ParentFrom = 39 ParentTo = 39
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    39, 39, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level,
                    "__ydb_parent = 39, __ydb_id = 40, __ydb_centroid = 00\3\n"
                    "__ydb_parent = 39, __ydb_id = 41, __ydb_centroid = 22\3\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 40, key = 1, embedding = 00\3, data = one\n"
                    "__ydb_parent = 41, key = 2, embedding = 22\3, data = two\n");
                recreate();
            }
        }

        { // ParentFrom = 40 ParentTo = 40
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    40, 40, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level,
                    "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = 11\3\n"
                    "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = mm\3\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 41, key = 1, embedding = \x30\x30\3, data = one\n"
                    "__ydb_parent = 41, key = 2, embedding = \x31\x31\3, data = two\n"
                    "__ydb_parent = 41, key = 3, embedding = \x32\x32\3, data = three\n"
                    "__ydb_parent = 42, key = 4, embedding = \x65\x65\3, data = four\n"
                    "__ydb_parent = 42, key = 5, embedding = \x75\x75\3, data = five\n");
                recreate();
            }
        }

        { // ParentFrom = 41 ParentTo = 41
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    41, 41, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level,
                    "__ydb_parent = 41, __ydb_id = 42, __ydb_centroid = uu\3\n"
                    "__ydb_parent = 41, __ydb_id = 43, __ydb_centroid = vv\3\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 42, key = 5, embedding = uu\3, data = five2\n"
                    "__ydb_parent = 43, key = 6, embedding = vv\3, data = six\n");
                recreate();
            }
        }

        { // ParentFrom = 39 ParentTo = 40
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    39, 40, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level,
                    "__ydb_parent = 39, __ydb_id = 41, __ydb_centroid = 00\3\n"
                    "__ydb_parent = 39, __ydb_id = 42, __ydb_centroid = 22\3\n"
                    "__ydb_parent = 40, __ydb_id = 43, __ydb_centroid = 11\3\n"
                    "__ydb_parent = 40, __ydb_id = 44, __ydb_centroid = mm\3\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 41, key = 1, embedding = 00\3, data = one\n"
                    "__ydb_parent = 42, key = 2, embedding = 22\3, data = two\n"
                    "__ydb_parent = 43, key = 1, embedding = \x30\x30\3, data = one\n"
                    "__ydb_parent = 43, key = 2, embedding = \x31\x31\3, data = two\n"
                    "__ydb_parent = 43, key = 3, embedding = \x32\x32\3, data = three\n"
                    "__ydb_parent = 44, key = 4, embedding = \x65\x65\3, data = four\n"
                    "__ydb_parent = 44, key = 5, embedding = \x75\x75\3, data = five\n");
                recreate();
            }
        }

        {  // ParentFrom = 40 ParentTo = 41
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    40, 41, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level,
                    "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = 11\3\n"
                    "__ydb_parent = 40, __ydb_id = 43, __ydb_centroid = mm\3\n"
                    "__ydb_parent = 41, __ydb_id = 44, __ydb_centroid = uu\3\n"
                    "__ydb_parent = 41, __ydb_id = 45, __ydb_centroid = vv\3\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 42, key = 1, embedding = \x30\x30\3, data = one\n"
                    "__ydb_parent = 42, key = 2, embedding = \x31\x31\3, data = two\n"
                    "__ydb_parent = 42, key = 3, embedding = \x32\x32\3, data = three\n"
                    "__ydb_parent = 43, key = 4, embedding = \x65\x65\3, data = four\n"
                    "__ydb_parent = 43, key = 5, embedding = \x75\x75\3, data = five\n"
                    "__ydb_parent = 44, key = 5, embedding = uu\3, data = five2\n"
                    "__ydb_parent = 45, key = 6, embedding = vv\3, data = six\n");
                recreate();
            }
        }

        {  // ParentFrom = 39 ParentTo = 41
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    39, 41, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level,
                        "__ydb_parent = 39, __ydb_id = 42, __ydb_centroid = 00\3\n"
                        "__ydb_parent = 39, __ydb_id = 43, __ydb_centroid = 22\3\n"
                        "__ydb_parent = 40, __ydb_id = 44, __ydb_centroid = 11\3\n"
                        "__ydb_parent = 40, __ydb_id = 45, __ydb_centroid = mm\3\n"
                        "__ydb_parent = 41, __ydb_id = 46, __ydb_centroid = uu\3\n"
                        "__ydb_parent = 41, __ydb_id = 47, __ydb_centroid = vv\3\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 42, key = 1, embedding = 00\3, data = one\n"
                    "__ydb_parent = 43, key = 2, embedding = 22\3, data = two\n"
                    "__ydb_parent = 44, key = 1, embedding = \x30\x30\3, data = one\n"
                    "__ydb_parent = 44, key = 2, embedding = \x31\x31\3, data = two\n"
                    "__ydb_parent = 44, key = 3, embedding = \x32\x32\3, data = three\n"
                    "__ydb_parent = 45, key = 4, embedding = \x65\x65\3, data = four\n"
                    "__ydb_parent = 45, key = 5, embedding = \x75\x75\3, data = five\n"
                    "__ydb_parent = 46, key = 5, embedding = uu\3, data = five2\n"
                    "__ydb_parent = 47, key = 6, embedding = vv\3, data = six\n");
                recreate();
            }
        }

        {  // ParentFrom = 30 ParentTo = 50
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    30, 50, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level,
                        "__ydb_parent = 39, __ydb_id = 69, __ydb_centroid = 00\3\n"
                        "__ydb_parent = 39, __ydb_id = 70, __ydb_centroid = 22\3\n"
                        "__ydb_parent = 40, __ydb_id = 71, __ydb_centroid = 11\3\n"
                        "__ydb_parent = 40, __ydb_id = 72, __ydb_centroid = mm\3\n"
                        "__ydb_parent = 41, __ydb_id = 73, __ydb_centroid = uu\3\n"
                        "__ydb_parent = 41, __ydb_id = 74, __ydb_centroid = vv\3\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 69, key = 1, embedding = 00\3, data = one\n"
                    "__ydb_parent = 70, key = 2, embedding = 22\3, data = two\n"
                    "__ydb_parent = 71, key = 1, embedding = \x30\x30\3, data = one\n"
                    "__ydb_parent = 71, key = 2, embedding = \x31\x31\3, data = two\n"
                    "__ydb_parent = 71, key = 3, embedding = \x32\x32\3, data = three\n"
                    "__ydb_parent = 72, key = 4, embedding = \x65\x65\3, data = four\n"
                    "__ydb_parent = 72, key = 5, embedding = \x75\x75\3, data = five\n"
                    "__ydb_parent = 73, key = 5, embedding = uu\3, data = five2\n"
                    "__ydb_parent = 74, key = 6, embedding = vv\3, data = six\n");
                recreate();
            }
        }

        {  // ParentFrom = 30 ParentTo = 31
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    30, 31, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level, "");
                UNIT_ASSERT_VALUES_EQUAL(posting, "");
                recreate();
            }
        }

        {  // ParentFrom = 100 ParentTo = 101
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    100, 101, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows);
                UNIT_ASSERT_VALUES_EQUAL(level, "");
                UNIT_ASSERT_VALUES_EQUAL(posting, "");
                recreate();
            }
        }
    }
}

}
