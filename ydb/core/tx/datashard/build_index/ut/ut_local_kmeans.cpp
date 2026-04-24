#include "ut_helpers.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
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
using namespace NTableIndex::NKMeans;

static std::atomic<ui64> sId = 1;
static const TString kDatabaseName = "/Root";
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

        rec.SetDatabaseName(kDatabaseName);
        rec.SetLevelName(kLevelTable);
        rec.SetOutputName(kPostingTable);

        setupRequest(rec);

        NKikimr::DoBadRequest<TEvDataShard::TEvLocalKMeansResponse>(server, sender, std::move(ev), datashards[0], expectedError, expectedErrorSubstring, expectedStatus);
    }

    static std::tuple<TString, TString> DoLocalKMeans(
        Tests::TServer::TPtr server, TActorId sender, NTableIndex::NKMeans::TClusterId parentFrom, NTableIndex::NKMeans::TClusterId parentTo, ui64 seed, ui64 k,
        NKikimrTxDataShard::EKMeansState upload, VectorIndexSettings::VectorType type,
        VectorIndexSettings::Metric metric, ui32 maxBatchRows = 50000, ui32 overlapClusters = 0, bool expectEmpty = false,
        std::optional<TSerializedTableRange> keyRange = {})
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

                rec.SetDatabaseName(kDatabaseName);

                rec.SetOverlapClusters(overlapClusters);
                rec.SetOverlapRatio(2);
                rec.SetOverlapOutForeign(upload == NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD ||
                    upload == NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD);

                rec.SetLevelName(kLevelTable);
                rec.SetOutputName(kPostingTable);

                rec.MutableScanSettings()->SetMaxBatchRows(maxBatchRows);

                if (keyRange) {
                    keyRange->Serialize(*rec.MutableKeyRange());
                }
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
            UNIT_ASSERT_EQUAL(reply->Record.GetIsEmpty(), expectEmpty);
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
        WaitTxNotification(server, txId);
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
                "(1, \"\x30\x30\2\", \"one\"),"
                "(2, \"\x31\x31\2\", \"two\"),"
                "(3, \"\x32\x32\2\", \"three\"),"
                "(4, \"\x65\x65\2\", \"four\"),"
                "(5, \"\x75\x75\2\", \"five\");");

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
        }, "{ <main>: Error: vector_type should be set }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.MutableSettings()->set_metric(VectorIndexSettings::METRIC_UNSPECIFIED);
        }, "{ <main>: Error: either distance or similarity should be set }");

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
                "(1, \"\x30\x30\2\", \"one\"),"
                "(2, \"\x31\x31\2\", \"two\"),"
                "(3, \"\x32\x32\2\", \"three\"),"
                "(4, \"\x65\x65\2\", \"four\"),"
                "(5, \"\x75\x75\2\", \"five\");");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvLocalKMeansRequest& request) {
            request.SetChild(Max<ui64>() - 100);
        }, "Condition violated: `!HasPostingParentFlag(parent)'", true, NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
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
                "(1, \"\x30\x30\2\", \"one\"),"
                "(2, \"\x31\x31\2\", \"two\"),"
                "(3, \"\x32\x32\2\", \"three\"),"
                "(4, \"\x65\x65\2\", \"four\"),"
                "(5, \"\x75\x75\2\", \"five\");");

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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = mm\2\n"
                                            "__ydb_parent = 0, __ydb_id = 9223372036854775810, __ydb_centroid = 11\2\n");
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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = 11\2\n"
                                            "__ydb_parent = 0, __ydb_id = 9223372036854775810, __ydb_centroid = mm\2\n");
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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = II\2\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
                                              "__ydb_parent = 9223372036854775809, key = 2, data = two\n"
                                              "__ydb_parent = 9223372036854775809, key = 3, data = three\n"
                                              "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
                                              "__ydb_parent = 9223372036854775809, key = 5, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST (MainToPostingWithOverlap) {
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
        ExecSQL(server, sender, MainTableForOverlap);

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreatePostingTable(server, sender, options);
        };
        create();

        ui64 seed = 100;
        ui64 k = 3; // 3 (clusters) > 2 (overlap)
        auto similarity = VectorIndexSettings::DISTANCE_COSINE;
        auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
            VectorIndexSettings::VECTOR_TYPE_UINT8, similarity, 50000, 2);

        UNIT_ASSERT_VALUES_EQUAL(level,
            "__ydb_parent = 0, __ydb_id = 9223372036854775809, __ydb_centroid = \x10\x80\x02\n"
            "__ydb_parent = 0, __ydb_id = 9223372036854775810, __ydb_centroid = \x80\x10\x02\n"
            "__ydb_parent = 0, __ydb_id = 9223372036854775811, __ydb_centroid = \x0E\x0E\x02\n");
        UNIT_ASSERT_VALUES_EQUAL(posting,
            "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
            "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
            "__ydb_parent = 9223372036854775809, key = 5, data = five\n"
            "__ydb_parent = 9223372036854775809, key = 11, data = ffff\n"
            "__ydb_parent = 9223372036854775810, key = 2, data = two\n"
            "__ydb_parent = 9223372036854775810, key = 6, data = aaa\n"
            "__ydb_parent = 9223372036854775810, key = 7, data = bbbb\n"
            "__ydb_parent = 9223372036854775810, key = 10, data = eee\n"
            "__ydb_parent = 9223372036854775811, key = 3, data = three\n"
            "__ydb_parent = 9223372036854775811, key = 8, data = ccccc\n"
            "__ydb_parent = 9223372036854775811, key = 9, data = dddd\n"
            "__ydb_parent = 9223372036854775811, key = 10, data = eee\n"
            "__ydb_parent = 9223372036854775811, key = 11, data = ffff\n"
            "__ydb_parent = 9223372036854775811, key = 12, data = ggggg\n"
            "__ydb_parent = 9223372036854775811, key = 13, data = hhhh\n");
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
                "(1, \"\x30\x30\2\", \"one\"),"
                "(2, \"\x31\x31\2\", \"two\"),"
                "(3, \"\x32\x32\2\", \"three\"),"
                "(4, \"\x65\x65\2\", \"four\"),"
                "(5, \"\x75\x75\2\", \"five\");");

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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 1, __ydb_centroid = mm\2\n"
                                            "__ydb_parent = 0, __ydb_id = 2, __ydb_centroid = 11\2\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 4, embedding = \x65\x65\2, data = four\n"
                                              "__ydb_parent = 1, key = 5, embedding = \x75\x75\2, data = five\n"
                                              "__ydb_parent = 2, key = 1, embedding = \x30\x30\2, data = one\n"
                                              "__ydb_parent = 2, key = 2, embedding = \x31\x31\2, data = two\n"
                                              "__ydb_parent = 2, key = 3, embedding = \x32\x32\2, data = three\n");
            recreate();
        }

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 1, __ydb_centroid = 11\2\n"
                                            "__ydb_parent = 0, __ydb_id = 2, __ydb_centroid = mm\2\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 1, embedding = \x30\x30\2, data = one\n"
                                              "__ydb_parent = 1, key = 2, embedding = \x31\x31\2, data = two\n"
                                              "__ydb_parent = 1, key = 3, embedding = \x32\x32\2, data = three\n"
                                              "__ydb_parent = 2, key = 4, embedding = \x65\x65\2, data = four\n"
                                              "__ydb_parent = 2, key = 5, embedding = \x75\x75\2, data = five\n");
            recreate();
        }
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 0, __ydb_id = 1, __ydb_centroid = II\2\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 1, embedding = \x30\x30\2, data = one\n"
                                              "__ydb_parent = 1, key = 2, embedding = \x31\x31\2, data = two\n"
                                              "__ydb_parent = 1, key = 3, embedding = \x32\x32\2, data = three\n"
                                              "__ydb_parent = 1, key = 4, embedding = \x65\x65\2, data = four\n"
                                              "__ydb_parent = 1, key = 5, embedding = \x75\x75\2, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST (MainToBuildWithOverlap) {
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
        ExecSQL(server, sender, MainTableForOverlap);

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreateBuildTableWithForeignOut(server, sender, options, "table-posting");
        };
        create();

        ui64 seed = 100;
        ui64 k = 3; // 3 (clusters) > 2 (overlap)
        auto similarity = VectorIndexSettings::DISTANCE_COSINE;
        auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
            VectorIndexSettings::VECTOR_TYPE_UINT8, similarity, 50000, 2);

        UNIT_ASSERT_VALUES_EQUAL(level,
            "__ydb_parent = 0, __ydb_id = 1, __ydb_centroid = \x10\x80\x02\n"
            "__ydb_parent = 0, __ydb_id = 2, __ydb_centroid = \x80\x10\x02\n"
            "__ydb_parent = 0, __ydb_id = 3, __ydb_centroid = \x0E\x0E\x02\n");
        UNIT_ASSERT_VALUES_EQUAL(posting,
            "key = 1, __ydb_parent = 1, __ydb_foreign = 0, __ydb_distance = 0, embedding = \x10\x80\x02, data = one\n"
            "key = 2, __ydb_parent = 2, __ydb_foreign = 0, __ydb_distance = 0, embedding = \x80\x10\x02, data = two\n"
            "key = 3, __ydb_parent = 3, __ydb_foreign = 0, __ydb_distance = 0, embedding = \x10\x10\x02, data = three\n"
            "key = 4, __ydb_parent = 1, __ydb_foreign = 0, __ydb_distance = 2.226386727e-05, embedding = \x11\x81\x02, data = four\n"
            "key = 5, __ydb_parent = 1, __ydb_foreign = 0, __ydb_distance = 2.952767713e-05, embedding = \x11\x80\x02, data = five\n"
            "key = 6, __ydb_parent = 2, __ydb_foreign = 0, __ydb_distance = 2.226386727e-05, embedding = \x81\x11\x02, data = aaa\n"
            "key = 7, __ydb_parent = 2, __ydb_foreign = 0, __ydb_distance = 4.552470524e-07, embedding = \x81\x10\x02, data = bbbb\n"
            "key = 8, __ydb_parent = 3, __ydb_foreign = 0, __ydb_distance = 0.0004588208546, embedding = \x11\x10\x02, data = ccccc\n"
            "key = 9, __ydb_parent = 3, __ydb_foreign = 0, __ydb_distance = 0.0004588208546, embedding = \x10\x11\x02, data = dddd\n"
            "key = 10, __ydb_parent = 2, __ydb_foreign = 1, __ydb_distance = 0.06500247368, embedding = \x11\x09\x02, data = eee\n"
            "key = 10, __ydb_parent = 3, __ydb_foreign = 0, __ydb_distance = 0.04422099128, embedding = \x11\x09\x02, data = eee\n"
            "key = 11, __ydb_parent = 1, __ydb_foreign = 1, __ydb_distance = 0.06500247368, embedding = \x09\x11\x02, data = ffff\n"
            "key = 11, __ydb_parent = 3, __ydb_foreign = 0, __ydb_distance = 0.04422099128, embedding = \x09\x11\x02, data = ffff\n"
            "key = 12, __ydb_parent = 3, __ydb_foreign = 0, __ydb_distance = 0, embedding = \x09\x09\x02, data = ggggg\n"
            "key = 13, __ydb_parent = 3, __ydb_foreign = 0, __ydb_distance = 0, embedding = \x11\x11\x02, data = hhhh\n");
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
                "(39, 1, \"\x30\x30\2\", \"one\"),"
                "(40, 1, \"\x30\x30\2\", \"one\"),"
                "(40, 2, \"\x31\x31\2\", \"two\"),"
                "(40, 3, \"\x32\x32\2\", \"three\"),"
                "(40, 4, \"\x65\x65\2\", \"four\"),"
                "(40, 5, \"\x75\x75\2\", \"five\"),"
                "(41, 5, \"\x75\x75\2\", \"five\");");

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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = mm\2\n"
                                            "__ydb_parent = 40, __ydb_id = 9223372036854775850, __ydb_centroid = 11\2\n");
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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = 11\2\n"
                                            "__ydb_parent = 40, __ydb_id = 9223372036854775850, __ydb_centroid = mm\2\n");
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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = II\2\n");
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
                "(39, 1, \"\x30\x30\2\", \"one\"),"
                "(40, 1, \"\x30\x30\2\", \"one\"),"
                "(40, 2, \"\x31\x31\2\", \"two\"),"
                "(40, 3, \"\x32\x32\2\", \"three\"),"
                "(40, 4, \"\x65\x65\2\", \"four\"),"
                "(40, 5, \"\x75\x75\2\", \"five\"),"
                "(41, 5, \"\x75\x75\2\", \"five\");");

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
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = mm\2\n"
                                            "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = 11\2\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 4, embedding = \x65\x65\2, data = four\n"
                                              "__ydb_parent = 41, key = 5, embedding = \x75\x75\2, data = five\n"
                                              "__ydb_parent = 42, key = 1, embedding = \x30\x30\2, data = one\n"
                                              "__ydb_parent = 42, key = 2, embedding = \x31\x31\2, data = two\n"
                                              "__ydb_parent = 42, key = 3, embedding = \x32\x32\2, data = three\n");
            recreate();
        }

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = 11\2\n"
                                            "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = mm\2\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 1, embedding = \x30\x30\2, data = one\n"
                                              "__ydb_parent = 41, key = 2, embedding = \x31\x31\2, data = two\n"
                                              "__ydb_parent = 41, key = 3, embedding = \x32\x32\2, data = three\n"
                                              "__ydb_parent = 42, key = 4, embedding = \x65\x65\2, data = four\n"
                                              "__ydb_parent = 42, key = 5, embedding = \x75\x75\2, data = five\n");
            recreate();
        }
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
                                                  NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                                  VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(level, "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = II\2\n");
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 1, embedding = \x30\x30\2, data = one\n"
                                              "__ydb_parent = 41, key = 2, embedding = \x31\x31\2, data = two\n"
                                              "__ydb_parent = 41, key = 3, embedding = \x32\x32\2, data = three\n"
                                              "__ydb_parent = 41, key = 4, embedding = \x65\x65\2, data = four\n"
                                              "__ydb_parent = 41, key = 5, embedding = \x75\x75\2, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST (BuildToBuildWithOverlap) {
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

        CreateBuildTableWithForeignIn(server, sender, options, "table-main");
        // Upsert some initial values
        ExecSQL(server, sender, BuildTableWithOverlapIn);

        auto create = [&] {
            CreateLevelTable(server, sender, options);
            CreateBuildTableWithForeignOut(server, sender, options, "table-posting");
        };
        create();

        ui64 seed = 100;
        ui64 k = 2; // simpler setup: 2 clusters, 2 overlaps
        auto similarity = VectorIndexSettings::DISTANCE_COSINE;
        auto [level, posting] = DoLocalKMeans(server, sender, 40, 40, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
            VectorIndexSettings::VECTOR_TYPE_UINT8, similarity, 50000, 2);

        UNIT_ASSERT_VALUES_EQUAL(level,
            "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = \x0e\x35\x02\n"
            "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = \x64\x0e\x02\n");
        UNIT_ASSERT_VALUES_EQUAL(posting, BuildToBuildWithOverlapOut);
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
                "(39, 1, \"\x30\x30\2\", \"one\"),"
                "(39, 2, \"\x32\x32\2\", \"two\"),"
                "(40, 1, \"\x30\x30\2\", \"one\"),"
                "(40, 2, \"\x31\x31\2\", \"two\"),"
                "(40, 3, \"\x32\x32\2\", \"three\"),"
                "(40, 4, \"\x65\x65\2\", \"four\"),"
                "(40, 5, \"\x75\x75\2\", \"five\"),"
                "(41, 5, \"\x75\x75\2\", \"five2\"),"
                "(41, 6, \"\x76\x76\2\", \"six\");");

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
                    "__ydb_parent = 39, __ydb_id = 40, __ydb_centroid = 00\2\n"
                    "__ydb_parent = 39, __ydb_id = 41, __ydb_centroid = 22\2\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 40, key = 1, embedding = 00\2, data = one\n"
                    "__ydb_parent = 41, key = 2, embedding = 22\2, data = two\n");
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
                    "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = 11\2\n"
                    "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = mm\2\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 41, key = 1, embedding = \x30\x30\2, data = one\n"
                    "__ydb_parent = 41, key = 2, embedding = \x31\x31\2, data = two\n"
                    "__ydb_parent = 41, key = 3, embedding = \x32\x32\2, data = three\n"
                    "__ydb_parent = 42, key = 4, embedding = \x65\x65\2, data = four\n"
                    "__ydb_parent = 42, key = 5, embedding = \x75\x75\2, data = five\n");
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
                    "__ydb_parent = 41, __ydb_id = 42, __ydb_centroid = uu\2\n"
                    "__ydb_parent = 41, __ydb_id = 43, __ydb_centroid = vv\2\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 42, key = 5, embedding = uu\2, data = five2\n"
                    "__ydb_parent = 43, key = 6, embedding = vv\2, data = six\n");
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
                    "__ydb_parent = 39, __ydb_id = 41, __ydb_centroid = 00\2\n"
                    "__ydb_parent = 39, __ydb_id = 42, __ydb_centroid = 22\2\n"
                    "__ydb_parent = 40, __ydb_id = 43, __ydb_centroid = 11\2\n"
                    "__ydb_parent = 40, __ydb_id = 44, __ydb_centroid = mm\2\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 41, key = 1, embedding = 00\2, data = one\n"
                    "__ydb_parent = 42, key = 2, embedding = 22\2, data = two\n"
                    "__ydb_parent = 43, key = 1, embedding = \x30\x30\2, data = one\n"
                    "__ydb_parent = 43, key = 2, embedding = \x31\x31\2, data = two\n"
                    "__ydb_parent = 43, key = 3, embedding = \x32\x32\2, data = three\n"
                    "__ydb_parent = 44, key = 4, embedding = \x65\x65\2, data = four\n"
                    "__ydb_parent = 44, key = 5, embedding = \x75\x75\2, data = five\n");
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
                    "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = 11\2\n"
                    "__ydb_parent = 40, __ydb_id = 43, __ydb_centroid = mm\2\n"
                    "__ydb_parent = 41, __ydb_id = 44, __ydb_centroid = uu\2\n"
                    "__ydb_parent = 41, __ydb_id = 45, __ydb_centroid = vv\2\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 42, key = 1, embedding = \x30\x30\2, data = one\n"
                    "__ydb_parent = 42, key = 2, embedding = \x31\x31\2, data = two\n"
                    "__ydb_parent = 42, key = 3, embedding = \x32\x32\2, data = three\n"
                    "__ydb_parent = 43, key = 4, embedding = \x65\x65\2, data = four\n"
                    "__ydb_parent = 43, key = 5, embedding = \x75\x75\2, data = five\n"
                    "__ydb_parent = 44, key = 5, embedding = uu\2, data = five2\n"
                    "__ydb_parent = 45, key = 6, embedding = vv\2, data = six\n");
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
                        "__ydb_parent = 39, __ydb_id = 42, __ydb_centroid = 00\2\n"
                        "__ydb_parent = 39, __ydb_id = 43, __ydb_centroid = 22\2\n"
                        "__ydb_parent = 40, __ydb_id = 44, __ydb_centroid = 11\2\n"
                        "__ydb_parent = 40, __ydb_id = 45, __ydb_centroid = mm\2\n"
                        "__ydb_parent = 41, __ydb_id = 46, __ydb_centroid = uu\2\n"
                        "__ydb_parent = 41, __ydb_id = 47, __ydb_centroid = vv\2\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 42, key = 1, embedding = 00\2, data = one\n"
                    "__ydb_parent = 43, key = 2, embedding = 22\2, data = two\n"
                    "__ydb_parent = 44, key = 1, embedding = \x30\x30\2, data = one\n"
                    "__ydb_parent = 44, key = 2, embedding = \x31\x31\2, data = two\n"
                    "__ydb_parent = 44, key = 3, embedding = \x32\x32\2, data = three\n"
                    "__ydb_parent = 45, key = 4, embedding = \x65\x65\2, data = four\n"
                    "__ydb_parent = 45, key = 5, embedding = \x75\x75\2, data = five\n"
                    "__ydb_parent = 46, key = 5, embedding = uu\2, data = five2\n"
                    "__ydb_parent = 47, key = 6, embedding = vv\2, data = six\n");
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
                        "__ydb_parent = 39, __ydb_id = 69, __ydb_centroid = 00\2\n"
                        "__ydb_parent = 39, __ydb_id = 70, __ydb_centroid = 22\2\n"
                        "__ydb_parent = 40, __ydb_id = 71, __ydb_centroid = 11\2\n"
                        "__ydb_parent = 40, __ydb_id = 72, __ydb_centroid = mm\2\n"
                        "__ydb_parent = 41, __ydb_id = 73, __ydb_centroid = uu\2\n"
                        "__ydb_parent = 41, __ydb_id = 74, __ydb_centroid = vv\2\n");
                UNIT_ASSERT_VALUES_EQUAL(posting,
                    "__ydb_parent = 69, key = 1, embedding = 00\2, data = one\n"
                    "__ydb_parent = 70, key = 2, embedding = 22\2, data = two\n"
                    "__ydb_parent = 71, key = 1, embedding = \x30\x30\2, data = one\n"
                    "__ydb_parent = 71, key = 2, embedding = \x31\x31\2, data = two\n"
                    "__ydb_parent = 71, key = 3, embedding = \x32\x32\2, data = three\n"
                    "__ydb_parent = 72, key = 4, embedding = \x65\x65\2, data = four\n"
                    "__ydb_parent = 72, key = 5, embedding = \x75\x75\2, data = five\n"
                    "__ydb_parent = 73, key = 5, embedding = uu\2, data = five2\n"
                    "__ydb_parent = 74, key = 6, embedding = vv\2, data = six\n");
                recreate();
            }
        }

        {  // ParentFrom = 30 ParentTo = 31
            for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
                auto [level, posting] = DoLocalKMeans(server, sender,
                    30, 31, 111, 2,
                    NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
                    maxBatchRows, 0, true);
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
                    maxBatchRows, 0, true);
                UNIT_ASSERT_VALUES_EQUAL(level, "");
                UNIT_ASSERT_VALUES_EQUAL(posting, "");
                recreate();
            }
        }
    }

    Y_UNIT_TEST(MainToPostingWithKeyRange) {
        // Verify that specifying a KeyRange in the request causes only rows
        // in that range to be scanned (resume-from-checkpoint semantics).
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

        ExecSQL(server, sender,
            R"(UPSERT INTO `/Root/table-main` (key, embedding, data) VALUES )"
            "(1, \"mm\2\", \"one\"),"
            "(2, \"mm\2\", \"two\"),"
            "(3, \"mm\2\", \"three\"),"
            "(4, \"11\2\", \"four\"),"
            "(5, \"11\2\", \"five\");");

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

        ui64 seed = 0;
        ui64 k = 2;

        // Full scan (no KeyRange): all 5 rows clustered.
        auto [fullLevel, fullPosting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
            VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN);
        recreate();

        // Resumed scan: start strictly after key=2.
        TCell fromCell = TCell::Make(ui32(2));
        TSerializedTableRange resumeRange{TArrayRef<const TCell>{&fromCell, 1}, false, {}, false};
        auto [resumedLevel, resumedPosting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
            VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
            50000, 0, false, resumeRange);

        // Resumed scan should only contain rows 3, 4, 5.
        UNIT_ASSERT(!resumedPosting.Contains("key = 1,"));
        UNIT_ASSERT(!resumedPosting.Contains("key = 2,"));
        UNIT_ASSERT(resumedPosting.Contains("key = 3,"));
        UNIT_ASSERT(resumedPosting.Contains("key = 4,"));
        UNIT_ASSERT(resumedPosting.Contains("key = 5,"));

        // Full scan covers all rows.
        UNIT_ASSERT(fullPosting.Contains("key = 1,"));
        UNIT_ASSERT(fullPosting.Contains("key = 2,"));
        UNIT_ASSERT(fullPosting.Contains("key = 3,"));
        UNIT_ASSERT(fullPosting.Contains("key = 4,"));
        UNIT_ASSERT(fullPosting.Contains("key = 5,"));
    }

    Y_UNIT_TEST(BuildToBuildWithKeyRange) {
        // Verify that KeyRange restricts scanning for the build-to-build upload mode.
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
        CreateBuildTable(server, sender, options, "table-main");

        // Insert rows under parent cluster 1 (keys 1..5) — used as scan input.
        ExecSQL(server, sender,
            R"(UPSERT INTO `/Root/table-main` (__ydb_parent, key, embedding, data) VALUES )"
            "(1, 1, \"mm\2\", \"one\"),"
            "(1, 2, \"mm\2\", \"two\"),"
            "(1, 3, \"mm\2\", \"three\"),"
            "(1, 4, \"11\2\", \"four\"),"
            "(1, 5, \"11\2\", \"five\");");

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

        ui64 seed = 0;
        ui64 k = 2;

        // Full scan of parent=1.
        auto [fullLevel, fullPosting] = DoLocalKMeans(server, sender, 1, 1, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
            VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN);
        recreate();

        // Resume from (parent=1, key=3) exclusive — should only see keys 4 and 5.
        TCell fromCells[2] = {TCell::Make(ui64(1)), TCell::Make(ui32(3))};
        TSerializedTableRange resumeRange{TArrayRef<const TCell>{fromCells, 2}, false, {}, false};
        auto [resumedLevel, resumedPosting] = DoLocalKMeans(server, sender, 1, 1, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
            VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN,
            50000, 0, false, resumeRange);

        UNIT_ASSERT(!resumedPosting.Contains("key = 1,"));
        UNIT_ASSERT(!resumedPosting.Contains("key = 2,"));
        UNIT_ASSERT(!resumedPosting.Contains("key = 3,"));
        UNIT_ASSERT(resumedPosting.Contains("key = 4,"));
        UNIT_ASSERT(resumedPosting.Contains("key = 5,"));

        UNIT_ASSERT(fullPosting.Contains("key = 1,"));
        UNIT_ASSERT(fullPosting.Contains("key = 4,"));
    }

    // Helper: sends a single LocalKMeans request and checks the reply.
    // Returns the status and parsed issues string.
    static std::pair<NKikimrIndexBuilder::EBuildStatus, TString> DoLocalKMeansCheckIssues(
        Tests::TServer::TPtr server, TActorId sender)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        auto tid = datashards[0];
        auto ev = std::make_unique<TEvDataShard::TEvLocalKMeansRequest>();
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
        settings.set_vector_type(VectorIndexSettings::VECTOR_TYPE_UINT8);
        settings.set_metric(VectorIndexSettings::DISTANCE_COSINE);
        *rec.MutableSettings() = settings;

        rec.SetK(2);
        rec.SetSeed(0);
        rec.SetUpload(NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING);
        rec.SetNeedsRounds(300);
        rec.SetParentFrom(0);
        rec.SetParentTo(0);
        rec.SetChild(1);
        rec.SetEmbeddingColumn("embedding");
        rec.AddDataColumns("data");
        rec.SetDatabaseName(kDatabaseName);
        rec.SetLevelName(kLevelTable);
        rec.SetOutputName(kPostingTable);

        runtime.SendToPipe(tid, sender, ev.release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto* reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvLocalKMeansResponse>(handle);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
        return {reply->Record.GetStatus(), issues.ToOneLineString()};
    }

    Y_UNIT_TEST(InvalidEmbeddingError) {
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

        // 2 valid rows (with \x02 format byte), 1 invalid row with wrong format byte
        ExecSQL(server, sender,
            R"(UPSERT INTO `/Root/table-main` (key, embedding, data) VALUES )"
            "(1, \"\x30\x30\2\", \"one\"),"
            "(2, \"\x31\x31\2\", \"two\"),"
            "(3, \"invalid\", \"three\");");

        CreateLevelTable(server, sender, options);
        CreatePostingTable(server, sender, options);

        auto [status, issuesStr] = DoLocalKMeansCheckIssues(server, sender);
        UNIT_ASSERT_EQUAL(status, NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(issuesStr, "Invalid vector format byte");
    }

    Y_UNIT_TEST(EmptyEmbeddingError) {
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

        // Row with empty embedding
        ExecSQL(server, sender,
            R"(UPSERT INTO `/Root/table-main` (key, embedding, data) VALUES )"
            "(1, \"\", \"one\");");

        CreateLevelTable(server, sender, options);
        CreatePostingTable(server, sender, options);

        auto [status, issuesStr] = DoLocalKMeansCheckIssues(server, sender);
        UNIT_ASSERT_EQUAL(status, NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(issuesStr, "Empty vector data");
    }

    Y_UNIT_TEST(NullEmbedding) {
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

        // 2 valid rows, 1 row with NULL embedding (column omitted)
        ExecSQL(server, sender,
            R"(UPSERT INTO `/Root/table-main` (key, embedding, data) VALUES )"
            "(1, \"\x30\x30\2\", \"one\"),"
            "(2, \"\x31\x31\2\", \"two\");");
        ExecSQL(server, sender,
            R"(UPSERT INTO `/Root/table-main` (key, data) VALUES )"
            "(3, \"null_embed\");");

        CreateLevelTable(server, sender, options);
        CreatePostingTable(server, sender, options);

        ui64 seed = 0, k = 2;
        auto [level, posting] = DoLocalKMeans(server, sender, 0, 0, seed, k,
            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
            VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_COSINE);

        // Valid rows should be in posting, null embedding row should be skipped
        UNIT_ASSERT(posting.Contains("key = 1,"));
        UNIT_ASSERT(posting.Contains("key = 2,"));
        UNIT_ASSERT(!posting.Contains("key = 3,"));
    }
}

}
