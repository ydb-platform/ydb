#include <ydb/core/base/table_index.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/protos/index_builder.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
using namespace Tests;
using Ydb::Table::VectorIndexSettings;
using namespace NTableIndex::NTableVectorKmeansTreeIndex;

static std::atomic<ui64> sId = 1;
static constexpr const char* kMainTable = "/Root/table-main";
static constexpr const char* kPostingTable = "/Root/table-posting";

Y_UNIT_TEST_SUITE (TTxDataShardReshuffleKMeansScan) {

    static void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvReshuffleKMeansRequest&)> setupRequest,
        TString expectedError, bool expectedErrorSubstring = false)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        TStringBuilder data;
        TString err;
        UNIT_ASSERT(datashards.size() == 1);

        auto ev = std::make_unique<TEvDataShard::TEvReshuffleKMeansRequest>();
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

        rec.SetUpload(NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING);

        rec.SetParent(0);
        rec.SetChild(1);

        rec.AddClusters("abc");

        rec.SetEmbeddingColumn("embedding");

        rec.SetOutputName(kPostingTable);

        setupRequest(rec);

        runtime.SendToPipe(datashards[0], sender, ev.release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReshuffleKMeansResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
        if (expectedErrorSubstring) {
            UNIT_ASSERT_STRING_CONTAINS(issues.ToOneLineString(), expectedError);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(issues.ToOneLineString(), expectedError);
        }
    }

    static TString DoReshuffleKMeans(Tests::TServer::TPtr server, TActorId sender, NTableIndex::TClusterId parent,
                                     const std::vector<TString>& level,
                                     NKikimrTxDataShard::EKMeansState upload,
                                     VectorIndexSettings::VectorType type, VectorIndexSettings::Metric metric)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        TString err;

        for (auto tid : datashards) {
            auto ev1 = std::make_unique<TEvDataShard::TEvReshuffleKMeansRequest>();
            auto ev2 = std::make_unique<TEvDataShard::TEvReshuffleKMeansRequest>();
            auto fill = [&](std::unique_ptr<TEvDataShard::TEvReshuffleKMeansRequest>& ev) {
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

                rec.SetUpload(upload);

                *rec.MutableClusters() = {level.begin(), level.end()};

                rec.SetParent(parent);
                rec.SetChild(parent + 1);

                rec.SetEmbeddingColumn("embedding");
                rec.AddDataColumns("data");

                rec.SetOutputName(kPostingTable);
            };
            fill(ev1);
            fill(ev2);

            runtime.SendToPipe(tid, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tid, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReshuffleKMeansResponse>(handle);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
            UNIT_ASSERT_EQUAL_C(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE,
                                issues.ToOneLineString());
        }

        auto posting = ReadShardedTable(server, kPostingTable);
        Cerr << "Posting:" << Endl;
        Cerr << posting << Endl;
        return std::move(posting);
    }

    static void DropTable(Tests::TServer::TPtr server, TActorId sender, const char* name)
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

    static void CreateBuildTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options,
                                 const char* name)
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

    Y_UNIT_TEST(BadRequest) {
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

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kMainTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.SetSnapshotStep(request.GetSnapshotStep() + 1);
        }, "Error: Unknown snapshot", true);
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
        }, "Error: Unknown snapshot", true);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED);
        }, "{ <main>: Error: Wrong vector type }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_BIT);
        }, "{ <main>: Error: TODO(mbkkt) bit vector type is not supported }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.MutableSettings()->set_metric(VectorIndexSettings::METRIC_UNSPECIFIED);
        }, "{ <main>: Error: Wrong similarity }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::UNSPECIFIED);
        }, "{ <main>: Error: Wrong upload }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::SAMPLE);
        }, "{ <main>: Error: Wrong upload }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.ClearClusters();
        }, "{ <main>: Error: Should be requested for at least one cluster }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.ClearClusters();
            request.AddClusters("something");
        }, "{ <main>: Error: Clusters have invalid format }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.ClearOutputName();
        }, "{ <main>: Error: Empty output table name }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.SetEmbeddingColumn("some");
        }, "{ <main>: Error: Unknown embedding column: some }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.AddDataColumns("some");
        }, "{ <main>: Error: Unknown data column: some }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvReshuffleKMeansRequest& request) {
            request.ClearClusters();
            request.SetEmbeddingColumn("some");
        }, "[ { <main>: Error: Unknown embedding column: some } { <main>: Error: Should be requested for at least one cluster } ]");
    }

    Y_UNIT_TEST(MainToPosting) {
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

        auto create = [&] { CreatePostingTable(server, sender, options); };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-posting");
            create();
        };

        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "mm\3",
                "11\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 0, level,
                                            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
                                            VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775809, key = 5, data = five\n"
                                            "__ydb_parent = 9223372036854775810, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775810, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775810, key = 3, data = three\n");
            recreate();
        }
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "11\3",
                "mm\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 0, level,
                                            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
                                            VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775809, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775809, key = 3, data = three\n"
                                            "__ydb_parent = 9223372036854775810, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775810, key = 5, data = five\n");
            recreate();
        }
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            std::vector<TString> level = {
                "II\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 0, level,
                                            NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_POSTING,
                                            VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775809, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775809, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775809, key = 3, data = three\n"
                                            "__ydb_parent = 9223372036854775809, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775809, key = 5, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST(MainToBuild) {
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

        auto create = [&] { CreateBuildTable(server, sender, options, "table-posting"); };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-posting");
            create();
        };

        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "mm\3",
                "11\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 0, level,
                                             NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                             VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 1, key = 5, embedding = \x75\x75\3, data = five\n"
                                              "__ydb_parent = 2, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 2, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 2, key = 3, embedding = \x32\x32\3, data = three\n");
            recreate();
        }
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "11\3",
                "mm\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 0, level,
                                             NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                             VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 1, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 1, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 2, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 2, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            std::vector<TString> level = {
                "II\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 0, level,
                                             NKikimrTxDataShard::EKMeansState::UPLOAD_MAIN_TO_BUILD,
                                             VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 1, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 1, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 1, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 1, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 1, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST(BuildToPosting) {
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

        auto create = [&] { CreatePostingTable(server, sender, options); };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-posting");
            create();
        };

        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "mm\3",
                "11\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 40, level,
                                            NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                                            VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775849, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775849, key = 5, data = five\n"
                                            "__ydb_parent = 9223372036854775850, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775850, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775850, key = 3, data = three\n");
            recreate();
        }
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "11\3",
                "mm\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 40, level,
                                            NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                                            VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775849, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775849, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775849, key = 3, data = three\n"
                                            "__ydb_parent = 9223372036854775850, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775850, key = 5, data = five\n");
            recreate();
        }
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            std::vector<TString> level = {
                "II\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 40, level,
                                            NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                                            VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 9223372036854775849, key = 1, data = one\n"
                                            "__ydb_parent = 9223372036854775849, key = 2, data = two\n"
                                            "__ydb_parent = 9223372036854775849, key = 3, data = three\n"
                                            "__ydb_parent = 9223372036854775849, key = 4, data = four\n"
                                            "__ydb_parent = 9223372036854775849, key = 5, data = five\n");
            recreate();
        }
    }

    Y_UNIT_TEST(BuildToBuild) {
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

        auto create = [&] { CreateBuildTable(server, sender, options, "table-posting"); };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-posting");
            create();
        };

        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "mm\3",
                "11\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 40, level,
                                             NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                             VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 41, key = 5, embedding = \x75\x75\3, data = five\n"
                                              "__ydb_parent = 42, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 42, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 42, key = 3, embedding = \x32\x32\3, data = three\n");
            recreate();
        }
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            std::vector<TString> level = {
                "11\3",
                "mm\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 40, level,
                                             NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                             VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 41, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 41, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 42, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 42, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE,
                                VectorIndexSettings::DISTANCE_COSINE})
        {
            std::vector<TString> level = {
                "II\3",
            };
            auto posting = DoReshuffleKMeans(server, sender, 40, level,
                                             NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                                             VectorIndexSettings::VECTOR_TYPE_UINT8, similarity);
            UNIT_ASSERT_VALUES_EQUAL(posting, "__ydb_parent = 41, key = 1, embedding = \x30\x30\3, data = one\n"
                                              "__ydb_parent = 41, key = 2, embedding = \x31\x31\3, data = two\n"
                                              "__ydb_parent = 41, key = 3, embedding = \x32\x32\3, data = three\n"
                                              "__ydb_parent = 41, key = 4, embedding = \x65\x65\3, data = four\n"
                                              "__ydb_parent = 41, key = 5, embedding = \x75\x75\3, data = five\n");
            recreate();
        }
    }
}

}
