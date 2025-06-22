#include <ydb/core/base/table_index.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/index_builder.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
using namespace Tests;
using Ydb::Table::VectorIndexSettings;
using namespace NTableIndex::NTableVectorKmeansTreeIndex;

static std::atomic<ui64> sId = 1;
static constexpr const char* kMainTable = "/Root/table-main";

Y_UNIT_TEST_SUITE (TTxDataShardRecomputeKMeansScan) {

    static void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvRecomputeKMeansRequest&)> setupRequest,
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

        auto ev = std::make_unique<TEvDataShard::TEvRecomputeKMeansRequest>();
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

        rec.SetParent(0);
        rec.AddClusters("abc");
        rec.SetEmbeddingColumn("embedding");

        setupRequest(rec);

        runtime.SendToPipe(datashards[0], sender, ev.release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvRecomputeKMeansResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
        if (expectedErrorSubstring) {
            UNIT_ASSERT_STRING_CONTAINS(issues.ToOneLineString(), expectedError);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(issues.ToOneLineString(), expectedError);
        }
    }

    static TString DoRecomputeKMeans(Tests::TServer::TPtr server, TActorId sender, NTableIndex::TClusterId parent,
                                     const std::vector<TString>& level,
                                     VectorIndexSettings::VectorType type, VectorIndexSettings::Metric metric)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        TStringBuilder data;
        TString err;

        for (auto tid : datashards) {
            auto ev1 = std::make_unique<TEvDataShard::TEvRecomputeKMeansRequest>();
            auto ev2 = std::make_unique<TEvDataShard::TEvRecomputeKMeansRequest>();
            auto fill = [&](std::unique_ptr<TEvDataShard::TEvRecomputeKMeansRequest>& ev) {
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

                rec.SetParent(parent);
                *rec.MutableClusters() = {level.begin(), level.end()};
                rec.SetEmbeddingColumn("embedding");
            };
            fill(ev1);
            fill(ev2);

            runtime.SendToPipe(tid, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tid, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvRecomputeKMeansResponse>(handle);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
            UNIT_ASSERT_EQUAL_C(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE,
                                issues.ToOneLineString());

            const auto& rows = reply->Record.GetClusters();
            const auto& sizes = reply->Record.GetClusterSizes();
            UNIT_ASSERT((size_t)rows.size() == level.size());
            UNIT_ASSERT((size_t)sizes.size() == level.size());

            for (int i = 0; i < rows.size(); i++) {
                data.Out << "cluster = " << rows[i] << " size = " << sizes[i] << "\n";
            }
        }

        return data;
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
        options.Shards(1);
        CreateMainTable(server, sender, options);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kMainTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.SetSnapshotStep(request.GetSnapshotStep() + 1);
        }, "Error: Unknown snapshot", true);
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
        }, "Error: Unknown snapshot", true);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED);
        }, "{ <main>: Error: Wrong vector type }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_BIT);
        }, "{ <main>: Error: TODO(mbkkt) bit vector type is not supported }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.MutableSettings()->set_metric(VectorIndexSettings::METRIC_UNSPECIFIED);
        }, "{ <main>: Error: Wrong similarity }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.ClearClusters();
        }, "{ <main>: Error: Should be requested for at least one cluster }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.ClearClusters();
            request.AddClusters("something");
        }, "{ <main>: Error: Clusters have invalid format }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.SetEmbeddingColumn("some");
        }, "{ <main>: Error: Unknown embedding column: some }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvRecomputeKMeansRequest& request) {
            request.ClearClusters();
            request.SetEmbeddingColumn("some");
        }, "[ { <main>: Error: Unknown embedding column: some } { <main>: Error: Should be requested for at least one cluster } ]");
    }

    Y_UNIT_TEST(MainTable) {
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
        options.Shards(1);
        CreateMainTable(server, sender, options);

        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (key, embedding, data)
        VALUES )"
                "(1, \"\x30\x30\3\", \"one\"),"
                "(2, \"\x31\x28\3\", \"two\"),"
                "(3, \"\x29\x31\3\", \"three\"),"
                "(4, \"\x20\x40\3\", \"four\"),"
                "(5, \"\x15\x40\3\", \"five\"),"
                "(6, \"\x10\x40\3\", \"six\");");

        std::vector<TString> level = { "\x30\x30\3", "\x10\x40\3" };

        auto recomputed = DoRecomputeKMeans(server, sender, 0, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x17\x40\3 size = 3\n");

        recomputed = DoRecomputeKMeans(server, sender, 0, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x17\x40\3 size = 3\n");

        recomputed = DoRecomputeKMeans(server, sender, 0, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2A\x32\3 size = 4\ncluster = \x12\x40\3 size = 2\n");

        recomputed = DoRecomputeKMeans(server, sender, 0, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::SIMILARITY_COSINE);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x17\x40\3 size = 3\n");

        recomputed = DoRecomputeKMeans(server, sender, 0, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_COSINE);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x17\x40\3 size = 3\n");

    }

    Y_UNIT_TEST(BuildTable) {
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
        options.Shards(1);

        CreateBuildTable(server, sender, options, "table-main");
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (__ydb_parent, key, embedding, data)
        VALUES )"
                "(10, 1, \"\x30\x30\3\", \"one\"),"
                "(10, 2, \"\x31\x28\3\", \"two\"),"
                "(10, 3, \"\x29\x31\3\", \"three\"),"
                "(10, 4, \"\x20\x40\3\", \"four\"),"
                "(11, 5, \"\x15\x40\3\", \"five\"),"
                "(11, 6, \"\x10\x40\3\", \"six\");");

        std::vector<TString> level = { "\x30\x30\3", "\x20\x40\3" };

        auto recomputed = DoRecomputeKMeans(server, sender, 10, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x20\x40\3 size = 1\n");

        recomputed = DoRecomputeKMeans(server, sender, 10, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_MANHATTAN);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x20\x40\3 size = 1\n");

        recomputed = DoRecomputeKMeans(server, sender, 10, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x30\x2C\3 size = 2\ncluster = \x24\x38\3 size = 2\n");

        recomputed = DoRecomputeKMeans(server, sender, 10, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::SIMILARITY_COSINE);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x20\x40\3 size = 1\n");

        recomputed = DoRecomputeKMeans(server, sender, 10, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::DISTANCE_COSINE);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x2E\x2D\3 size = 3\ncluster = \x20\x40\3 size = 1\n");
    }

    Y_UNIT_TEST(EmptyCluster) {
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
        options.Shards(1);
        CreateMainTable(server, sender, options);

        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (key, embedding, data)
        VALUES )"
                "(1, \"\x30\x30\3\", \"one\"),"
                "(2, \"\x31\x28\3\", \"two\"),"
                "(3, \"\x29\x31\3\", \"three\"),"
                "(4, \"\x20\x40\3\", \"four\"),"
                "(5, \"\x15\x40\3\", \"five\"),"
                "(6, \"\x10\x40\3\", \"six\");");

        std::vector<TString> level = { "\x30\x30\3", "\x10\x10\3" };
        auto recomputed = DoRecomputeKMeans(server, sender, 0, level, VectorIndexSettings::VECTOR_TYPE_UINT8, VectorIndexSettings::SIMILARITY_COSINE);
        UNIT_ASSERT_VALUES_EQUAL(recomputed, "cluster = \x22\x36\3 size = 6\ncluster = \x10\x10\3 size = 0\n");

    }

}

}
