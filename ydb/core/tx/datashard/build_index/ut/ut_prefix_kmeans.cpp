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
static const TString kPrefixTable = "/Root/table-prefix";
static const TString kLevelTable = "/Root/table-level";
static const TString kPostingTable = "/Root/table-posting";

Y_UNIT_TEST_SUITE (TTxDataShardPrefixKMeansScan) {

    static void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvPrefixKMeansRequest&)> setupRequest,
        const TString& expectedError, bool expectedErrorSubstring = false)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        TStringBuilder data;
        TString err;
        UNIT_ASSERT(datashards.size() == 1);

        auto ev = std::make_unique<TEvDataShard::TEvPrefixKMeansRequest>();
        auto& rec = ev->Record;
        rec.SetId(1);

        rec.SetSeqNoGeneration(id);
        rec.SetSeqNoRound(1);

        rec.SetTabletId(datashards[0]);
        if (!rec.HasPathId()) {
            tableId.PathId.ToProto(rec.MutablePathId());
        }

        VectorIndexSettings settings;
        settings.set_vector_dimension(2);
        settings.set_vector_type(VectorIndexSettings::VECTOR_TYPE_UINT8);
        settings.set_metric(VectorIndexSettings::DISTANCE_COSINE);
        *rec.MutableSettings() = settings;

        rec.SetK(2);
        rec.SetSeed(1337);

        rec.SetUpload(NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING);

        rec.SetNeedsRounds(3);

        rec.SetChild(1);

        rec.SetEmbeddingColumn("embedding");
        rec.SetPrefixColumns(1);
        rec.AddSourcePrimaryKeyColumns("key");

        rec.SetPrefixName(kPrefixTable);

        rec.SetLevelName(kLevelTable);
        rec.SetOutputName(kPostingTable);
        rec.SetPrefixName(kPrefixTable);

        setupRequest(rec);

        NKikimr::DoBadRequest<TEvDataShard::TEvPrefixKMeansResponse>(server, sender, std::move(ev), datashards[0], expectedError, expectedErrorSubstring);
    }

    static std::tuple<TString, TString, TString> DoPrefixKMeans(
        Tests::TServer::TPtr server, TActorId sender, NTableIndex::TClusterId parent, ui64 seed, ui64 k,
        NKikimrTxDataShard::EKMeansState upload, VectorIndexSettings::VectorType type,
        VectorIndexSettings::Metric metric, ui32 maxBatchRows)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        TString err;

        for (auto tid : datashards) {
            auto ev1 = std::make_unique<TEvDataShard::TEvPrefixKMeansRequest>();
            auto ev2 = std::make_unique<TEvDataShard::TEvPrefixKMeansRequest>();
            auto fill = [&](std::unique_ptr<TEvDataShard::TEvPrefixKMeansRequest>& ev) {
                auto& rec = ev->Record;
                rec.SetId(1);

                rec.SetSeqNoGeneration(id);
                rec.SetSeqNoRound(1);

                rec.SetTabletId(tid);
                tableId.PathId.ToProto(rec.MutablePathId());

                VectorIndexSettings settings;
                settings.set_vector_dimension(2);
                settings.set_vector_type(type);
                settings.set_metric(metric);
                *rec.MutableSettings() = settings;

                rec.SetK(k);
                rec.SetSeed(seed);

                rec.SetUpload(upload);

                rec.SetNeedsRounds(300);

                rec.SetChild(parent);

                rec.SetEmbeddingColumn("embedding");
                rec.AddDataColumns("data");
                rec.SetPrefixColumns(1);
                rec.AddSourcePrimaryKeyColumns("key");

                rec.SetPrefixName(kPrefixTable);
                rec.SetLevelName(kLevelTable);
                rec.SetOutputName(kPostingTable);

                rec.MutableScanSettings()->SetMaxBatchRows(maxBatchRows);
            };
            fill(ev1);
            fill(ev2);

            runtime.SendToPipe(tid, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tid, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvPrefixKMeansResponse>(handle);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
            UNIT_ASSERT_EQUAL_C(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE,
                                issues.ToOneLineString());
        }

        auto prefix = ReadShardedTable(server, kPrefixTable);
        auto level = ReadShardedTable(server, kLevelTable);
        auto posting = ReadShardedTable(server, kPostingTable);
        Cerr << "Prefix:" << Endl;
        Cerr << prefix << Endl;
        Cerr << "Level:" << Endl;
        Cerr << level << Endl;
        Cerr << "Posting:" << Endl;
        Cerr << posting << Endl;
        return {std::move(prefix), std::move(level), std::move(posting)};
    }

    static void DropTable(Tests::TServer::TPtr server, TActorId sender, const TString& name)
    {
        ui64 txId = AsyncDropTable(server, sender, "/Root", name);
        WaitTxNotification(server, sender, txId);
    }

    static void CreatePrefixTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options)
    {
        options.AllowSystemColumnNames(true);
        options.Columns({
            {"user", "String", true, true},
            {IdColumn, NTableIndex::ClusterIdTypeName, true, true},
        });
        CreateShardedTable(server, sender, "/Root", "table-prefix", options);
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

    static void CreateBuildPrefixTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options, const TString& name)
    {
        options.Columns({
            {"user", "String", true, true},
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
        CreateBuildPrefixTable(server, sender, options, "table-main");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kMainTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.ClearSourcePrimaryKeyColumns();
        }, "{ <main>: Error: Request should include source primary key columns }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED);
        }, "{ <main>: Error: Wrong vector type }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_BIT);
        }, "{ <main>: Error: TODO(mbkkt) bit vector type is not supported }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.MutableSettings()->set_metric(VectorIndexSettings::METRIC_UNSPECIFIED);
        }, "{ <main>: Error: Wrong similarity }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::UNSPECIFIED);
        }, "{ <main>: Error: Wrong upload }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::SAMPLE);
        }, "{ <main>: Error: Wrong upload }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD);
        }, "{ <main>: Error: Wrong upload }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetK(0);
        }, "{ <main>: Error: Should be requested partition on at least two rows }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetK(1);
        }, "{ <main>: Error: Should be requested partition on at least two rows }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.ClearLevelName();
        }, "{ <main>: Error: Empty level table name }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.ClearOutputName();
        }, "{ <main>: Error: Empty output table name }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.ClearPrefixName();
        }, "{ <main>: Error: Empty prefix table name }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetEmbeddingColumn("some");
        }, "{ <main>: Error: Unknown embedding column: some }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.AddDataColumns("some");
        }, "{ <main>: Error: Unknown data column: some }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetPrefixColumns(0);
        }, "{ <main>: Error: Should be requested on at least one prefix column }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetPrefixColumns(99);
        }, "{ <main>: Error: Should not be requested on more than 2 prefix columns }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvPrefixKMeansRequest& request) {
            request.SetK(1);
            request.SetEmbeddingColumn("some");
        }, "[ { <main>: Error: Should be requested partition on at least two rows } { <main>: Error: Unknown embedding column: some } ]");
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

        CreateBuildPrefixTable(server, sender, options, "table-main");
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (user, key, embedding, data)
        VALUES )"
                "(\"user-1\", 11, \"\x30\x30\3\", \"1-one\"),"
                "(\"user-1\", 12, \"\x31\x31\3\", \"1-two\"),"
                "(\"user-1\", 13, \"\x32\x32\3\", \"1-three\"),"
                "(\"user-1\", 14, \"\x65\x65\3\", \"1-four\"),"
                "(\"user-1\", 15, \"\x75\x75\3\", \"1-five\"),"

                "(\"user-2\", 21, \"\x30\x30\3\", \"2-one\"),"
                "(\"user-2\", 22, \"\x31\x31\3\", \"2-two\"),"
                "(\"user-2\", 23, \"\x32\x32\3\", \"2-three\"),"
                "(\"user-2\", 24, \"\x65\x65\3\", \"2-four\"),"
                "(\"user-2\", 25, \"\x75\x75\3\", \"2-five\");");

        auto create = [&] {
            CreatePrefixTable(server, sender, options);
            CreateLevelTable(server, sender, options);
            CreatePostingTable(server, sender, options);
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-prefix");
            DropTable(server, sender, "table-level");
            DropTable(server, sender, "table-posting");
            create();
        };

        ui64 seed, k;
        k = 2;

        seed = 0;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto [prefix, level, posting] = DoPrefixKMeans(server, sender, 40, seed, k,
                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                VectorIndexSettings::VECTOR_TYPE_UINT8, distance, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(prefix,
                "user = user-1, __ydb_id = 40\n"

                "user = user-2, __ydb_id = 43\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(level,
                "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = mm\3\n"
                "__ydb_parent = 40, __ydb_id = 9223372036854775850, __ydb_centroid = 11\3\n"

                "__ydb_parent = 43, __ydb_id = 9223372036854775852, __ydb_centroid = 11\3\n"
                "__ydb_parent = 43, __ydb_id = 9223372036854775853, __ydb_centroid = mm\3\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 9223372036854775849, key = 14, data = 1-four\n"
                "__ydb_parent = 9223372036854775849, key = 15, data = 1-five\n"
                "__ydb_parent = 9223372036854775850, key = 11, data = 1-one\n"
                "__ydb_parent = 9223372036854775850, key = 12, data = 1-two\n"
                "__ydb_parent = 9223372036854775850, key = 13, data = 1-three\n"

                "__ydb_parent = 9223372036854775852, key = 21, data = 2-one\n"
                "__ydb_parent = 9223372036854775852, key = 22, data = 2-two\n"
                "__ydb_parent = 9223372036854775852, key = 23, data = 2-three\n"
                "__ydb_parent = 9223372036854775853, key = 24, data = 2-four\n"
                "__ydb_parent = 9223372036854775853, key = 25, data = 2-five\n"
            );
            recreate();
        }}

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto [prefix, level, posting] = DoPrefixKMeans(server, sender, 40, seed, k,
                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                VectorIndexSettings::VECTOR_TYPE_UINT8, distance, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(prefix,
                "user = user-1, __ydb_id = 40\n"

                "user = user-2, __ydb_id = 43\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(level,
                "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = 11\3\n"
                "__ydb_parent = 40, __ydb_id = 9223372036854775850, __ydb_centroid = mm\3\n"

                "__ydb_parent = 43, __ydb_id = 9223372036854775852, __ydb_centroid = 11\3\n"
                "__ydb_parent = 43, __ydb_id = 9223372036854775853, __ydb_centroid = mm\3\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 9223372036854775849, key = 11, data = 1-one\n"
                "__ydb_parent = 9223372036854775849, key = 12, data = 1-two\n"
                "__ydb_parent = 9223372036854775849, key = 13, data = 1-three\n"
                "__ydb_parent = 9223372036854775850, key = 14, data = 1-four\n"
                "__ydb_parent = 9223372036854775850, key = 15, data = 1-five\n"

                "__ydb_parent = 9223372036854775852, key = 21, data = 2-one\n"
                "__ydb_parent = 9223372036854775852, key = 22, data = 2-two\n"
                "__ydb_parent = 9223372036854775852, key = 23, data = 2-three\n"
                "__ydb_parent = 9223372036854775853, key = 24, data = 2-four\n"
                "__ydb_parent = 9223372036854775853, key = 25, data = 2-five\n"
            );
            recreate();
        }}
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE, VectorIndexSettings::DISTANCE_COSINE}) {
        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto [prefix, level, posting] = DoPrefixKMeans(server, sender, 40, seed, k,
                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING,
                VectorIndexSettings::VECTOR_TYPE_UINT8, similarity, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(prefix,
                "user = user-1, __ydb_id = 40\n"

                "user = user-2, __ydb_id = 43\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(level,
                "__ydb_parent = 40, __ydb_id = 9223372036854775849, __ydb_centroid = II\3\n"

                "__ydb_parent = 43, __ydb_id = 9223372036854775852, __ydb_centroid = II\3\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 9223372036854775849, key = 11, data = 1-one\n"
                "__ydb_parent = 9223372036854775849, key = 12, data = 1-two\n"
                "__ydb_parent = 9223372036854775849, key = 13, data = 1-three\n"
                "__ydb_parent = 9223372036854775849, key = 14, data = 1-four\n"
                "__ydb_parent = 9223372036854775849, key = 15, data = 1-five\n"

                "__ydb_parent = 9223372036854775852, key = 21, data = 2-one\n"
                "__ydb_parent = 9223372036854775852, key = 22, data = 2-two\n"
                "__ydb_parent = 9223372036854775852, key = 23, data = 2-three\n"
                "__ydb_parent = 9223372036854775852, key = 24, data = 2-four\n"
                "__ydb_parent = 9223372036854775852, key = 25, data = 2-five\n"
            );
            recreate();
        }}
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

        CreateBuildPrefixTable(server, sender, options, "table-main");
        // Upsert some initial values
        ExecSQL(server, sender,
                R"(
        UPSERT INTO `/Root/table-main`
            (user, key, embedding, data)
        VALUES )"
                "(\"user-1\", 11, \"\x30\x30\3\", \"1-one\"),"
                "(\"user-1\", 12, \"\x31\x31\3\", \"1-two\"),"
                "(\"user-1\", 13, \"\x32\x32\3\", \"1-three\"),"
                "(\"user-1\", 14, \"\x65\x65\3\", \"1-four\"),"
                "(\"user-1\", 15, \"\x75\x75\3\", \"1-five\"),"

                "(\"user-2\", 21, \"\x30\x30\3\", \"2-one\"),"
                "(\"user-2\", 22, \"\x31\x31\3\", \"2-two\"),"
                "(\"user-2\", 23, \"\x32\x32\3\", \"2-three\"),"
                "(\"user-2\", 24, \"\x65\x65\3\", \"2-four\"),"
                "(\"user-2\", 25, \"\x75\x75\3\", \"2-five\");");

        auto create = [&] {
            CreatePrefixTable(server, sender, options);
            CreateLevelTable(server, sender, options);
            CreateBuildTable(server, sender, options, "table-posting");
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-prefix");
            DropTable(server, sender, "table-level");
            DropTable(server, sender, "table-posting");
            create();
        };

        ui64 seed, k;
        k = 2;

        seed = 0;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto [prefix, level, posting] = DoPrefixKMeans(server, sender, 40, seed, k,
                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                VectorIndexSettings::VECTOR_TYPE_UINT8, distance, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(prefix,
                "user = user-1, __ydb_id = 40\n"

                "user = user-2, __ydb_id = 43\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(level,
                "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = mm\3\n"
                "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = 11\3\n"

                "__ydb_parent = 43, __ydb_id = 44, __ydb_centroid = 11\3\n"
                "__ydb_parent = 43, __ydb_id = 45, __ydb_centroid = mm\3\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 14, embedding = \x65\x65\3, data = 1-four\n"
                "__ydb_parent = 41, key = 15, embedding = \x75\x75\3, data = 1-five\n"
                "__ydb_parent = 42, key = 11, embedding = \x30\x30\3, data = 1-one\n"
                "__ydb_parent = 42, key = 12, embedding = \x31\x31\3, data = 1-two\n"
                "__ydb_parent = 42, key = 13, embedding = \x32\x32\3, data = 1-three\n"

                "__ydb_parent = 44, key = 21, embedding = \x30\x30\3, data = 2-one\n"
                "__ydb_parent = 44, key = 22, embedding = \x31\x31\3, data = 2-two\n"
                "__ydb_parent = 44, key = 23, embedding = \x32\x32\3, data = 2-three\n"
                "__ydb_parent = 45, key = 24, embedding = \x65\x65\3, data = 2-four\n"
                "__ydb_parent = 45, key = 25, embedding = \x75\x75\3, data = 2-five\n"
            );
            recreate();
        }}

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto [prefix, level, posting] = DoPrefixKMeans(server, sender, 40, seed, k,
                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                VectorIndexSettings::VECTOR_TYPE_UINT8, distance, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(prefix,
                "user = user-1, __ydb_id = 40\n"

                "user = user-2, __ydb_id = 43\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(level,
                "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = 11\3\n"
                "__ydb_parent = 40, __ydb_id = 42, __ydb_centroid = mm\3\n"

                "__ydb_parent = 43, __ydb_id = 44, __ydb_centroid = 11\3\n"
                "__ydb_parent = 43, __ydb_id = 45, __ydb_centroid = mm\3\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 11, embedding = \x30\x30\3, data = 1-one\n"
                "__ydb_parent = 41, key = 12, embedding = \x31\x31\3, data = 1-two\n"
                "__ydb_parent = 41, key = 13, embedding = \x32\x32\3, data = 1-three\n"
                "__ydb_parent = 42, key = 14, embedding = \x65\x65\3, data = 1-four\n"
                "__ydb_parent = 42, key = 15, embedding = \x75\x75\3, data = 1-five\n"

                "__ydb_parent = 44, key = 21, embedding = \x30\x30\3, data = 2-one\n"
                "__ydb_parent = 44, key = 22, embedding = \x31\x31\3, data = 2-two\n"
                "__ydb_parent = 44, key = 23, embedding = \x32\x32\3, data = 2-three\n"
                "__ydb_parent = 45, key = 24, embedding = \x65\x65\3, data = 2-four\n"
                "__ydb_parent = 45, key = 25, embedding = \x75\x75\3, data = 2-five\n"
            );
            recreate();
        }}
        seed = 32;
        for (auto similarity : {VectorIndexSettings::SIMILARITY_INNER_PRODUCT, VectorIndexSettings::SIMILARITY_COSINE, VectorIndexSettings::DISTANCE_COSINE}) {
        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto [prefix, level, posting] = DoPrefixKMeans(server, sender, 40, seed, k,
                NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD,
                VectorIndexSettings::VECTOR_TYPE_UINT8, similarity, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(prefix,
                "user = user-1, __ydb_id = 40\n"

                "user = user-2, __ydb_id = 43\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(level,
                "__ydb_parent = 40, __ydb_id = 41, __ydb_centroid = II\3\n"

                "__ydb_parent = 43, __ydb_id = 44, __ydb_centroid = II\3\n"
            );
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 11, embedding = \x30\x30\3, data = 1-one\n"
                "__ydb_parent = 41, key = 12, embedding = \x31\x31\3, data = 1-two\n"
                "__ydb_parent = 41, key = 13, embedding = \x32\x32\3, data = 1-three\n"
                "__ydb_parent = 41, key = 14, embedding = \x65\x65\3, data = 1-four\n"
                "__ydb_parent = 41, key = 15, embedding = \x75\x75\3, data = 1-five\n"

                "__ydb_parent = 44, key = 21, embedding = \x30\x30\3, data = 2-one\n"
                "__ydb_parent = 44, key = 22, embedding = \x31\x31\3, data = 2-two\n"
                "__ydb_parent = 44, key = 23, embedding = \x32\x32\3, data = 2-three\n"
                "__ydb_parent = 44, key = 24, embedding = \x65\x65\3, data = 2-four\n"
                "__ydb_parent = 44, key = 25, embedding = \x75\x75\3, data = 2-five\n"
            );
            recreate();
        }}
    }
}

}
