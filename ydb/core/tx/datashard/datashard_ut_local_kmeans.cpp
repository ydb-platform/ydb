#include "defs.h"
#include "datashard_ut_common_kqp.h"
#include "upload_stats.h"

#include <thread>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/protos/index_builder.pb.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
using namespace Tests;
using Ydb::Table::VectorIndexSettings;
using namespace NTableIndex::NTableVectorKmeansTreeIndex;

static ui64 sId = 1;
static constexpr const char* kMainTable = "/Root/table-main";
static constexpr const char* kLevelTable = "/Root/table-level";
static constexpr const char* kPostingTable = "/Root/table-posting";

Y_UNIT_TEST_SUITE (TTxDataShardLocalKMeansScan) {
    static std::tuple<TString, TString> DoLocalKMeans(Tests::TServer::TPtr server, TActorId sender, ui64 seed, ui64 k,
                                                      VectorIndexSettings::VectorType type, VectorIndexSettings::Distance distance) {
        auto id = sId++;
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
                PathIdFromPathId(tableId.PathId, rec.MutablePathId());

                rec.SetSnapshotTxId(snapshot.TxId);
                rec.SetSnapshotStep(snapshot.Step);

                VectorIndexSettings settings;
                settings.set_vector_dimension(2);
                settings.set_vector_type(type);
                settings.set_distance(distance);
                *rec.MutableSettings() = settings;

                rec.SetK(k);
                rec.SetSeed(seed);

                rec.SetState(NKikimrTxDataShard::TEvLocalKMeansRequest::SAMPLE);
                rec.SetUpload(NKikimrTxDataShard::TEvLocalKMeansRequest::UPLOAD_MAIN_TO_POSTING);

                rec.SetDoneRounds(0);
                rec.SetNeedsRounds(3);

                rec.SetParent(0);
                rec.SetChild(1);

                rec.SetEmbeddingColumn("embedding");
                rec.AddDataColumns("data");

                rec.SetLevelName(kLevelTable);
                rec.SetPostingName(kPostingTable);
            };
            fill(ev1);
            fill(ev2);

            runtime.SendToPipe(tid, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tid, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvLocalKMeansProgressResponse>(handle);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
            UNIT_ASSERT_EQUAL_C(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE, issues.ToOneLineString());
        }

        auto level = ReadShardedTable(server, kLevelTable);
        auto posting = ReadShardedTable(server, kPostingTable);
        return {std::move(level), std::move(posting)};
    }

    static void DropTable(Tests::TServer::TPtr server, TActorId sender, const char* name) {
        ui64 txId = AsyncDropTable(server, sender, "/Root", name);
        WaitTxNotification(server, sender, txId);
    }

    static void CreateLevelTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options) {
        options.AllowSystemColumnNames(true);
        options.Columns({
            {LevelTable_ParentIdColumn, "Uint32", true, true},
            {LevelTable_IdColumn, "Uint32", true, true},
            {LevelTable_EmbeddingColumn, "String", false, true},
        });
        CreateShardedTable(server, sender, "/Root", "table-level", options);
    }

    static void CreatePostingTable(Tests::TServer::TPtr server, TActorId sender, TShardedTableOptions options) {
        options.AllowSystemColumnNames(true);
        options.Columns({
            {PostingTable_ParentIdColumn, "Uint32", true, true},
            {"key", "Uint32", true, true},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-posting", options);
    }

    Y_UNIT_TEST (RunScan) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TShardedTableOptions options;
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?
        options.Shards(1);              // single shard request

        options.AllowSystemColumnNames(false);
        options.Columns({
            {"key", "Uint32", true, true},
            {"embedding", "String", false, false},
            {"data", "String", false, false},
        });
        options.EnableOutOfOrder(true); // TODO(mbkkt) what is it?

        CreateShardedTable(server, sender, "/Root", "table-main", options);

        // Upsert some initial values
        ExecSQL(server, sender, R"(
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

        seed = 0;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            k = 2;
            auto [level, posting] = DoLocalKMeans(server, sender, seed, k, VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level,
                                     "__ydb_parent = 0, __ydb_id = 1, __ydb_embedding = mm\3\n"
                                     "__ydb_parent = 0, __ydb_id = 2, __ydb_embedding = 11\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting,
                                     "__ydb_parent = 1, key = 4, data = four\n"
                                     "__ydb_parent = 1, key = 5, data = five\n"
                                     "__ydb_parent = 2, key = 1, data = one\n"
                                     "__ydb_parent = 2, key = 2, data = two\n"
                                     "__ydb_parent = 2, key = 3, data = three\n");
            recreate();
        }

        seed = 111;
        for (auto distance : {VectorIndexSettings::DISTANCE_MANHATTAN, VectorIndexSettings::DISTANCE_EUCLIDEAN}) {
            k = 2;
            auto [level, posting] = DoLocalKMeans(server, sender, seed, k, VectorIndexSettings::VECTOR_TYPE_UINT8, distance);
            UNIT_ASSERT_VALUES_EQUAL(level,
                                     "__ydb_parent = 0, __ydb_id = 1, __ydb_embedding = 11\3\n"
                                     "__ydb_parent = 0, __ydb_id = 2, __ydb_embedding = mm\3\n");
            UNIT_ASSERT_VALUES_EQUAL(posting,
                                     "__ydb_parent = 1, key = 1, data = one\n"
                                     "__ydb_parent = 1, key = 2, data = two\n"
                                     "__ydb_parent = 1, key = 3, data = three\n"
                                     "__ydb_parent = 2, key = 4, data = four\n"
                                     "__ydb_parent = 2, key = 5, data = five\n");
            recreate();
        }
    }
}

} // namespace NKikimr
