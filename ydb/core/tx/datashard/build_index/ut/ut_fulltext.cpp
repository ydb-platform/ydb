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
static const TString kDatabaseName = "/Root";
static const TString kMainTable = "/Root/table-main";
static const TString kIndexTable = "/Root/table-index";
static const TString kDocsTable = "/Root/table-docs";

Y_UNIT_TEST_SUITE(TTxDataShardBuildFulltextIndexScan) {

    ui64 FillRequest(Tests::TServer::TPtr server, TActorId sender,
        NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request,
        std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);

        auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
        auto datashards = GetTableShards(server, sender, kMainTable);
        TTableId tableId = ResolveTableId(server, sender, kMainTable);

        UNIT_ASSERT(datashards.size() == 1);

        request.SetId(1);
        request.SetSeqNoGeneration(id);
        request.SetSeqNoRound(1);

        request.SetTabletId(datashards[0]);
        tableId.PathId.ToProto(request.MutablePathId());

        request.SetSnapshotTxId(snapshot.TxId);
        request.SetSnapshotStep(snapshot.Step);

        request.SetIndexType(NKikimrTxDataShard::EFulltextIndexType::FulltextPlain);

        FulltextIndexSettings settings;
        auto column = settings.add_columns();
        column->set_column("text");
        column->mutable_analyzers()->set_tokenizer(FulltextIndexSettings::WHITESPACE);
        *request.MutableSettings() = settings;

        request.SetDatabaseName(kDatabaseName);
        request.SetIndexName(kIndexTable);
        request.SetDocsTableName(kDocsTable);

        setupRequest(request);

        return datashards[0];
    }

    void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest,
        const TString& expectedError, bool expectedErrorSubstring = false, NKikimrIndexBuilder::EBuildStatus expectedStatus = NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST)
    {
        auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();

        auto tabletId = FillRequest(server, sender, ev->Record, setupRequest);

        NKikimr::DoBadRequest<TEvDataShard::TEvBuildFulltextIndexResponse>(server, sender, std::move(ev), tabletId, expectedError, expectedErrorSubstring, expectedStatus);
    }

    TEvDataShard::TEvBuildFulltextIndexResponse::TPtr DoBuildRaw(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest) {
        auto ev1 = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
        auto tabletId = FillRequest(server, sender, ev1->Record, setupRequest);

        auto ev2 = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
        ev2->Record.CopyFrom(ev1->Record);

        auto& runtime = *server->GetRuntime();
        runtime.SendToPipe(tabletId, sender, ev1.release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tabletId, sender, ev2.release(), 0, GetPipeConfigWithRetries());

        auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);

        UNIT_ASSERT_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE, reply->Get()->Record.ShortDebugString());

        return reply;
    }

    TString DoBuild(Tests::TServer::TPtr server, TActorId sender, std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest) {
        DoBuildRaw(server, sender, setupRequest);
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
                (2, "red apple and blue apple", "two"),
                (3, "yellow apple", "three"),
                (4, "red car", "four")
        )");
    }

    void CreateIndexTable(Tests::TServer::TPtr server, TActorId sender, bool withRelevance) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        options.AllowSystemColumnNames(true);
        if (withRelevance) {
            options.Columns({
                {TokenColumn, "String", true, true},
                {"key", "Uint32", true, true},
                {FreqColumn, TokenCountTypeName, false, true},
            });
        } else {
            options.Columns({
                {TokenColumn, "String", true, true},
                {"key", "Uint32", true, true},
                {"data", "String", false, false},
            });
        }
        CreateShardedTable(server, sender, "/Root", "table-index", options);
    }

    void CreateDocsTable(Tests::TServer::TPtr server, TActorId sender) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        options.AllowSystemColumnNames(true);
        options.Columns({
            {"key", "Uint32", true, true},
            {"data", "String", false, false},
            {DocLengthColumn, TokenCountTypeName, false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-docs", options);
    }

    void Setup(Tests::TServer::TPtr server, TActorId sender, bool withRelevance = false) {
        server->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateMainTable(server, sender);
        FillMainTable(server, sender);
        CreateIndexTable(server, sender, withRelevance);
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
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.SetSnapshotStep(request.GetSnapshotStep() + 1);
        }, "Error: Unknown snapshot", true);
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.ClearSnapshotStep();
        }, "{ <main>: Error: Missing snapshot }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
        }, "Error: Unknown snapshot", true);
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.ClearSnapshotTxId();
        }, "{ <main>: Error: Missing snapshot }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.clear_settings();
        }, "{ <main>: Error: Missing fulltext index settings }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.MutableSettings()->clear_columns();
        }, "{ <main>: Error: columns should be set }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.MutableSettings()->mutable_columns()->at(0).mutable_analyzers()->clear_tokenizer();
        }, "{ <main>: Error: tokenizer should be set }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.ClearIndexName();
        }, "{ <main>: Error: Empty index table name }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.SetIndexType(NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance);
            request.ClearDocsTableName();
        }, "{ <main>: Error: Empty index documents table name }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.MutableSettings()->mutable_columns()->at(0).set_column("some");
        }, "{ <main>: Error: Unknown key column: some }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.AddDataColumns("some");
        }, "{ <main>: Error: Unknown data column: some }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
            request.ClearIndexName();
            request.AddDataColumns("some");
        }, "[ { <main>: Error: Empty index table name } { <main>: Error: Unknown data column: some } ]");
    }

    Y_UNIT_TEST(Build) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        Setup(server, sender);

        auto result = DoBuild(server, sender, [](auto&){});

        UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = and, key = 2, data = (empty maybe)
__ydb_token = apple, key = 1, data = (empty maybe)
__ydb_token = apple, key = 2, data = (empty maybe)
__ydb_token = apple, key = 3, data = (empty maybe)
__ydb_token = blue, key = 2, data = (empty maybe)
__ydb_token = car, key = 4, data = (empty maybe)
__ydb_token = green, key = 1, data = (empty maybe)
__ydb_token = red, key = 2, data = (empty maybe)
__ydb_token = red, key = 4, data = (empty maybe)
__ydb_token = yellow, key = 3, data = (empty maybe)
)");
    }

    Y_UNIT_TEST(BuildWithData) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        Setup(server, sender);

        auto result = DoBuild(server, sender, [](auto& request) {
            request.AddDataColumns("data");
        });

        UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = and, key = 2, data = two
__ydb_token = apple, key = 1, data = one
__ydb_token = apple, key = 2, data = two
__ydb_token = apple, key = 3, data = three
__ydb_token = blue, key = 2, data = two
__ydb_token = car, key = 4, data = four
__ydb_token = green, key = 1, data = one
__ydb_token = red, key = 2, data = two
__ydb_token = red, key = 4, data = four
__ydb_token = yellow, key = 3, data = three
)");
    }

    Y_UNIT_TEST(BuildWithTextData) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        InitRoot(server, sender);

        CreateMainTable(server, sender);
        FillMainTable(server, sender);

        { // CreateIndexTable with text column
            TShardedTableOptions options;
            options.EnableOutOfOrder(true);
            options.Shards(1);
            options.AllowSystemColumnNames(true);
            options.Columns({
                {TokenColumn, "String", true, true},
                {"key", "Uint32", true, true},
                {"text", "String", false, false},
                {"data", "String", false, false},
            });
            CreateShardedTable(server, sender, "/Root", "table-index", options);
        }

        auto result = DoBuild(server, sender, [](auto& request) {
            request.AddDataColumns("text");
            request.AddDataColumns("data");
        });

        UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = and, key = 2, text = red apple and blue apple, data = two
__ydb_token = apple, key = 1, text = green apple, data = one
__ydb_token = apple, key = 2, text = red apple and blue apple, data = two
__ydb_token = apple, key = 3, text = yellow apple, data = three
__ydb_token = blue, key = 2, text = red apple and blue apple, data = two
__ydb_token = car, key = 4, text = red car, data = four
__ydb_token = green, key = 1, text = green apple, data = one
__ydb_token = red, key = 2, text = red apple and blue apple, data = two
__ydb_token = red, key = 4, text = red car, data = four
__ydb_token = yellow, key = 3, text = yellow apple, data = three
)");
    }

    Y_UNIT_TEST(BuildWithTextFromKey) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        server->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        { // CreateMainTable
            TShardedTableOptions options;
            options.EnableOutOfOrder(true);
            options.Shards(1);
            options.AllowSystemColumnNames(false);
            options.Columns({
                {"key", "Uint32", true, true},
                {"text", "String", true, true},
                {"subkey", "Uint32", true, true},
                {"data", "String", false, false},
            });
            CreateShardedTable(server, sender, "/Root", "table-main", options);
        }
        { // FillMainTable
            ExecSQL(server, sender, R"(
                UPSERT INTO `/Root/table-main` (key, text, subkey, data) VALUES
                    (1, "green apple", 11, "one"),
                    (2, "red apple", 22, "two"),
                    (3, "yellow apple", 33, "three"),
                    (4, "red car", 44, "four")
            )");
        }
        { // CreateIndexTable
            TShardedTableOptions options;
            options.EnableOutOfOrder(true);
            options.Shards(1);
            options.AllowSystemColumnNames(true);
            options.Columns({
                {TokenColumn, "String", true, true},
                {"key", "Uint32", true, true},
                {"text", "String", true, true},
                {"subkey", "Uint32", true, true},
                {"data", "String", false, false},
            });
            CreateShardedTable(server, sender, "/Root", "table-index", options);
        }

        auto result = DoBuild(server, sender, [](auto& request) {
            request.AddDataColumns("data");
        });

        UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = apple, key = 1, text = green apple, subkey = 11, data = one
__ydb_token = apple, key = 2, text = red apple, subkey = 22, data = two
__ydb_token = apple, key = 3, text = yellow apple, subkey = 33, data = three
__ydb_token = car, key = 4, text = red car, subkey = 44, data = four
__ydb_token = green, key = 1, text = green apple, subkey = 11, data = one
__ydb_token = red, key = 2, text = red apple, subkey = 22, data = two
__ydb_token = red, key = 4, text = red car, subkey = 44, data = four
__ydb_token = yellow, key = 3, text = yellow apple, subkey = 33, data = three
)");
    }

    Y_UNIT_TEST(BuildWithRelevance) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        Setup(server, sender, true);
        CreateDocsTable(server, sender);

        auto reply = DoBuildRaw(server, sender, [](auto& request) {
            request.SetIndexType(NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance);
        });
        auto& record = reply->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(record.GetDocCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(record.GetTotalDocLength(), 11);

        auto index = ReadShardedTable(server, kIndexTable);
        Cerr << "Index:" << Endl;
        Cerr << index << Endl;
        auto docs = ReadShardedTable(server, kDocsTable);
        Cerr << "Docs:" << Endl;
        Cerr << docs << Endl;

        UNIT_ASSERT_VALUES_EQUAL(index, R"(__ydb_token = and, key = 2, __ydb_freq = 1
__ydb_token = apple, key = 1, __ydb_freq = 1
__ydb_token = apple, key = 2, __ydb_freq = 2
__ydb_token = apple, key = 3, __ydb_freq = 1
__ydb_token = blue, key = 2, __ydb_freq = 1
__ydb_token = car, key = 4, __ydb_freq = 1
__ydb_token = green, key = 1, __ydb_freq = 1
__ydb_token = red, key = 2, __ydb_freq = 1
__ydb_token = red, key = 4, __ydb_freq = 1
__ydb_token = yellow, key = 3, __ydb_freq = 1
)");
        UNIT_ASSERT_VALUES_EQUAL(docs, R"(key = 1, data = (empty maybe), __ydb_length = 2
key = 2, data = (empty maybe), __ydb_length = 5
key = 3, data = (empty maybe), __ydb_length = 2
key = 4, data = (empty maybe), __ydb_length = 2
)");
    }
}

}
