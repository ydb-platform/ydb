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
static const TString kIndexTable = "/Root/table-index";
static const TString kDictTable = "/Root/table-dict";

Y_UNIT_TEST_SUITE(TTxDataShardBuildFulltextDictScan) {

    ui64 FillRequest(Tests::TServer::TPtr server, TActorId sender,
        NKikimrTxDataShard::TEvBuildFulltextDictRequest& request)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);

        auto datashards = GetTableShards(server, sender, kIndexTable);
        TTableId tableId = ResolveTableId(server, sender, kIndexTable);

        UNIT_ASSERT(datashards.size() == 1);

        request.SetId(1);
        request.SetSeqNoGeneration(id);
        request.SetSeqNoRound(1);

        request.SetTabletId(datashards[0]);
        tableId.PathId.ToProto(request.MutablePathId());

        FulltextIndexSettings settings;
        auto column = settings.add_columns();
        column->set_column("text");
        column->mutable_analyzers()->set_tokenizer(FulltextIndexSettings::WHITESPACE);
        *request.MutableSettings() = settings;

        request.SetDatabaseName(kDatabaseName);
        request.SetOutputName(kDictTable);

        return datashards[0];
    }

    void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvBuildFulltextDictRequest&)> setupRequest,
        const TString& expectedError, bool expectedErrorSubstring = false, NKikimrIndexBuilder::EBuildStatus expectedStatus = NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST)
    {
        auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextDictRequest>();

        auto tabletId = FillRequest(server, sender, ev->Record);

        auto snapshot = CreateVolatileSnapshot(server, {kIndexTable});
        ev->Record.SetSnapshotTxId(snapshot.TxId);
        ev->Record.SetSnapshotStep(snapshot.Step);

        setupRequest(ev->Record);

        NKikimr::DoBadRequest<TEvDataShard::TEvBuildFulltextDictResponse>(server, sender, std::move(ev), tabletId, expectedError, expectedErrorSubstring, expectedStatus);
    }

    TEvDataShard::TEvBuildFulltextDictResponse::TPtr DoBuild(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvBuildFulltextDictRequest&)> setupRequest) {
        auto ev1 = std::make_unique<TEvDataShard::TEvBuildFulltextDictRequest>();
        auto tabletId = FillRequest(server, sender, ev1->Record);
        setupRequest(ev1->Record);

        auto ev2 = std::make_unique<TEvDataShard::TEvBuildFulltextDictRequest>();
        ev2->Record.CopyFrom(ev1->Record);

        auto& runtime = *server->GetRuntime();
        runtime.SendToPipe(tabletId, sender, ev1.release(), 0, GetPipeConfigWithRetries());
        runtime.SendToPipe(tabletId, sender, ev2.release(), 0, GetPipeConfigWithRetries());

        auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextDictResponse>(sender);

        UNIT_ASSERT_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE, reply->Get()->Record.ShortDebugString());

        return reply;
    }

    void CreateIndexTable(Tests::TServer::TPtr server, TActorId sender) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        options.AllowSystemColumnNames(true);
        options.Columns({
            {TokenColumn, "String", true, true},
            {"key", "Uint32", true, true},
            {FreqColumn, TokenCountTypeName, false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-index", options);
    }

    void FillIndexTable(Tests::TServer::TPtr server, TActorId sender) {
        ExecSQL(server, sender, Sprintf(R"(
            UPSERT INTO `/Root/table-index` (%s, key, %s) VALUES
                ("and", 2, 1),
                ("apple", 1, 1),
                ("apple", 2, 2),
                ("apple", 3, 1),
                ("blue", 2, 1),
                ("car", 4, 1),
                ("green", 1, 1),
                ("red", 2, 3),
                ("red", 4, 1),
                ("yellow", 3, 1)
        )", TokenColumn, FreqColumn));
    }

    void CreateDictTable(Tests::TServer::TPtr server, TActorId sender) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        options.AllowSystemColumnNames(true);
        options.Columns({
            {TokenColumn, "String", true, true},
            {FreqColumn, DocCountTypeName, false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-dict", options);
    }

    void Setup(Tests::TServer::TPtr server, TActorId sender) {
        server->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateIndexTable(server, sender);
        FillIndexTable(server, sender);
        CreateDictTable(server, sender);
    }

    Y_UNIT_TEST(BadRequest) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        Setup(server, sender);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kIndexTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.SetSnapshotStep(request.GetSnapshotStep() + 1);
        }, "Error: Unknown snapshot", true);
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
        }, "Error: Unknown snapshot", true);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.clear_settings();
        }, "{ <main>: Error: Missing fulltext index settings }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.MutableSettings()->clear_columns();
        }, "{ <main>: Error: columns should be set }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.MutableSettings()->mutable_columns()->at(0).mutable_analyzers()->clear_tokenizer();
        }, "{ <main>: Error: tokenizer should be set }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.ClearOutputName();
        }, "{ <main>: Error: Empty output table name }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextDictRequest& request) {
            request.clear_settings();
            request.ClearOutputName();
        }, "[ { <main>: Error: Empty output table name } { <main>: Error: Missing fulltext index settings } ]");
    }

    Y_UNIT_TEST_QUAD(Build, SkipFirst, SkipLast) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();

        Setup(server, sender);

        auto reply = DoBuild(server, sender, [](auto& request){
            request.SetSkipFirstToken(SkipFirst);
            request.SetSkipLastToken(SkipLast);
        });
        auto& record = reply->Get()->Record;

        TString expected = R"(__ydb_token = apple, __ydb_freq = 3
__ydb_token = blue, __ydb_freq = 1
__ydb_token = car, __ydb_freq = 1
__ydb_token = green, __ydb_freq = 1
__ydb_token = red, __ydb_freq = 2
)";

        if (SkipFirst) {
            UNIT_ASSERT_EQUAL(record.GetFirstToken(), "and");
            UNIT_ASSERT_EQUAL(record.GetFirstTokenRows(), 1);
        } else {
            expected = "__ydb_token = and, __ydb_freq = 1\n" + expected;
        }

        if (SkipLast) {
            UNIT_ASSERT_EQUAL(record.GetLastToken(), "yellow");
            UNIT_ASSERT_EQUAL(record.GetLastTokenRows(), 1);
        } else {
            expected += "__ydb_token = yellow, __ydb_freq = 1\n";
        }

        auto index = ReadShardedTable(server, kDictTable);
        Cerr << "Index:" << Endl;
        Cerr << index << Endl;

        UNIT_ASSERT_VALUES_EQUAL(index, expected);
    }
}

}
