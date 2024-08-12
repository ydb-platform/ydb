#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace Tests;
using namespace NDataShard::NKqpHelpers;

namespace {
    TString FillTableQuery() {
        TStringBuilder sql;
        sql << "UPSERT INTO `/Root/TestTable` (key, value) VALUES ";
        for (size_t i = 0; i < 1000; ++i) {
            sql << " (" << i << ", " << i << i << "),";
        }
        sql << " (10000, 10000);";
        return sql;
    }
}

Y_UNIT_TEST_SUITE(KqpStreamLookup) {
    Y_UNIT_TEST(ReadTableDuringSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "TestTable", 1);
        auto shards = GetTableShards(server, sender, "/Root/TestTable");

        ExecSQL(server, sender, FillTableQuery());

        bool readReceived = false;
        auto captureEvents = [&](TTestActorRuntimeBase &, TAutoPtr <IEventHandle> &ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                Cerr << "Captured TEvDataShard::TEvRead from " << runtime->FindActorName(ev->Sender) << " to " << runtime->FindActorName(ev->GetRecipientRewrite()) << Endl;
                if (runtime->FindActorName(ev->Sender) == "KQP_STREAM_LOOKUP_ACTOR") {
                    if (!readReceived) {
                        auto senderSplit = runtime->AllocateEdgeActor();
                        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/TestTable", shards[0], 500);
                        Cerr << "--- split started ---" << Endl;
                        WaitTxNotification(server, senderSplit, txId);
                        Cerr << "--- split finished ---" << Endl;
                        shards = GetTableShards(server, sender, "/Root/TestTable");
                        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

                        readReceived = true;
                    }
                }
            }

            return false;
        };

        server->GetRuntime()->SetEventFilter(captureEvents);

        SendSQL(server, sender, R"(
            $keys = SELECT key FROM `/Root/TestTable`;
            SELECT * FROM `/Root/TestTable` WHERE key IN $keys;
        )");

        auto reply = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        auto resp = reply->Get()->Record.GetRef().GetResponse();
        UNIT_ASSERT_VALUES_EQUAL(resp.YdbResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resp.GetYdbResults(0).rows_size(), 1000);
    }

    Y_UNIT_TEST(ReadTableWithIndexDuringSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "TestTable",
            TShardedTableOptions()
                .Indexes({
                    TShardedTableOptions::TIndex{
                        "by_value",
                        {"value"},
                        {},
                        NKikimrSchemeOp::EIndexTypeGlobal
                    }
                })
            );

        auto shards = GetTableShards(server, sender, "/Root/TestTable");

        ExecSQL(server, sender, FillTableQuery());

        bool readReceived = false;
        auto captureEvents = [&](TTestActorRuntimeBase &, TAutoPtr <IEventHandle> &ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                Cerr << "Captured TEvDataShard::TEvRead from " << runtime->FindActorName(ev->Sender) << " to " << runtime->FindActorName(ev->GetRecipientRewrite()) << Endl;
                if (runtime->FindActorName(ev->Sender) == "KQP_STREAM_LOOKUP_ACTOR") {
                    if (!readReceived) {
                        auto senderSplit = runtime->AllocateEdgeActor();
                        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/TestTable", shards[0], 500);
                        Cerr << "--- split started ---" << Endl;
                        WaitTxNotification(server, senderSplit, txId);
                        Cerr << "--- split finished ---" << Endl;
                        shards = GetTableShards(server, sender, "/Root/TestTable");
                        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

                        readReceived = true;
                    }
                }
            }

            return false;
        };

        server->GetRuntime()->SetEventFilter(captureEvents);

        SendSQL(server, sender, R"(
            SELECT * FROM `/Root/TestTable` VIEW by_value WHERE value = 500500;
        )");

        auto reply = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        auto resp = reply->Get()->Record.GetRef().GetResponse();
        UNIT_ASSERT_VALUES_EQUAL(resp.YdbResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resp.GetYdbResults(0).rows_size(), 1);
    }

} // Y_UNIT_TEST_SUITE(KqpStreamLookup)
} // namespace NKikimr
