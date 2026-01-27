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
using namespace NTableIndex::NKMeans;

static std::atomic<ui64> sId = 1;
static const char* kDatabaseName = "/Root";
static const char* kBuildTable = "/Root/table-main";
static const char* kPostingTable = "/Root/table-posting";

Y_UNIT_TEST_SUITE (TTxDataShardFilterKMeansScan) {

    static void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvFilterKMeansRequest&)> setupRequest,
        const TString& expectedError, bool expectedErrorSubstring = false)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto datashards = GetTableShards(server, sender, kBuildTable);
        TTableId tableId = ResolveTableId(server, sender, kBuildTable);

        TStringBuilder data;
        TString err;
        UNIT_ASSERT(datashards.size() == 1);

        auto ev = std::make_unique<TEvDataShard::TEvFilterKMeansRequest>();
        auto& rec = ev->Record;
        rec.SetId(1);

        rec.SetSeqNoGeneration(id);
        rec.SetSeqNoRound(1);

        rec.SetTabletId(datashards[0]);
        if (!rec.HasPathId()) {
            tableId.PathId.ToProto(rec.MutablePathId());
        }

        rec.SetUpload(NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING);
        rec.SetOutputName(kPostingTable);
        rec.SetDatabaseName(kDatabaseName);

        rec.SetOverlapClusters(2);
        rec.SetOverlapRatio(1.5);

        setupRequest(rec);

        NKikimr::DoBadRequest<TEvDataShard::TEvFilterKMeansResponse>(server, sender, std::move(ev), datashards[0], expectedError, expectedErrorSubstring);
    }

    static TString DoFilterKMeans(Tests::TServer::TPtr server, TActorId sender, NKikimrTxDataShard::EKMeansState upload, ui32 maxBatchRows,
        const TVector<TString> firstKeyRows = {}, const TVector<TString> lastKeyRows = {})
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto datashards = GetTableShards(server, sender, kBuildTable);
        TTableId tableId = ResolveTableId(server, sender, kBuildTable);

        TString err;

        for (auto tid : datashards) {
            auto ev1 = std::make_unique<TEvDataShard::TEvFilterKMeansRequest>();
            auto ev2 = std::make_unique<TEvDataShard::TEvFilterKMeansRequest>();
            auto fill = [&](std::unique_ptr<TEvDataShard::TEvFilterKMeansRequest>& ev) {
                auto& rec = ev->Record;
                rec.SetId(1);

                rec.SetSeqNoGeneration(id);
                rec.SetSeqNoRound(1);

                rec.SetTabletId(tid);
                tableId.PathId.ToProto(rec.MutablePathId());

                rec.SetUpload(upload);
                rec.SetOutputName(kPostingTable);
                rec.SetDatabaseName(kDatabaseName);

                rec.SetOverlapClusters(2);
                rec.SetOverlapRatio(1.5);

                rec.SetSkipFirstKey(firstKeyRows.size() > 0);
                rec.SetSkipLastKey(lastKeyRows.size() > 0);

                rec.MutableScanSettings()->SetMaxBatchRows(maxBatchRows);
            };
            fill(ev1);
            fill(ev2);

            runtime.SendToPipe(tid, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tid, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvFilterKMeansResponse>(handle);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
            UNIT_ASSERT_EQUAL_C(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE, issues.ToOneLineString());

            UNIT_ASSERT_EQUAL(firstKeyRows, TVector<TString>(reply->Record.GetFirstKeyRows().begin(), reply->Record.GetFirstKeyRows().end()));
            UNIT_ASSERT_EQUAL(lastKeyRows, TVector<TString>(reply->Record.GetLastKeyRows().begin(), reply->Record.GetLastKeyRows().end()));
        }

        return ReadShardedTable(server, kPostingTable);
    }

    static void DropTable(Tests::TServer::TPtr server, TActorId sender, const TString& name) {
        ui64 txId = AsyncDropTable(server, sender, "/Root", name);
        WaitTxNotification(server, sender, txId);
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
        CreateBuildTableWithForeignOut(server, sender, options, "table-main");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kBuildTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::UNSPECIFIED);
        }, "{ <main>: Error: Wrong upload }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::SAMPLE);
        }, "{ <main>: Error: Wrong upload }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD);
        }, "{ <main>: Error: Wrong upload }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING);
        }, "{ <main>: Error: Wrong upload }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.ClearOutputName();
        }, "{ <main>: Error: Empty output table name }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetOverlapClusters(1);
        }, "{ <main>: Error: OverlapClusters should be > 1 }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetOverlapRatio(-0.5);
        }, "{ <main>: Error: OverlapRatio should be >= 0 }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvFilterKMeansRequest& request) {
            request.SetUpload(NKikimrTxDataShard::SAMPLE);
            request.SetOverlapClusters(1);
        }, "[ { <main>: Error: Wrong upload } { <main>: Error: OverlapClusters should be > 1 } ]");
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
        options.AllowSystemColumnNames(true);
        options.Columns({
            {"key", "Uint32", true, true},
            {ParentColumn, NTableIndex::NKMeans::ClusterIdTypeName, true, true},
            {DistanceColumn, "Double", false, true},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-main", options);

        // Upsert some initial values
        ExecSQL(server, sender, R"(UPSERT INTO `/Root/table-main`
            (key, __ydb_parent, __ydb_distance, data) VALUES

            (1, 41, 0.12, "one"),
            (1, 42, 0.10, "one"),

            (2, 41, 0.10, "two"),

            (3, 41, 0.10, "three"),
            (3, 42, 0.12, "three"),

            (4, 41, 0.16, "four"),
            (4, 42, 0.10, "four"),

            (5, 41, 0.31, "five"),
            (5, 42, 0.20, "five"),
            (5, 43, 0.25, "five"),

            (6, 41, 0.12, "aaa"),
            (6, 42, 0.11, "aaa"),
            (6, 43, 0.10, "aaa");
        )");

        TVector<TString> firstKeyRows;
        firstKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)1), TCell::Make((ui64)41), TCell::Make((double)0.12), TCell("one", 3)}));
        firstKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)1), TCell::Make((ui64)42), TCell::Make((double)0.10), TCell("one", 3)}));

        TVector<TString> lastKeyRows;
        lastKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)6), TCell::Make((ui64)41), TCell::Make((double)0.12), TCell("aaa", 3)}));
        lastKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)6), TCell::Make((ui64)42), TCell::Make((double)0.11), TCell("aaa", 3)}));
        lastKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)6), TCell::Make((ui64)43), TCell::Make((double)0.10), TCell("aaa", 3)}));

        auto create = [&] {
            CreatePostingTable(server, sender, options);
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-posting");
            create();
        };

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 1, data = one\n"
                "__ydb_parent = 41, key = 2, data = two\n"
                "__ydb_parent = 41, key = 3, data = three\n"

                "__ydb_parent = 42, key = 1, data = one\n"
                "__ydb_parent = 42, key = 3, data = three\n"
                "__ydb_parent = 42, key = 4, data = four\n"
                "__ydb_parent = 42, key = 5, data = five\n"
                "__ydb_parent = 42, key = 6, data = aaa\n"

                "__ydb_parent = 43, key = 5, data = five\n"
                "__ydb_parent = 43, key = 6, data = aaa\n"
            );
            recreate();
        }

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING, maxBatchRows,
                firstKeyRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 2, data = two\n"
                "__ydb_parent = 41, key = 3, data = three\n"

                "__ydb_parent = 42, key = 3, data = three\n"
                "__ydb_parent = 42, key = 4, data = four\n"
                "__ydb_parent = 42, key = 5, data = five\n"
                "__ydb_parent = 42, key = 6, data = aaa\n"

                "__ydb_parent = 43, key = 5, data = five\n"
                "__ydb_parent = 43, key = 6, data = aaa\n"
            );
            recreate();
        }

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING, maxBatchRows,
                {}, lastKeyRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 1, data = one\n"
                "__ydb_parent = 41, key = 2, data = two\n"
                "__ydb_parent = 41, key = 3, data = three\n"

                "__ydb_parent = 42, key = 1, data = one\n"
                "__ydb_parent = 42, key = 3, data = three\n"
                "__ydb_parent = 42, key = 4, data = four\n"
                "__ydb_parent = 42, key = 5, data = five\n"

                "__ydb_parent = 43, key = 5, data = five\n"
            );
            recreate();
        }

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_POSTING, maxBatchRows,
                firstKeyRows, lastKeyRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 2, data = two\n"
                "__ydb_parent = 41, key = 3, data = three\n"

                "__ydb_parent = 42, key = 3, data = three\n"
                "__ydb_parent = 42, key = 4, data = four\n"
                "__ydb_parent = 42, key = 5, data = five\n"

                "__ydb_parent = 43, key = 5, data = five\n"
            );
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
        CreateBuildTableWithForeignOut(server, sender, options, "table-main");

        // Upsert some initial values
        ExecSQL(server, sender, R"(UPSERT INTO `/Root/table-main`
            (key, __ydb_parent, __ydb_foreign, __ydb_distance, embedding, data) VALUES

            (1, 41, true,  0.12, "\x10\x80\x02", "one"),
            (1, 42, false, 0.10, "\x10\x80\x02", "one"),

            (2, 41, false, 0.10, "\x80\x10\x02", "two"),

            (3, 41, false, 0.10, "\x10\x10\x02", "three"),
            (3, 42, true,  0.12, "\x10\x10\x02", "three"),

            (4, 41, true,  0.16, "\x11\x81\x02", "four"),
            (4, 42, false, 0.10, "\x11\x81\x02", "four"),

            (5, 41, true,  0.31, "\x11\x80\x02", "five"),
            (5, 42, false, 0.20, "\x11\x80\x02", "five"),
            (5, 43, true,  0.25, "\x11\x80\x02", "five"),

            (6, 41, true,  0.12, "\x81\x11\x02", "aaa"),
            (6, 42, true,  0.11, "\x81\x11\x02", "aaa"),
            (6, 43, false, 0.10, "\x81\x11\x02", "aaa");
        )");

        TVector<TString> firstKeyRows;
        firstKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)1), TCell::Make((ui64)41),
            TCell::Make((double)0.12), TCell::Make(true), TCell("\x10\x80\x02", 3), TCell("one", 3)}));
        firstKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)1), TCell::Make((ui64)42),
            TCell::Make((double)0.10), TCell::Make(false), TCell("\x10\x80\x02", 3), TCell("one", 3)}));

        TVector<TString> lastKeyRows;
        lastKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)6), TCell::Make((ui64)41),
            TCell::Make((double)0.12), TCell::Make(true), TCell("\x81\x11\x02", 3), TCell("aaa", 3)}));
        lastKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)6), TCell::Make((ui64)42),
            TCell::Make((double)0.11), TCell::Make(true), TCell("\x81\x11\x02", 3), TCell("aaa", 3)}));
        lastKeyRows.push_back(TSerializedCellVec::Serialize({TCell::Make((ui32)6), TCell::Make((ui64)43),
            TCell::Make((double)0.10), TCell::Make(false), TCell("\x81\x11\x02", 3), TCell("aaa", 3)}));

        auto create = [&] {
            CreateBuildTableWithForeignIn(server, sender, options, "table-posting");
        };
        create();
        auto recreate = [&] {
            DropTable(server, sender, "table-posting");
            create();
        };

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, maxBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 1, __ydb_foreign = 1, embedding = \x10\x80\x02, data = one\n"
                "__ydb_parent = 41, key = 2, __ydb_foreign = 0, embedding = \x80\x10\x02, data = two\n"
                "__ydb_parent = 41, key = 3, __ydb_foreign = 0, embedding = \x10\x10\x02, data = three\n"

                "__ydb_parent = 42, key = 1, __ydb_foreign = 0, embedding = \x10\x80\x02, data = one\n"
                "__ydb_parent = 42, key = 3, __ydb_foreign = 1, embedding = \x10\x10\x02, data = three\n"
                "__ydb_parent = 42, key = 4, __ydb_foreign = 0, embedding = \x11\x81\x02, data = four\n"
                "__ydb_parent = 42, key = 5, __ydb_foreign = 0, embedding = \x11\x80\x02, data = five\n"
                "__ydb_parent = 42, key = 6, __ydb_foreign = 1, embedding = \x81\x11\x02, data = aaa\n"

                "__ydb_parent = 43, key = 5, __ydb_foreign = 1, embedding = \x11\x80\x02, data = five\n"
                "__ydb_parent = 43, key = 6, __ydb_foreign = 0, embedding = \x81\x11\x02, data = aaa\n"
            );
            recreate();
        }

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, maxBatchRows,
                firstKeyRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 2, __ydb_foreign = 0, embedding = \x80\x10\x02, data = two\n"
                "__ydb_parent = 41, key = 3, __ydb_foreign = 0, embedding = \x10\x10\x02, data = three\n"

                "__ydb_parent = 42, key = 3, __ydb_foreign = 1, embedding = \x10\x10\x02, data = three\n"
                "__ydb_parent = 42, key = 4, __ydb_foreign = 0, embedding = \x11\x81\x02, data = four\n"
                "__ydb_parent = 42, key = 5, __ydb_foreign = 0, embedding = \x11\x80\x02, data = five\n"
                "__ydb_parent = 42, key = 6, __ydb_foreign = 1, embedding = \x81\x11\x02, data = aaa\n"

                "__ydb_parent = 43, key = 5, __ydb_foreign = 1, embedding = \x11\x80\x02, data = five\n"
                "__ydb_parent = 43, key = 6, __ydb_foreign = 0, embedding = \x81\x11\x02, data = aaa\n"
            );
            recreate();
        }

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, maxBatchRows,
                {}, lastKeyRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 1, __ydb_foreign = 1, embedding = \x10\x80\x02, data = one\n"
                "__ydb_parent = 41, key = 2, __ydb_foreign = 0, embedding = \x80\x10\x02, data = two\n"
                "__ydb_parent = 41, key = 3, __ydb_foreign = 0, embedding = \x10\x10\x02, data = three\n"

                "__ydb_parent = 42, key = 1, __ydb_foreign = 0, embedding = \x10\x80\x02, data = one\n"
                "__ydb_parent = 42, key = 3, __ydb_foreign = 1, embedding = \x10\x10\x02, data = three\n"
                "__ydb_parent = 42, key = 4, __ydb_foreign = 0, embedding = \x11\x81\x02, data = four\n"
                "__ydb_parent = 42, key = 5, __ydb_foreign = 0, embedding = \x11\x80\x02, data = five\n"

                "__ydb_parent = 43, key = 5, __ydb_foreign = 1, embedding = \x11\x80\x02, data = five\n"
            );
            recreate();
        }

        for (ui32 maxBatchRows : {0, 1, 4, 5, 6, 50000}) {
            auto posting = DoFilterKMeans(server, sender, NKikimrTxDataShard::EKMeansState::UPLOAD_BUILD_TO_BUILD, maxBatchRows,
                firstKeyRows, lastKeyRows);
            UNIT_ASSERT_VALUES_EQUAL(posting,
                "__ydb_parent = 41, key = 2, __ydb_foreign = 0, embedding = \x80\x10\x02, data = two\n"
                "__ydb_parent = 41, key = 3, __ydb_foreign = 0, embedding = \x10\x10\x02, data = three\n"

                "__ydb_parent = 42, key = 3, __ydb_foreign = 1, embedding = \x10\x10\x02, data = three\n"
                "__ydb_parent = 42, key = 4, __ydb_foreign = 0, embedding = \x11\x81\x02, data = four\n"
                "__ydb_parent = 42, key = 5, __ydb_foreign = 0, embedding = \x11\x80\x02, data = five\n"

                "__ydb_parent = 43, key = 5, __ydb_foreign = 1, embedding = \x11\x80\x02, data = five\n"
            );
            recreate();
        }
    }
}

}
