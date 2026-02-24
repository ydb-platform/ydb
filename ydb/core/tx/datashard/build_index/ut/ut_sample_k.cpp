#include "defs.h"
#include "ut_helpers.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

static std::atomic<ui64> sId = 1;

using Ydb::Table::VectorIndexSettings;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

static const TString kTable = "/Root/table-1";

Y_UNIT_TEST_SUITE (TTxDataShardSampleKScan) {

    static void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
        std::function<void(NKikimrTxDataShard::TEvSampleKRequest&)> setupRequest,
        const TString& expectedError, bool expectedErrorSubstring = false)
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto snapshot = CreateVolatileSnapshot(server, {kTable});
        auto datashards = GetTableShards(server, sender, kTable);
        TTableId tableId = ResolveTableId(server, sender, kTable);

        TStringBuilder data;
        TString err;
        UNIT_ASSERT(datashards.size() == 1);

        auto ev = std::make_unique<TEvDataShard::TEvSampleKRequest>();
        auto& rec = ev->Record;
        rec.SetId(1);

        rec.SetSeqNoGeneration(id);
        rec.SetSeqNoRound(1);

        rec.SetTabletId(datashards[0]);

        tableId.PathId.ToProto(rec.MutablePathId());

        rec.AddColumns("value");
        rec.AddColumns("key");

        rec.SetSnapshotTxId(snapshot.TxId);
        rec.SetSnapshotStep(snapshot.Step);

        rec.SetMaxProbability(std::numeric_limits<uint64_t>::max());
        rec.SetSeed(1337);
        rec.SetK(1);

        setupRequest(rec);

        NKikimr::DoBadRequest<TEvDataShard::TEvSampleKResponse>(server, sender, std::move(ev), datashards[0], expectedError, expectedErrorSubstring);
    }

    static TString DoSampleK(Tests::TServer::TPtr server, TActorId sender, const TString& tableFrom, const TRowVersion& snapshot,
        ui64 seed, ui64 k, std::function<void(NKikimrTxDataShard::TEvSampleKRequest&)> setupRequest = nullptr) {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto& runtime = *server->GetRuntime();
        auto datashards = GetTableShards(server, sender, tableFrom);
        TTableId tableId = ResolveTableId(server, sender, tableFrom);

        TStringBuilder data;
        TString err;

        for (auto tid : datashards) {
            auto ev1 = std::make_unique<TEvDataShard::TEvSampleKRequest>();
            auto ev2 = std::make_unique<TEvDataShard::TEvSampleKRequest>();
            auto fill = [&](auto& ev) {
                auto& rec = ev->Record;
                rec.SetId(1);

                rec.SetSeqNoGeneration(id);
                rec.SetSeqNoRound(1);

                rec.SetTabletId(tid);
                tableId.PathId.ToProto(rec.MutablePathId());

                rec.AddColumns("value");
                rec.AddColumns("key");

                if (snapshot.TxId) {
                    rec.SetSnapshotTxId(snapshot.TxId);
                    rec.SetSnapshotStep(snapshot.Step);
                }

                rec.SetMaxProbability(std::numeric_limits<uint64_t>::max());
                rec.SetSeed(seed);
                rec.SetK(k);

                if (setupRequest) {
                    setupRequest(rec);
                }
            };
            fill(ev1);
            fill(ev2);

            runtime.SendToPipe(tid, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tid, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvSampleKResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE);

            const auto& rows = reply->Record.GetRows();
            UNIT_ASSERT(!rows.empty());
            for (auto& row : rows) {
                TSerializedCellVec vec;
                UNIT_ASSERT(TSerializedCellVec::TryParse(row, vec));
                const auto& cells = vec.GetCells();
                UNIT_ASSERT_EQUAL(cells.size(), 2);
                data.Out << "value = " << cells[0].AsBuf() << ", key = ";
                UNIT_ASSERT(cells[1].ToStream<i32>(data.Out, err));
                data.Out << "\n";
            }
            auto& probabilities = reply->Record.GetProbabilities();
            UNIT_ASSERT(rows.size() == probabilities.size());
            UNIT_ASSERT(std::is_sorted(probabilities.begin(), probabilities.end()));
        }
        return data;
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

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.SetTabletId(0);
        }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kTable)[0] << " }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            TPathId(0, 0).ToProto(request.MutablePathId());
        }, "{ <main>: Error: Unknown table id: 0 }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.SetSnapshotStep(request.GetSnapshotStep() + 1);
        }, "Error: Unknown snapshot", true);
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
        }, "Error: Unknown snapshot", true);

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.SetK(0);
        }, "{ <main>: Error: Should be requested on at least one row }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.SetMaxProbability(0);
        }, "{ <main>: Error: Max probability should be positive }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.ClearColumns();
        }, "{ <main>: Error: Should be requested at least one column }");
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.AddColumns("some");
        }, "{ <main>: Error: Unknown column: some }");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.MutableSettings()->set_vector_type(VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED);
        }, "{ <main>: Error: either distance or similarity should be set }");

        // test multiple issues:
        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.SetK(0);
            request.AddColumns("some");
        }, "[ { <main>: Error: Should be requested on at least one row } { <main>: Error: Unknown column: some } ]");

        DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvSampleKRequest& request) {
            request.AddColumns("some");
        }, "{ <main>: Error: Unknown column: some }");
    }

    Y_UNIT_TEST(RunScan) {
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
        options.AllowSystemColumnNames(true);
        options.Columns({
            {"key", "Uint32", true, true},
            {"value", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-1", options);

        // Upsert some initial values
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value)"
            " VALUES (1, \"a\"), (2, \"b\"), (3, \"c\"), (4, \"d\"), (5, \"e\");");

        auto snapshot = CreateVolatileSnapshot(server, {kTable});

        ui64 seed, k;
        TString data;

        seed = 0;
        {
            k = 1;
            data = DoSampleK(server, sender, kTable, snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = c, key = 3\n");

            k = 3;
            data = DoSampleK(server, sender, kTable, snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = c, key = 3\n"
                                     "value = b, key = 2\n"
                                     "value = e, key = 5\n");

            k = 9;
            data = DoSampleK(server, sender, kTable, snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = c, key = 3\n"
                                     "value = b, key = 2\n"
                                     "value = e, key = 5\n"
                                     "value = d, key = 4\n"
                                     "value = a, key = 1\n");
        }
        snapshot = {};
        seed = 111;
        {
            k = 1;
            data = DoSampleK(server, sender, kTable, snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = a, key = 1\n");

            k = 3;
            data = DoSampleK(server, sender, kTable, snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = a, key = 1\n"
                                     "value = b, key = 2\n"
                                     "value = c, key = 3\n");

            k = 9;
            data = DoSampleK(server, sender, kTable, snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = a, key = 1\n"
                                     "value = b, key = 2\n"
                                     "value = c, key = 3\n"
                                     "value = e, key = 5\n"
                                     "value = d, key = 4\n");
        }
    }

    Y_UNIT_TEST(SkipForeign) {
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
        options.AllowSystemColumnNames(true);
        options.Columns({
            {"key", "Uint32", true, true},
            {"value", "String", false, false},
            {NTableIndex::NKMeans::IsForeignColumn, "Bool", false, true},
        });
        CreateShardedTable(server, sender, "/Root", "table-1", options);

        // Upsert some initial values
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value, __ydb_foreign) VALUES "
            "(1, \"a\", true), (2, \"b\", true), (3, \"c\", true), (4, \"d\", true), (5, \"e\", false);");

        auto snapshot = CreateVolatileSnapshot(server, {kTable});

        ui64 seed = 0, k = 2;
        TString data = DoSampleK(server, sender, kTable, snapshot, seed, k, [&](NKikimrTxDataShard::TEvSampleKRequest& rec) {
            rec.AddColumns(NTableIndex::NKMeans::IsForeignColumn);
        });
        UNIT_ASSERT_VALUES_EQUAL(data, "value = e, key = 5\n");
    }

    Y_UNIT_TEST(ValidateVectors) {
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
        options.AllowSystemColumnNames(true);
        options.Columns({
            {"key", "Uint32", true, true},
            {"value", "String", false, false},
            {NTableIndex::NKMeans::IsForeignColumn, "Bool", false, true},
        });
        CreateShardedTable(server, sender, "/Root", "table-1", options);

        // Upsert some initial values
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value, __ydb_foreign) VALUES "
            "(1, \"ab\", false), (2, \"ab\x02\", false), (3, \"\", false), (4, \"cdef\", false), (5, \"de\x02\", false);");

        auto snapshot = CreateVolatileSnapshot(server, {kTable});

        ui64 seed = 0, k = 2;
        TString data = DoSampleK(server, sender, kTable, snapshot, seed, k, [&](NKikimrTxDataShard::TEvSampleKRequest& rec) {
            rec.AddColumns(NTableIndex::NKMeans::IsForeignColumn);
            VectorIndexSettings settings;
            settings.set_vector_dimension(2);
            settings.set_vector_type(VectorIndexSettings::VECTOR_TYPE_UINT8);
            settings.set_metric(VectorIndexSettings::DISTANCE_COSINE);
            *rec.MutableSettings() = settings;
        });
        UNIT_ASSERT_VALUES_EQUAL(data, "value = de\x02, key = 5\nvalue = ab\x02, key = 2\n");
    }
}

} // namespace NKikimr
