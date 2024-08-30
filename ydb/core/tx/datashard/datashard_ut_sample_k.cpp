#include "defs.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/protos/index_builder.pb.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

template <>
inline void Out<NKikimrIndexBuilder::EBuildStatus>(IOutputStream& o, NKikimrIndexBuilder::EBuildStatus status) {
    o << NKikimrIndexBuilder::EBuildStatus_Name(status);
}

namespace NKikimr {

static ui64 sId = 1;

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE (TTxDataShardSampleKScan) {
    static void DoSampleKBad(Tests::TServer::TPtr server, TActorId sender,
                             const TString& tableFrom, const TRowVersion& snapshot, std::unique_ptr<TEvDataShard::TEvSampleKRequest>& ev) {
        auto id = sId++;
        auto& runtime = *server->GetRuntime();
        auto datashards = GetTableShards(server, sender, tableFrom);
        TTableId tableId = ResolveTableId(server, sender, tableFrom);

        TStringBuilder data;
        TString err;
        UNIT_ASSERT(datashards.size() == 1);

        for (auto tid : datashards) {
            auto& rec = ev->Record;
            rec.SetId(1);

            rec.SetSeqNoGeneration(id);
            rec.SetSeqNoRound(1);

            if (!rec.HasTabletId()) {
                rec.SetTabletId(tid);
            }

            if (!rec.HasPathId()) {
                PathIdFromPathId(tableId.PathId, rec.MutablePathId());
            }

            if (rec.ColumnsSize() == 0) {
                rec.AddColumns("value");
                rec.AddColumns("key");
            } else {
                rec.ClearColumns();
            }

            rec.SetSnapshotTxId(snapshot.TxId);
            rec.SetSnapshotStep(snapshot.Step);

            rec.SetSeed(1337);
            if (!rec.HasK()) {
                rec.SetK(1);
            }

            runtime.SendToPipe(tid, sender, ev.release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvSampleKResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
        }
    }

    static TString DoSampleK(Tests::TServer::TPtr server, TActorId sender,
                             const TString& tableFrom, const TRowVersion& snapshot, ui64 seed, ui64 k) {
        auto id = sId++;
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
                PathIdFromPathId(tableId.PathId, rec.MutablePathId());

                rec.AddColumns("value");
                rec.AddColumns("key");

                rec.SetSnapshotTxId(snapshot.TxId);
                rec.SetSnapshotStep(snapshot.Step);

                rec.SetSeed(seed);
                rec.SetK(k);
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
                data.Out << "value = ";
                UNIT_ASSERT(cells[0].ToStream<i32>(data.Out, err));
                data.Out << ", key = ";
                UNIT_ASSERT(cells[1].ToStream<i32>(data.Out, err));
                data.Out << "\n";
            }
            auto& probabilities = reply->Record.GetProbabilities();
            UNIT_ASSERT(rows.size() == probabilities.size());
            UNIT_ASSERT(std::is_sorted(probabilities.begin(), probabilities.end()));
        }
        return data;
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

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        // Upsert some initial values
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);");

        auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-1"});

        ui64 seed, k;
        TString data;

        seed = 0;
        {
            k = 1;
            data = DoSampleK(server, sender, "/Root/table-1", snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = 30, key = 3\n");

            k = 3;
            data = DoSampleK(server, sender, "/Root/table-1", snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = 30, key = 3\n"
                                     "value = 20, key = 2\n"
                                     "value = 50, key = 5\n");

            k = 9;
            data = DoSampleK(server, sender, "/Root/table-1", snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = 30, key = 3\n"
                                     "value = 20, key = 2\n"
                                     "value = 50, key = 5\n"
                                     "value = 40, key = 4\n"
                                     "value = 10, key = 1\n");
        }
        seed = 111;
        {
            k = 1;
            data = DoSampleK(server, sender, "/Root/table-1", snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = 10, key = 1\n");

            k = 3;
            data = DoSampleK(server, sender, "/Root/table-1", snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = 10, key = 1\n"
                                     "value = 20, key = 2\n"
                                     "value = 30, key = 3\n");

            k = 9;
            data = DoSampleK(server, sender, "/Root/table-1", snapshot, seed, k);
            UNIT_ASSERT_VALUES_EQUAL(data,
                                     "value = 10, key = 1\n"
                                     "value = 20, key = 2\n"
                                     "value = 30, key = 3\n"
                                     "value = 50, key = 5\n"
                                     "value = 40, key = 4\n");
        }
    }

    Y_UNIT_TEST (ScanBadParameters) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root");

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-1"});

        {
            auto ev = std::make_unique<TEvDataShard::TEvSampleKRequest>();
            auto& rec = ev->Record;

            rec.SetK(0);
            DoSampleKBad(server, sender, "/Root/table-1", snapshot, ev);
        }
        {
            auto ev = std::make_unique<TEvDataShard::TEvSampleKRequest>();
            auto& rec = ev->Record;

            rec.AddColumns();
            DoSampleKBad(server, sender, "/Root/table-1", snapshot, ev);
        }
        {
            auto ev = std::make_unique<TEvDataShard::TEvSampleKRequest>();
            auto& rec = ev->Record;

            rec.SetTabletId(0);
            DoSampleKBad(server, sender, "/Root/table-1", snapshot, ev);
        }
        {
            auto ev = std::make_unique<TEvDataShard::TEvSampleKRequest>();
            auto& rec = ev->Record;

            PathIdFromPathId({0, 0}, rec.MutablePathId());
            DoSampleKBad(server, sender, "/Root/table-1", snapshot, ev);
        }
        {
            auto ev = std::make_unique<TEvDataShard::TEvSampleKRequest>();

            auto snapshotCopy = snapshot;
            snapshotCopy.Step++;
            DoSampleKBad(server, sender, "/Root/table-1", snapshotCopy, ev);
        }
        {
            auto ev = std::make_unique<TEvDataShard::TEvSampleKRequest>();

            auto snapshotCopy = snapshot;
            snapshotCopy.TxId++;
            DoSampleKBad(server, sender, "/Root/table-1", snapshotCopy, ev);
        }
    }
}

} // namespace NKikimr
