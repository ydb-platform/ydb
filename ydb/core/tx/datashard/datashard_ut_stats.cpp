#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardStats) {

    NKikimrTxDataShard::TEvPeriodicTableStats WaitTableStats(TTestActorRuntime& runtime, size_t minPartCount = 0) {
        NKikimrTxDataShard::TEvPeriodicTableStats stats;
        bool captured = false;

        auto observerFunc = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvPeriodicTableStats::EventType: {
                    stats = ev->Get<TEvDataShard::TEvPeriodicTableStats>()->Record;
                    if (stats.GetTableStats().GetPartCount() >= minPartCount) {
                        captured = true;
                    }
                    break;
                }
                default: {
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(observerFunc);

        for (int i = 0; i < 5 && !captured; ++i) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return captured;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(5));
        }

        runtime.SetObserverFunc(prevObserverFunc);
        UNIT_ASSERT(captured);

        return stats;
    }

    NKikimrTxDataShard::TEvCompactTableResult CompactTable(TTestActorRuntime& runtime, ui64 tabletId, const TPathId& pathId) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvDataShard::TEvCompactTable>(pathId.OwnerId, pathId.LocalPathId);
        runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

        auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
        return ev->Get()->Record;
    }

    Y_UNIT_TEST(SmallStatsNotLostOnCompaction) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3)");

        TPathId pathId;
        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetDataSize(), 0u);
            pathId = TPathId(stats.GetTableOwnerId(), stats.GetTableLocalId());
        }

        CompactTable(runtime, shards.at(0), pathId);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, /* minPartCount */ 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetDataSize(), 0u);
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardStats)

} // namespace NKikimr
