#include "pool_stats_collector.h"

#include "counters.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/yql/minikql/aligned_page_pool.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/helpers/pool_stats_collector.h>

namespace NKikimr {

// Periodically collects stats from executor threads and exposes them as mon counters
class TStatsCollectingActor : public NActors::TStatsCollectingActor {
public:
    TStatsCollectingActor(
        ui32 intervalSec,
        const TActorSystemSetup& setup,
        ::NMonitoring::TDynamicCounterPtr counters)
        : NActors::TStatsCollectingActor(intervalSec, setup, GetServiceCounters(counters, "utils"))
    {
        MiniKQLPoolStats.Init(Counters.Get());
    }

private:
    class TMiniKQLPoolStats {
    public:
        void Init(::NMonitoring::TDynamicCounters* group) {
            CounterGroup = group->GetSubgroup("subsystem", "mkqlalloc");
            TotalBytes = CounterGroup->GetCounter("GlobalPoolTotalBytes", false);
        }

        void Update() {
            *TotalBytes = TAlignedPagePool::GetGlobalPagePoolSize();
        }

    private:
        TIntrusivePtr<::NMonitoring::TDynamicCounters> CounterGroup;
        ::NMonitoring::TDynamicCounters::TCounterPtr TotalBytes;
    };

    void OnWakeup(const TActorContext &ctx) override {
        MiniKQLPoolStats.Update();

        TVector<std::tuple<TString, double, ui32>> pools;
        for (const auto& pool : PoolCounters) {
            pools.emplace_back(pool.Name, pool.Usage, pool.Threads);
        }

        ctx.Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId()), new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate(pools));
    }

private:
    TMiniKQLPoolStats MiniKQLPoolStats;
};


IActor *CreateStatsCollector(ui32 intervalSec,
                             const TActorSystemSetup& setup,
                             ::NMonitoring::TDynamicCounterPtr counters)
{
    return new TStatsCollectingActor(intervalSec, setup, counters);
}

} // namespace NKikimr
