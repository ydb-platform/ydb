#include "pool_stats_collector.h"

#include "counters.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <yql/essentials/minikql/aligned_page_pool.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/helpers/pool_stats_collector.h>
#include <ydb/library/actors/helpers/collector_counters.h>

#include <ydb/core/graph/api/service.h>
#include <ydb/core/graph/api/events.h>

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
        MiniKQLPoolStats.Init(counters.Get());
    }

private:
    class TMiniKQLPoolStats {
    public:
        void Init(::NMonitoring::TDynamicCounters* group) {
            CounterGroup = group->GetSubgroup("subsystem", "mkqlalloc");
            TotalBytes = CounterGroup->GetCounter("GlobalPoolTotalBytes", false);
            TotalMmapped = CounterGroup->GetCounter("TotalMmappedBytes", false);
            TotalFreeList = CounterGroup->GetCounter("TotalFreeListBytes", false);
        }

        void Update() {
            TAlignedPagePool::DoCleanupGlobalFreeList(1 << 30); // keep 1Gb of cached pages

            *TotalBytes = TAlignedPagePool::GetGlobalPagePoolSize();
            *TotalMmapped = ::NKikimr::GetTotalMmapedBytes();
            *TotalFreeList = ::NKikimr::GetTotalFreeListBytes();
        }

    private:
        TIntrusivePtr<::NMonitoring::TDynamicCounters> CounterGroup;
        ::NMonitoring::TDynamicCounters::TCounterPtr TotalBytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr TotalMmapped;
        ::NMonitoring::TDynamicCounters::TCounterPtr TotalFreeList;
    };

    void OnWakeup(const TActorContext &ctx) override {
        MiniKQLPoolStats.Update();

        auto systemUpdate = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate>();
        ui32 coresTotal = 0;
        double coresUsed = 0;
        for (const auto& pool : GetPoolCounters()) {
            auto& pb = *systemUpdate->Record.AddPoolStats();
            pb.SetName(pool.Name);
            pb.SetUsage(pool.Usage);
            pb.SetThreads(static_cast<ui32>(pool.Threads));
            pb.SetLimit(static_cast<ui32>(pool.LimitThreads));
            if (pool.Name != "IO") {
                coresTotal += static_cast<ui32>(pool.DefaultThreads);
            }
            coresUsed += pool.Usage * pool.LimitThreads;
        }
        systemUpdate->Record.SetCoresTotal(coresTotal);
        systemUpdate->Record.SetCoresUsed(coresUsed);

        ctx.Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId()), systemUpdate.release());
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
