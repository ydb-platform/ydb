#include "pool_stats_collector.h"
#include "collector_counters.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <util/generic/vector.h>

namespace NActors {

class TStatsCollectingActor::TImpl {
private:
    friend class TStatsCollectingActor;

    const ui32 IntervalSec;
    TInstant StartOfCollecting;
    NMonitoring::TDynamicCounterPtr Counters;

    TVector<TExecutorPoolCounters> PoolCounters;
    TActorSystemCounters ActorSystemCounters;

public:
    TImpl(ui32 intervalSec, const TActorSystemSetup& setup, NMonitoring::TDynamicCounterPtr counters)
        : IntervalSec(intervalSec)
        , Counters(counters)
    {
        PoolCounters.resize(setup.GetExecutorsCount());
        for (size_t poolId = 0; poolId < PoolCounters.size(); ++poolId) {
            PoolCounters[poolId].Init(Counters.Get(), setup.GetPoolName(poolId), setup.GetThreads(poolId));
        }
        ActorSystemCounters.Init(Counters.Get());
    }

    void Bootstrap(const TActorContext& ctx, TStatsCollectingActor* actor) {
        ctx.Schedule(TDuration::Seconds(IntervalSec), new TEvents::TEvWakeup());
        actor->Become(&TStatsCollectingActor::StateWork);
    }

    void Wakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx, TStatsCollectingActor* actor) {
        auto *event = ev->Get();
        if (event->Tag == 0) {
            StartOfCollecting = ctx.Now();
        }
        if (event->Tag < PoolCounters.size()) {
            ui16 poolId = event->Tag;
            TVector<TExecutorThreadStats> stats;
            TVector<TExecutorThreadStats> sharedStats;
            TExecutorPoolStats poolStats;
            ctx.ActorSystem()->GetPoolStats(poolId, poolStats, stats, sharedStats);
            SetAggregatedCounters(PoolCounters[poolId], poolStats, stats, sharedStats);
            ctx.Schedule(TDuration::MilliSeconds(1), new TEvents::TEvWakeup(poolId + 1));
            return;
        }
        THarmonizerStats harmonizerStats = ctx.ActorSystem()->GetHarmonizerStats();
        ActorSystemCounters.Set(harmonizerStats);
        actor->OnWakeup(ctx);
        ctx.Schedule(TDuration::Seconds(IntervalSec) - (ctx.Now() - StartOfCollecting), new TEvents::TEvWakeup(0));
    }

    void SetAggregatedCounters(TExecutorPoolCounters& poolCounters, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& stats, TVector<TExecutorThreadStats>& sharedStats) {
        // Sum all per-thread counters into the 0th element
        TExecutorThreadStats aggregated;
        for (ui32 idx = 0; idx < stats.size(); ++idx) {
            aggregated.Aggregate(stats[idx]);
        }
        for (ui32 idx = 0; idx < sharedStats.size(); ++idx) {
            aggregated.Aggregate(sharedStats[idx]);
        }
        if (stats.size()) {
            poolCounters.Set(poolStats, aggregated);
        }
    }
};

// Реализация методов класса TStatsCollectingActor

TStatsCollectingActor::TStatsCollectingActor(
        ui32 intervalSec,
        const TActorSystemSetup& setup,
        NMonitoring::TDynamicCounterPtr counters)
    : Impl(std::make_unique<TImpl>(intervalSec, setup, counters))
{
}

TStatsCollectingActor::~TStatsCollectingActor() = default;

void TStatsCollectingActor::Bootstrap(const TActorContext& ctx) {
    Impl->Bootstrap(ctx, this);
}

void TStatsCollectingActor::OnWakeup(const TActorContext &ctx) {
    Y_UNUSED(ctx);
}

STFUNC(TStatsCollectingActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, Wakeup);
    }
}

void TStatsCollectingActor::Wakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
    Impl->Wakeup(ev, ctx, this);
}

const TVector<TExecutorPoolCounters>& TStatsCollectingActor::GetPoolCounters() const {
    return Impl->PoolCounters;
}

const TActorSystemCounters& TStatsCollectingActor::GetActorSystemCounters() const {
    return Impl->ActorSystemCounters;  
}

} // NActors
