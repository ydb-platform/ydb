#pragma once

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/executor_thread.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/printf.h>

namespace NActors {

// Periodically collects stats from executor threads and exposes them as mon counters
class TStatsCollectingActor : public TActorBootstrapped<TStatsCollectingActor> {
private:
    struct THistogramCounters {
        void Init(NMonitoring::TDynamicCounters* group, const TString& baseName, const TString& unit, ui64 maxVal) {
            for (size_t i = 0; (1ull<<i) <= maxVal; ++i) {
                TString bucketName = ToString(1ull<<i) + " " + unit;
                Buckets.push_back(group->GetSubgroup("sensor", baseName)->GetNamedCounter("range", bucketName, true));
            }
            Buckets.push_back(group->GetSubgroup("sensor", baseName)->GetNamedCounter("range", "INF", true));
        }

        void Set(const TLogHistogram& data) {
            ui32 i = 0;
            for (;i < Y_ARRAY_SIZE(data.Buckets) && i < Buckets.size()-1; ++i)
                *Buckets[i] = data.Buckets[i];
            ui64 last = 0;
            for (;i < Y_ARRAY_SIZE(data.Buckets); ++i)
                last += data.Buckets[i];
            *Buckets.back() = last;
        }

        void Set(const TLogHistogram& data, double factor) {
            ui32 i = 0;
            for (;i < Y_ARRAY_SIZE(data.Buckets) && i < Buckets.size()-1; ++i)
                *Buckets[i] = data.Buckets[i]*factor;
            ui64 last = 0;
            for (;i < Y_ARRAY_SIZE(data.Buckets); ++i)
                last += data.Buckets[i];
            *Buckets.back() = last*factor;
        }

    private:
        TVector<NMonitoring::TDynamicCounters::TCounterPtr> Buckets;
    };

    struct TActivityStats {
        void Init(NMonitoring::TDynamicCounterPtr group) {
            Group = group;

            ElapsedMicrosecByActivityBuckets.resize(GetActivityTypeCount());
            ReceivedEventsByActivityBuckets.resize(GetActivityTypeCount());
            ActorsAliveByActivityBuckets.resize(GetActivityTypeCount());
            ScheduledEventsByActivityBuckets.resize(GetActivityTypeCount());
            StuckActorsByActivityBuckets.resize(GetActivityTypeCount());
        }

        void Set(const TExecutorThreadStats& stats) {
            for (ui32 i : xrange(stats.MaxActivityType())) {
                Y_VERIFY(i < GetActivityTypeCount());
                ui64 ticks = stats.ElapsedTicksByActivity[i];
                ui64 events = stats.ReceivedEventsByActivity[i];
                ui64 actors = stats.ActorsAliveByActivity[i];
                ui64 scheduled = stats.ScheduledEventsByActivity[i];
                ui64 stuck = stats.StuckActorsByActivity[i];

                if (!ActorsAliveByActivityBuckets[i]) {
                    if (ticks || events || actors || scheduled) {
                        InitCountersForActivity(i);
                    } else {
                        continue;
                    }
                }

                *ElapsedMicrosecByActivityBuckets[i] = ::NHPTimer::GetSeconds(ticks)*1000000;
                *ReceivedEventsByActivityBuckets[i] = events;
                *ActorsAliveByActivityBuckets[i] = actors;
                *ScheduledEventsByActivityBuckets[i] = scheduled;
                *StuckActorsByActivityBuckets[i] = stuck;
            }
        }

    private:
        void InitCountersForActivity(ui32 activityType) {
            Y_VERIFY(activityType < GetActivityTypeCount());

            auto bucketName = TString(GetActivityTypeName(activityType));

            ElapsedMicrosecByActivityBuckets[activityType] =
                Group->GetSubgroup("sensor", "ElapsedMicrosecByActivity")->GetNamedCounter("activity", bucketName, true);
            ReceivedEventsByActivityBuckets[activityType] =
                Group->GetSubgroup("sensor", "ReceivedEventsByActivity")->GetNamedCounter("activity", bucketName, true);
            ActorsAliveByActivityBuckets[activityType] =
                Group->GetSubgroup("sensor", "ActorsAliveByActivity")->GetNamedCounter("activity", bucketName, false);
            ScheduledEventsByActivityBuckets[activityType] =
                Group->GetSubgroup("sensor", "ScheduledEventsByActivity")->GetNamedCounter("activity", bucketName, true);
            StuckActorsByActivityBuckets[activityType] =
                Group->GetSubgroup("sensor", "StuckActorsByActivity")->GetNamedCounter("activity", bucketName, false);
        }

    private:
        NMonitoring::TDynamicCounterPtr Group;

        TVector<NMonitoring::TDynamicCounters::TCounterPtr> ElapsedMicrosecByActivityBuckets;
        TVector<NMonitoring::TDynamicCounters::TCounterPtr> ReceivedEventsByActivityBuckets;
        TVector<NMonitoring::TDynamicCounters::TCounterPtr> ActorsAliveByActivityBuckets;
        TVector<NMonitoring::TDynamicCounters::TCounterPtr> ScheduledEventsByActivityBuckets;
        TVector<NMonitoring::TDynamicCounters::TCounterPtr> StuckActorsByActivityBuckets;
    };

    struct TExecutorPoolCounters {
        TIntrusivePtr<NMonitoring::TDynamicCounters> PoolGroup;

        NMonitoring::TDynamicCounters::TCounterPtr SentEvents;
        NMonitoring::TDynamicCounters::TCounterPtr ReceivedEvents;
        NMonitoring::TDynamicCounters::TCounterPtr PreemptedEvents;
        NMonitoring::TDynamicCounters::TCounterPtr NonDeliveredEvents;
        NMonitoring::TDynamicCounters::TCounterPtr DestroyedActors;
        NMonitoring::TDynamicCounters::TCounterPtr EmptyMailboxActivation;
        NMonitoring::TDynamicCounters::TCounterPtr CpuMicrosec;
        NMonitoring::TDynamicCounters::TCounterPtr ElapsedMicrosec;
        NMonitoring::TDynamicCounters::TCounterPtr ParkedMicrosec;
        NMonitoring::TDynamicCounters::TCounterPtr ActorRegistrations;
        NMonitoring::TDynamicCounters::TCounterPtr ActorsAlive;
        NMonitoring::TDynamicCounters::TCounterPtr AllocatedMailboxes;
        NMonitoring::TDynamicCounters::TCounterPtr MailboxPushedOutBySoftPreemption;
        NMonitoring::TDynamicCounters::TCounterPtr MailboxPushedOutByTime;
        NMonitoring::TDynamicCounters::TCounterPtr MailboxPushedOutByEventCount;
        NMonitoring::TDynamicCounters::TCounterPtr WrongWakenedThreadCount;
        NMonitoring::TDynamicCounters::TCounterPtr CurrentThreadCount;
        NMonitoring::TDynamicCounters::TCounterPtr PotentialMaxThreadCount;
        NMonitoring::TDynamicCounters::TCounterPtr DefaultThreadCount;
        NMonitoring::TDynamicCounters::TCounterPtr MaxThreadCount;
        NMonitoring::TDynamicCounters::TCounterPtr IsNeedy;
        NMonitoring::TDynamicCounters::TCounterPtr IsStarved;
        NMonitoring::TDynamicCounters::TCounterPtr IsHoggish;
        NMonitoring::TDynamicCounters::TCounterPtr IncreasingThreadsByNeedyState;
        NMonitoring::TDynamicCounters::TCounterPtr DecreasingThreadsByStarvedState;
        NMonitoring::TDynamicCounters::TCounterPtr DecreasingThreadsByHoggishState;
        NMonitoring::TDynamicCounters::TCounterPtr NotEnoughCpuExecutions;
        NMonitoring::TDynamicCounters::TCounterPtr MaxConsumedCpu;
        NMonitoring::TDynamicCounters::TCounterPtr MinConsumedCpu;
        NMonitoring::TDynamicCounters::TCounterPtr MaxBookedCpu;
        NMonitoring::TDynamicCounters::TCounterPtr MinBookedCpu;


        THistogramCounters LegacyActivationTimeHistogram;
        NMonitoring::THistogramPtr ActivationTimeHistogram;
        THistogramCounters LegacyEventDeliveryTimeHistogram;
        NMonitoring::THistogramPtr EventDeliveryTimeHistogram;
        THistogramCounters LegacyEventProcessingCountHistogram;
        NMonitoring::THistogramPtr EventProcessingCountHistogram;
        THistogramCounters LegacyEventProcessingTimeHistogram;
        NMonitoring::THistogramPtr EventProcessingTimeHistogram;

        TActivityStats ActivityStats;
        NMonitoring::TDynamicCounters::TCounterPtr MaxUtilizationTime;

        double Usage = 0;
        double LastElapsedSeconds = 0;
        THPTimer UsageTimer;
        TString Name;
        ui32 Threads;

        void Init(NMonitoring::TDynamicCounters* group, const TString& poolName, ui32 threads) {
            LastElapsedSeconds = 0;
            Usage = 0;
            UsageTimer.Reset();
            Name = poolName;
            Threads = threads;

            PoolGroup = group->GetSubgroup("execpool", poolName);

            SentEvents          = PoolGroup->GetCounter("SentEvents", true);
            ReceivedEvents      = PoolGroup->GetCounter("ReceivedEvents", true);
            PreemptedEvents     = PoolGroup->GetCounter("PreemptedEvents", true);
            NonDeliveredEvents  = PoolGroup->GetCounter("NonDeliveredEvents", true);
            DestroyedActors     = PoolGroup->GetCounter("DestroyedActors", true);
            CpuMicrosec         = PoolGroup->GetCounter("CpuMicrosec", true);
            ElapsedMicrosec     = PoolGroup->GetCounter("ElapsedMicrosec", true);
            ParkedMicrosec      = PoolGroup->GetCounter("ParkedMicrosec", true);
            EmptyMailboxActivation = PoolGroup->GetCounter("EmptyMailboxActivation", true);
            ActorRegistrations  = PoolGroup->GetCounter("ActorRegistrations", true);
            ActorsAlive         = PoolGroup->GetCounter("ActorsAlive", false);
            AllocatedMailboxes  = PoolGroup->GetCounter("AllocatedMailboxes", false);
            MailboxPushedOutBySoftPreemption = PoolGroup->GetCounter("MailboxPushedOutBySoftPreemption", true);
            MailboxPushedOutByTime = PoolGroup->GetCounter("MailboxPushedOutByTime", true);
            MailboxPushedOutByEventCount = PoolGroup->GetCounter("MailboxPushedOutByEventCount", true);
            WrongWakenedThreadCount = PoolGroup->GetCounter("WrongWakenedThreadCount", true);
            CurrentThreadCount = PoolGroup->GetCounter("CurrentThreadCount", false);
            PotentialMaxThreadCount = PoolGroup->GetCounter("PotentialMaxThreadCount", false);
            DefaultThreadCount = PoolGroup->GetCounter("DefaultThreadCount", false);
            MaxThreadCount = PoolGroup->GetCounter("MaxThreadCount", false);
            IsNeedy = PoolGroup->GetCounter("IsNeedy", false);
            IsStarved = PoolGroup->GetCounter("IsStarved", false);
            IsHoggish = PoolGroup->GetCounter("IsHoggish", false);
            IncreasingThreadsByNeedyState = PoolGroup->GetCounter("IncreasingThreadsByNeedyState", true);
            DecreasingThreadsByStarvedState = PoolGroup->GetCounter("DecreasingThreadsByStarvedState", true);
            DecreasingThreadsByHoggishState = PoolGroup->GetCounter("DecreasingThreadsByHoggishState", true);
            NotEnoughCpuExecutions = PoolGroup->GetCounter("NotEnoughCpuExecutions", true);
            MaxConsumedCpu = PoolGroup->GetCounter("MaxConsumedCpuByPool", false);
            MinConsumedCpu = PoolGroup->GetCounter("MinConsumedCpuByPool", false);
            MaxBookedCpu = PoolGroup->GetCounter("MaxBookedCpuByPool", false);
            MinBookedCpu = PoolGroup->GetCounter("MinBookedCpuByPool", false);

            LegacyActivationTimeHistogram.Init(PoolGroup.Get(), "ActivationTime", "usec", 5*1000*1000);
            ActivationTimeHistogram = PoolGroup->GetHistogram(
                "ActivationTimeUs", NMonitoring::ExponentialHistogram(24, 2, 1));
            LegacyEventDeliveryTimeHistogram.Init(PoolGroup.Get(), "EventDeliveryTime", "usec", 5*1000*1000);
            EventDeliveryTimeHistogram = PoolGroup->GetHistogram(
                "EventDeliveryTimeUs", NMonitoring::ExponentialHistogram(24, 2, 1));
            LegacyEventProcessingCountHistogram.Init(PoolGroup.Get(), "EventProcessingCount", "usec", 5*1000*1000);
            EventProcessingCountHistogram = PoolGroup->GetHistogram(
                "EventProcessingCountUs", NMonitoring::ExponentialHistogram(24, 2, 1));
            LegacyEventProcessingTimeHistogram.Init(PoolGroup.Get(), "EventProcessingTime", "usec", 5*1000*1000);
            EventProcessingTimeHistogram = PoolGroup->GetHistogram(
                "EventProcessingTimeUs", NMonitoring::ExponentialHistogram(24, 2, 1));

            ActivityStats.Init(PoolGroup.Get());

            MaxUtilizationTime = PoolGroup->GetCounter("MaxUtilizationTime", true);
        }

        void Set(const TExecutorPoolStats& poolStats, const TExecutorThreadStats& stats, ui32 numThreads) {
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
            *SentEvents         = stats.SentEvents;
            *ReceivedEvents     = stats.ReceivedEvents;
            *PreemptedEvents     = stats.PreemptedEvents;
            *NonDeliveredEvents = stats.NonDeliveredEvents;
            *DestroyedActors    = stats.PoolDestroyedActors;
            *EmptyMailboxActivation = stats.EmptyMailboxActivation;
            *CpuMicrosec        = stats.CpuUs;
            *ElapsedMicrosec    = ::NHPTimer::GetSeconds(stats.ElapsedTicks)*1000000;
            *ParkedMicrosec     = ::NHPTimer::GetSeconds(stats.ParkedTicks)*1000000;
            *ActorRegistrations = stats.PoolActorRegistrations;
            *ActorsAlive        = stats.PoolActorRegistrations - stats.PoolDestroyedActors;
            *AllocatedMailboxes = stats.PoolAllocatedMailboxes;
            *MailboxPushedOutBySoftPreemption = stats.MailboxPushedOutBySoftPreemption;
            *MailboxPushedOutByTime = stats.MailboxPushedOutByTime;
            *MailboxPushedOutByEventCount = stats.MailboxPushedOutByEventCount;
            *WrongWakenedThreadCount = poolStats.WrongWakenedThreadCount;
            *CurrentThreadCount = poolStats.CurrentThreadCount;
            *PotentialMaxThreadCount = poolStats.PotentialMaxThreadCount;
            *DefaultThreadCount = poolStats.DefaultThreadCount;
            *MaxThreadCount = poolStats.MaxThreadCount;
            *IsNeedy = poolStats.IsNeedy;
            *IsStarved = poolStats.IsStarved;
            *IsHoggish = poolStats.IsHoggish;
            *IncreasingThreadsByNeedyState = poolStats.IncreasingThreadsByNeedyState;
            *DecreasingThreadsByStarvedState = poolStats.DecreasingThreadsByStarvedState;
            *DecreasingThreadsByHoggishState = poolStats.DecreasingThreadsByHoggishState;
            *NotEnoughCpuExecutions = stats.NotEnoughCpuExecutions;

            LegacyActivationTimeHistogram.Set(stats.ActivationTimeHistogram);
            ActivationTimeHistogram->Reset();
            ActivationTimeHistogram->Collect(stats.ActivationTimeHistogram);

            LegacyEventDeliveryTimeHistogram.Set(stats.EventDeliveryTimeHistogram);
            EventDeliveryTimeHistogram->Reset();
            EventDeliveryTimeHistogram->Collect(stats.EventDeliveryTimeHistogram);

            LegacyEventProcessingCountHistogram.Set(stats.EventProcessingCountHistogram);
            EventProcessingCountHistogram->Reset();
            EventProcessingCountHistogram->Collect(stats.EventProcessingCountHistogram);

            double toMicrosec = 1000000 / NHPTimer::GetClockRate();
            LegacyEventProcessingTimeHistogram.Set(stats.EventProcessingTimeHistogram, toMicrosec);
            EventProcessingTimeHistogram->Reset();
            for (ui32 i = 0; i < stats.EventProcessingTimeHistogram.Count(); ++i) {
                EventProcessingTimeHistogram->Collect(
                    stats.EventProcessingTimeHistogram.UpperBound(i),
                    stats.EventProcessingTimeHistogram.Value(i) * toMicrosec);
            }

            ActivityStats.Set(stats);

            *MaxUtilizationTime = poolStats.MaxUtilizationTime;

            double seconds = UsageTimer.PassedReset();

            // TODO[serxa]: It doesn't account for contention. Use 1 - parkedTicksDelta / seconds / numThreads KIKIMR-11916
            const double elapsed = NHPTimer::GetSeconds(stats.ElapsedTicks);
            const double currentUsage = numThreads > 0 ? ((elapsed - LastElapsedSeconds) / seconds / numThreads) : 0;
            LastElapsedSeconds = elapsed;

            // update usage factor according to smoothness
            const double smoothness = 0.5;
            Usage = currentUsage * smoothness + Usage * (1.0 - smoothness);
#else
            Y_UNUSED(poolStats);
            Y_UNUSED(stats);
            Y_UNUSED(numThreads);
#endif
        }
    };

    struct TActorSystemCounters {
        TIntrusivePtr<NMonitoring::TDynamicCounters> Group;

        NMonitoring::TDynamicCounters::TCounterPtr MaxConsumedCpu;
        NMonitoring::TDynamicCounters::TCounterPtr MinConsumedCpu;
        NMonitoring::TDynamicCounters::TCounterPtr MaxBookedCpu;
        NMonitoring::TDynamicCounters::TCounterPtr MinBookedCpu;

        void Init(NMonitoring::TDynamicCounters* group) {
            Group = group;

            MaxConsumedCpu = Group->GetCounter("MaxConsumedCpu", false);
            MinConsumedCpu = Group->GetCounter("MinConsumedCpu", false);
            MaxBookedCpu = Group->GetCounter("MaxBookedCpu", false);
            MinBookedCpu = Group->GetCounter("MinBookedCpu", false);
        }

        void Set(const THarmonizerStats& harmonizerStats) {
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
            *MaxConsumedCpu = harmonizerStats.MaxConsumedCpu;
            *MinConsumedCpu = harmonizerStats.MinConsumedCpu;
            *MaxBookedCpu = harmonizerStats.MaxBookedCpu;
            *MinBookedCpu = harmonizerStats.MinBookedCpu;
#else
            Y_UNUSED(poolStats);
            Y_UNUSED(stats);
            Y_UNUSED(numThreads);
#endif
        }

    };

public:
    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::ACTORLIB_STATS;
    }

    TStatsCollectingActor(
            ui32 intervalSec,
            const TActorSystemSetup& setup,
            NMonitoring::TDynamicCounterPtr counters)
        : IntervalSec(intervalSec)
        , Counters(counters)
    {
        PoolCounters.resize(setup.GetExecutorsCount());
        for (size_t poolId = 0; poolId < PoolCounters.size(); ++poolId) {
            PoolCounters[poolId].Init(Counters.Get(), setup.GetPoolName(poolId), setup.GetThreads(poolId));
        }
        ActorSystemCounters.Init(Counters.Get());
    }

    void Bootstrap(const TActorContext& ctx) {
        ctx.Schedule(TDuration::Seconds(IntervalSec), new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, Wakeup);
        }
    }

private:
    virtual void OnWakeup(const TActorContext &ctx) {
        Y_UNUSED(ctx);
    }

    void Wakeup(const TActorContext &ctx) {
        for (size_t poolId = 0; poolId < PoolCounters.size(); ++poolId) {
            TVector<TExecutorThreadStats> stats;
            TExecutorPoolStats poolStats;
            ctx.ExecutorThread.ActorSystem->GetPoolStats(poolId, poolStats, stats);
            SetAggregatedCounters(PoolCounters[poolId], poolStats, stats);
        }
        THarmonizerStats harmonizerStats = ctx.ExecutorThread.ActorSystem->GetHarmonizerStats();
        ActorSystemCounters.Set(harmonizerStats);

        OnWakeup(ctx);

        ctx.Schedule(TDuration::Seconds(IntervalSec), new TEvents::TEvWakeup());
    }

    void SetAggregatedCounters(TExecutorPoolCounters& poolCounters, TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& stats) {
        // Sum all per-thread counters into the 0th element
        for (ui32 idx = 1; idx < stats.size(); ++idx) {
            stats[0].Aggregate(stats[idx]);
        }
        if (stats.size()) {
            poolCounters.Set(poolStats, stats[0], stats.size() - 1);
        }
    }

protected:
    const ui32 IntervalSec;
    NMonitoring::TDynamicCounterPtr Counters;

    TVector<TExecutorPoolCounters> PoolCounters;
    TActorSystemCounters ActorSystemCounters;
};

} // NActors
