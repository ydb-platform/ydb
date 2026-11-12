#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/string/printf.h>

namespace NActors {

struct THistogramCounters {
    void Init(NMonitoring::TDynamicCounters* group, const TString& baseName, const TString& unit, ui64 maxVal);
    void Set(const TLogHistogram& data);
    void Set(const TLogHistogram& data, double factor);

private:
    TVector<NMonitoring::TDynamicCounters::TCounterPtr> Buckets;
};

struct TActivityStats {
    void Init(NMonitoring::TDynamicCounterPtr group);
    void Set(const TExecutorThreadStats& stats);

private:
    void InitCountersForActivity(ui32 activityType);

private:
    NMonitoring::TDynamicCounterPtr Group;

    TVector<NMonitoring::TDynamicCounters::TCounterPtr> CurrentActivationTimeByActivity;
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
    NMonitoring::TDynamicCounters::TCounterPtr CurrentThreadCountPercent;
    NMonitoring::TDynamicCounters::TCounterPtr PotentialMaxThreadCountPercent;
    NMonitoring::TDynamicCounters::TCounterPtr PossibleMaxThreadCountPercent;
    NMonitoring::TDynamicCounters::TCounterPtr DefaultThreadCountPercent;
    NMonitoring::TDynamicCounters::TCounterPtr MaxThreadCountPercent;
    NMonitoring::TDynamicCounters::TCounterPtr IsNeedy;
    NMonitoring::TDynamicCounters::TCounterPtr IsStarved;
    NMonitoring::TDynamicCounters::TCounterPtr IsHoggish;
    NMonitoring::TDynamicCounters::TCounterPtr HasFullOwnSharedThread;
    NMonitoring::TDynamicCounters::TCounterPtr HasHalfOfOwnSharedThread;
    NMonitoring::TDynamicCounters::TCounterPtr HasHalfOfOtherSharedThread;
    NMonitoring::TDynamicCounters::TCounterPtr IncreasingThreadsByNeedyState;
    NMonitoring::TDynamicCounters::TCounterPtr IncreasingThreadsByExchange;
    NMonitoring::TDynamicCounters::TCounterPtr DecreasingThreadsByStarvedState;
    NMonitoring::TDynamicCounters::TCounterPtr DecreasingThreadsByHoggishState;
    NMonitoring::TDynamicCounters::TCounterPtr DecreasingThreadsByExchange;
    NMonitoring::TDynamicCounters::TCounterPtr NotEnoughCpuExecutions;
    NMonitoring::TDynamicCounters::TCounterPtr SpinningTimeUs;
    NMonitoring::TDynamicCounters::TCounterPtr SpinThresholdUs;


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
    double Threads;
    double LimitThreads;
    double DefaultThreads;

    void Init(NMonitoring::TDynamicCounters* group, const TString& poolName, ui32 threads);
    void Set(const TExecutorPoolStats& poolStats, const TExecutorThreadStats& stats);
};

struct TActorSystemCounters {
    TIntrusivePtr<NMonitoring::TDynamicCounters> Group;

    NMonitoring::TDynamicCounters::TCounterPtr MaxUsedCpuPercent;
    NMonitoring::TDynamicCounters::TCounterPtr MinUsedCpuPercent;
    NMonitoring::TDynamicCounters::TCounterPtr MaxElapsedCpuPercent;
    NMonitoring::TDynamicCounters::TCounterPtr MinElapsedCpuPercent;

    NMonitoring::TDynamicCounters::TCounterPtr AvgAwakeningTimeNs;
    NMonitoring::TDynamicCounters::TCounterPtr AvgWakingUpTimeNs;


    void Init(NMonitoring::TDynamicCounters* group);
    void Set(const THarmonizerStats& harmonizerStats);
};

} // NActors
