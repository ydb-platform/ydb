#include "collector_counters.h"

namespace NActors {

// THistogramCounters

void THistogramCounters::Init(NMonitoring::TDynamicCounters* group, const TString& baseName, const TString& unit, ui64 maxVal) {
    for (size_t i = 0; (1ull<<i) <= maxVal; ++i) {
        TString bucketName = ToString(1ull<<i) + " " + unit;
        Buckets.push_back(group->GetSubgroup("sensor", baseName)->GetNamedCounter("range", bucketName, true));
    }
    Buckets.push_back(group->GetSubgroup("sensor", baseName)->GetNamedCounter("range", "INF", true));
}

void THistogramCounters::Set(const TLogHistogram& data) {
    ui32 i = 0;
    for (;i < Y_ARRAY_SIZE(data.Buckets) && i < Buckets.size()-1; ++i)
        *Buckets[i] = data.Buckets[i];
    ui64 last = 0;
    for (;i < Y_ARRAY_SIZE(data.Buckets); ++i)
        last += data.Buckets[i];
    *Buckets.back() = last;
}

void THistogramCounters::Set(const TLogHistogram& data, double factor) {
    ui32 i = 0;
    for (;i < Y_ARRAY_SIZE(data.Buckets) && i < Buckets.size()-1; ++i)
        *Buckets[i] = data.Buckets[i]*factor;
    ui64 last = 0;
    for (;i < Y_ARRAY_SIZE(data.Buckets); ++i)
        last += data.Buckets[i];
    *Buckets.back() = last*factor;
}

// TActivityStats

void TActivityStats::Init(NMonitoring::TDynamicCounterPtr group) {
    Group = group;

    CurrentActivationTimeByActivity.resize(GetActivityTypeCount());
    ElapsedMicrosecByActivityBuckets.resize(GetActivityTypeCount());
    ReceivedEventsByActivityBuckets.resize(GetActivityTypeCount());
    ActorsAliveByActivityBuckets.resize(GetActivityTypeCount());
    ScheduledEventsByActivityBuckets.resize(GetActivityTypeCount());
    StuckActorsByActivityBuckets.resize(GetActivityTypeCount());
}

void TActivityStats::Set(const TExecutorThreadStats& stats) {
    for (ui32 i : xrange(stats.MaxActivityType())) {
        Y_ABORT_UNLESS(i < GetActivityTypeCount());
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

        *CurrentActivationTimeByActivity[i] = 0;
        *ElapsedMicrosecByActivityBuckets[i] = ::NHPTimer::GetSeconds(ticks)*1000000;
        *ReceivedEventsByActivityBuckets[i] = events;
        *ActorsAliveByActivityBuckets[i] = actors;
        *ScheduledEventsByActivityBuckets[i] = scheduled;
        *StuckActorsByActivityBuckets[i] = stuck;
    }

    auto setActivationTime = [&](TActivationTime activation) {
        if (!ActorsAliveByActivityBuckets[activation.LastActivity]) {
            InitCountersForActivity(activation.LastActivity);
        }
        *CurrentActivationTimeByActivity[activation.LastActivity] = activation.TimeUs;
    };
    if (stats.CurrentActivationTime.TimeUs) {
        setActivationTime(stats.CurrentActivationTime);
    }
    std::vector<TActivationTime> activationTimes = stats.AggregatedCurrentActivationTime;
    Sort(activationTimes.begin(), activationTimes.end(), [](auto &left, auto &right) {
        return left.LastActivity < right.LastActivity ||
            left.LastActivity == right.LastActivity && left.TimeUs > right.TimeUs;
    });
    ui32 prevActivity = Max<ui32>();
    for (auto &activationTime : activationTimes) {
        if (activationTime.LastActivity == prevActivity) {
            continue;
        }
        setActivationTime(activationTime);
        prevActivity = activationTime.LastActivity;
    }
}

void TActivityStats::InitCountersForActivity(ui32 activityType) {
    Y_ABORT_UNLESS(activityType < GetActivityTypeCount());

    auto bucketName = TString(GetActivityTypeName(activityType));

    CurrentActivationTimeByActivity[activityType] =
        Group->GetSubgroup("sensor", "CurrentActivationTimeUsByActivity")->GetNamedCounter("activity", bucketName, false);
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

// TExecutorPoolCounters

void TExecutorPoolCounters::Init(NMonitoring::TDynamicCounters* group, const TString& poolName, ui32 threads) {
    LastElapsedSeconds = 0;
    Usage = 0;
    UsageTimer.Reset();
    Name = poolName;
    Threads = threads;
    LimitThreads = threads;
    DefaultThreads = threads;

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

    CurrentThreadCountPercent = PoolGroup->GetCounter("CurrentThreadCountPercent", false);
    PotentialMaxThreadCountPercent  = PoolGroup->GetCounter("PotentialMaxThreadCountPercent", false);
    PossibleMaxThreadCountPercent  = PoolGroup->GetCounter("PossibleMaxThreadCountPercent", false);
    DefaultThreadCountPercent  = PoolGroup->GetCounter("DefaultThreadCountPercent", false);
    MaxThreadCountPercent  = PoolGroup->GetCounter("MaxThreadCountPercent", false);

    IsNeedy = PoolGroup->GetCounter("IsNeedy", false);
    IsStarved = PoolGroup->GetCounter("IsStarved", false);
    IsHoggish = PoolGroup->GetCounter("IsHoggish", false);
    HasFullOwnSharedThread = PoolGroup->GetCounter("HasFullOwnSharedThread", false);
    HasHalfOfOwnSharedThread = PoolGroup->GetCounter("HasHalfOfOwnSharedThread", false);
    HasHalfOfOtherSharedThread = PoolGroup->GetCounter("HasHalfOfOtherSharedThread", false);
    IncreasingThreadsByNeedyState = PoolGroup->GetCounter("IncreasingThreadsByNeedyState", true);
    IncreasingThreadsByExchange = PoolGroup->GetCounter("IncreasingThreadsByExchange", true);
    DecreasingThreadsByStarvedState = PoolGroup->GetCounter("DecreasingThreadsByStarvedState", true);
    DecreasingThreadsByHoggishState = PoolGroup->GetCounter("DecreasingThreadsByHoggishState", true);
    DecreasingThreadsByExchange = PoolGroup->GetCounter("DecreasingThreadsByExchange", true);
    NotEnoughCpuExecutions = PoolGroup->GetCounter("NotEnoughCpuExecutions", true);
    SpinningTimeUs = PoolGroup->GetCounter("SpinningTimeUs", true);
    SpinThresholdUs = PoolGroup->GetCounter("SpinThresholdUs", false);


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

void TExecutorPoolCounters::Set(const TExecutorPoolStats& poolStats, const TExecutorThreadStats& stats) {
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
    double elapsedSeconds = ::NHPTimer::GetSeconds(stats.ElapsedTicks);
    *SentEvents         = stats.SentEvents;
    *ReceivedEvents     = stats.ReceivedEvents;
    *PreemptedEvents     = stats.PreemptedEvents;
    *NonDeliveredEvents = stats.NonDeliveredEvents;
    *DestroyedActors    = stats.PoolDestroyedActors;
    *EmptyMailboxActivation = stats.EmptyMailboxActivation;
    *CpuMicrosec        = stats.CpuUs;
    *ElapsedMicrosec    = elapsedSeconds*1000000;
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

    *CurrentThreadCountPercent = poolStats.CurrentThreadCount * 100;
    *PotentialMaxThreadCountPercent = poolStats.PotentialMaxThreadCount * 100;
    *PossibleMaxThreadCountPercent = poolStats.PotentialMaxThreadCount * 100;
    *DefaultThreadCountPercent = poolStats.DefaultThreadCount * 100;
    *MaxThreadCountPercent = poolStats.MaxThreadCount * 100;

    *IsNeedy = poolStats.IsNeedy;
    *IsStarved = poolStats.IsStarved;
    *IsHoggish = poolStats.IsHoggish;

    *HasFullOwnSharedThread = poolStats.HasFullOwnSharedThread;
    *HasHalfOfOwnSharedThread = poolStats.HasHalfOfOwnSharedThread;
    *HasHalfOfOtherSharedThread = poolStats.HasHalfOfOtherSharedThread;
    *IncreasingThreadsByNeedyState = poolStats.IncreasingThreadsByNeedyState;
    *IncreasingThreadsByExchange = poolStats.IncreasingThreadsByExchange;
    *DecreasingThreadsByStarvedState = poolStats.DecreasingThreadsByStarvedState;
    *DecreasingThreadsByHoggishState = poolStats.DecreasingThreadsByHoggishState;
    *DecreasingThreadsByExchange = poolStats.DecreasingThreadsByExchange;
    *NotEnoughCpuExecutions = stats.NotEnoughCpuExecutions;

    *SpinningTimeUs = poolStats.SpinningTimeUs;
    *SpinThresholdUs = poolStats.SpinThresholdUs;

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
    Threads = poolStats.CurrentThreadCount;
    LimitThreads = poolStats.PotentialMaxThreadCount;
    const double currentUsage = LimitThreads > 0 ? ((elapsedSeconds - LastElapsedSeconds) / seconds / LimitThreads) : 0;

    // update usage factor according to smoothness
    const double smoothness = 0.5;
    Usage = currentUsage * smoothness + Usage * (1.0 - smoothness);
    LastElapsedSeconds = elapsedSeconds;
#else
    Y_UNUSED(stats);
    Y_UNUSED(poolStats);
#endif
}

// TActorSystemCounters

void TActorSystemCounters::Init(NMonitoring::TDynamicCounters* group) {
    Group = group;

    MaxUsedCpuPercent = Group->GetCounter("MaxUsedCpuPercent", false);
    MinUsedCpuPercent = Group->GetCounter("MinUsedCpuPercent", false);
    MaxElapsedCpuPercent = Group->GetCounter("MaxElapsedCpuPercent", false);
    MinElapsedCpuPercent = Group->GetCounter("MinElapsedCpuPercent", false);
    AvgAwakeningTimeNs = Group->GetCounter("AvgAwakeningTimeNs", false);
    AvgWakingUpTimeNs = Group->GetCounter("AvgWakingUpTimeNs", false);
}

void TActorSystemCounters::Set(const THarmonizerStats& harmonizerStats) {
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
    *MaxUsedCpuPercent = harmonizerStats.MaxUsedCpu;
    *MinUsedCpuPercent = harmonizerStats.MinUsedCpu;
    *MaxElapsedCpuPercent = harmonizerStats.MaxElapsedCpu;
    *MinElapsedCpuPercent = harmonizerStats.MinElapsedCpu;

    *AvgAwakeningTimeNs = harmonizerStats.AvgAwakeningTimeUs * 1000;
    *AvgWakingUpTimeNs = harmonizerStats.AvgWakingUpTimeUs * 1000;
#else
    Y_UNUSED(harmonizerStats);
#endif
}

} // NActors 
