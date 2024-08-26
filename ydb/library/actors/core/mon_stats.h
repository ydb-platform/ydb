#pragma once

#include "defs.h"
//#include "actor.h"
#include <ydb/library/actors/util/local_process_key.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>
#include <util/system/hp_timer.h>

namespace NActors {
    struct TLogHistogram : public NMonitoring::IHistogramSnapshot {
        TLogHistogram();

        inline void Add(ui64 val, ui64 inc = 1) {
            size_t ind = 0;
#if defined(__clang__) && __clang_major__ == 3 && __clang_minor__ == 7
            asm volatile("" ::
                             : "memory");
#endif
            if (val > 1) {
                ind = GetValueBitCount(val - 1);
            }
#if defined(__clang__) && __clang_major__ == 3 && __clang_minor__ == 7
            asm volatile("" ::
                             : "memory");
#endif
            RelaxedStore(&TotalSamples, RelaxedLoad(&TotalSamples) + inc);
            RelaxedStore(&Buckets[ind], RelaxedLoad(&Buckets[ind]) + inc);
        }

        void Aggregate(const TLogHistogram& other);

        // IHistogramSnapshot
        ui32 Count() const override;

        NMonitoring::TBucketBound UpperBound(ui32 index) const override;

        NMonitoring::TBucketValue Value(ui32 index) const override;

        ui64 TotalSamples = 0;
        ui64 Buckets[65];
    };

    struct TExecutorPoolStats {
        ui64 MaxUtilizationTime = 0;
        ui64 IncreasingThreadsByNeedyState = 0;
        ui64 IncreasingThreadsByExchange = 0;
        ui64 DecreasingThreadsByStarvedState = 0;
        ui64 DecreasingThreadsByHoggishState = 0;
        ui64 DecreasingThreadsByExchange = 0;
        i64 MaxConsumedCpuUs = 0;
        i64 MinConsumedCpuUs = 0;
        i64 MaxBookedCpuUs = 0;
        i64 MinBookedCpuUs = 0;
        double SpinningTimeUs = 0;
        double SpinThresholdUs = 0;
        i16 WrongWakenedThreadCount = 0;
        double CurrentThreadCount = 0;
        double PotentialMaxThreadCount = 0;
        double DefaultThreadCount = 0;
        double MaxThreadCount = 0;
        bool IsNeedy = false;
        bool IsStarved = false;
        bool IsHoggish = false;
        bool HasFullOwnSharedThread = false;
        bool HasHalfOfOwnSharedThread = false;
        bool HasHalfOfOtherSharedThread = false;
    };

    struct TActivationTime {
        i64 TimeUs = 0;
        ui32 LastActivity = 0;
    };

    struct TExecutorThreadStats {
        ui64 SentEvents = 0;
        ui64 ReceivedEvents = 0;
        ui64 PreemptedEvents = 0; // Number of events experienced hard preemption
        ui64 NonDeliveredEvents = 0;
        ui64 EmptyMailboxActivation = 0;
        ui64 CpuUs = 0; // microseconds thread was executing on CPU (accounts for preemtion)
        ui64 SafeElapsedTicks = 0;
        ui64 WorstActivationTimeUs = 0;

        TActivationTime CurrentActivationTime;

        NHPTimer::STime ElapsedTicks = 0;
        NHPTimer::STime ParkedTicks = 0;
        NHPTimer::STime BlockedTicks = 0;
        TLogHistogram ActivationTimeHistogram;
        TLogHistogram EventDeliveryTimeHistogram;
        TLogHistogram EventProcessingCountHistogram;
        TLogHistogram EventProcessingTimeHistogram;
        TVector<NHPTimer::STime> ElapsedTicksByActivity;
        TVector<ui64> LongActivationDetectionsByActivity;
        TVector<ui64> ReceivedEventsByActivity;
        TVector<i64> ActorsAliveByActivity; // the sum should be positive, but per-thread might be negative
        TVector<ui64> ScheduledEventsByActivity;
        TVector<ui64> StuckActorsByActivity;
        TVector<TActivationTime> AggregatedCurrentActivationTime;
        TVector<std::array<ui64, 10>> UsageByActivity;
        ui64 PoolActorRegistrations = 0;
        ui64 PoolDestroyedActors = 0;
        ui64 PoolAllocatedMailboxes = 0;
        ui64 MailboxPushedOutByTailSending = 0;
        ui64 MailboxPushedOutBySoftPreemption = 0;
        ui64 MailboxPushedOutByTime = 0;
        ui64 MailboxPushedOutByEventCount = 0;
        ui64 NotEnoughCpuExecutions = 0;

        TExecutorThreadStats();

        void Aggregate(const TExecutorThreadStats& other);

        size_t MaxActivityType() const;
    };

}
