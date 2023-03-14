#pragma once

#include "defs.h"
//#include "actor.h"
#include <library/cpp/monlib/metrics/histogram_snapshot.h>
#include <util/system/hp_timer.h>

namespace NActors {
    struct TLogHistogram : public NMonitoring::IHistogramSnapshot {
        TLogHistogram() {
            memset(Buckets, 0, sizeof(Buckets));
        }

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

        void Aggregate(const TLogHistogram& other) {
            const ui64 inc = RelaxedLoad(&other.TotalSamples);
            RelaxedStore(&TotalSamples, RelaxedLoad(&TotalSamples) + inc);
            for (size_t i = 0; i < Y_ARRAY_SIZE(Buckets); ++i) {
                Buckets[i] += RelaxedLoad(&other.Buckets[i]);
            }
        }

        // IHistogramSnapshot
        ui32 Count() const override {
            return Y_ARRAY_SIZE(Buckets);
        }

        NMonitoring::TBucketBound UpperBound(ui32 index) const override {
            Y_ASSERT(index < Y_ARRAY_SIZE(Buckets));
            if (index == 0) {
                return 1;
            }
            return NMonitoring::TBucketBound(1ull << (index - 1)) * 2.0;
        }

        NMonitoring::TBucketValue Value(ui32 index) const override {
            Y_ASSERT(index < Y_ARRAY_SIZE(Buckets));
            return Buckets[index];
        }

        ui64 TotalSamples = 0;
        ui64 Buckets[65];
    };

    struct TExecutorPoolStats {
        ui64 MaxUtilizationTime = 0;
        ui64 IncreasingThreadsByNeedyState = 0;
        ui64 DecreasingThreadsByStarvedState = 0;
        ui64 DecreasingThreadsByHoggishState = 0;
        i16 WrongWakenedThreadCount = 0;
        i16 CurrentThreadCount = 0;
        i16 PotentialMaxThreadCount = 0;
        i16 DefaultThreadCount = 0;
        i16 MaxThreadCount = 0;
        bool IsNeedy = false;
        bool IsStarved = false;
        bool IsHoggish = false;
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
        NHPTimer::STime ElapsedTicks = 0;
        NHPTimer::STime ParkedTicks = 0;
        NHPTimer::STime BlockedTicks = 0;
        TLogHistogram ActivationTimeHistogram;
        TLogHistogram EventDeliveryTimeHistogram;
        TLogHistogram EventProcessingCountHistogram;
        TLogHistogram EventProcessingTimeHistogram;
        TVector<NHPTimer::STime> ElapsedTicksByActivity;
        TVector<ui64> ReceivedEventsByActivity;
        TVector<i64> ActorsAliveByActivity; // the sum should be positive, but per-thread might be negative
        TVector<ui64> ScheduledEventsByActivity;
        ui64 PoolActorRegistrations = 0;
        ui64 PoolDestroyedActors = 0;
        ui64 PoolAllocatedMailboxes = 0;
        ui64 MailboxPushedOutByTailSending = 0;
        ui64 MailboxPushedOutBySoftPreemption = 0;
        ui64 MailboxPushedOutByTime = 0;
        ui64 MailboxPushedOutByEventCount = 0;

        TExecutorThreadStats(size_t activityVecSize = 5) // must be not empty as 0 used as default
            : ElapsedTicksByActivity(activityVecSize)
            , ReceivedEventsByActivity(activityVecSize)
            , ActorsAliveByActivity(activityVecSize)
            , ScheduledEventsByActivity(activityVecSize)
        {}

        template <typename T>
        static void AggregateOne(TVector<T>& self, const TVector<T>& other) {
            const size_t selfSize = self.size();
            const size_t otherSize = other.size();
            if (selfSize < otherSize)
                self.resize(otherSize);
            for (size_t at = 0; at < otherSize; ++at)
                self[at] += RelaxedLoad(&other[at]);
        }

        void Aggregate(const TExecutorThreadStats& other) {
            SentEvents += RelaxedLoad(&other.SentEvents);
            ReceivedEvents += RelaxedLoad(&other.ReceivedEvents);
            PreemptedEvents += RelaxedLoad(&other.PreemptedEvents);
            NonDeliveredEvents += RelaxedLoad(&other.NonDeliveredEvents);
            EmptyMailboxActivation += RelaxedLoad(&other.EmptyMailboxActivation);
            CpuUs += RelaxedLoad(&other.CpuUs);
            SafeElapsedTicks += RelaxedLoad(&other.SafeElapsedTicks);
            RelaxedStore(
                &WorstActivationTimeUs,
                std::max(RelaxedLoad(&WorstActivationTimeUs), RelaxedLoad(&other.WorstActivationTimeUs)));
            ElapsedTicks += RelaxedLoad(&other.ElapsedTicks);
            ParkedTicks += RelaxedLoad(&other.ParkedTicks);
            BlockedTicks += RelaxedLoad(&other.BlockedTicks);
            MailboxPushedOutByTailSending += RelaxedLoad(&other.MailboxPushedOutByTailSending);
            MailboxPushedOutBySoftPreemption += RelaxedLoad(&other.MailboxPushedOutBySoftPreemption);
            MailboxPushedOutByTime += RelaxedLoad(&other.MailboxPushedOutByTime);
            MailboxPushedOutByEventCount += RelaxedLoad(&other.MailboxPushedOutByEventCount);

            ActivationTimeHistogram.Aggregate(other.ActivationTimeHistogram);
            EventDeliveryTimeHistogram.Aggregate(other.EventDeliveryTimeHistogram);
            EventProcessingCountHistogram.Aggregate(other.EventProcessingCountHistogram);
            EventProcessingTimeHistogram.Aggregate(other.EventProcessingTimeHistogram);

            AggregateOne(ElapsedTicksByActivity, other.ElapsedTicksByActivity);
            AggregateOne(ReceivedEventsByActivity, other.ReceivedEventsByActivity);
            AggregateOne(ActorsAliveByActivity, other.ActorsAliveByActivity);
            AggregateOne(ScheduledEventsByActivity, other.ScheduledEventsByActivity);

            RelaxedStore(
                &PoolActorRegistrations,
                std::max(RelaxedLoad(&PoolActorRegistrations), RelaxedLoad(&other.PoolActorRegistrations)));
            RelaxedStore(
                &PoolDestroyedActors,
                std::max(RelaxedLoad(&PoolDestroyedActors), RelaxedLoad(&other.PoolDestroyedActors)));
            RelaxedStore(
                &PoolAllocatedMailboxes,
                std::max(RelaxedLoad(&PoolAllocatedMailboxes), RelaxedLoad(&other.PoolAllocatedMailboxes)));
        }

        size_t MaxActivityType() const {
            return ActorsAliveByActivity.size();
        }
    };

}
