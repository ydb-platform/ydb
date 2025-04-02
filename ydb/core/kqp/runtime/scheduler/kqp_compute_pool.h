#pragma once

#include "utils.h"

#include <ydb/core/kqp/counters/kqp_counters.h>

namespace NKikimr::NKqp::NScheduler {

    struct TSchedulerEntity;

    class TPool {
        friend struct TSchedulerEntity;

        public:
            TPool(const TString& name, THolder<IObservableValue<double>> share, TIntrusivePtr<TKqpCounters> counters);

            void AddEntity(THolder<TSchedulerEntity>& entity);
            void RemoveEntity(THolder<TSchedulerEntity>& entity);

            // TODO: pool should be always enabled.
            void Enable();
            void Disable();
            bool IsDisabled() const;

            void AdvanceTime(TMonotonic now, TDuration smoothPeriod, TDuration forgetInterval);
            void UpdateGuarantee(ui64 value);

            const TString& GetName() const;
            bool IsActive() const; // TODO: better name - HasEntities()?

        private:
            void InitCounters(const TIntrusivePtr<TKqpCounters>& counters);

        private:
            struct TMutableStats {
                double Capacity = 0;
                TMonotonic LastNowRecalc;
                bool Disabled = false;
                i64 EntitiesWeight = 0;
                double MaxLimitDeviation = 0;

                ssize_t TrackedBefore = 0;

                double Limit(TMonotonic now) const;
            };

            const TString Name;
            const bool HasCounters;

            std::atomic<ui64> EntitiesCount = 0;
            std::atomic<i64> TrackedMicroSeconds = 0;
            std::atomic<i64> ThrottledMicroSeconds = 0;
            std::atomic<i64> DelayedSumBatches = 0;
            std::atomic<i64> DelayedCount = 0;

            THolder<IObservableValue<double>> Share;

            ::NMonitoring::TDynamicCounters::TCounterPtr Vtime;
            ::NMonitoring::TDynamicCounters::TCounterPtr EntitiesWeight;
            ::NMonitoring::TDynamicCounters::TCounterPtr OldLimit;
            ::NMonitoring::TDynamicCounters::TCounterPtr Weight;

            ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerClock;
            ::NMonitoring::TDynamicCounters::TCounterPtr SchedulerLimitUs;

            ::NMonitoring::TDynamicCounters::TCounterPtr Limit;
            ::NMonitoring::TDynamicCounters::TCounterPtr Guarantee;
            ::NMonitoring::TDynamicCounters::TCounterPtr Demand;
            ::NMonitoring::TDynamicCounters::TCounterPtr Usage;
            ::NMonitoring::TDynamicCounters::TCounterPtr Throttle;
            ::NMonitoring::TDynamicCounters::TCounterPtr FairShare;

            mutable TMultithreadPublisher<TMutableStats> MutableStats;
    };

}
