#pragma once

#include "snapshot.h"

#include <ydb/core/kqp/counters/kqp_counters.h>

namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic {

    template <class TSnapshotPtr>
    class TSnapshotSwitch {
        public:
            // returns previous snapshot
            TSnapshotPtr SetSnapshot(const TSnapshotPtr& snapshot) {
                ui8 oldSnapshotIdx = SnapshotIdx;
                ui8 newSnapshotIdx = 1 - SnapshotIdx;
                Snapshots.at(newSnapshotIdx) = snapshot;
                SnapshotIdx = newSnapshotIdx;
                return Snapshots.at(oldSnapshotIdx);
            }

            TSnapshotPtr GetSnapshot() const {
                return Snapshots.at(SnapshotIdx);
            }

        private:
            std::array<TSnapshotPtr, 2> Snapshots;
            std::atomic<ui8> SnapshotIdx = 0;
    };

    struct TTreeElement : public virtual NHdrf::TTreeElementBase<ETreeType::DYNAMIC> {
        std::atomic<ui64> Usage = 0;
        std::atomic<ui64> UsageExtra = 0;
        std::atomic<ui64> Demand = 0;
        std::atomic<ui64> Throttle = 0;

        std::atomic<ui64> BurstUsage = 0;
        std::atomic<ui64> BurstUsageResume = 0;
        std::atomic<ui64> BurstUsageExtra = 0;
        std::atomic<ui64> BurstThrottle = 0;

        explicit TTreeElement(const TId& id, const TStaticAttributes& attrs = {}) : TTreeElementBase(id, attrs) {}

        TPool* GetParent() const;

        virtual NSnapshot::TTreeElement* TakeSnapshot() const = 0;
    };

    class TQuery : public TTreeElement, public NHdrf::TQuery<ETreeType::DYNAMIC>, public TSnapshotSwitch<NSnapshot::TQueryPtr> {
    public:
        // TODO: pass delay params directly to actors from table_service_config
        TQuery(const TQueryId& id, const TDelayParams* delayParams, const TStaticAttributes& attrs = {});

        NSnapshot::TQuery* TakeSnapshot() const override;

        TSchedulableTaskList::iterator AddTask(const TSchedulableTaskPtr& task);
        void RemoveTask(const TSchedulableTaskList::iterator& it);
        ui32 ResumeTasks(ui32 count);

    public:
        std::atomic<ui64> CurrentTasksTime = 0; // sum of average execution time for all active tasks
        std::atomic<ui64> WaitingTasksTime = 0; // sum of average execution time for all throttled tasks

        NMonitoring::THistogramPtr Delay; // TODO: hacky counter for delays from queries - initialize from pool
        const TDelayParams* const DelayParams; // owned by scheduler

    private:
        TRWMutex TasksMutex;
        TSchedulableTaskList SchedulableTasks; // protected by TasksMutex
    };

    class TPool : public TTreeElement, public NHdrf::TPool<ETreeType::DYNAMIC> {
    public:
        TPool(const TPoolId& id, const TIntrusivePtr<TKqpCounters>& counters, const TStaticAttributes& attrs = {});

        NSnapshot::TPool* TakeSnapshot() const override;

        // TODO: override AddQuery() to initialize query->Delay = Counters.Delay
    };

    class TDatabase : public TPool {
    public:
        explicit TDatabase(const TDatabaseId& id, const TStaticAttributes& attrs = {});

        NSnapshot::TDatabase* TakeSnapshot() const override;
    };

    class TRoot : public TPool, public TSnapshotSwitch<NSnapshot::TRootPtr> {
    public:
        explicit TRoot(TIntrusivePtr<TKqpCounters> counters);

        void AddDatabase(const TDatabasePtr& database);
        void RemoveDatabase(const TDatabaseId& databaseId);
        TDatabasePtr GetDatabase(const TDatabaseId& databaseId) const;

        NSnapshot::TRoot* TakeSnapshot() const override;

    public:
        ui64 TotalLimit = Infinity();

    private:
        struct {
            NMonitoring::TDynamicCounters::TCounterPtr TotalLimit;
        } Counters;
    };

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic
