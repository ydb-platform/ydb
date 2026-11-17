#pragma once

#include "snapshot.h"

#include <ydb/core/kqp/counters/kqp_counters.h>

namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic {

    template <class TSnapshotPtr>
    class TSnapshotSwitch {
        public:
            // returns previous snapshot
            TSnapshotPtr SetSnapshot(const TSnapshotPtr& snapshot) {
                TGuard lock(mutex);
                ui8 oldSnapshotIdx = SnapshotIdx;
                ui8 newSnapshotIdx = 1 - SnapshotIdx;
                Snapshots.at(newSnapshotIdx) = snapshot;
                SnapshotIdx = newSnapshotIdx;
                return Snapshots.at(oldSnapshotIdx);
            }

            TSnapshotPtr GetSnapshot() const {
                TGuard lock(mutex);
                return Snapshots.at(SnapshotIdx);
            }

        private:
            TAdaptiveLock mutex;
            std::array<TSnapshotPtr, 2> Snapshots; // protected by mutex
            std::atomic<ui8> SnapshotIdx = 0;      // protected by mutex
    };

    struct TTreeElement : public virtual NHdrf::TTreeElementBase<ETreeType::DYNAMIC> {
        std::atomic<ui64> CpuUsage = 0;
        std::atomic<ui64> CpuMaxDemand = 0;
        std::atomic<ui64> CpuThrottle = 0;

        std::atomic<ui64> CpuBurstUsage = 0;
        std::atomic<ui64> CpuBurstUsageResume = 0;
        std::atomic<ui64> CpuBurstThrottle = 0;
        std::atomic<ui64> ReadBurstUsage = 0;

        std::atomic<ui64> MemoryUsage = 0;
        std::atomic<ui64> MemoryDemand = 0;

        // TODO: implement Read resource - for now it's only per datashard.

        explicit TTreeElement(const TId& id) : TTreeElementBase(id) {}

        TPool* GetParent() const;

        virtual NSnapshot::TTreeElement* TakeSnapshot() = 0;
    };

    class TQuery : public TTreeElement, public NHdrf::TQuery<ETreeType::DYNAMIC>, public TSnapshotSwitch<NSnapshot::TQueryPtr>, public std::enable_shared_from_this<TQuery> {
    public:
        // TODO: pass delay params directly to actors from table_service_config
        TQuery(const TQueryId& id, const TDelayParams* delayParams, bool allowMinFairShare);

        NSnapshot::TQuery* TakeSnapshot() override;

        bool GetAllowMinFairShare() const;

        // Gives the new query the fair-share of its pool right away - not to wait for its first snapshot.
        // The initial snapshot is not attached to the snapshot tree, so it has no parent.
        void InitSnapshot(const NSnapshot::TPool& pool);

        TFullPoolId GetFullPoolId() const;

        TSchedulableTaskList::iterator AddTask(const TSchedulableTaskPtr& task);
        ui32 ResumeTasks(ui32 count);

    public:
        std::atomic<ui64> CurrentTasksTime = 0; // sum of average execution time for all active tasks
        std::atomic<ui64> WaitingTasksTime = 0; // sum of average execution time for all throttled tasks

        const TDelayParams* const DelayParams; // owned by scheduler

        const bool AllowMinFairShare; // tasks should look at this in case of missing snapshot

    private:
        // used to calculate adjusted satisfaction and actual demand between snapshots
        ui64 PrevCpuBurstUsage = 0;
        ui64 PrevCpuBurstThrottle = 0;

        TMutex TasksMutex;
        TSchedulableTaskList SchedulableTasks; // protected by TasksMutex
    };

    class TPool : public TTreeElement, public NHdrf::TPool<ETreeType::DYNAMIC> {
    public:
        TPool(const TPoolId& id, const TIntrusivePtr<TKqpCounters>& counters);

        NSnapshot::TPool* TakeSnapshot() override;

        // Resolves the stable runtime path in one version of the static tree.
        // The returned reference owns that version, including the node's ancestors.
        NStatic::TPoolPtr GetStaticPool() const;
        TStaticAttributes GetConfiguredAttributes() const;

        void CollectDelay(ui64 microseconds) const {
            if (Counters) {
                Counters->Delay->Collect(microseconds);
            }
        }

    protected:
        virtual NSnapshot::TPool* CreateSnapshot(const NStatic::TPoolPtr& pool) const;
    };

    // The memory of the default pool is accounted, but not limited by the pool itself
    class TDefaultPool : public TPool {
    public:
        TDefaultPool(const TPoolId& id, const TIntrusivePtr<TKqpCounters>& counters);

    protected:
        NSnapshot::TPool* CreateSnapshot(const NStatic::TPoolPtr& pool) const override;
    };

    class TDatabase : public TPool {
    public:
        explicit TDatabase(const TDatabaseId& id);

        NSnapshot::TDatabase* TakeSnapshot() override;
    };

    class TRoot : public TPool, public TSnapshotSwitch<NSnapshot::TRootPtr> {
    public:
        explicit TRoot(const TIntrusivePtr<TKqpCounters>& counters);

        void AddDatabase(const TDatabasePtr& database);
        void RemoveDatabase(const TDatabaseId& databaseId);
        TDatabasePtr GetDatabase(const TDatabaseId& databaseId) const;

        NSnapshot::TRoot* TakeSnapshot() override;

        NStatic::TRootPtr GetStaticTree() const {
            return std::atomic_load(&StaticTree);
        }

        void SetStaticTree(NStatic::TRootPtr tree) {
            std::atomic_store(&StaticTree, std::move(tree));
        }

    private:
        NStatic::TRootPtr StaticTree = std::make_shared<const NStatic::TRoot>();
        struct {
            NMonitoring::TDynamicCounters::TCounterPtr TotalCpuLimit;
            NMonitoring::TDynamicCounters::TCounterPtr TotalMemoryLimit;
        } Counters;
    };

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NDynamic
