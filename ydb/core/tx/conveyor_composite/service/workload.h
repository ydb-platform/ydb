#pragma once

#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_task.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>

namespace NKikimr::NConveyorComposite {

    class TConveyorWorkUnit: public TNonCopyable {
    public:
        TConveyorWorkUnit(TWorkloadContext context, const NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr& query);
        ~TConveyorWorkUnit();

        [[nodiscard]] bool TryStart();
        void Finish(TDuration actual);

        TDuration CalculateDelay() const;

        const TWorkloadContext& GetContext() const {
            return Context;
        }

    private:
        void LeaveThrottle(TMonotonic now);

        const TWorkloadContext Context;
        std::shared_ptr<NKqp::NScheduler::TSchedulableTask> SchedulableTask;
        bool Running = false;
        bool Throttled = false;
        TMonotonic ThrottleStart;
    };

    using TConveyorWorkUnitPtr = std::unique_ptr<TConveyorWorkUnit>;
    using TConveyorWorkUnits = THashMap<ui64, TConveyorWorkUnitPtr>;

    class TWorkloadScheduler {
    public:
        explicit TWorkloadScheduler(NKqp::NScheduler::TComputeSchedulerPtr scheduler);

        bool IsEnabled() const {
            return Scheduler && Scheduler->IsEnabled();
        }

        void RegisterProcess(const TWorkloadContext& context);
        void UnregisterProcess(const TWorkloadContext& context);
        void OnQueryResponse(ui64 queryId, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query);

        [[nodiscard]] bool TryAddToBatch(const TWorkloadContext& context, TConveyorWorkUnits& workUnits);

        std::optional<TMonotonic> ExtractNextWakeup();

    private:
        struct TQueryEntry {
            TWorkloadContext Context;
            NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr Query;
            TConveyorWorkUnitPtr PendingWorkUnit;
            ui64 ProcessRefCount = 0;
            bool RegistrationPending = false;
            bool OwnsRegistration = false;
        };

        void EnsureQueryRegistration(TQueryEntry& entry);
        void RemoveQuery(ui64 queryId);
        void RegisterRetry(TDuration delay);

        NKqp::NScheduler::TComputeSchedulerPtr Scheduler;
        THashMap<ui64, TQueryEntry> Queries;
        std::optional<TMonotonic> NextWakeup;
    };

} // namespace NKikimr::NConveyorComposite
