#pragma once

#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_task.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>
#include <ydb/core/util/token_bucket.h>

namespace NKikimr::NConveyorComposite {

    class TCpuQuotaBucket {
    public:
        void Update(const NKqp::NScheduler::TCpuQuotaSettings& settings, TMonotonic now);
        std::optional<TDuration> CalculateDelay(TMonotonic now);
        void Account(TDuration actual, TMonotonic now);

    private:
        TTokenBucketBase<TMonotonic> Bucket;
    };

    class TWorkloadPoolState: public TNonCopyable {
    public:
        void UpdateQuota(const NKqp::NScheduler::TCpuQuotaSettings& settings, TMonotonic now);
        std::optional<TDuration> CalculateQuotaDelay(TMonotonic now);
        void Account(TDuration actual, TMonotonic now);

    private:
        TCpuQuotaBucket Quota;
    };

    using TWorkloadPoolStatePtr = std::shared_ptr<TWorkloadPoolState>;

    class TWorkloadQueryState: public TNonCopyable {
    public:
        TWorkloadQueryState(TWorkloadContext context, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query,
                            TWorkloadPoolStatePtr pool);

        void UpdateQuota(const NKqp::NScheduler::TWorkloadCpuQuotaSettings& settings, TMonotonic now);
        std::optional<TDuration> CalculateQuotaDelay(TMonotonic now);
        void Account(TDuration actual, TMonotonic now);

        const TWorkloadContext& GetContext() const {
            return Context;
        }

        const NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr& GetQuery() const {
            return Query;
        }

    private:
        const TWorkloadContext Context;
        const NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr Query;
        const TWorkloadPoolStatePtr Pool;
        TCpuQuotaBucket Quota;
    };

    using TWorkloadQueryStatePtr = std::shared_ptr<TWorkloadQueryState>;

    class TConveyorWorkUnit: public TNonCopyable {
    public:
        explicit TConveyorWorkUnit(TWorkloadQueryStatePtr queryState);
        ~TConveyorWorkUnit();

        [[nodiscard]] bool TryStart();
        void Finish(TDuration actual);

        TDuration CalculateDelay() const;

        const TWorkloadContext& GetContext() const {
            return QueryState->GetContext();
        }

    private:
        TDuration CalculateHdrfDelay() const;
        void LeaveThrottle(TMonotonic now);

        const TWorkloadQueryStatePtr QueryState;
        std::shared_ptr<NKqp::NScheduler::TSchedulableTask> SchedulableTask;
        bool Running = false;
        bool Throttled = false;
        TMonotonic ThrottleStart;
        TDuration RetryDelay;
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
            TWorkloadQueryStatePtr QueryState;
            TConveyorWorkUnitPtr PendingWorkUnit;
            ui64 ProcessRefCount = 0;
            bool RegistrationPending = false;
            bool OwnsRegistration = false;
        };

        void EnsureQueryRegistration(TQueryEntry& entry);
        TWorkloadQueryStatePtr MakeQueryState(
            const TWorkloadContext& context, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query);
        void RemoveQuery(ui64 queryId);
        void RegisterRetry(TDuration delay);

        NKqp::NScheduler::TComputeSchedulerPtr Scheduler;
        THashMap<ui64, TQueryEntry> Queries;
        THashMap<std::pair<TString, TString>, TWorkloadPoolStatePtr> Pools;
        std::optional<TMonotonic> NextWakeup;
    };

} // namespace NKikimr::NConveyorComposite
