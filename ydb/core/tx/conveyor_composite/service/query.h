#pragma once

#include <ydb/core/tx/conveyor_composite/service/work_status.h_serialized.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>

#include <ydb/core/kqp/runtime/scheduler/fwd.h>

#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <memory>
#include <optional>
#include <ranges>
#include <variant>
#include <vector>

namespace NKikimr::NConveyorComposite {

    struct TDrainContext {
        TMonotonic Now;
        TMonotonic AverageWakeUpDeadline;
        THashSet<TSchedulerQueryIdentity> RetryQueries;
    };

    struct TSchedulableWorkControl;

    struct TSchedulableWorkCell {
        std::unique_ptr<NYql::NDq::IDqSchedulableWork> Work;
        std::shared_ptr<TSchedulableWorkControl> Control;
        ESchedulableWorkStatus Status = ESchedulableWorkStatus::IDLE;
    };

    class TSchedulerLease {
    private:
        std::shared_ptr<TSchedulableWorkCell> Cell;

        explicit TSchedulerLease(std::shared_ptr<TSchedulableWorkCell> cell);

        void Reset();

        friend class TSchedulableWorkState;

    public:
        TSchedulerLease() = delete;
        TSchedulerLease(const TSchedulerLease&) = delete;
        TSchedulerLease& operator=(const TSchedulerLease&) = delete;

        TSchedulerLease(TSchedulerLease&& other) noexcept;
        TSchedulerLease& operator=(TSchedulerLease&& other) noexcept;
        ~TSchedulerLease();

        explicit operator bool() const;
    };

    using TTryStartResult = std::variant<TSchedulerLease, TMonotonic>;

    class TSchedulableWorkState {
    private:
        friend class TSchedulerQueryState;
        std::vector<std::shared_ptr<TSchedulableWorkCell>> Cells;
        std::shared_ptr<TSchedulableWorkControl> Control;

        void Close(bool force);
        void StopThrottled(TSchedulableWorkCell& cell);
        ui64 GetCount(ESchedulableWorkStatus status) const;
        void IncreaseCapacity(ui64 workersCount, NYql::NDq::IDqSchedulableWorkFactory& factory);
        void DecreaseCapacity(ui64 workersCount);
        void ForcedUpdateWorkCapacity(ui64 workersCount, NYql::NDq::IDqSchedulableWorkFactory& factory);

    public:
        TSchedulableWorkState();
        TSchedulableWorkState(const TSchedulableWorkState&) = delete;
        TSchedulableWorkState& operator=(const TSchedulableWorkState&) = delete;
        TSchedulableWorkState(TSchedulableWorkState&&) noexcept;
        TSchedulableWorkState& operator=(TSchedulableWorkState&&) noexcept;
        ~TSchedulableWorkState();

        TTryStartResult TryStart(TMonotonic now);
        void PrepareForRemoval();
    };

    class TSchedulerQueryState {
    private:
        friend class TQueryRegistry;

        ui64 ProcessesCount = 0;
        ui64 CpuCount = 0;
        std::optional<TMonotonic> WakeUpDeadline;
        std::unique_ptr<NYql::NDq::IDqSchedulableWorkFactory> WorkFactory;
        TSchedulableWorkState Works;

        bool SetWorkFactory(std::unique_ptr<NYql::NDq::IDqSchedulableWorkFactory>&& factory);

    public:
        void RegisterProcess();
        void UnregisterProcess();
        void PrepareWorkCapacity(ui64 cpuCount);
        void ApplyWorkCapacity();
        TTryStartResult TryStart(TMonotonic now);
        void PrepareForRemoval();

        bool IsReady() const;
        bool HasWorksCapacity() const;
        bool IsWaitRelease() const;
        bool IsReadyToRelease() const;
        const std::optional<TMonotonic>& GetWakeUpDeadline() const;
    };

    class TQueryRegistry {
    private:
        THashMap<TSchedulerQueryIdentity, TSchedulerQueryState> Queries;

    public:
        TQueryRegistry();

        bool RegisterProcess(const TSchedulerQueryIdentity& identity);
        void UnregisterProcess(const TSchedulerQueryIdentity& identity);
        bool TryReleaseQuery(const TSchedulerQueryIdentity& identity);
        ui64 MovePendingQueryToService(const TSchedulerQueryIdentity& identity);
        bool SetQuery(const TSchedulerQueryIdentity& identity, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query);
        void PrepareWorkCapacity(const TSchedulerQueryIdentity& identity, ui64 cpuCount);
        void ApplyWorkCapacity(const TSchedulerQueryIdentity& identity);

        auto GetIdentitiesView() const {
            return Queries | std::views::keys;
        }

        TMonotonic GetAverageWakeUpDeadline(TMonotonic now) const;
        TSchedulerQueryState& GetStateVerified(const TSchedulerQueryIdentity& identity);
        const TSchedulerQueryState& GetStateVerified(const TSchedulerQueryIdentity& identity) const;
    };

} // namespace NKikimr::NConveyorComposite
