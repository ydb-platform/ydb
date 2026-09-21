#include "query.h"

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_work_factory.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <util/generic/yexception.h>

#include <algorithm>
#include <utility>

namespace NKikimr::NConveyorComposite {

    namespace {

        class TAlwaysReadySchedulableWork final: public NYql::NDq::IDqSchedulableWork {
        public:
            std::optional<TDuration> TryStartExecution(TMonotonic /*now*/) override {
                return std::nullopt;
            }

            void StopExecution() override {
            }

            void NotifyResumed(bool /*byScheduler*/) override {
            }

            void RegisterForResume(const NActors::TActorId& /*actorId*/) override {
            }

            NYql::NDq::TWorkScope GetWorkScope() const override {
                return {};
            }
        };

        class TAlwaysReadySchedulableWorkFactory final: public NYql::NDq::IDqSchedulableWorkFactory {
        public:
            std::unique_ptr<NYql::NDq::IDqSchedulableWork> CreateSchedulableWork() override {
                return std::make_unique<TAlwaysReadySchedulableWork>();
            }

            NYql::NDq::TWorkScope GetWorkScope() const override {
                return {};
            }
        };

    } // namespace

    TSchedulerLease::TSchedulerLease(TSchedulableWorkCell& cell)
        : Cell(&cell)
    {
    }

    TSchedulerLease::TSchedulerLease(TSchedulerLease&& other) noexcept
        : Cell(std::exchange(other.Cell, nullptr))
    {
    }

    TSchedulerLease& TSchedulerLease::operator=(TSchedulerLease&& other) noexcept {
        if (this != &other) {
            Reset();
            Cell = std::exchange(other.Cell, nullptr);
        }
        return *this;
    }

    TSchedulerLease::~TSchedulerLease() {
        Reset();
    }

    TSchedulerLease::operator bool() const {
        return Cell != nullptr;
    }

    void TSchedulerLease::Reset() {
        if (!Cell) {
            return;
        }
        Y_ENSURE(Cell->Status == ESchedulableWorkStatus::STARTED, "scheduler lease does not own a started work");
        Cell->Work->StopExecution();
        Cell->Status = ESchedulableWorkStatus::IDLE;
        Cell = nullptr;
    }

    TSchedulableWorkState::TSchedulableWorkState() = default;
    TSchedulableWorkState::TSchedulableWorkState(TSchedulableWorkState&&) noexcept = default;
    TSchedulableWorkState& TSchedulableWorkState::operator=(TSchedulableWorkState&&) noexcept = default;

    TSchedulableWorkState::~TSchedulableWorkState() {
        for (auto& cell : Cells) {
            if (cell->Status != ESchedulableWorkStatus::IDLE) {
                cell->Work->StopExecution();
                cell->Status = ESchedulableWorkStatus::IDLE;
            }
        }
    }

    void TSchedulableWorkState::StopThrottled(TSchedulableWorkCell& cell) {
        Y_ENSURE(cell.Status == ESchedulableWorkStatus::THROTTLED, "only a throttled work can be stopped by this path");
        cell.Work->StopExecution();
        cell.Status = ESchedulableWorkStatus::IDLE;
    }

    TTryStartResult TSchedulableWorkState::TryStart(const TMonotonic now) {
        Y_ENSURE(!Cells.empty(), "query has no schedulable works");

        for (const auto status : {ESchedulableWorkStatus::THROTTLED, ESchedulableWorkStatus::IDLE}) {
            for (auto& cell : Cells) {
                if (cell->Status != status) {
                    continue;
                }
                if (status == ESchedulableWorkStatus::THROTTLED) {
                    cell->Work->NotifyResumed(false);
                }
                if (const auto delay = cell->Work->TryStartExecution(now)) {
                    cell->Status = ESchedulableWorkStatus::THROTTLED;
                    return now + *delay;
                }
                cell->Status = ESchedulableWorkStatus::STARTED;
                return TSchedulerLease(*cell);
            }
        }

        Y_ENSURE(false, "all schedulable works are already started");
    }

    void TSchedulableWorkState::IncreaseCapacity(
        const ui64 workersCount, NYql::NDq::IDqSchedulableWorkFactory& factory) {
        Y_ENSURE(Cells.size() < workersCount, "schedulable work capacity increase has no additional workers");
        while (Cells.size() < workersCount) {
            auto work = factory.CreateSchedulableWork();
            Y_ENSURE(work, "schedulable work factory returned null");
            Cells.emplace_back(std::make_unique<TSchedulableWorkCell>(std::move(work)));
        }
    }

    void TSchedulableWorkState::DecreaseCapacity(const ui64 workersCount) {
        Y_ENSURE(workersCount < Cells.size(), "schedulable work capacity decrease has no removed workers");
        Y_ENSURE(GetCount(ESchedulableWorkStatus::STARTED) <= workersCount,
                 "cannot shrink schedulable work capacity below the number of started works");
        ui64 toRemove = Cells.size() - workersCount;
        for (auto it = Cells.begin(); it != Cells.end() && toRemove;) {
            auto& cell = **it;
            if (cell.Status == ESchedulableWorkStatus::STARTED) {
                ++it;
                continue;
            }
            if (cell.Status == ESchedulableWorkStatus::THROTTLED) {
                StopThrottled(cell);
            }
            it = Cells.erase(it);
            --toRemove;
        }
        Y_ENSURE(toRemove == 0, "not enough non-started schedulable works to shrink capacity");
    }

    void TSchedulableWorkState::ReconcileCapacity(
        const ui64 workersCount, NYql::NDq::IDqSchedulableWorkFactory& factory) {
        if (Cells.size() > workersCount) {
            DecreaseCapacity(workersCount);
            return;
        }
        if (Cells.size() < workersCount) {
            IncreaseCapacity(workersCount, factory);
            return;
        }
    }

    void TSchedulableWorkState::PrepareForRemoval() {
        Y_ENSURE(GetCount(ESchedulableWorkStatus::STARTED) == 0, "cannot remove query with started schedulable works");
        for (auto& cell : Cells) {
            if (cell->Status == ESchedulableWorkStatus::THROTTLED) {
                StopThrottled(*cell);
            }
        }
        Cells.clear();
    }

    ui64 TSchedulableWorkState::GetCount(const ESchedulableWorkStatus status) const {
        return std::count_if(Cells.begin(), Cells.end(), [status](const auto& cell) { return cell->Status == status; });
    }

    void TSchedulerQueryState::RegisterProcess() {
        ++ProcessesCount;
    }

    void TSchedulerQueryState::UnregisterProcess() {
        Y_ENSURE(ProcessesCount, "query has no registered process");
        --ProcessesCount;
    }

    bool TSchedulerQueryState::SetWorkFactory(std::unique_ptr<NYql::NDq::IDqSchedulableWorkFactory>&& factory) {
        Y_ENSURE(factory, "schedulable work factory is null");
        if (WorkFactory) {
            return false;
        }
        WorkFactory = std::move(factory);
        return true;
    }

    bool TSchedulerQueryState::IsReady() const {
        return WorkFactory != nullptr;
    }

    ui64 TSchedulerQueryState::GetProcessesCount() const {
        return ProcessesCount;
    }

    const std::optional<TMonotonic>& TSchedulerQueryState::GetWakeUpDeadline() const {
        return WakeUpDeadline;
    }

    void TSchedulerQueryState::UpdateWorkCapacity(const ui64 workersCount) {
        if (WorkFactory) {
            Works.ReconcileCapacity(workersCount, *WorkFactory);
        }
    }

    TTryStartResult TSchedulerQueryState::TryStart(const TMonotonic now) {
        Y_ENSURE(IsReady(), "query is not ready for scheduling");
        auto result = Works.TryStart(now);
        if (std::holds_alternative<TSchedulerLease>(result)) {
            WakeUpDeadline.reset();
        } else {
            const auto deadline = std::get<TMonotonic>(result);
            if (!WakeUpDeadline || deadline < *WakeUpDeadline) {
                WakeUpDeadline = deadline;
            }
        }
        return result;
    }

    void TSchedulerQueryState::PrepareForRemoval() {
        Works.PrepareForRemoval();
        WorkFactory.reset();
        WakeUpDeadline.reset();
    }

    TQueryRegistry::TQueryRegistry() {
        auto [it, inserted] = Queries.try_emplace(TSchedulerQueryIdentity{});
        Y_ENSURE(inserted);
        it->second.SetWorkFactory(std::make_unique<TAlwaysReadySchedulableWorkFactory>());
    }

    bool TQueryRegistry::RegisterProcess(const TSchedulerQueryIdentity& identity) {
        auto [it, inserted] = Queries.try_emplace(identity);
        it->second.RegisterProcess();
        return inserted;
    }

    bool TQueryRegistry::UnregisterProcess(const TSchedulerQueryIdentity& identity) {
        auto& state = GetStateVerified(identity);
        state.UnregisterProcess();
        if (state.GetProcessesCount()) {
            return false;
        }
        if (identity.IsDefault()) {
            return false;
        }
        state.PrepareForRemoval();
        Y_ENSURE(Queries.erase(identity) == 1, "cannot erase an unregistered query");
        return true;
    }

    bool TQueryRegistry::SetQuery(
        const TSchedulerQueryIdentity& identity, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query) {
        const auto it = Queries.find(identity);
        if (it == Queries.end() || it->second.IsReady() || !query) {
            return false;
        }
        const auto& fullPoolId = query->GetFullPoolId();
        Y_ENSURE(fullPoolId.DatabaseId == identity.DatabaseId, "scheduler query database does not match requested identity");
        Y_ENSURE(fullPoolId.PoolId == identity.PoolId, "scheduler query pool does not match requested identity");
        Y_ENSURE(std::get<NKqp::NScheduler::NHdrf::TQueryId>(query->GetId()) == identity.QueryId,
                 "scheduler query id does not match requested identity");
        return it->second.SetWorkFactory(
            std::make_unique<NKqp::NScheduler::TSchedulableWorkFactory>(std::move(query), true));
    }

    void TQueryRegistry::UpdateWorkCapacity(const TSchedulerQueryIdentity& identity, const ui64 workersCount) {
        GetStateVerified(identity).UpdateWorkCapacity(workersCount);
    }

    // Average algorithm without ui64 overflow
    TMonotonic TQueryRegistry::GetAverageWakeUpDeadline(const TMonotonic now) const {
        auto states = Queries | std::views::values;
        const ui64 count = std::ranges::count_if(states, [](const auto& state) { return state.GetWakeUpDeadline().has_value(); });
        if (!count) {
            return now;
        }

        ui64 average = 0;
        ui64 remainder = 0;
        for (const auto& state : states) {
            if (!state.GetWakeUpDeadline()) {
                continue;
            }
            const ui64 value = state.GetWakeUpDeadline()->GetValue();
            average += value / count;
            const ui64 valueRemainder = value % count;
            if (remainder >= count - valueRemainder) {
                ++average;
                remainder -= count - valueRemainder;
            } else {
                remainder += valueRemainder;
            }
        }
        return TMonotonic::FromValue(average);
    }

    std::optional<TMonotonic> TQueryRegistry::GetMinWakeUpDeadline() const {
        std::optional<TMonotonic> result;
        for (const auto& [identity, state] : Queries) {
            Y_UNUSED(identity);
            if (const auto& deadline = state.GetWakeUpDeadline(); deadline && (!result || *deadline < *result)) {
                result = deadline;
            }
        }
        return result;
    }

    TSchedulerQueryState& TQueryRegistry::GetStateVerified(const TSchedulerQueryIdentity& identity) {
        auto it = Queries.find(identity);
        Y_ENSURE(it != Queries.end(), "scheduler query is not registered");
        return it->second;
    }

    const TSchedulerQueryState& TQueryRegistry::GetStateVerified(const TSchedulerQueryIdentity& identity) const {
        auto it = Queries.find(identity);
        Y_ENSURE(it != Queries.end(), "scheduler query is not registered");
        return it->second;
    }

} // namespace NKikimr::NConveyorComposite
