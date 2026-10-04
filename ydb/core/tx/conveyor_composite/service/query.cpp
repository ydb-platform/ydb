#include "query.h"

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_work_factory.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <util/generic/yexception.h>

#include <algorithm>
#include <array>
#include <mutex>
#include <utility>

namespace NKikimr::NConveyorComposite {

    // Cells can outlive the registry. This block owns no cells and is shared with their leases.
    struct TSchedulableWorkControl {
        std::mutex Mutex;
        std::array<ui64, GetEnumItemsCount<ESchedulableWorkStatus>()> StatusCounts{};
        bool Closed = false;

        void SetStatus(TSchedulableWorkCell& cell, ESchedulableWorkStatus status);
    };

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

    TSchedulerLease::TSchedulerLease(std::shared_ptr<TSchedulableWorkCell> cell)
        : Cell(std::move(cell))
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
        auto cell = std::exchange(Cell, nullptr);
        if (!cell) {
            return;
        }
        std::lock_guard guard(cell->Control->Mutex);
        if (cell->Control->Closed) {
            return; // Forced stop already ran; the HDRF tree may no longer exist.
        }
        Y_ENSURE(cell->Status == ESchedulableWorkStatus::STARTED, "scheduler lease does not own a started work");
        cell->Work->StopExecution();
        cell->Control->SetStatus(*cell, ESchedulableWorkStatus::IDLE);
    }

    TSchedulableWorkState::TSchedulableWorkState()
        : Control(std::make_shared<TSchedulableWorkControl>())
    {
    }
    TSchedulableWorkState::TSchedulableWorkState(TSchedulableWorkState&& other) noexcept
        : Cells(std::exchange(other.Cells, {}))
        , Control(std::move(other.Control))
    {
    }

    TSchedulableWorkState& TSchedulableWorkState::operator=(TSchedulableWorkState&& other) noexcept {
        if (this != &other) {
            PrepareForRemoval();
            Cells = std::exchange(other.Cells, {});
            Control = std::move(other.Control);
        }
        return *this;
    }

    TSchedulableWorkState::~TSchedulableWorkState() {
        Close(true);
    }

    void TSchedulableWorkControl::SetStatus(TSchedulableWorkCell& cell, const ESchedulableWorkStatus status) {
        AFL_VERIFY(cell.Control.get() == this);
        AFL_VERIFY(static_cast<size_t>(cell.Status) < StatusCounts.size());
        AFL_VERIFY(static_cast<size_t>(status) < StatusCounts.size());
        if (cell.Status == status) {
            return;
        }
        auto& oldCount = StatusCounts[static_cast<size_t>(cell.Status)];
        AFL_VERIFY(oldCount);
        --oldCount;
        ++StatusCounts[static_cast<size_t>(status)];
        cell.Status = status;
    }

    void TSchedulableWorkState::StopThrottled(TSchedulableWorkCell& cell) {
        Y_ENSURE(cell.Status == ESchedulableWorkStatus::THROTTLED, "only a throttled work can be stopped by this path");
        cell.Work->StopExecution();
        Control->SetStatus(cell, ESchedulableWorkStatus::IDLE);
    }

    TTryStartResult TSchedulableWorkState::TryStart(const TMonotonic now) {
        Y_ENSURE(Control, "schedulable work state was moved");
        std::lock_guard guard(Control->Mutex);
        Y_ENSURE(!Control->Closed, "schedulable work state is closed");
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
                    Control->SetStatus(*cell, ESchedulableWorkStatus::THROTTLED);
                    return now + *delay;
                }
                Control->SetStatus(*cell, ESchedulableWorkStatus::STARTED);
                return TSchedulerLease(cell);
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
            Cells.emplace_back(std::make_shared<TSchedulableWorkCell>(std::move(work), Control));
            ++Control->StatusCounts[static_cast<size_t>(ESchedulableWorkStatus::IDLE)];
        }
    }

    void TSchedulableWorkState::DecreaseCapacity(const ui64 workersCount) {
        Y_ENSURE(workersCount < Cells.size(), "schedulable work capacity decrease has no removed workers");
        Y_ENSURE(Control->StatusCounts[static_cast<size_t>(ESchedulableWorkStatus::STARTED)] <= workersCount,
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
            --Control->StatusCounts[static_cast<size_t>(cell.Status)];
            it = Cells.erase(it);
            --toRemove;
        }
        Y_ENSURE(toRemove == 0, "not enough non-started schedulable works to shrink capacity");
    }

    void TSchedulableWorkState::ForcedUpdateWorkCapacity(
        const ui64 workersCount, NYql::NDq::IDqSchedulableWorkFactory& factory) {
        Y_ENSURE(Control, "schedulable work state was moved");
        std::lock_guard guard(Control->Mutex);
        Y_ENSURE(!Control->Closed, "schedulable work state is closed");
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
        Close(false);
    }

    void TSchedulableWorkState::Close(const bool force) {
        if (!Control) {
            return;
        }
        std::lock_guard guard(Control->Mutex);
        Y_ENSURE(force || Control->StatusCounts[static_cast<size_t>(ESchedulableWorkStatus::STARTED)] == 0,
                 "cannot remove query with started schedulable works");
        Control->Closed = true;
        for (auto& cell : Cells) {
            if (cell->Status != ESchedulableWorkStatus::IDLE) {
                cell->Work->StopExecution();
                Control->SetStatus(*cell, ESchedulableWorkStatus::IDLE);
            }
        }
        Cells.clear();
        Control->StatusCounts.fill(0);
    }

    ui64 TSchedulableWorkState::GetCount(const ESchedulableWorkStatus status) const {
        if (!Control) {
            return 0;
        }
        std::lock_guard guard(Control->Mutex);
        Y_ENSURE(static_cast<size_t>(status) < Control->StatusCounts.size());
        return Control->StatusCounts[static_cast<size_t>(status)];
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

    bool TSchedulerQueryState::HasWorksCapacity() const {
        return Works.GetCount(ESchedulableWorkStatus::STARTED) < CpuCount;
    }

    bool TSchedulerQueryState::IsWaitRelease() const {
        return ProcessesCount == 0;
    }

    bool TSchedulerQueryState::IsReadyToRelease() const {
        return IsReady() && Works.GetCount(ESchedulableWorkStatus::STARTED) == 0;
    }

    const std::optional<TMonotonic>& TSchedulerQueryState::GetWakeUpDeadline() const {
        return WakeUpDeadline;
    }

    void TSchedulerQueryState::PrepareWorkCapacity(const ui64 cpuCount) {
        CpuCount = cpuCount;
    }

    void TSchedulerQueryState::ApplyWorkCapacity() {
        if (WorkFactory && Works.GetCount(ESchedulableWorkStatus::STARTED) <= CpuCount) {
            Works.ForcedUpdateWorkCapacity(CpuCount, *WorkFactory);
        }
    }

    TTryStartResult TSchedulerQueryState::TryStart(const TMonotonic now) {
        Y_ENSURE(IsReady(), "query is not ready for scheduling");
        auto result = Works.TryStart(now);
        if (std::holds_alternative<TSchedulerLease>(result)) {
            WakeUpDeadline.reset();
        } else {
            WakeUpDeadline = std::get<TMonotonic>(result);
        }
        return result;
    }

    void TSchedulerQueryState::PrepareForRemoval() {
        Works.PrepareForRemoval();
        WorkFactory.reset();
        WakeUpDeadline.reset();
    }

    TQueryRegistry::TQueryRegistry() {
        auto [it, inserted] = Queries.try_emplace(kServiceQueryIdentity);
        Y_ENSURE(inserted);
        it->second.SetWorkFactory(std::make_unique<TAlwaysReadySchedulableWorkFactory>());
    }

    bool TQueryRegistry::RegisterProcess(const TSchedulerQueryIdentity& identity) {
        auto [it, inserted] = Queries.try_emplace(identity);
        it->second.RegisterProcess();
        return inserted;
    }

    void TQueryRegistry::UnregisterProcess(const TSchedulerQueryIdentity& identity) {
        GetStateVerified(identity).UnregisterProcess();
    }

    bool TQueryRegistry::TryReleaseQuery(const TSchedulerQueryIdentity& identity) {
        auto it = Queries.find(identity);
        if (it == Queries.end()) {
            return false;
        }
        auto& state = it->second;
        if (!state.IsWaitRelease() || !state.IsReadyToRelease()) {
            return false;
        }
        state.PrepareForRemoval();
        Y_ENSURE(Queries.erase(identity) == 1, "cannot erase an unregistered query");
        return true;
    }

    ui64 TQueryRegistry::MovePendingQueryToService(const TSchedulerQueryIdentity& identity) {
        auto& state = GetStateVerified(identity);
        Y_ENSURE(!state.IsReady(), "only a pending query can migrate to service");
        state.PrepareForRemoval(); // Also verifies that no schedulable work is started.
        const ui64 count = state.ProcessesCount;
        GetStateVerified(kServiceQueryIdentity).ProcessesCount += count;
        Y_ENSURE(Queries.erase(identity) == 1, "cannot erase an unregistered query");
        return count;
    }

    bool TQueryRegistry::SetQuery(
        const TSchedulerQueryIdentity& identity, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query) {
        Y_ENSURE(query, "scheduler query pointer is null");
        const auto it = Queries.find(identity);
        if (it == Queries.end() || it->second.IsReady()) {
            return false;
        }
        Y_ENSURE(std::get<NKqp::NScheduler::NHdrf::TQueryId>(query->GetId()) == identity.QueryId,
                 "scheduler query id does not match requested identity");
        auto factory = std::make_unique<NKqp::NScheduler::TSchedulableWorkFactory>(std::move(query), true);
        return it->second.SetWorkFactory(std::move(factory));
    }

    void TQueryRegistry::PrepareWorkCapacity(const TSchedulerQueryIdentity& identity, const ui64 cpuCount) {
        GetStateVerified(identity).PrepareWorkCapacity(cpuCount);
    }

    void TQueryRegistry::ApplyWorkCapacity(const TSchedulerQueryIdentity& identity) {
        GetStateVerified(identity).ApplyWorkCapacity();
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
