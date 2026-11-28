#include "hnsw_memory_tracker.h"

#include <util/generic/algorithm.h>

#include <atomic>
#include <utility>

namespace NKikimr::NDataShard {

    THnswCacheMemoryTracker::THnswCacheMemoryTracker(bool growthHeadroom)
        : GrowthHeadroom(growthHeadroom)
    {}

    void THnswCacheMemoryTracker::Request(ui64 bytes, ui64 owner) {
        std::lock_guard guard(Mutex);
        auto& account = Accounts[owner];
        ExcessDemand -= account.GetExcessDemand();
        account.Demand = Max(account.Demand, bytes);
        ExcessDemand += account.GetExcessDemand();
        Report();
    }

    ui64 THnswCacheMemoryTracker::GetDemand(ui64 owner) const {
        std::lock_guard guard(Mutex);
        const auto* account = Accounts.FindPtr(owner);
        return account ? Max(account->Used, account->Demand) : 0;
    }

    void THnswCacheMemoryTracker::SetConsumer(TIntrusivePtr<NMemory::IMemoryConsumer> consumer) {
        std::lock_guard guard(Mutex);
        Consumer = std::move(consumer);
        Report();
    }

    void THnswCacheMemoryTracker::SetLimit(ui64 limit) noexcept {
        std::lock_guard guard(Mutex);
        Limit = limit;
        LimitKnown = true;
    }

    bool THnswCacheMemoryTracker::HasLimit() const noexcept {
        std::lock_guard guard(Mutex);
        return LimitKnown;
    }

    ui64 THnswCacheMemoryTracker::GetLimit() const noexcept {
        std::lock_guard guard(Mutex);
        return Limit;
    }

    ui64 THnswCacheMemoryTracker::GetUsed() const noexcept {
        std::lock_guard guard(Mutex);
        return Used;
    }

    ui64 THnswCacheMemoryTracker::GetUsed(ui64 owner) const noexcept {
        std::lock_guard guard(Mutex);
        const auto* account = Accounts.FindPtr(owner);
        return account ? account->Used : 0;
    }

    THashMap<ui64, ui64> THnswCacheMemoryTracker::GetUsageByOwner() const {
        std::lock_guard guard(Mutex);
        THashMap<ui64, ui64> usage;
        for (const auto& [owner, account] : Accounts) {
            usage.emplace(owner, account.Used);
        }
        return usage;
    }

    bool THnswCacheMemoryTracker::TryAcquire(ui64 bytes, ui64 owner, bool requestOnFailure) {
        std::lock_guard guard(Mutex);
        if (const auto* account = Accounts.FindPtr(owner); account && account->Retiring) {
            return false;
        }
        if (!Limit || Used > Limit || bytes > Limit - Used) {
            if (requestOnFailure) {
                auto& account = Accounts[owner];
                ExcessDemand -= account.GetExcessDemand();
                account.Demand = Max(account.Demand, SaturatingAdd(account.Used, bytes));
                ExcessDemand += account.GetExcessDemand();
                Report();
            }
            return false;
        }
        if (bytes) {
            auto& account = Accounts[owner];
            ExcessDemand -= account.GetExcessDemand();
            account.Used += bytes;
            ExcessDemand += account.GetExcessDemand();
            Used += bytes;
            Report();
        }
        return true;
    }

    void THnswCacheMemoryTracker::Release(ui64 bytes, ui64 owner) noexcept {
        std::lock_guard guard(Mutex);
        if (auto* account = Accounts.FindPtr(owner)) {
            const ui64 released = Min(bytes, account->Used);
            ExcessDemand -= account->GetExcessDemand();
            account->Used -= released;
            ExcessDemand += account->GetExcessDemand();
            Used -= released;
            if (account->Retiring) {
                RetiringBytes -= released;
            }
            if (!account->Used && !account->Demand) {
                Accounts.erase(owner);
            }
        }
        Report();
    }

    void THnswCacheMemoryTracker::ResetDemand(ui64 owner) {
        std::lock_guard guard(Mutex);
        if (auto* account = Accounts.FindPtr(owner)) {
            ExcessDemand -= account->GetExcessDemand();
            account->Demand = 0;
            if (!account->Used) {
                Accounts.erase(owner);
            }
        }
        Report();
    }

    ui64 THnswCacheMemoryTracker::AllocateOwner() noexcept {
        static std::atomic<ui64> next = 0;
        return next.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    bool THnswCacheMemoryTracker::HasPendingPressure() const {
        std::lock_guard guard(Mutex);
        return GetRequiredBytes() > Limit;
    }

    void THnswCacheMemoryTracker::SetReclaimable(ui64 bytes) {
        std::lock_guard guard(Mutex);
        Reclaimable = bytes;
        Report();
    }

    ui64 THnswCacheMemoryTracker::RetireIfOverLimit(ui64 owner) {
        std::lock_guard guard(Mutex);
        if (Used - RetiringBytes > Limit) {
            if (auto* account = Accounts.FindPtr(owner); account && !account->Retiring && account->Used) {
                account->Retiring = true;
                RetiringBytes += account->Used;
                return account->Used;
            }
        }
        return 0;
    }

    void THnswCacheMemoryTracker::Retire(ui64 owner) {
        std::lock_guard guard(Mutex);
        if (auto* account = Accounts.FindPtr(owner); account && !account->Retiring) {
            account->Retiring = true;
            RetiringBytes += account->Used;
        }
    }

    ui64 THnswCacheMemoryTracker::SaturatingAdd(ui64 a, ui64 b) {
        return b > Max<ui64>() - a ? Max<ui64>() : a + b;
    }

    bool THnswCacheMemoryTracker::IsRetiring(ui64 owner) const {
        std::lock_guard guard(Mutex);
        const auto* account = Accounts.FindPtr(owner);
        return account && account->Retiring;
    }

    ui64 THnswCacheMemoryTracker::GetRequiredBytes() const {
        const auto demand = Used + ExcessDemand;
        return demand > Max<ui64>() ? Max<ui64>() : static_cast<ui64>(demand);
    }

    void THnswCacheMemoryTracker::Report() {
        if (Consumer) {
            ui64 demand = GetRequiredBytes();
            if (GrowthHeadroom && Used) {
                demand = Max(demand, SaturatingAdd(Used, Max<ui64>(64 * 1024, Used / 8)));
            }
            Consumer->SetReport({.Used = Used, .Demand = demand, .Reclaimable = Min(Used, Reclaimable)});
        }
    }

    std::shared_ptr<void> TryReserveHnswMemory(
        const std::shared_ptr<THnswCacheMemoryTracker>& tracker, ui64 bytes, ui64 owner, bool requestOnFailure)
    {
        struct TReservation {
            std::shared_ptr<THnswCacheMemoryTracker> Tracker;
            ui64 Owner;
            ui64 Bytes = 0;
            ~TReservation() {
                if (Bytes) {
                    Tracker->Release(Bytes, Owner);
                }
            }
        };
        auto reservation = std::make_shared<TReservation>(TReservation{tracker, owner});
        if (!tracker->TryAcquire(bytes, owner, requestOnFailure)) {
            return {};
        }
        reservation->Bytes = bytes;
        return reservation;
    }

} // namespace NKikimr::NDataShard
