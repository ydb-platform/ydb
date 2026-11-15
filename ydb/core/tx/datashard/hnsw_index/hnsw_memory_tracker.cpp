#include "hnsw_memory_tracker.h"

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NDataShard {

    THnswCacheMemoryTracker::THnswCacheMemoryTracker(bool growthHeadroom)
        : GrowthHeadroom(growthHeadroom)
    {}

    void THnswCacheMemoryTracker::Request(ui64 bytes, ui64 owner) {
        std::lock_guard guard(Mutex);
        auto& account = Accounts[owner];
        account.Demand = Max(account.Demand, bytes);
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

    bool THnswCacheMemoryTracker::TryAcquire(ui64 bytes, ui64 owner) {
        std::lock_guard guard(Mutex);
        auto& account = Accounts[owner];
        if (!Limit || Used > Limit || bytes > Limit - Used) {
            account.Demand = Max(account.Demand, SaturatingAdd(account.Used, bytes));
            Report();
            return false;
        }
        account.Used += bytes;
        Used += bytes;
        Report();
        return true;
    }

    void THnswCacheMemoryTracker::Release(ui64 bytes, ui64 owner) noexcept {
        std::lock_guard guard(Mutex);
        if (auto* account = Accounts.FindPtr(owner)) {
            const ui64 released = Min(bytes, account->Used);
            account->Used -= released;
            Used -= released;
            if (!account->Used && !account->Demand) {
                Accounts.erase(owner);
            }
        }
        Report();
    }

    void THnswCacheMemoryTracker::ResetDemand(ui64 owner) {
        std::lock_guard guard(Mutex);
        if (auto* account = Accounts.FindPtr(owner)) {
            account->Demand = 0;
            if (!account->Used) {
                Accounts.erase(owner);
            }
        }
        Report();
    }

    void THnswCacheMemoryTracker::ResetAllDemand() {
        std::lock_guard guard(Mutex);
        for (auto it = Accounts.begin(); it != Accounts.end();) {
            it->second.Demand = 0;
            if (!it->second.Used) {
                Accounts.erase(it++);
            } else {
                ++it;
            }
        }
        Report();
    }

    ui64 THnswCacheMemoryTracker::SaturatingAdd(ui64 a, ui64 b) {
        return b > Max<ui64>() - a ? Max<ui64>() : a + b;
    }

    void THnswCacheMemoryTracker::Report() {
        if (Consumer) {
            ui64 demand = Used;
            for (const auto& [_, account] : Accounts) {
                demand = SaturatingAdd(demand, account.Demand > account.Used ? account.Demand - account.Used : 0);
            }
            if (GrowthHeadroom && Used) {
                demand = Max(demand, SaturatingAdd(Used, Max<ui64>(64 * 1024, Used / 8)));
            }
            // Serialize reports with reservations released by worker threads.
            Consumer->SetReport({.Used = Used, .Demand = demand});
        }
    }

} // namespace NKikimr::NDataShard
