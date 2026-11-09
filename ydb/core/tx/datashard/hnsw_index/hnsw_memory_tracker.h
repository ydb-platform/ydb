#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

#include <mutex>
#include <utility>

namespace NKikimr::NDataShard {

    // Per-tablet reservations for graphs, scan buffers and MVCC journals. The
    // existing memory controller supplies the grant; graphs are not shared here.
    class THnswCacheMemoryTracker {
    public:
        void SetConsumer(TIntrusivePtr<NMemory::IMemoryConsumer> consumer) {
            std::lock_guard guard(Mutex);
            Consumer = std::move(consumer);
            Report();
        }

        void SetLimit(ui64 limit) noexcept {
            std::lock_guard guard(Mutex);
            Limit = limit;
            LimitKnown = true;
        }

        bool HasLimit() const noexcept {
            std::lock_guard guard(Mutex);
            return LimitKnown;
        }

        ui64 GetLimit() const noexcept {
            std::lock_guard guard(Mutex);
            return Limit;
        }

        ui64 GetUsed() const noexcept {
            std::lock_guard guard(Mutex);
            return Used;
        }

        ui64 GetUsed(ui64 owner) const noexcept {
            std::lock_guard guard(Mutex);
            const auto* account = Accounts.FindPtr(owner);
            return account ? account->Used : 0;
        }

        bool TryAcquire(ui64 bytes, ui64 owner = 0) {
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

        void Release(ui64 bytes, ui64 owner = 0) noexcept {
            std::lock_guard guard(Mutex);
            if (auto* account = Accounts.FindPtr(owner)) {
                const ui64 released = Min(bytes, account->Used);
                account->Used -= released;
                Used -= released;
                if (!account->Used && !account->Demand) { Accounts.erase(owner); }
            }
            Report();
        }

        void ResetDemand(ui64 owner = 0) {
            std::lock_guard guard(Mutex);
            if (auto* account = Accounts.FindPtr(owner)) {
                account->Demand = 0;
                if (!account->Used) { Accounts.erase(owner); }
            }
            Report();
        }

        void ResetAllDemand() {
            std::lock_guard guard(Mutex);
            for (auto it = Accounts.begin(); it != Accounts.end();) {
                it->second.Demand = 0;
                if (!it->second.Used) { it = Accounts.erase(it); } else { ++it; }
            }
            Report();
        }

    private:
        static ui64 SaturatingAdd(ui64 a, ui64 b) {
            return b > Max<ui64>() - a ? Max<ui64>() : a + b;
        }

        void Report() {
            if (Consumer) {
                ui64 demand = Used;
                for (const auto& [_, account] : Accounts) {
                    demand = SaturatingAdd(demand, account.Demand > account.Used ? account.Demand - account.Used : 0);
                }
                // Serialize reports with reservations released by worker threads.
                Consumer->SetReport({.Used = Used, .Demand = demand});
            }
        }

        struct TAccount {
            ui64 Used = 0;
            ui64 Demand = 0;
        };
        THashMap<ui64, TAccount> Accounts;
        mutable std::mutex Mutex;
        ui64 Limit = 0;
        bool LimitKnown = false;
        ui64 Used = 0;
        TIntrusivePtr<NMemory::IMemoryConsumer> Consumer;
    };

} // namespace NKikimr::NDataShard
