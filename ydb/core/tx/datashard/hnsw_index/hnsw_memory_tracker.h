#pragma once

#include <ydb/core/base/memory_controller_iface.h>

#include <util/generic/hash.h>

#include <memory>
#include <mutex>

namespace NKikimr::NDataShard {

    // Node-wide reservations for shared graphs, scan buffers and private journals.
    class THnswCacheMemoryTracker {
    public:
        explicit THnswCacheMemoryTracker(bool growthHeadroom = false);

        static ui64 AllocateOwner() noexcept;
        bool HasPendingPressure() const;
        void SetReclaimable(ui64 bytes);
        ui64 RetireIfOverLimit(ui64 owner);
        void Retire(ui64 owner);
        void Request(ui64 bytes, ui64 owner);
        ui64 GetDemand(ui64 owner) const;
        void SetConsumer(TIntrusivePtr<NMemory::IMemoryConsumer> consumer);
        void SetLimit(ui64 limit) noexcept;
        bool HasLimit() const noexcept;
        ui64 GetLimit() const noexcept;
        ui64 GetUsed() const noexcept;
        ui64 GetUsed(ui64 owner) const noexcept;
        THashMap<ui64, ui64> GetUsageByOwner() const;
        bool TryAcquire(ui64 bytes, ui64 owner = 0);
        void Release(ui64 bytes, ui64 owner = 0) noexcept;
        void ResetDemand(ui64 owner = 0);

    private:
        static ui64 SaturatingAdd(ui64 a, ui64 b);
        void Report();
        ui64 GetRequiredBytes() const;

        struct TAccount {
            ui64 Used = 0;
            ui64 Demand = 0;
            bool Retiring = false;
        };
        THashMap<ui64, TAccount> Accounts;
        mutable std::mutex Mutex;
        const bool GrowthHeadroom;
        ui64 Limit = 0;
        bool LimitKnown = false;
        ui64 Used = 0;
        ui64 Reclaimable = 0;
        ui64 RetiringBytes = 0;
        TIntrusivePtr<NMemory::IMemoryConsumer> Consumer;
    };

    std::shared_ptr<void> TryReserveHnswMemory(
        const std::shared_ptr<THnswCacheMemoryTracker>& tracker, ui64 bytes, ui64 owner = 0);

} // namespace NKikimr::NDataShard
