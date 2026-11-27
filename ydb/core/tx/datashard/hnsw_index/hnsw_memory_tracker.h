#pragma once

#include <ydb/core/base/memory_controller_iface.h>

#include <util/generic/hash.h>

#include <mutex>

namespace NKikimr::NDataShard {

    // Per-tablet reservations for graphs, scan buffers and MVCC journals. The
    // existing memory controller supplies the grant; graphs are not shared here.
    class THnswCacheMemoryTracker {
    public:
        explicit THnswCacheMemoryTracker(bool growthHeadroom = false);

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
        void ResetAllDemand();

    private:
        static ui64 SaturatingAdd(ui64 a, ui64 b);
        void Report();

        struct TAccount {
            ui64 Used = 0;
            ui64 Demand = 0;
        };
        THashMap<ui64, TAccount> Accounts;
        mutable std::mutex Mutex;
        const bool GrowthHeadroom;
        ui64 Limit = 0;
        bool LimitKnown = false;
        ui64 Used = 0;
        TIntrusivePtr<NMemory::IMemoryConsumer> Consumer;
    };

} // namespace NKikimr::NDataShard
