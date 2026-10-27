#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <util/generic/algorithm.h>

#include <memory>
#include <mutex>
#include <utility>

namespace NKikimr::NDataShard {

    // Node-wide reservations for immutable graphs, scan buffers and private
    // MVCC journals, managed by the HNSW cache service.
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

        bool HasPendingPressure() const noexcept {
            std::lock_guard guard(Mutex);
            return Max(Used, PendingDemand) > Limit;
        }

        void SetReclaimable(ui64 bytes) {
            std::lock_guard guard(Mutex);
            Reclaimable = bytes;
            Report();
        }

        bool TryAcquire(ui64 bytes) noexcept {
            std::lock_guard guard(Mutex);
            if (bytes > Max<ui64>() - Used) {
                return false;
            }
            // A graph must fit as a whole. Report failed reservations as demand so
            // the controller can grow our share beyond the bootstrap allowance.
            if (!Limit || Used > Limit || bytes > Limit - Used) {
                PendingDemand = Max(PendingDemand, Used + bytes);
                Report();
                return false;
            }
            Used += bytes;
            Report();
            return true;
        }

        void Release(ui64 bytes) noexcept {
            std::lock_guard guard(Mutex);
            const ui64 released = Min(bytes, Used);
            Used -= released;
            Report();
        }

        void ResetDemand() {
            std::lock_guard guard(Mutex);
            PendingDemand = 0;
            Report();
        }

    private:
        void Report() {
            if (Consumer) {
                // Serialize reports with reservations: a release on a build/reader
                // thread must not overwrite a newer report with stale usage.
                Consumer->SetReport({.Used = Used, .Demand = Max(Used, PendingDemand),
                    .Reclaimable = Min(Used, Reclaimable)});
            }
        }

        mutable std::mutex Mutex;
        ui64 Limit = 0;
        bool LimitKnown = false;
        ui64 Used = 0;
        ui64 Reclaimable = 0;
        // Preserve the whole request when an unsuccessful build releases its
        // partial reservations. Cleared on completion, invalidation, or eviction.
        ui64 PendingDemand = 0;
        TIntrusivePtr<NMemory::IMemoryConsumer> Consumer;
    };

    inline std::shared_ptr<void> TryReserveHnswMemory(
            const std::shared_ptr<THnswCacheMemoryTracker>& tracker, ui64 bytes) {
        struct TReservation {
            std::shared_ptr<THnswCacheMemoryTracker> Tracker;
            ui64 Bytes = 0;
            ~TReservation() { Tracker->Release(Bytes); }
        };
        auto reservation = std::make_shared<TReservation>();
        reservation->Tracker = tracker;
        if (!tracker->TryAcquire(bytes)) {
            return {};
        }
        reservation->Bytes = bytes;
        return reservation;
    }

} // namespace NKikimr::NDataShard
