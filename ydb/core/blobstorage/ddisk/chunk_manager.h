#pragma once

#include <util/generic/vector.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cstddef>
#include <deque>
#include <optional>
#include <utility>
#include <variant>

namespace NKikimr::NDDisk {

// Matches logical allocation requests to the shared physical chunk reserve.
// The actor owns PDisk requests, formatting, integrity placement and logging;
// only chunks ready for allocation are returned to this manager.
class TChunkManager {
public:
    struct TChunkForData {
        ui64 TabletId;
        ui64 VChunkIndex;
        ui64 Token = 0;
    };

    struct TChunkForPersistentBuffer {};
    struct TChunkForIntegrity { ui64 Token = 0; };

    using TAllocation = std::variant<TChunkForData, TChunkForPersistentBuffer, TChunkForIntegrity>;
    using TAllocationResult = std::pair<TAllocation, ui32>;

    void Enqueue(TAllocation allocation) {
        PendingAllocations.push_back(std::move(allocation));
    }

    std::optional<TAllocationResult> TakeAllocation() {
        if (PendingAllocations.empty() || ReservedChunks.empty()) {
            return std::nullopt;
        }
        TAllocationResult result{std::move(PendingAllocations.front()), ReservedChunks.front()};
        PendingAllocations.pop_front();
        ReservedChunks.pop_front();
        return result;
    }

    void ReturnChunk(ui32 chunkIdx) {
        ReservedChunks.push_back(chunkIdx);
    }

    TVector<ui32> ExtractReservations() {
        TVector<ui32> result(ReservedChunks.begin(), ReservedChunks.end());
        ReservedChunks.clear();
        return result;
    }

    bool HasPendingAllocations() const {
        return !PendingAllocations.empty();
    }

    size_t GetPendingAllocationCount() const {
        return PendingAllocations.size();
    }

    size_t GetReservedChunkCount() const {
        return ReservedChunks.size();
    }

    size_t CountPendingPersistentBufferAllocations() const {
        return std::count_if(PendingAllocations.begin(), PendingAllocations.end(), [](const TAllocation& allocation) {
            return std::holds_alternative<TChunkForPersistentBuffer>(allocation);
        });
    }

    void RetainPersistentBufferAllocations() {
        std::erase_if(PendingAllocations, [](const TAllocation& allocation) {
            return !std::holds_alternative<TChunkForPersistentBuffer>(allocation);
        });
    }

    size_t GetRefillCount(size_t minReserved, size_t formattingCount = 0) const {
        const size_t available = ReservedChunks.size() + formattingCount;
        return !ReservationInFlight && available < minReserved ? minReserved - available : 0;
    }

    bool IsReservationInFlight() const {
        return ReservationInFlight;
    }

    void BeginReservation() {
        Y_ABORT_UNLESS(!ReservationInFlight);
        ReservationInFlight = true;
    }

    void FinishReservation() {
        Y_ABORT_UNLESS(ReservationInFlight);
        ReservationInFlight = false;
    }

private:
    std::deque<TAllocation> PendingAllocations;
    std::deque<ui32> ReservedChunks;
    bool ReservationInFlight = false;
};

} // namespace NKikimr::NDDisk
