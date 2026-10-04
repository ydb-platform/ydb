#pragma once

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <contrib/restricted/abseil-cpp/absl/hash/hash.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <deque>
#include <utility>
#include <vector>

namespace NKikimr::NNbsDbgLike {

inline ui64 WireVChunkIndex(ui32 dbgIndex, ui32 numVChunks, ui32 localVChunk) {
    return ui64(dbgIndex) * numVChunks + localVChunk;
}

// A run uses aligned, equal-sized slots. Different DBG workers use disjoint
// wire VChunk IDs, so this table owns every conflicting range in one worker.
// Versions are linked in acceptance order. A second list links only the
// versions that still need Sync, so flushing a failed version is O(1).
class TSlotIoCoordinator {
public:
    struct TSlot {
        ui32 VChunk = 0;
        ui32 Index = 0;

        friend bool operator==(const TSlot& lhs, const TSlot& rhs) {
            return lhs.VChunk == rhs.VChunk && lhs.Index == rhs.Index;
        }
    };

    struct TSlotHash {
        size_t operator()(TSlot slot) const noexcept {
            return absl::Hash<ui64>{}((ui64(slot.VChunk) << 32) | slot.Index);
        }
    };

    void Reserve(size_t slots) {
        Slots.reserve(slots);
        Versions.reserve(slots);
    }

    void Clear() {
        Slots.clear();
        Versions.clear();
    }

    size_t SlotCapacity() const {
        return Slots.capacity();
    }

    void Accept(TSlot slot, ui64 lsn) {
        auto& state = Slots[slot];
        const bool inserted = Versions.emplace(lsn, TVersion{}).second;
        Y_ABORT_UNLESS(inserted);
        auto& version = Versions.at(lsn);
        version.Newer = 0;
        version.Older = state.Newest;
        if (state.Newest) {
            Versions.at(state.Newest).Newer = lsn;
        } else {
            state.Oldest = lsn;
        }
        state.Newest = lsn;

        version.OnUnflushed = true;
        version.UnflushedNewer = 0;
        version.UnflushedOlder = state.NewestUnflushed;
        if (state.NewestUnflushed) {
            Versions.at(state.NewestUnflushed).UnflushedNewer = lsn;
        } else {
            state.OldestUnflushed = lsn;
        }
        state.NewestUnflushed = lsn;
        ++state.VersionCount;
    }

    void MakeVisible(TSlot slot, ui64 lsn) {
        auto& state = Slots.at(slot);
        Y_ABORT_UNLESS(Versions.contains(lsn));
        if (lsn > state.VisibleLsn) {
            state.VisibleLsn = lsn;
        }
    }

    ui64 VisibleLsn(TSlot slot) const {
        const auto it = Slots.find(slot);
        return it == Slots.end() ? 0 : it->second.VisibleLsn;
    }

    ui64 OldestUnflushed(TSlot slot) const {
        const auto it = Slots.find(slot);
        return it == Slots.end() ? 0 : it->second.OldestUnflushed;
    }

    ui64 OldestVersion(TSlot slot) const {
        const auto it = Slots.find(slot);
        return it == Slots.end() ? 0 : it->second.Oldest;
    }

    bool CanFlush(TSlot slot, ui64 lsn) const {
        const auto it = Slots.find(slot);
        return it != Slots.end() && !it->second.DDiskReaders && it->second.OldestUnflushed == lsn;
    }

    void MarkFlushed(TSlot slot, ui64 lsn) {
        auto& state = Slots.at(slot);
        UnlinkUnflushed(state, Versions.at(lsn));
    }

    bool CanErase(TSlot slot, ui64 lsn) const {
        const auto it = Slots.find(slot);
        if (it == Slots.end() || it->second.Oldest != lsn) {
            return false;
        }
        const auto& version = Versions.at(lsn);
        return version.Flushed && !version.PBReaders;
    }

    void PinPBRead(TSlot /*slot*/, ui64 lsn) {
        ++Versions.at(lsn).PBReaders;
    }

    // True when this unpin releases the last PB reader of the version.
    bool UnpinPBRead(TSlot /*slot*/, ui64 lsn) {
        auto& readers = Versions.at(lsn).PBReaders;
        Y_ABORT_UNLESS(readers);
        --readers;
        return readers == 0;
    }

    void PinDDiskRead(TSlot slot) {
        ++Slots[slot].DDiskReaders;
    }

    // True when this unpin releases the last DDisk reader of the slot.
    bool UnpinDDiskRead(TSlot slot) {
        auto& state = Slots.at(slot);
        Y_ABORT_UNLESS(state.DDiskReaders);
        --state.DDiskReaders;
        const bool last = state.DDiskReaders == 0;
        if (last && !state.VersionCount) {
            Slots.erase(slot);
        }
        return last;
    }

    void Retire(TSlot slot, ui64 lsn) {
        auto& state = Slots.at(slot);
        Y_ABORT_UNLESS(CanErase(slot, lsn));
        auto& version = Versions.at(lsn);
        Y_ABORT_UNLESS(!version.OnUnflushed);
        state.Oldest = version.Newer;
        if (version.Newer) {
            Versions.at(version.Newer).Older = 0;
        } else {
            state.Newest = 0;
        }
        if (state.VisibleLsn == lsn) {
            state.VisibleLsn = 0;
        }
        Versions.erase(lsn);
        --state.VersionCount;
        if (!state.VersionCount && !state.DDiskReaders) {
            Slots.erase(slot);
        }
    }

private:
    struct TVersion {
        ui64 Older = 0;
        ui64 Newer = 0;
        ui64 UnflushedOlder = 0;
        ui64 UnflushedNewer = 0;
        bool Flushed = false;
        bool OnUnflushed = false;
        ui32 PBReaders = 0;
    };

    struct TSlotState {
        ui64 Oldest = 0;
        ui64 Newest = 0;
        ui64 OldestUnflushed = 0;
        ui64 NewestUnflushed = 0;
        ui64 VisibleLsn = 0;
        ui32 DDiskReaders = 0;
        ui32 VersionCount = 0;
    };

    void UnlinkUnflushed(TSlotState& state, TVersion& version) {
        Y_ABORT_UNLESS(version.OnUnflushed);
        if (version.UnflushedOlder) {
            Versions.at(version.UnflushedOlder).UnflushedNewer = version.UnflushedNewer;
        } else {
            state.OldestUnflushed = version.UnflushedNewer;
        }
        if (version.UnflushedNewer) {
            Versions.at(version.UnflushedNewer).UnflushedOlder = version.UnflushedOlder;
        } else {
            state.NewestUnflushed = version.UnflushedOlder;
        }
        version.OnUnflushed = false;
        version.UnflushedOlder = 0;
        version.UnflushedNewer = 0;
        version.Flushed = true;
    }

    absl::flat_hash_map<TSlot, TSlotState, TSlotHash> Slots;
    absl::flat_hash_map<ui64, TVersion> Versions;
};

// Cohorts are the ready records present when the gate opens. Admitted records
// stay admitted across overlap waits, read pins, partial batches, and retries.
// Later completions form the next cohort and do not inherit an LSN watermark.
class TMaintenanceScheduler {
public:
    struct TStats {
        ui64 FlushSlotsExamined = 0;
        ui64 EraseSlotsExamined = 0;
    };

    void Reserve(size_t records) {
        Records.reserve(records);
        UnadmittedFlush.reserve(records);
        UnadmittedErase.reserve(records);
    }

    void Clear() {
        Records.clear();
        UnadmittedFlush.clear();
        UnadmittedErase.clear();
        FlushQueue.clear();
        EraseQueue.clear();
        Examined = {};
    }

    const TStats& Stats() const {
        return Examined;
    }

    void NoteFlushReady(ui64 lsn, TSlotIoCoordinator::TSlot slot) {
        auto& record = Records[lsn];
        record.Slot = slot;
        if (record.FlushReady || record.FlushAdmitted) {
            return;
        }
        record.FlushReady = true;
        UnadmittedFlush.push_back(lsn);
    }

    void NoteEraseReady(ui64 lsn, TSlotIoCoordinator::TSlot slot) {
        auto& record = Records[lsn];
        record.Slot = slot;
        if (record.EraseReady || record.EraseAdmitted) {
            return;
        }
        record.EraseReady = true;
        UnadmittedErase.push_back(lsn);
    }

    void BypassEraseGate(TSlotIoCoordinator& slots, ui64 lsn, TSlotIoCoordinator::TSlot slot) {
        auto& record = Records[lsn];
        record.Slot = slot;
        record.EraseAdmitted = true;
        ConsiderErase(slots, slot);
    }

    ui32 UnadmittedFlushCount() const {
        return static_cast<ui32>(UnadmittedFlush.size());
    }

    ui32 UnadmittedEraseCount() const {
        return static_cast<ui32>(UnadmittedErase.size());
    }

    size_t UnadmittedFlushCapacity() const {
        return UnadmittedFlush.capacity();
    }

    size_t UnadmittedEraseCapacity() const {
        return UnadmittedErase.capacity();
    }

    // The predicate must be synchronous and must not append ready records.
    // Call both passes before pumping either queue to share an activity snapshot.
    template <typename TIdle>
    ui32 AdmitIdleFlush(TSlotIoCoordinator& slots, const TIdle& idle) {
        return AdmitSelected(slots, UnadmittedFlush, true, idle);
    }

    template <typename TIdle>
    ui32 AdmitIdleErase(TSlotIoCoordinator& slots, const TIdle& idle) {
        return AdmitSelected(slots, UnadmittedErase, false, idle);
    }

    bool FlushAdmitted(ui64 lsn) const {
        const auto it = Records.find(lsn);
        return it != Records.end() && it->second.FlushAdmitted;
    }

    bool EraseAdmitted(ui64 lsn) const {
        const auto it = Records.find(lsn);
        return it != Records.end() && it->second.EraseAdmitted;
    }

    // Admit exactly the records ready now when `force` is set or the
    // unadmitted set has reached `threshold`. Each admitted slot is examined
    // once; previously admitted blocked slots are not visited.
    ui32 AdmitFlush(TSlotIoCoordinator& slots, ui32 threshold, bool force) {
        return Admit(slots, UnadmittedFlush, /*flush=*/true, threshold, force);
    }

    ui32 AdmitErase(TSlotIoCoordinator& slots, ui32 threshold, bool force) {
        return Admit(slots, UnadmittedErase, /*flush=*/false, threshold, force);
    }

    ui64 ConsiderFlush(TSlotIoCoordinator& slots, TSlotIoCoordinator::TSlot slot) {
        ++Examined.FlushSlotsExamined;
        const ui64 lsn = slots.OldestUnflushed(slot);
        if (!lsn || !FlushAdmitted(lsn) || !slots.CanFlush(slot, lsn)) {
            return 0;
        }
        EnqueueFlush(lsn);
        return lsn;
    }

    ui64 ConsiderErase(TSlotIoCoordinator& slots, TSlotIoCoordinator::TSlot slot) {
        ++Examined.EraseSlotsExamined;
        const ui64 lsn = slots.OldestVersion(slot);
        if (!lsn || !EraseAdmitted(lsn) || !slots.CanErase(slot, lsn)) {
            return 0;
        }
        EnqueueErase(lsn);
        return lsn;
    }

    bool HasFlushWork() const {
        return !FlushQueue.empty();
    }

    bool HasEraseWork() const {
        return !EraseQueue.empty();
    }

    ui32 FlushQueueSize() const {
        return static_cast<ui32>(FlushQueue.size());
    }

    ui32 EraseQueueSize() const {
        return static_cast<ui32>(EraseQueue.size());
    }

    ui64 PeekFlush() const {
        return FlushQueue.empty() ? 0 : FlushQueue.front();
    }

    ui64 PeekErase() const {
        return EraseQueue.empty() ? 0 : EraseQueue.front();
    }

    ui64 PopFlush() {
        Y_ABORT_UNLESS(!FlushQueue.empty());
        const ui64 lsn = FlushQueue.front();
        FlushQueue.pop_front();
        auto it = Records.find(lsn);
        if (it != Records.end()) {
            it->second.InFlushQueue = false;
        }
        return lsn;
    }

    ui64 PopErase() {
        Y_ABORT_UNLESS(!EraseQueue.empty());
        const ui64 lsn = EraseQueue.front();
        EraseQueue.pop_front();
        auto it = Records.find(lsn);
        if (it != Records.end()) {
            it->second.InEraseQueue = false;
        }
        return lsn;
    }

    void Forget(ui64 lsn) {
        Unqueue(lsn);
        auto it = Records.find(lsn);
        if (it == Records.end()) {
            return;
        }
        Y_ABORT_UNLESS(!it->second.FlushReady || it->second.FlushAdmitted);
        Y_ABORT_UNLESS(!it->second.EraseReady || it->second.EraseAdmitted);
        Records.erase(it);
    }

private:
    struct TRecord {
        TSlotIoCoordinator::TSlot Slot;
        bool FlushReady = false;
        bool FlushAdmitted = false;
        bool EraseReady = false;
        bool EraseAdmitted = false;
        bool InFlushQueue = false;
        bool InEraseQueue = false;
    };

    ui32 Admit(
        TSlotIoCoordinator& slots,
        std::vector<ui64>& unadmitted,
        bool flush,
        ui32 threshold,
        bool force)
    {
        if (unadmitted.empty() || (!force && unadmitted.size() < threshold)) {
            return 0;
        }
        // Admission cannot append ready records. Keep the allocation for the
        // next cohort and bound this pass to the prefix present on entry.
        const size_t count = unadmitted.size();
        for (size_t i = 0; i < count; ++i) {
            AdmitRecord(slots, unadmitted[i], flush);
        }
        unadmitted.clear();
        return static_cast<ui32>(count);
    }

    template <typename TIdle>
    ui32 AdmitSelected(
        TSlotIoCoordinator& slots,
        std::vector<ui64>& unadmitted,
        bool flush,
        const TIdle& idle)
    {
        const size_t count = unadmitted.size();
        size_t retained = 0;
        for (size_t i = 0; i < count; ++i) {
            const ui64 lsn = unadmitted[i];
            if (idle(Records.at(lsn).Slot.VChunk)) {
                AdmitRecord(slots, lsn, flush);
            } else {
                unadmitted[retained++] = lsn;
            }
        }
        unadmitted.resize(retained);
        return static_cast<ui32>(count - retained);
    }

    void AdmitRecord(TSlotIoCoordinator& slots, ui64 lsn, bool flush) {
        auto& record = Records.at(lsn);
        if (flush) {
            record.FlushAdmitted = true;
            ConsiderFlush(slots, record.Slot);
        } else {
            record.EraseAdmitted = true;
            ConsiderErase(slots, record.Slot);
        }
    }

    void Unqueue(ui64 lsn) {
        auto it = Records.find(lsn);
        if (it == Records.end()) {
            return;
        }
        if (it->second.InFlushQueue) {
            std::erase(FlushQueue, lsn);
            it->second.InFlushQueue = false;
        }
        if (it->second.InEraseQueue) {
            std::erase(EraseQueue, lsn);
            it->second.InEraseQueue = false;
        }
    }

    void EnqueueFlush(ui64 lsn) {
        auto& record = Records.at(lsn);
        if (record.InFlushQueue) {
            return;
        }
        record.InFlushQueue = true;
        FlushQueue.push_back(lsn);
    }

    void EnqueueErase(ui64 lsn) {
        auto& record = Records.at(lsn);
        if (record.InEraseQueue) {
            return;
        }
        record.InEraseQueue = true;
        EraseQueue.push_back(lsn);
    }

    absl::flat_hash_map<ui64, TRecord> Records;
    std::vector<ui64> UnadmittedFlush;
    std::vector<ui64> UnadmittedErase;
    std::deque<ui64> FlushQueue;
    std::deque<ui64> EraseQueue;
    TStats Examined;
};

} // namespace NKikimr::NNbsDbgLike
