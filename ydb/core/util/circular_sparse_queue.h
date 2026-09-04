#pragma once

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/bitops.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <expected>
#include <type_traits>
#include <utility>
#include <vector>

namespace NKikimr {

// A fixed-size container tailored for "inflight" style usage where:
//   - items are added at the back and receive a monotonically increasing index;
//   - items may be removed at any position by their index;
//   - the front (oldest) index only advances when the front item is removed,
//     skipping over any already-removed items right after it (like a pop).
//
// At most MaxSize items can be live simultaneously. The backing storage is
// rounded up to the nearest power of 2 >= 2*MaxSize. In the common case (LIFO
// churn with regular front advances) this guarantees the target slot is always
// free. However, a single long-lived (stuck) front item can pin Begin while
// up to MaxSize live items churn, making End-Begin reach BufferSize. In that
// case Emplace returns an error (the queue is treated as full) rather than
// overwriting the stuck live entry. The caller must wait until the stuck item
// is erased and Begin advances before new items can be added.
//
// SlotIndex[slot] records which index is stored in each slot, enabling
// Find/Erase to detect any index/slot mismatch (defense-in-depth).
//
// Deleted slots are tracked with a separate bitmask (a std::vector<bool>).
// Erased and cleared slots are reset to T() even though Deleted[] is the
// authoritative "live" indicator and Find never reads a deleted slot. This is
// a deliberate choice: it releases any resources (heap allocations, etc.) held
// by the erased value promptly rather than keeping them alive until the slot
// is overwritten by the next Emplace. For the current user (TInflightEntry,
// four scalar fields) the cost is negligible.
template <typename T>
class TCircularSparseQueue {
public:
    explicit TCircularSparseQueue(size_t size)
        : MaxSize(size)
        , BufferSize(size <= 1 ? 2 : FastClp2(size * 2))
        , SlotMask(BufferSize - 1)
        , Data(BufferSize)
        , Deleted(BufferSize, true)
        , SlotIndex(BufferSize, 0)
    {
        static_assert(std::is_default_constructible_v<T>,
            "TCircularSparseQueue requires T to be default-constructible"
            " (used for storage initialization and Clear/Erase)");
        static_assert(std::is_move_assignable_v<T> || std::is_copy_assignable_v<T>,
            "TCircularSparseQueue requires T to be move-assignable or copy-assignable");
        Y_ABORT_UNLESS(MaxSize > 0, "TCircularSparseQueue must not have zero size");
    }

    // Returns {index, pointer-to-emplaced-element} on success, or an error string.
    // The pointer is valid until the element is Erase()d or the queue is Clear()ed.
    // Note: T& cannot be used inside std::expected (its internal union forbids
    // reference members), so T* is used instead.
    template <typename... Args>
    std::expected<std::pair<ui64, T*>, TString> Emplace(Args&&... args) {
        if (LiveCount >= MaxSize) {
            return std::unexpected(
                TStringBuilder() << "TCircularSparseQueue is full, MaxSize# " << MaxSize);
        }

        const ui64 index = End;
        const size_t slot = Slot(index);

        // If the target slot is still occupied by a live (stuck) front item,
        // treat the queue as full until that item is erased and Begin advances.
        if (!Deleted[slot]) {
            return std::unexpected(TStringBuilder()
                << "TCircularSparseQueue slot occupied by stuck item,"
                << " End# " << End
                << " Begin# " << Begin
                << " BufferSize# " << BufferSize
                << " LiveCount# " << LiveCount
                << " StuckIndex# " << SlotIndex[slot]);
        }

        Data[slot] = T(std::forward<Args>(args)...);
        SlotIndex[slot] = index;
        Deleted[slot] = false;
        ++End;
        ++LiveCount;
        return std::pair<ui64, T*>{index, &Data[slot]};
    }

    template <typename U>
    std::expected<ui64, TString> Push(U&& item) {
        auto res = Emplace(std::forward<U>(item));
        if (!res) {
            return std::unexpected(std::move(res.error()));
        }
        return res->first;
    }

    T* Find(ui64 index) {
        if (!InRange(index)) {
            return nullptr;
        }
        const size_t slot = Slot(index);
        if (Deleted[slot]) {
            return nullptr;
        }
        Y_DEBUG_ABORT_UNLESS(SlotIndex[slot] == index,
            "TCircularSparseQueue index/slot mismatch: index# %lu stored# %lu",
            (unsigned long)index, (unsigned long)SlotIndex[slot]);
        if (SlotIndex[slot] != index) {
            return nullptr;
        }
        return &Data[slot];
    }

    const T* Find(ui64 index) const {
        if (!InRange(index)) {
            return nullptr;
        }
        const size_t slot = Slot(index);
        if (Deleted[slot]) {
            return nullptr;
        }
        Y_DEBUG_ABORT_UNLESS(SlotIndex[slot] == index,
            "TCircularSparseQueue index/slot mismatch: index# %lu stored# %lu",
            (unsigned long)index, (unsigned long)SlotIndex[slot]);
        if (SlotIndex[slot] != index) {
            return nullptr;
        }
        return &Data[slot];
    }

    bool Erase(ui64 index) {
        if (!InRange(index)) {
            return false;
        }
        const size_t slot = Slot(index);
        if (Deleted[slot]) {
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(SlotIndex[slot] == index,
            "TCircularSparseQueue index/slot mismatch on Erase: index# %lu stored# %lu",
            (unsigned long)index, (unsigned long)SlotIndex[slot]);
        if (SlotIndex[slot] != index) {
            return false;
        }

        Deleted[slot] = true;
        Data[slot] = T();
        --LiveCount;

        // Begin only moves here: pop consecutive deleted items from the front.
        if (index == Begin) {
            while (Begin < End && Deleted[Slot(Begin)]) {
                ++Begin;
            }
        }
        return true;
    }

    size_t Size() const {
        return LiveCount;
    }

    bool Empty() const {
        return LiveCount == 0;
    }

    size_t Capacity() const {
        return MaxSize;
    }

    ui64 FrontIndex() const {
        return Begin;
    }

    ui64 NextIndex() const {
        return End;
    }

    void Clear() {
        for (ui64 slot = 0; slot < BufferSize; ++slot) {
            Data[slot] = T();
            Deleted[slot] = true;
            SlotIndex[slot] = 0;
        }
        Begin = 0;
        End = 0;
        LiveCount = 0;
    }

private:
    size_t Slot(ui64 index) const {
        return index & SlotMask;
    }

    bool InRange(ui64 index) const {
        return index >= Begin && index < End;
    }

private:
    const size_t MaxSize;
    const ui64 BufferSize;
    const ui64 SlotMask;

    std::vector<T> Data;
    std::vector<bool> Deleted;
    std::vector<ui64> SlotIndex; // which index occupies each slot

    ui64 Begin = 0;
    ui64 End = 0;
    size_t LiveCount = 0;
};

} // namespace NKikimr
