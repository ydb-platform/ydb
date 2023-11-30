#pragma once

#include "defs.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/ptr.h>

#include <atomic>

namespace NKikimr {

class TSharedPageGCList;

class TSharedPageHandle : public TThrRefBase {
    friend class TSharedPageGCList;

public:
    /**
     * Returns true if handle is initialized
     *
     * Unitialized handle may not be shared with other threads.
     */
    bool IsInitialized() const noexcept {
        return Flags.load(std::memory_order_relaxed) != FlagsUninitialized;
    }

    /**
     * Returns true if page is currently marked for garbage collection (no synchronization)
     */
    bool IsGarbage() const noexcept {
        return (Flags.load(std::memory_order_relaxed) & FlagGarbage) != 0;
    }

    /**
     * Returns true if page is currently dropped and may not be used again (no synchronization)
     */
    bool IsDropped() const noexcept {
        return (Flags.load(std::memory_order_relaxed) & FlagDropped) != 0;
    }

    /**
     * Returns the number of currently active uses (no synchronization)
     */
    size_t UseCount() const noexcept {
        return static_cast<size_t>((Flags.load(std::memory_order_relaxed) & UseCountMask) >> UseCountShift);
    }

    /**
     * Returns the number of currently active pins (no synchronization)
     */
    size_t PinCount() const noexcept {
        return static_cast<size_t>((Flags.load(std::memory_order_relaxed) & PinCountMask) >> PinCountShift);
    }

    /**
     * Increments use count on a page and returns true if a page contents is
     * locked in cache and would not be dropped until all uses have stopped.
     *
     * Returns false if page contents have been dropped and need to be loaded
     * from disk again if needed.
     */
    bool Use() noexcept {
        ui64 flags = Flags.fetch_add(UseCountInc, std::memory_order_acquire);
        Y_DEBUG_ABORT_UNLESS((flags & UseCountMask) != UseCountMask,
            "Use count overflow, too many actors are using the same page");

        if (flags & FlagDropped) {
            // Page data has been dropped, so it cannot be used anymore
            Flags.fetch_sub(UseCountInc, std::memory_order_acquire);
            return false;
        }

        return true;
    }

    /**
     * Decrements use count on a page and returns true if use count dropped to
     * zero and page has been marked as potential garbage. Garbage is collected
     * asynchronously, page content is still valid until that happens and may
     * be locked in cache again. However caller is expected to notify shared
     * cache about this page, e.g. by adding it to a gc list.
     */
    bool UnUse() noexcept {
        ui64 flags = Flags.fetch_sub(UseCountInc, std::memory_order_release);
        Y_DEBUG_ABORT_UNLESS((flags & UseCountMask) != 0,
            "Use count underflow, possible Use/UnUse mismatch");

        flags -= UseCountInc;
        while ((flags & UseCountMask) == 0 && !(flags & FlagGarbage)) {
            // Last user is responsible for handling a gc list
            if (Flags.compare_exchange_weak(flags, flags | FlagGarbage, std::memory_order_release)) {
                // Last user that added garbage flag is the only one handling gc list
                return true;
            }
        }

        return false;
    }

    /**
     * Pins page data in memory so it may be processed.
     *
     * Page data may move in memory for optimization purposes unless it is
     * pinned. Page data should not be pinned for long periods of time, as it
     * may negatively affect memory usage. The ideal scenario is to pin data
     * just before it is accessed (e.g. during iteration) and to unpin it as
     * soon as it is no longer needed (e.g. as soon as iteration moves to the
     * next page).
     */
    TSharedData Pin() noexcept {
        // Pin needs to be fast, so we increment first and verify later
        ui64 flags = Flags.fetch_add(PinCountInc, std::memory_order_acquire);
        Y_DEBUG_ABORT_UNLESS((flags & PinCountMask) != PinCountMask,
            "Pin count overflow, too many actors are pinning the same page");

        Y_DEBUG_ABORT_UNLESS(!(flags & FlagDropped) || (flags & PinCountMask) > 0,
            "Pin on a dropped handle, only live pages may be pinned");

        TSharedData ref = Data[flags & DataIndexMask];
        Y_DEBUG_ABORT_UNLESS(ref, "Pinned page has an unexpected empty shared data");
        return ref;
    }

    /**
     * Unpins page data in memory so it may be moved asynchronously.
     *
     * For best performance pinned page data should be released first.
     */
    void UnPin() noexcept {
        // UnPin needs to be fast, so we decrement first and verify later
        ui64 flags = Flags.fetch_sub(PinCountInc, std::memory_order_release);
        Y_DEBUG_ABORT_UNLESS((flags & PinCountMask) != 0,
            "Pin count underflow, possible Pin/UnPin mismatch");

        if ((flags & FlagDropped) && (flags & PinCountMask) == PinCountInc) {
            // This is the last pin of dropped data, safe to release
            Data[flags & DataIndexMask] = { };
        }
    }

    /**
     * Tries to atomically swap page data and returns true on success
     *
     * This operation may fail if old data is currently pinned in memory
     */
    bool TryMove(const TSharedData& to) noexcept {
        Y_DEBUG_ABORT_UNLESS(to, "Cannot move to empty data");

        ui64 flags = Flags.load(std::memory_order_relaxed);
        size_t oldIndex = flags & DataIndexMask;
        size_t newIndex = oldIndex ^ DataIndexMask;
        Data[newIndex] = to;

        do {
            Y_DEBUG_ABORT_UNLESS(!(flags & FlagDropped), "Trying to move a dropped page");
            Y_DEBUG_ABORT_UNLESS((flags & DataIndexMask) == oldIndex, "Page data index must not change during move");

            if ((flags & PinCountMask) != 0) {
                // Data is currently pinned and cannot be moved
                Data[newIndex] = { };
                return false;
            }
        } while (!Flags.compare_exchange_weak(flags, flags ^ DataIndexMask, std::memory_order_acq_rel, std::memory_order_relaxed));

        // We have successfully published the new data, drop the old data
        Data[oldIndex] = { };
        return true;
    }

    /**
     * Tries to atomically drop page data and returns dropped data on success
     *
     * This operation may fail if handle has a non-zero use count, in which
     * case garbage flag would be dropped.
     */
    TSharedData TryDrop() noexcept {
        ui64 flags = Flags.load(std::memory_order_relaxed);
        size_t index = flags & DataIndexMask;

        for (;;) {
            Y_DEBUG_ABORT_UNLESS((flags & FlagGarbage), "Trying to drop a handle not marked as garbage");
            Y_DEBUG_ABORT_UNLESS(!(flags & FlagDropped), "Trying to drop a handle that is already dropped");
            Y_DEBUG_ABORT_UNLESS((flags & DataIndexMask) == index, "Page data index must not change during drop");

            if ((flags & UseCountMask) != 0) {
                // Data is currently used and cannot be dropped, drop the garbage flag
                if (Flags.compare_exchange_weak(flags, flags & ~FlagGarbage, std::memory_order_relaxed)) {
                    return { };
                }
            } else {
                Y_DEBUG_ABORT_UNLESS((flags & PinCountMask) != PinCountMask, "Pin count overflow");
                if (Flags.compare_exchange_weak(flags, (flags + PinCountInc) | FlagDropped, std::memory_order_acquire, std::memory_order_relaxed)) {
                    break;
                }
            }
        }

        TSharedData data = Data[index];

        // Drop the pin we acquired above, it may or may not be the last one
        UnPin();

        return data;
    }

protected:
    /**
     * Initializes handle with the specified data and a use count of 1
     */
    void Initialize(TSharedData data) {
        Y_DEBUG_ABORT_UNLESS(data);
        Y_DEBUG_ABORT_UNLESS(!IsInitialized());
        Data[0] = std::move(data);
        Flags.store(UseCountInc, std::memory_order_release);
    }

private:
    enum {
        UseCountShift = 3,
        UseCountBits = 30, // ~1 billion users
        PinCountShift = UseCountShift + UseCountBits,
        PinCountBits = 30, // ~1 billion pins
        FlagsBits = PinCountShift + PinCountBits,
    };

    enum : ui64 {
        DataIndexMask = 1LL,
        FlagGarbage = 1LL << 1,
        FlagDropped = 1LL << 2,
        UseCountInc = (1LL << UseCountShift),
        UseCountMask = ((1LL << UseCountBits) - 1) << UseCountShift,
        PinCountInc = (1LL << PinCountShift),
        PinCountMask = ((1LL << PinCountBits) - 1) << PinCountShift,
        FlagsUninitialized = static_cast<ui64>(-1),
    };

    using TFlags = std::atomic<ui64>;
    static_assert(TFlags::is_always_lock_free, "TFlags must be lock free");

private:
    TFlags Flags{ FlagsUninitialized };
    TSharedData Data[2];
    TSharedPageHandle* GCNext = nullptr;
};

class TSharedPageGCList : public TThrRefBase {
public:
    ~TSharedPageGCList() {
        // Destructor implies no other thread has references
        // This implies no other thread may push handles into us
        // Since we own pushed handles we need to clean them up
        while (PopGC()) {
            // nothing
        }
    }

    void PushGC(TSharedPageHandle* handle) noexcept {
        handle->Ref();

        TSharedPageHandle* current = GCList.load(std::memory_order_relaxed);
        do {
            handle->GCNext = current;
        } while (!GCList.compare_exchange_weak(current, handle, std::memory_order_release));
    }

    TIntrusivePtr<TSharedPageHandle> PopGC() noexcept {
        TSharedPageHandle* current = GCList.load(std::memory_order_acquire);

        while (current != nullptr) {
            if (GCList.compare_exchange_weak(current, current->GCNext, std::memory_order_acquire)) {
                break;
            }
        }

        TIntrusivePtr<TSharedPageHandle> result(current);

        if (current) {
            current->GCNext = nullptr;
            current->DecRef();
        }

        return result;
    }

private:
    using TGCList = std::atomic<TSharedPageHandle*>;
    static_assert(TGCList::is_always_lock_free, "TGCList must be lock free");

private:
    TGCList GCList{ nullptr };
};

/**
 * A smart reference to a page handle
 *
 * Allows easier tracking of all active users of a particular page handle
 */
class TSharedPageRef {
public:
    TSharedPageRef() noexcept
        : Handle(nullptr)
        , GCList(nullptr)
        , Used(false)
    { }

    TSharedPageRef(
            TIntrusivePtr<TSharedPageHandle> handle,
            TIntrusivePtr<TSharedPageGCList> gcList) noexcept
        : Handle(std::move(handle))
        , GCList(std::move(gcList))
        , Used(false)
    { }

    ~TSharedPageRef() {
        Drop();
    }

    TSharedPageRef(const TSharedPageRef& ref) noexcept
        : Handle(ref.Handle)
        , GCList(ref.GCList)
        , Used(false)
    {
        if (ref.Used) {
            Y_ABORT_UNLESS(Use());
        }
    }

    TSharedPageRef(TSharedPageRef&& ref) noexcept
        : Handle(std::move(ref.Handle))
        , GCList(std::move(ref.GCList))
        , Used(std::exchange(ref.Used, false))
    { }

    TSharedPageRef& operator=(const TSharedPageRef& ref) noexcept {
        if (this != &ref) {
            Drop();
            Handle = ref.Handle;
            GCList = ref.GCList;
            if (ref.Used) {
                Y_ABORT_UNLESS(Use());
            }
        }

        return *this;
    }

    TSharedPageRef& operator=(TSharedPageRef&& ref) noexcept {
        if (this != &ref) {
            Drop();
            Handle = std::move(ref.Handle);
            GCList = std::move(ref.GCList);
            Used = std::exchange(ref.Used, false);
        }

        return *this;
    }

    static TSharedPageRef MakeUsed(
        TIntrusivePtr<TSharedPageHandle> handle,
        TIntrusivePtr<TSharedPageGCList> gcList) noexcept
    {
        TSharedPageRef ref(std::move(handle), std::move(gcList));
        ref.Use();
        return ref;
    }

    static TSharedPageRef MakePrivate(TSharedData data) {
        return MakeUsed(TPrivateDataWrapper::Make(std::move(data)), nullptr);
    }

    explicit operator bool() const {
        return bool(Handle);
    }

    const TIntrusivePtr<TSharedPageHandle>& GetHandle() const {
        return Handle;
    }

    bool IsUsed() const {
        return Used;
    }

    bool Use() noexcept {
        if (Used) {
            return true;
        }

        if (Handle && Handle->Use()) {
            Used = true;
            return true;
        }

        return false;
    }

    bool UnUse() noexcept {
        if (std::exchange(Used, false)) {
            Y_DEBUG_ABORT_UNLESS(Handle);

            if (Handle->UnUse()) {
                if (GCList) {
                    GCList->PushGC(Handle.Get());
                }
                return true;
            }
        }

        return false;
    }

    void Drop() {
        UnUse();
        GCList.Drop();
        Handle.Drop();
    }

private:
    class TPrivateDataWrapper : public TSharedPageHandle {
        TPrivateDataWrapper(TSharedData data) {
            Initialize(data);
        }

    public:
        static TIntrusivePtr<TSharedPageHandle> Make(TSharedData data) {
            return new TPrivateDataWrapper(std::move(data));
        }
    };

private:
    TIntrusivePtr<TSharedPageHandle> Handle;
    TIntrusivePtr<TSharedPageGCList> GCList;
    bool Used;
};

/**
 * A smart reference to a page pinned in memory
 *
 * Allows easier tracking of all page data users with automatic unpinning
 */
class TPinnedPageRef {
public:
    TPinnedPageRef() = default;

    explicit TPinnedPageRef(TSharedData data) noexcept
        : Data_(std::move(data))
    { }

    explicit TPinnedPageRef(const TSharedPageRef& ref) noexcept {
        Y_ABORT_UNLESS(ref.IsUsed(), "Cannot pin pages not marked as used");
        Data_ = ref.GetHandle()->Pin();
        Handle_ = ref.GetHandle();
    }

    ~TPinnedPageRef() noexcept {
        Drop();
    }

    TPinnedPageRef(const TPinnedPageRef& ref) noexcept {
        if (ref.Handle_) {
            Data_ = ref.Handle_->Pin();
            Handle_ = ref.Handle_;
        } else {
            Data_ = ref.Data_;
        }
    }

    TPinnedPageRef(TPinnedPageRef&& ref) noexcept
        : Data_(std::move(ref.Data_))
        , Handle_(std::move(ref.Handle_))
    {
        Y_DEBUG_ABORT_UNLESS(!ref.Handle_);
    }

    TPinnedPageRef& operator=(const TPinnedPageRef& ref) noexcept {
        if (this != &ref) {
            Drop();
            if (ref.Handle_) {
                Data_ = ref.Handle_->Pin();
                Handle_ = ref.Handle_;
            } else {
                Data_ = ref.Data_;
            }
        }

        return *this;
    };

    TPinnedPageRef& operator=(TPinnedPageRef&& ref) noexcept {
        if (this != &ref) {
            Drop();
            Data_ = std::move(ref.Data_);
            Handle_ = std::move(ref.Handle_);
            Y_DEBUG_ABORT_UNLESS(!ref.Handle_);
        }

        return *this;
    }

    explicit operator bool() const {
        return bool(Data_);
    }

    const TSharedData& GetData() const {
        return Data_;
    }

    const TSharedData* operator->() const {
        return &Data_;
    }

    void Drop() {
        Data_ = { };
        if (Handle_) {
            Handle_->UnPin();
            Handle_.Drop();
        }
    }

private:
    TSharedData Data_;
    TIntrusivePtr<TSharedPageHandle> Handle_;
};

}
