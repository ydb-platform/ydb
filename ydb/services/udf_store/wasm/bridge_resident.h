#pragma once

#include "bridge_types.h"

#include <ydb/library/wasm/api/compartment.h>

#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr::NUdfStore::NWasm {

//! Cap on the bytes the bridge keeps resident in compartment linear memory,
//! counted as the blocks the arena hands out: a block is what leaves linear
//! memory, and counting the length asked for instead let a guest looping
//! one-byte allocations grow the arena to many times the cap. Size classes
//! are fine enough that the rounding costs little of what the cap is for.
//! Exceeding it evicts pins untouched by the current Run, and fails the call
//! when that frees too little -- pins the current Run holds are not evictable,
//! so the cap has to hold within one Run too. A single value larger than the
//! budget is still pinned (the guest has to see it) and does not count
//! against the cap, so the rest of the Run still gets its own budget.
//! Guest AllocResident and per-Run scratch share the same budget so a guest
//! cannot grow the arena without bound by looping BridgeAllocResident.
inline constexpr ui64 DefaultResidentBudgetBytes = 64ull << 20;

//! Materialization cache over compartment linear memory, shared by every
//! bridge node of one query compartment.
//!
//! Memory comes from a host-side arena built on AllocateDetachedBytes
//! (growMemory) and never from guest malloc: guest allocation re-enters the
//! WASM runtime and traps for multi-MiB blobs while a UDF frame is live.
//! Each grown region is then fenced off with ReserveGuestHeapBelow so the
//! guest allocator cannot hand the same bytes out again.
//!
//! Fencing does call one guest export, "sbrk", and that is the only guest code
//! the cache reaches. It is not the re-entrancy the arena exists to avoid:
//! memory has already been grown by then, so sbrk only moves a break pointer
//! upward inside linear memory. It allocates nothing, cannot grow memory and
//! cannot trap, which is why calling it under a live UDF frame is safe where
//! calling malloc is not. What it is not free to do is call back into the
//! bridge -- the cache is mid-update and the caller may be holding a reference
//! into a live value -- so the call runs under TGuestCallbackGuard and every
//! host intrinsic refuses to be served from inside it.
//!
//! Entries are keyed by value identity (BridgeIdentityKey), not by node, so
//! they survive node death and are reused on the next row even when the guest
//! forgot to BridgeRef its handle. Offsets stay valid for the Run that asked
//! for them: eviction may recycle a region between rows, so the guest must
//! re-ask (a hash lookup, not a copy) instead of caching offsets across rows.
class TCompartmentResidentCache: public TNonCopyable {
public:
    explicit TCompartmentResidentCache(
        NYdb::NWasm::IWebAssemblyCompartment* compartment,
        ui64 budgetBytes = DefaultResidentBudgetBytes);

    //! Copy `bytes` into linear memory once per key; repeat calls are lookups.
    //! `owner` keeps the source alive, so `key` stays valid while pinned.
    ui64 Pin(
        const TBridgeIdentity& key,
        const NYql::NUdf::TUnboxedValue& owner,
        NYql::NUdf::TStringRef bytes);

    //! Copy of `bytes` for values without stable identity (embedded strings,
    //! freshly built blobs). Released at the next BeginRun, so the arena block
    //! is reused row after row instead of growing linear memory.
    ui64 PinScratch(NYql::NUdf::TStringRef bytes);

    //! Raw region for host scratch such as the per-Run result slot.
    ui64 Alloc(ui64 length);
    void Free(ui64 offset);

    //! Same arena, but owned by the guest (BridgeAllocResident). Only these
    //! offsets may come back through FreeGuest, so a guest cannot free a pin
    //! or the host's result slot and end up aliasing someone else's bytes.
    ui64 AllocGuest(ui64 length);
    void FreeGuest(ui64 offset);

    //! Guest-owned lazily built state (a parsed index, a built trie, ...),
    //! keyed by value identity just like pins, so it survives node death and
    //! is found again on the next row without any BridgeRef discipline.
    //! 0 means "nothing cached yet".
    ui64 GetUserData(const TBridgeIdentity& key) const;
    void SetUserData(const TBridgeIdentity& key, const NYql::NUdf::TUnboxedValue& owner, ui64 value);

    //! User-data of entries this cache dropped. The guest drains the queue and
    //! frees the values itself, because the host has no way to call the guest's
    //! deleter: unlike sbrk, that would mean running arbitrary guest code (and
    //! its allocator) from inside a host intrinsic.
    bool PopReleasedUserData(ui64& value);

    size_t UserDataCount() const {
        return UserStates_.size();
    }

    //! Released values dropped because the guest let the queue grow past its
    //! bound instead of draining it.
    ui64 DroppedUserDataCount() const {
        return DroppedUserData_;
    }

    //! A new Run starts: earlier pins become evictable and scratch is reused.
    void BeginRun();

    ui64 PinnedBytes() const {
        return PinnedBytes_;
    }

    ui64 ArenaBytes() const {
        return ArenaBytes_;
    }

    size_t PinCount() const {
        return Pins_.size();
    }

    ui64 EvictionCount() const {
        return Evictions_;
    }

    ui64 GuestBytes() const {
        return GuestBytes_;
    }

private:
    struct TPin {
        NYql::NUdf::TUnboxedValue Owner;
        ui64 Offset = 0;
        //! Bytes of the value, for what the messages say.
        ui64 Length = 0;
        //! Bytes of the block holding it, which is what the budget counts.
        ui64 Charge = 0;
        ui64 LastRun = 0;
        TList<TBridgeIdentity>::iterator LruIt;
    };

    struct TUserState {
        NYql::NUdf::TUnboxedValue Owner;
        ui64 Value = 0;
        TList<TBridgeIdentity>::iterator LruIt;
    };

    //! What the cache keeps in linear memory right now and charges to the
    //! budget: pins, per-Run scratch and guest-owned blocks draw on the one
    //! budget. Pins of values too large for the budget are left out -- they
    //! are taken anyway, and charging them would refuse everything after.
    ui64 ResidentBytes() const {
        return (PinnedBytes_ - OversizedBytes_) + ScratchBytes_ + GuestBytes_;
    }

    ui64 AllocBlock(ui64 length);
    void GrowArena(ui64 length);
    //! Drop one pin and everything keyed on the same identity, answering the
    //! LRU position after it.
    TList<TBridgeIdentity>::iterator EvictPin(TList<TBridgeIdentity>::iterator it, TPin& pin);
    void EvictFor(ui64 length);
    //! Drop pins of values too large for the budget that no longer belong to
    //! the current Run. They are outside the budget, so EvictFor never reaches
    //! them and only this gives their bytes back.
    void EvictOversized();
    //! Hand a value back to the guest to free, dropping the oldest ones when
    //! the guest lets the queue grow past MaxReleasedUserData.
    void QueueReleasedUserData(ui64 value);
    //! Drop the guest state cached for `key` and queue its value for the guest
    //! to free. Returns false when there was nothing cached under that key.
    //! Takes the key by value: callers hand us LRU front()/iterators, and the
    //! identity lives inside the list node that this method erases.
    bool ReleaseUserState(TBridgeIdentity key);
    void Touch(const TBridgeIdentity& key, TPin& pin);
    void WriteBytes(ui64 offset, NYql::NUdf::TStringRef bytes);

    NYdb::NWasm::IWebAssemblyCompartment* const Compartment_;
    const ui64 Budget_;

    THashMap<TBridgeIdentity, TPin> Pins_;
    //! Front is the least recently used pin.
    TList<TBridgeIdentity> Lru_;
    ui64 PinnedBytes_ = 0;
    //! Part of PinnedBytes_ held by pins of values larger than the whole
    //! budget, which the budget does not count. At most one is resident.
    ui64 OversizedBytes_ = 0;
    ui64 Evictions_ = 0;
    ui64 CurrentRun_ = 1;

    //! Arena: bump pointer over grown chunks plus per-size-class free lists.
    //! Blocks_ holds the live blocks only; a freed offset moves to FreeBlocks_.
    THashMap<ui64 /*offset*/, ui64 /*block size*/> Blocks_;
    THashMap<ui64 /*block size*/, TVector<ui64 /*offset*/>> FreeBlocks_;
    //! Offsets handed out through AllocGuest, with the length each was asked
    //! for: the block they live in is wider, and the budget counts lengths.
    THashMap<ui64 /*offset*/, ui64 /*length*/> GuestBlocks_;
    //! Live bytes handed out through AllocGuest; counted against Budget_.
    ui64 GuestBytes_ = 0;
    //! Live bytes in ScratchBlocks_; counted against Budget_ until BeginRun.
    ui64 ScratchBytes_ = 0;
    ui64 BumpOffset_ = 0;
    ui64 BumpRemaining_ = 0;
    ui64 ArenaBytes_ = 0;

    TVector<ui64> ScratchBlocks_;

    THashMap<TBridgeIdentity, TUserState> UserStates_;
    TList<TBridgeIdentity> UserStatesLru_;
    TList<ui64> ReleasedUserData_;
    ui64 DroppedUserData_ = 0;
};

} // namespace NKikimr::NUdfStore::NWasm
