#pragma once

#include <ydb/library/wasm/api/compartment.h>

#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr::NUdfStore::NWasm {

//! Cap on bytes the bridge keeps resident in compartment linear memory.
//! Exceeding it evicts pins untouched by the current Run; a single value
//! larger than the budget is still pinned (the guest has to see it).
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
//! calling malloc is not.
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
        const void* key,
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
    ui64 GetUserData(const void* key) const;
    void SetUserData(const void* key, const NYql::NUdf::TUnboxedValue& owner, ui64 value);

    //! User-data of entries this cache dropped. The guest drains the queue and
    //! frees the values itself, because the host has no way to call the guest's
    //! deleter: unlike sbrk, that would mean running arbitrary guest code (and
    //! its allocator) from inside a host intrinsic.
    bool PopReleasedUserData(ui64& value);

    size_t UserDataCount() const {
        return UserStates_.size();
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

private:
    struct TPin {
        NYql::NUdf::TUnboxedValue Owner;
        ui64 Offset = 0;
        ui64 Length = 0;
        ui64 BlockSize = 0;
        ui64 LastRun = 0;
        TList<const void*>::iterator LruIt;
    };

    struct TUserState {
        NYql::NUdf::TUnboxedValue Owner;
        ui64 Value = 0;
        TList<const void*>::iterator LruIt;
    };

    ui64 AllocBlock(ui64 length);
    void GrowArena(ui64 length);
    void EvictFor(ui64 length);
    void Touch(const void* key, TPin& pin);
    void WriteBytes(ui64 offset, NYql::NUdf::TStringRef bytes);

    NYdb::NWasm::IWebAssemblyCompartment* const Compartment_;
    const ui64 Budget_;

    THashMap<const void*, TPin> Pins_;
    //! Front is the least recently used pin.
    TList<const void*> Lru_;
    ui64 PinnedBytes_ = 0;
    ui64 Evictions_ = 0;
    ui64 CurrentRun_ = 1;

    //! Arena: bump pointer over grown chunks plus per-size-class free lists.
    //! Blocks_ holds the live blocks only; a freed offset moves to FreeBlocks_.
    THashMap<ui64 /*offset*/, ui64 /*block size*/> Blocks_;
    THashMap<ui64 /*block size*/, TVector<ui64 /*offset*/>> FreeBlocks_;
    THashSet<ui64 /*offset*/> GuestBlocks_;
    ui64 BumpOffset_ = 0;
    ui64 BumpRemaining_ = 0;
    ui64 ArenaBytes_ = 0;

    TVector<ui64> ScratchBlocks_;

    THashMap<const void*, TUserState> UserStates_;
    TList<const void*> UserStatesLru_;
    TList<ui64> ReleasedUserData_;
};

} // namespace NKikimr::NUdfStore::NWasm
