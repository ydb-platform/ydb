#include "bridge_resident.h"

#include <ydb/library/wasm/api/pointer.h>

#include <util/generic/utility.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

#include <bit>
#include <cstring>

namespace NKikimr::NUdfStore::NWasm {

using namespace NYql::NUdf;
using namespace NYdb::NWasm;

namespace {

constexpr ui64 MinBlockSize = 64;
//! Below this, round to a power of two (cheap reuse across rows); above it,
//! round to whole pages. The budget counts the block and not the length asked
//! for, so the rounding has to stay small: at a megabyte of granularity a
//! budget would hold barely half its size in strings just over a size class.
constexpr ui64 PowerOfTwoLimit = 64ull << 10;
//! Largest class the tail of a retired arena chunk is carved into.
constexpr ui64 TailBlockSize = 1ull << 20;
constexpr ui64 ArenaChunkSize = 4ull << 20;
constexpr ui64 WasmPageSize = 64ull << 10;
//! Distinct values whose guest state we remember. Beyond that the oldest one
//! is dropped and its user-data handed back to the guest to free.
constexpr size_t MaxUserStates = 1024;
//! Released user-data the guest has not drained yet. The queue is there so the
//! guest can free its own allocations, and a guest that never drains it must
//! not grow the host heap for the rest of the query. Past this many the oldest
//! entries are dropped: what leaks then is guest memory inside the guest's own
//! compartment, which the compartment already bounds.
constexpr size_t MaxReleasedUserData = 4096;

ui64 RoundUpTo(ui64 value, ui64 granularity) {
    return ((value + granularity - 1) / granularity) * granularity;
}

//! Deterministic size class: the same length always maps to the same block
//! size, which is what makes the free lists reusable row after row.
ui64 BlockSizeFor(ui64 length) {
    if (length <= MinBlockSize) {
        return MinBlockSize;
    }
    if (length <= PowerOfTwoLimit) {
        return std::bit_ceil(length);
    }
    // Rounding up wraps for lengths this close to the top and would hand back
    // a block smaller than asked for.
    if (length > Max<ui64>() - PowerOfTwoLimit + 1) {
        ythrow yexception() << "Bridge: resident block of " << length << " bytes is out of range";
    }
    return RoundUpTo(length, PowerOfTwoLimit);
}

} // namespace

TCompartmentResidentCache::TCompartmentResidentCache(
    IWebAssemblyCompartment* compartment,
    ui64 budgetBytes)
    : Compartment_(compartment)
    , Budget_(budgetBytes)
{
    if (!Compartment_) {
        ythrow yexception() << "Bridge: resident cache requires a compartment";
    }
}

void TCompartmentResidentCache::GrowArena(ui64 length) {
    const ui64 chunk = RoundUpTo(Max(length, ArenaChunkSize), WasmPageSize);
    const ui64 offset = Compartment_->AllocateDetachedBytes(chunk);
    if (offset == 0) {
        ythrow yexception() << "Bridge: resident arena failed to grow by " << chunk << " bytes";
    }
    // growMemory hands out pages above the guest allocator break; fence them
    // off or the next guest malloc returns the very same bytes. Refusing to
    // use the arena beats silently sharing it with the guest allocator.
    //
    // Fencing runs the guest's "sbrk", which is guest code however little it
    // does, and it runs it from the middle of a Pin whose bookkeeping is not
    // finished and whose bytes are still a reference into a live value. The
    // guard makes every bridge intrinsic refuse to be called from inside it,
    // so a module whose sbrk tries to unref that very value is turned away
    // instead of freeing memory the copy below is about to read.
    {
        const TGuestCallbackGuard guestCallback;
        if (!Compartment_->ReserveGuestHeapBelow(offset + chunk)) {
            ythrow yexception()
                << "Bridge: cannot fence " << chunk << " resident bytes at " << offset
                << " off the guest heap; the runtime library must export \"sbrk\"";
        }
    }
    // The bump pointer can only live in one chunk, so the tail of the old one
    // is gone unless it goes back through the free lists. Every block is a
    // multiple of MinBlockSize, so the tail is too and it carves into whole
    // size classes exactly. Whole megabytes first: free lists are keyed by
    // exact size, and a tail carved only into small classes would leave a
    // megabyte-sized request eating into the fresh chunk.
    while (BumpRemaining_ >= MinBlockSize) {
        const ui64 blockSize = BumpRemaining_ >= TailBlockSize
            ? TailBlockSize
            : Min(std::bit_floor(BumpRemaining_), PowerOfTwoLimit);
        FreeBlocks_[blockSize].push_back(BumpOffset_);
        BumpOffset_ += blockSize;
        BumpRemaining_ -= blockSize;
    }
    BumpOffset_ = offset;
    BumpRemaining_ = chunk;
    ArenaBytes_ += chunk;
}

ui64 TCompartmentResidentCache::AllocBlock(ui64 length) {
    const ui64 blockSize = BlockSizeFor(length);
    if (auto* free = FreeBlocks_.FindPtr(blockSize); free && !free->empty()) {
        const ui64 offset = free->back();
        free->pop_back();
        Blocks_.emplace(offset, blockSize);
        return offset;
    }
    if (BumpRemaining_ < blockSize) {
        GrowArena(blockSize);
    }
    const ui64 offset = BumpOffset_;
    BumpOffset_ += blockSize;
    BumpRemaining_ -= blockSize;
    Blocks_.emplace(offset, blockSize);
    return offset;
}

ui64 TCompartmentResidentCache::Alloc(ui64 length) {
    if (length == 0) {
        return 0;
    }
    return AllocBlock(length);
}

void TCompartmentResidentCache::Free(ui64 offset) {
    if (offset == 0) {
        return;
    }
    // Blocks_ holds only the live blocks, so BridgeFreeResident cannot put one
    // offset on a free list twice and hand the same bytes to two owners.
    auto it = Blocks_.find(offset);
    if (it == Blocks_.end()) {
        ythrow yexception()
            << "Bridge: resident free of unknown or already freed offset " << offset;
    }
    const ui64 blockSize = it->second;
    Blocks_.erase(it);
    FreeBlocks_[blockSize].push_back(offset);
}

ui64 TCompartmentResidentCache::AllocGuest(ui64 length) {
    if (length == 0) {
        return 0;
    }
    // What the arena hands out, not what the guest asked for: a block is what
    // leaves linear memory, and charging the length instead let a loop of
    // one-byte allocations grow the arena to MinBlockSize times the cap.
    const ui64 charge = BlockSizeFor(length);
    if (charge > Budget_) {
        ythrow yexception()
            << "Bridge: BridgeAllocResident of " << length
            << " bytes is larger than the whole resident budget (" << Budget_ << ")";
    }
    if (ResidentBytes() + charge > Budget_) {
        ythrow yexception()
            << "Bridge: BridgeAllocResident of " << length
            << " bytes would exceed the resident budget ("
            << ResidentBytes() << " + " << charge << " > " << Budget_ << ")";
    }
    const ui64 offset = Alloc(length);
    if (offset != 0) {
        GuestBlocks_.emplace(offset, charge);
        GuestBytes_ += charge;
    }
    return offset;
}

void TCompartmentResidentCache::FreeGuest(ui64 offset) {
    if (offset == 0) {
        return;
    }
    auto guest = GuestBlocks_.find(offset);
    if (guest == GuestBlocks_.end()) {
        ythrow yexception()
            << "Bridge: BridgeFreeResident on offset " << offset
            << ", which was not returned by BridgeAllocResident";
    }
    if (!Blocks_.contains(offset)) {
        ythrow yexception()
            << "Bridge: resident free of unknown or already freed offset " << offset;
    }
    Y_ENSURE(GuestBytes_ >= guest->second);
    GuestBytes_ -= guest->second;
    GuestBlocks_.erase(guest);
    Free(offset);
}

void TCompartmentResidentCache::WriteBytes(ui64 offset, TStringRef bytes) {
    char* destination = PtrFromVM(
        Compartment_,
        std::bit_cast<char*>(static_cast<uintptr_t>(offset)),
        bytes.Size());
    std::memcpy(destination, bytes.Data(), bytes.Size());
}

void TCompartmentResidentCache::Touch(const TBridgeIdentity& key, TPin& pin) {
    pin.LastRun = CurrentRun_;
    Lru_.erase(pin.LruIt);
    Lru_.push_back(key);
    pin.LruIt = std::prev(Lru_.end());
}

TList<TBridgeIdentity>::iterator TCompartmentResidentCache::EvictPin(
    TList<TBridgeIdentity>::iterator it,
    TPin& pin)
{
    Free(pin.Offset);
    PinnedBytes_ -= pin.Charge;
    if (pin.Charge > Budget_) {
        OversizedBytes_ -= pin.Charge;
    }
    ++Evictions_;
    // The guest keyed its own state on the same identity, and that state
    // usually points into the block that just went away. Hand it back the
    // way the user-data LRU does instead of leaving the guest to read an
    // offset whose bytes now belong to another pin.
    const TBridgeIdentity key = *it;
    ReleaseUserState(key);
    Pins_.erase(key);
    return Lru_.erase(it);
}

void TCompartmentResidentCache::EvictFor(ui64 length) {
    for (auto it = Lru_.begin(); it != Lru_.end() && ResidentBytes() + length > Budget_;) {
        auto* pin = Pins_.FindPtr(*it);
        if (!pin || pin->LastRun == CurrentRun_) {
            // In use by the Run that is running right now: its offset is live.
            ++it;
            continue;
        }
        it = EvictPin(it, *pin);
    }
}

void TCompartmentResidentCache::EvictOversized() {
    for (auto it = Lru_.begin(); it != Lru_.end() && OversizedBytes_ != 0;) {
        auto* pin = Pins_.FindPtr(*it);
        if (!pin || pin->LastRun == CurrentRun_ || pin->Charge <= Budget_) {
            ++it;
            continue;
        }
        it = EvictPin(it, *pin);
    }
}

ui64 TCompartmentResidentCache::Pin(
    const TBridgeIdentity& key,
    const TUnboxedValue& owner,
    TStringRef bytes)
{
    if (bytes.Size() == 0) {
        return 0;
    }
    if (auto* existing = Pins_.FindPtr(key)) {
        Touch(key, *existing);
        return existing->Offset;
    }

    const ui64 length = bytes.Size();
    // The block is what the value occupies in linear memory, so it is what the
    // budget counts. Size classes are fine enough that the rounding does not
    // eat into what the budget is meant to hold.
    const ui64 charge = BlockSizeFor(length);
    if (charge > Budget_) {
        // A value larger than the whole budget is pinned regardless -- the
        // guest has no other way to see it -- and stays out of the budget
        // counter, or every later pin of the same Run would be refused for a
        // limit that was already blown. Only one at a time: nothing else
        // bounds what such pins put in linear memory.
        //
        // EvictFor never reaches one: it stops as soon as the budget fits and
        // the budget does not count these. Without a pass of its own, the one
        // an earlier Run left behind would keep every later oversized value
        // out of linear memory for the rest of the query.
        EvictOversized();
        if (OversizedBytes_ != 0) {
            ythrow yexception()
                << "Bridge: pin of " << length
                << " bytes is larger than the whole resident budget (" << Budget_
                << ") and another such pin is still resident";
        }
    } else {
        EvictFor(charge);
        // Eviction leaves alone every pin the current Run touched -- their
        // offsets are live -- so a Run that keeps pinning finds nothing to
        // give back and has to be refused here, or the budget would not hold
        // within a row.
        if (ResidentBytes() + charge > Budget_) {
            ythrow yexception()
                << "Bridge: pin of " << length
                << " bytes would exceed the resident budget ("
                << ResidentBytes() << " + " << charge << " > " << Budget_ << ")";
        }
    }

    TPin pin;
    pin.Owner = owner;
    pin.Offset = AllocBlock(length);
    pin.Length = length;
    pin.Charge = charge;
    pin.LastRun = CurrentRun_;
    WriteBytes(pin.Offset, bytes);

    const ui64 offset = pin.Offset;
    Lru_.push_back(key);
    pin.LruIt = std::prev(Lru_.end());
    PinnedBytes_ += charge;
    if (charge > Budget_) {
        OversizedBytes_ += charge;
    }
    Pins_.emplace(key, std::move(pin));
    return offset;
}

ui64 TCompartmentResidentCache::PinScratch(TStringRef bytes) {
    if (bytes.Size() == 0) {
        return 0;
    }
    const ui64 length = bytes.Size();
    const ui64 charge = BlockSizeFor(length);
    if (ResidentBytes() + charge > Budget_) {
        ythrow yexception()
            << "Bridge: scratch pin of " << length
            << " bytes would exceed the resident budget ("
            << ResidentBytes() << " + " << charge << " > " << Budget_ << ")";
    }
    const ui64 offset = AllocBlock(length);
    ScratchBlocks_.push_back(offset);
    ScratchBytes_ += charge;
    WriteBytes(offset, bytes);
    return offset;
}

ui64 TCompartmentResidentCache::GetUserData(const TBridgeIdentity& key) const {
    const auto* state = UserStates_.FindPtr(key);
    return state ? state->Value : 0;
}

void TCompartmentResidentCache::SetUserData(
    const TBridgeIdentity& key,
    const TUnboxedValue& owner,
    ui64 value)
{
    if (auto* existing = UserStates_.FindPtr(key)) {
        if (existing->Value != value) {
            QueueReleasedUserData(existing->Value);
        }
        existing->Value = value;
        UserStatesLru_.erase(existing->LruIt);
        UserStatesLru_.push_back(key);
        existing->LruIt = std::prev(UserStatesLru_.end());
        return;
    }

    while (UserStates_.size() >= MaxUserStates && !UserStatesLru_.empty()) {
        if (!ReleaseUserState(UserStatesLru_.front())) {
            UserStatesLru_.pop_front();
        }
    }

    TUserState state;
    state.Owner = owner;
    state.Value = value;
    UserStatesLru_.push_back(key);
    state.LruIt = std::prev(UserStatesLru_.end());
    UserStates_.emplace(key, std::move(state));
}

bool TCompartmentResidentCache::ReleaseUserState(TBridgeIdentity key) {
    auto* state = UserStates_.FindPtr(key);
    if (!state) {
        return false;
    }
    QueueReleasedUserData(state->Value);
    UserStatesLru_.erase(state->LruIt);
    UserStates_.erase(key);
    return true;
}

void TCompartmentResidentCache::QueueReleasedUserData(ui64 value) {
    if (value == 0) {
        return;
    }
    ReleasedUserData_.push_back(value);
    // A guest that never drains the queue would otherwise grow the host heap
    // for as long as the query lives, one entry per BridgeSetUserData.
    while (ReleasedUserData_.size() > MaxReleasedUserData) {
        ReleasedUserData_.pop_front();
        ++DroppedUserData_;
    }
}

bool TCompartmentResidentCache::PopReleasedUserData(ui64& value) {
    if (ReleasedUserData_.empty()) {
        return false;
    }
    value = ReleasedUserData_.front();
    ReleasedUserData_.pop_front();
    return true;
}

void TCompartmentResidentCache::BeginRun() {
    for (const ui64 offset : ScratchBlocks_) {
        Free(offset);
    }
    ScratchBlocks_.clear();
    ScratchBytes_ = 0;
    ++CurrentRun_;
}

} // namespace NKikimr::NUdfStore::NWasm
