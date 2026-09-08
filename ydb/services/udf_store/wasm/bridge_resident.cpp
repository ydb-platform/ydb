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
//! round to whole megabytes so a 10 MiB blob does not reserve 16 MiB.
constexpr ui64 PowerOfTwoLimit = 1ull << 20;
constexpr ui64 ArenaChunkSize = 4ull << 20;
constexpr ui64 WasmPageSize = 64ull << 10;
//! Distinct values whose guest state we remember. Beyond that the oldest one
//! is dropped and its user-data handed back to the guest to free.
constexpr size_t MaxUserStates = 1024;

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
    if (!Compartment_->ReserveGuestHeapBelow(offset + chunk)) {
        ythrow yexception()
            << "Bridge: cannot fence " << chunk << " resident bytes at " << offset
            << " off the guest heap; the runtime library must export \"sbrk\"";
    }
    // The bump pointer can only live in one chunk, so the tail of the old one
    // is gone unless it goes back through the free lists. Every block is a
    // multiple of MinBlockSize, so the tail is too and it carves into whole
    // size classes exactly.
    while (BumpRemaining_ >= MinBlockSize) {
        const ui64 blockSize = Min(std::bit_floor(BumpRemaining_), PowerOfTwoLimit);
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
    if (length > Budget_) {
        ythrow yexception()
            << "Bridge: BridgeAllocResident of " << length
            << " bytes is larger than the whole resident budget (" << Budget_ << ")";
    }
    const ui64 blockSize = BlockSizeFor(length);
    if (ResidentBytes() + blockSize > Budget_) {
        ythrow yexception()
            << "Bridge: BridgeAllocResident of " << length
            << " bytes would exceed the resident budget ("
            << ResidentBytes() << " + " << blockSize << " > " << Budget_ << ")";
    }
    const ui64 offset = Alloc(length);
    if (offset != 0) {
        GuestBlocks_.insert(offset);
        GuestBytes_ += blockSize;
    }
    return offset;
}

void TCompartmentResidentCache::FreeGuest(ui64 offset) {
    if (offset == 0) {
        return;
    }
    if (!GuestBlocks_.contains(offset)) {
        ythrow yexception()
            << "Bridge: BridgeFreeResident on offset " << offset
            << ", which was not returned by BridgeAllocResident";
    }
    auto it = Blocks_.find(offset);
    if (it == Blocks_.end()) {
        ythrow yexception()
            << "Bridge: resident free of unknown or already freed offset " << offset;
    }
    Y_ENSURE(GuestBytes_ >= it->second);
    GuestBytes_ -= it->second;
    GuestBlocks_.erase(offset);
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

void TCompartmentResidentCache::EvictFor(ui64 length) {
    for (auto it = Lru_.begin(); it != Lru_.end() && ResidentBytes() + length > Budget_;) {
        auto* pin = Pins_.FindPtr(*it);
        if (!pin || pin->LastRun == CurrentRun_) {
            // In use by the Run that is running right now: its offset is live.
            ++it;
            continue;
        }
        Free(pin->Offset);
        PinnedBytes_ -= pin->BlockSize;
        ++Evictions_;
        // The guest keyed its own state on the same identity, and that state
        // usually points into the block that just went away. Hand it back the
        // way the user-data LRU does instead of leaving the guest to read an
        // offset whose bytes now belong to another pin.
        const TBridgeIdentity key = *it;
        ReleaseUserState(key);
        Pins_.erase(key);
        it = Lru_.erase(it);
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

    const ui64 blockSize = BlockSizeFor(bytes.Size());
    // Budget is tracked in BlockSize units, so compare like with like.
    EvictFor(blockSize);

    TPin pin;
    pin.Owner = owner;
    pin.Offset = AllocBlock(bytes.Size());
    pin.Length = bytes.Size();
    pin.BlockSize = blockSize;
    pin.LastRun = CurrentRun_;
    WriteBytes(pin.Offset, bytes);

    const ui64 offset = pin.Offset;
    Lru_.push_back(key);
    pin.LruIt = std::prev(Lru_.end());
    PinnedBytes_ += pin.BlockSize;
    Pins_.emplace(key, std::move(pin));
    return offset;
}

ui64 TCompartmentResidentCache::PinScratch(TStringRef bytes) {
    if (bytes.Size() == 0) {
        return 0;
    }
    const ui64 blockSize = BlockSizeFor(bytes.Size());
    if (ResidentBytes() + blockSize > Budget_) {
        ythrow yexception()
            << "Bridge: scratch pin of " << bytes.Size()
            << " bytes would exceed the resident budget ("
            << ResidentBytes() << " + " << blockSize << " > " << Budget_ << ")";
    }
    const ui64 offset = AllocBlock(bytes.Size());
    ScratchBlocks_.push_back(offset);
    ScratchBytes_ += blockSize;
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
        if (existing->Value != value && existing->Value != 0) {
            ReleasedUserData_.push_back(existing->Value);
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
    if (state->Value != 0) {
        ReleasedUserData_.push_back(state->Value);
    }
    UserStatesLru_.erase(state->LruIt);
    UserStates_.erase(key);
    return true;
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
