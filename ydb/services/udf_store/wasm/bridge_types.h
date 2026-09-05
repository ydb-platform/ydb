#pragma once

#include <util/generic/yexception.h>
#include <util/system/types.h>

namespace NKikimr::NUdfStore::NWasm {

//! Kind of a node stored in TWasmBridgeNodeTable (host-side).
enum class EBridgeNodeKind: ui8 {
    Unknown = 0,
    Scalar,
    String,
    Optional,
    List,
    ListIterator,
    Dict,
    DictIterator,
    Resource,
    Callable,
    Tuple,
    Struct,
    Variant,
    //! Carries a TType only (no value): lets the guest name the type of a
    //! container it wants the host to build.
    TypeRef,
};

//! Value kind exposed to guest via BridgeGetKind (shared ABI tag).
//! Numbers are frozen: already compiled guest modules switch on them.
enum class EBridgeValueKind: i32 {
    Null = 0,
    Int64 = 1,
    Uint64 = 2,
    Double = 3,
    Boolean = 4,
    String = 5,
    Optional = 6,
    List = 7,
    Dict = 8,
    Resource = 9,
    Callable = 10,
    Tuple = 11,
    Struct = 12,
    Variant = 13,
    Int8 = 14,
    Uint8 = 15,
    Int16 = 16,
    Uint16 = 17,
    Int32 = 18,
    Uint32 = 19,
    Float = 20,
    Utf8 = 21,
    Yson = 22,
    Json = 23,
    Date = 24,
    Datetime = 25,
    Timestamp = 26,
    Interval = 27,
    Decimal = 28,
};

//! Readable through the string intrinsics (length / copy / ensure).
inline bool IsBridgeStringKind(EBridgeValueKind kind) {
    switch (kind) {
        case EBridgeValueKind::String:
        case EBridgeValueKind::Utf8:
        case EBridgeValueKind::Yson:
        case EBridgeValueKind::Json:
            return true;
        default:
            return false;
    }
}

//! Handle layout: (generation << 32) | index. 0 is the null handle.
inline constexpr ui64 NullBridgeHandle = 0;

//! Both halves of a handle are 32 bits wide, and generation 0 is reserved so
//! that a packed handle can never come out equal to NullBridgeHandle.
inline constexpr ui64 MaxBridgeGeneration = 0xffffffffULL;
inline constexpr ui64 MaxBridgeNodeIndex = 0xffffffffULL;

inline ui64 PackBridgeHandle(ui64 generation, ui64 index) {
    if (generation == 0 || generation > MaxBridgeGeneration || index > MaxBridgeNodeIndex) {
        ythrow yexception()
            << "Bridge: cannot pack handle generation=" << generation
            << " index=" << index;
    }
    return (generation << 32) | index;
}

//! Map a monotonic ticket onto the 1..MaxBridgeGeneration range the handle
//! layout can carry. A generation only has to tell the live node table apart
//! from recently dead ones, so wrapping after four billion compartment
//! acquires is fine -- far more than can ever be in flight together -- while
//! letting the counter run past 32 bits would fail PackBridgeHandle instead.
inline ui64 BridgeGenerationFromTicket(ui64 ticket) {
    return ticket % MaxBridgeGeneration + 1;
}

inline ui64 BridgeHandleGeneration(ui64 handle) {
    return handle >> 32;
}

inline ui64 BridgeHandleIndex(ui64 handle) {
    return handle & MaxBridgeNodeIndex;
}

} // namespace NKikimr::NUdfStore::NWasm
