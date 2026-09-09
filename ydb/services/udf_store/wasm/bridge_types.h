#pragma once

#include <util/digest/multi.h>
#include <util/generic/yexception.h>
#include <util/system/types.h>

namespace NKikimr::NUdfStore::NWasm {

//! Stable identity of a value for cross-row reuse: a boxed object, or one
//! view over a refcounted string buffer. MiniKQL builds a substring as a new
//! offset and size over the very same buffer, so two views share Base while
//! meaning different bytes: keying on the buffer alone would hand the guest
//! one node, one pin and one user-data for both.
struct TBridgeIdentity {
    //! IBoxedValue* or TStringValue::TData*.
    const void* Base = nullptr;
    //! Start of the string view; nullptr for boxed values.
    const void* Data = nullptr;
    //! Size of the string view; 0 for boxed values.
    ui32 Size = 0;

    explicit operator bool() const {
        return Base != nullptr;
    }

    bool operator==(const TBridgeIdentity& other) const = default;
};

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

//! Coarse grouping of value kinds: what the host can tell about a value
//! without walking into it. Enough to catch a guest that returns a string
//! where a list was declared, while leaving the exact data slot (Int64 vs
//! Uint32, String vs Utf8) to MiniKQL, which stores those the same way.
enum class EBridgeKindFamily: ui8 {
    Null,
    Number,
    String,
    Optional,
    List,
    Dict,
    Tuple,
    Struct,
    Variant,
    Resource,
    Callable,
};

inline EBridgeKindFamily BridgeKindFamily(EBridgeValueKind kind) {
    switch (kind) {
        case EBridgeValueKind::Null:
            return EBridgeKindFamily::Null;
        case EBridgeValueKind::Int64:
        case EBridgeValueKind::Uint64:
        case EBridgeValueKind::Double:
        case EBridgeValueKind::Boolean:
        case EBridgeValueKind::Int8:
        case EBridgeValueKind::Uint8:
        case EBridgeValueKind::Int16:
        case EBridgeValueKind::Uint16:
        case EBridgeValueKind::Int32:
        case EBridgeValueKind::Uint32:
        case EBridgeValueKind::Float:
        case EBridgeValueKind::Date:
        case EBridgeValueKind::Datetime:
        case EBridgeValueKind::Timestamp:
        case EBridgeValueKind::Interval:
        case EBridgeValueKind::Decimal:
            return EBridgeKindFamily::Number;
        case EBridgeValueKind::String:
        case EBridgeValueKind::Utf8:
        case EBridgeValueKind::Yson:
        case EBridgeValueKind::Json:
            return EBridgeKindFamily::String;
        case EBridgeValueKind::Optional:
            return EBridgeKindFamily::Optional;
        case EBridgeValueKind::List:
            return EBridgeKindFamily::List;
        case EBridgeValueKind::Dict:
            return EBridgeKindFamily::Dict;
        case EBridgeValueKind::Tuple:
            return EBridgeKindFamily::Tuple;
        case EBridgeValueKind::Struct:
            return EBridgeKindFamily::Struct;
        case EBridgeValueKind::Variant:
            return EBridgeKindFamily::Variant;
        case EBridgeValueKind::Resource:
            return EBridgeKindFamily::Resource;
        case EBridgeValueKind::Callable:
            return EBridgeKindFamily::Callable;
    }
    return EBridgeKindFamily::Null;
}

//! Families MiniKQL keeps in a box. An Optional over one of these is
//! represented as the payload itself, so the two cannot be told apart; every
//! other family keeps its own representation under an Optional.
inline bool IsBridgeBoxedFamily(EBridgeKindFamily family) {
    switch (family) {
        case EBridgeKindFamily::List:
        case EBridgeKindFamily::Dict:
        case EBridgeKindFamily::Tuple:
        case EBridgeKindFamily::Struct:
        case EBridgeKindFamily::Variant:
        case EBridgeKindFamily::Resource:
        case EBridgeKindFamily::Callable:
            return true;
        default:
            return false;
    }
}

inline const char* BridgeKindFamilyAsStr(EBridgeKindFamily family) {
    switch (family) {
        case EBridgeKindFamily::Null:
            return "null";
        case EBridgeKindFamily::Number:
            return "scalar";
        case EBridgeKindFamily::String:
            return "string";
        case EBridgeKindFamily::Optional:
            return "optional";
        case EBridgeKindFamily::List:
            return "list";
        case EBridgeKindFamily::Dict:
            return "dict";
        case EBridgeKindFamily::Tuple:
            return "tuple";
        case EBridgeKindFamily::Struct:
            return "struct";
        case EBridgeKindFamily::Variant:
            return "variant";
        case EBridgeKindFamily::Resource:
            return "resource";
        case EBridgeKindFamily::Callable:
            return "callable";
    }
    return "unknown";
}

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

template <>
struct THash<NKikimr::NUdfStore::NWasm::TBridgeIdentity> {
    size_t operator()(const NKikimr::NUdfStore::NWasm::TBridgeIdentity& identity) const {
        return MultiHash(identity.Base, identity.Data, identity.Size);
    }
};
