#pragma once

#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/public/udf/udf_type_ops.h>

#include <util/system/unaligned_mem.h>
#include <util/system/yassert.h>

#include <cstring>

namespace NKikimr::NMiniKQL {
namespace NDqHashCombineLayoutPrivate {

using NUdf::EDataSlot;
using NUdf::TUnboxedValuePod;

template <typename T>
Y_FORCE_INLINE bool EqualNative(const void* left, const void* right, size_t offset) {
    return ReadUnaligned<T>(static_cast<const char*>(left) + offset) ==
        ReadUnaligned<T>(static_cast<const char*>(right) + offset);
}

template <typename T>
Y_FORCE_INLINE bool EqualNativeFloat(const void* left, const void* right, size_t offset) {
    const TUnboxedValuePod lhs(ReadUnaligned<T>(static_cast<const char*>(left) + offset));
    const TUnboxedValuePod rhs(ReadUnaligned<T>(static_cast<const char*>(right) + offset));
    return NUdf::EquateFloats<T>(lhs, rhs);
}

Y_FORCE_INLINE bool EqualNative(EDataSlot slot, const void* left, const void* right, size_t offset) {
    switch (slot) {
        case EDataSlot::Timestamp:
        case EDataSlot::Uint64: return EqualNative<ui64>(left, right, offset);
        case EDataSlot::Interval:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
        case EDataSlot::Int64: return EqualNative<i64>(left, right, offset);
        case EDataSlot::Double: return EqualNativeFloat<double>(left, right, offset);
        case EDataSlot::Datetime:
        case EDataSlot::Uint32: return EqualNative<ui32>(left, right, offset);
        case EDataSlot::Date32:
        case EDataSlot::Int32: return EqualNative<i32>(left, right, offset);
        case EDataSlot::Float: return EqualNativeFloat<float>(left, right, offset);
        case EDataSlot::Date:
        case EDataSlot::Uint16: return EqualNative<ui16>(left, right, offset);
        case EDataSlot::Int16: return EqualNative<i16>(left, right, offset);
        default: Y_ABORT("Unexpected native data slot");
    }
}

template <typename T>
Y_FORCE_INLINE bool EqualNativeLogical(const void* packed, size_t offset, const TUnboxedValuePod& logical) {
    return ReadUnaligned<T>(static_cast<const char*>(packed) + offset) == logical.Get<T>();
}

template <typename T>
Y_FORCE_INLINE bool EqualNativeFloatLogical(const void* packed, size_t offset, const TUnboxedValuePod& logical) {
    const TUnboxedValuePod value(ReadUnaligned<T>(static_cast<const char*>(packed) + offset));
    return NUdf::EquateFloats<T>(value, logical);
}

Y_FORCE_INLINE bool EqualNativeLogical(EDataSlot slot, const void* packed, size_t offset,
    const TUnboxedValuePod& logical)
{
    switch (slot) {
        case EDataSlot::Timestamp:
        case EDataSlot::Uint64: return EqualNativeLogical<ui64>(packed, offset, logical);
        case EDataSlot::Interval:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
        case EDataSlot::Int64: return EqualNativeLogical<i64>(packed, offset, logical);
        case EDataSlot::Double: return EqualNativeFloatLogical<double>(packed, offset, logical);
        case EDataSlot::Datetime:
        case EDataSlot::Uint32: return EqualNativeLogical<ui32>(packed, offset, logical);
        case EDataSlot::Date32:
        case EDataSlot::Int32: return EqualNativeLogical<i32>(packed, offset, logical);
        case EDataSlot::Float: return EqualNativeFloatLogical<float>(packed, offset, logical);
        case EDataSlot::Date:
        case EDataSlot::Uint16: return EqualNativeLogical<ui16>(packed, offset, logical);
        case EDataSlot::Int16: return EqualNativeLogical<i16>(packed, offset, logical);
        default: Y_ABORT("Unexpected native data slot");
    }
}

} // namespace NDqHashCombineLayoutPrivate

Y_FORCE_INLINE bool TDqHashCombineTupleLayout::IsPresent(const void* storage, const TItem& item) const {
    return ReadUnaligned<ui32>(static_cast<const char*>(storage) + item.ValidityOffset) & item.ValidityMask;
}

Y_FORCE_INLINE void TDqHashCombineTupleLayout::SetPresent(void* storage, const TItem& item) const {
    char* wordPtr = static_cast<char*>(storage) + item.ValidityOffset;
    WriteUnaligned<ui32>(wordPtr, ReadUnaligned<ui32>(wordPtr) | item.ValidityMask);
}

template <typename T>
Y_FORCE_INLINE void TDqHashCombineTupleLayout::PackNativeItem(
    const TItem& item, const NUdf::TUnboxedValuePod& value, void* storage) const
{
    if (item.ValidityMask) {
        if (!value.HasValue()) {
            WriteUnaligned<T>(static_cast<char*>(storage) + item.Offset, T{});
            return;
        }
        SetPresent(storage, item);
    }
    MKQL_ENSURE(value.HasValue(), "Empty value for required native column " << item.LogicalIndex);
    std::memcpy(static_cast<char*>(storage) + item.Offset, value.GetRawPtr(), sizeof(T));
}

template <typename T>
Y_FORCE_INLINE NUdf::TUnboxedValuePod TDqHashCombineTupleLayout::UnpackNativeItem(
    const TItem& item, const void* storage) const
{
    if (item.ValidityMask && !IsPresent(storage, item)) {
        return {};
    }
    // All supported native types share the embedded marker and payload offset
    NUdf::TUnboxedValuePod value(ui64{0});
    std::memcpy(value.GetRawPtr(), static_cast<const char*>(storage) + item.Offset, sizeof(T));
    return value;
}

Y_FORCE_INLINE void TDqHashCombineTupleLayout::PackBorrowed(
    TArrayRef<const NUdf::TUnboxedValuePod> values, void* storage) const
{
    Y_ABORT_UNLESS(values.size() == Items.size());
    if (ValidityWordCount) {
        std::memset(static_cast<char*>(storage) + ValidityOffset, 0, ValidityWordCount * sizeof(ui32));
    }
    auto* unboxed = static_cast<NUdf::TUnboxedValuePod*>(storage);
    for (size_t i = 0; i < UnboxedCount; ++i) {
        unboxed[i] = values[Items[i].LogicalIndex];
    }
    const size_t native64End = UnboxedCount + Native64Count;
    for (size_t i = UnboxedCount; i < native64End; ++i) {
        const auto& item = Items[i];
        PackNativeItem<ui64>(item, values[item.LogicalIndex], storage);
    }
    const size_t native32End = native64End + Native32Count;
    for (size_t i = native64End; i < native32End; ++i) {
        const auto& item = Items[i];
        PackNativeItem<ui32>(item, values[item.LogicalIndex], storage);
    }
    for (size_t i = native32End; i < Items.size(); ++i) {
        const auto& item = Items[i];
        PackNativeItem<ui16>(item, values[item.LogicalIndex], storage);
    }
}

Y_FORCE_INLINE void TDqHashCombineTupleLayout::PackWithRefs(
    TArrayRef<const NUdf::TUnboxedValuePod> values, void* storage) const
{
    try {
        PackBorrowed(values, storage);
    } catch (...) {
        // The copied values have not acquired references yet
        Clear(storage);
        throw;
    }
    auto* unboxed = static_cast<NUdf::TUnboxedValuePod*>(storage);
    for (size_t i = 0; i < UnboxedCount; ++i) {
        unboxed[i].Ref();
    }
}

Y_FORCE_INLINE void TDqHashCombineTupleLayout::PackMove(
    TArrayRef<NUdf::TUnboxedValue> values, void* storage) const
{
    Y_ABORT_UNLESS(values.size() == Items.size());
    PackMoveFrom(storage, [&](size_t index) { return std::move(values[index]); });
}

template <typename TGetValue>
Y_FORCE_INLINE void TDqHashCombineTupleLayout::PackMoveFrom(void* storage, TGetValue&& getValue) const {
    if (ValidityWordCount) {
        std::memset(static_cast<char*>(storage) + ValidityOffset, 0, ValidityWordCount * sizeof(ui32));
    }
    for (const auto& item : LogicalItems) {
        auto value = getValue(item.LogicalIndex);
        auto& pod = static_cast<NUdf::TUnboxedValuePod&>(value);
        if (item.Storage == EStorage::Unboxed) {
            *reinterpret_cast<NUdf::TUnboxedValuePod*>(static_cast<char*>(storage) + item.Offset) = pod;
        } else if (item.Storage == EStorage::Native64) {
            PackNativeItem<ui64>(item, value, storage);
        } else if (item.Storage == EStorage::Native32) {
            PackNativeItem<ui32>(item, value, storage);
        } else {
            PackNativeItem<ui16>(item, value, storage);
        }
        pod = NUdf::TUnboxedValuePod{};
    }
}

template <typename TGetValue>
Y_FORCE_INLINE void TDqHashCombineTupleLayout::PackMoveReplacingFrom(void* storage, TGetValue&& getValue) const {
    for (const auto& item : LogicalItems) {
        auto value = getValue(item.LogicalIndex);
        auto& pod = static_cast<NUdf::TUnboxedValuePod&>(value);
        if (item.Storage == EStorage::Unboxed) {
            auto* old = reinterpret_cast<NUdf::TUnboxedValuePod*>(static_cast<char*>(storage) + item.Offset);
            old->UnRef();
            *old = pod;
        } else {
            if (item.ValidityMask && !value.HasValue()) {
                char* wordPtr = static_cast<char*>(storage) + item.ValidityOffset;
                WriteUnaligned<ui32>(wordPtr, ReadUnaligned<ui32>(wordPtr) & ~item.ValidityMask);
            }
            if (item.Storage == EStorage::Native64) {
                PackNativeItem<ui64>(item, value, storage);
            } else if (item.Storage == EStorage::Native32) {
                PackNativeItem<ui32>(item, value, storage);
            } else {
                PackNativeItem<ui16>(item, value, storage);
            }
        }
        pod = NUdf::TUnboxedValuePod{};
    }
}

template <typename TGetDestination>
Y_FORCE_INLINE void TDqHashCombineTupleLayout::UnpackCopyTo(
    const void* storage, TGetDestination&& getDestination) const
{
    const auto* unboxed = static_cast<const NUdf::TUnboxedValuePod*>(storage);
    for (size_t i = 0; i < UnboxedCount; ++i) {
        auto& destination = getDestination(Items[i].LogicalIndex);
        unboxed[i].Ref();
        destination.UnRef();
        static_cast<NUdf::TUnboxedValuePod&>(destination) = unboxed[i];
    }
    const size_t native64End = UnboxedCount + Native64Count;
    for (size_t i = UnboxedCount; i < native64End; ++i) {
        const auto& item = Items[i];
        auto& destination = getDestination(item.LogicalIndex);
        destination.UnRef();
        static_cast<NUdf::TUnboxedValuePod&>(destination) = UnpackNativeItem<ui64>(item, storage);
    }
    const size_t native32End = native64End + Native32Count;
    for (size_t i = native64End; i < native32End; ++i) {
        const auto& item = Items[i];
        auto& destination = getDestination(item.LogicalIndex);
        destination.UnRef();
        static_cast<NUdf::TUnboxedValuePod&>(destination) = UnpackNativeItem<ui32>(item, storage);
    }
    for (size_t i = native32End; i < Items.size(); ++i) {
        const auto& item = Items[i];
        auto& destination = getDestination(item.LogicalIndex);
        destination.UnRef();
        static_cast<NUdf::TUnboxedValuePod&>(destination) = UnpackNativeItem<ui16>(item, storage);
    }
}

Y_FORCE_INLINE void TDqHashCombineTupleLayout::UnpackMove(
    void* storage, TArrayRef<NUdf::TUnboxedValue> values) const
{
    Y_ABORT_UNLESS(values.size() == Items.size());
    UnpackMoveTo(storage, [&](size_t index, NUdf::TUnboxedValue&& value) {
        values[index] = std::move(value);
    });
}

template <typename TSetValue>
Y_FORCE_INLINE void TDqHashCombineTupleLayout::UnpackMoveTo(void* storage, TSetValue&& setValue) const {
    auto* unboxed = static_cast<NUdf::TUnboxedValuePod*>(storage);
    for (size_t i = 0; i < UnboxedCount; ++i) {
        NUdf::TUnboxedValue value;
        static_cast<NUdf::TUnboxedValuePod&>(value) = unboxed[i];
        unboxed[i] = NUdf::TUnboxedValuePod{};
        setValue(Items[i].LogicalIndex, std::move(value));
    }
    const size_t native64End = UnboxedCount + Native64Count;
    for (size_t i = UnboxedCount; i < native64End; ++i) {
        const auto& item = Items[i];
        setValue(item.LogicalIndex, NUdf::TUnboxedValue(UnpackNativeItem<ui64>(item, storage)));
    }
    const size_t native32End = native64End + Native32Count;
    for (size_t i = native64End; i < native32End; ++i) {
        const auto& item = Items[i];
        setValue(item.LogicalIndex, NUdf::TUnboxedValue(UnpackNativeItem<ui32>(item, storage)));
    }
    for (size_t i = native32End; i < Items.size(); ++i) {
        const auto& item = Items[i];
        setValue(item.LogicalIndex, NUdf::TUnboxedValue(UnpackNativeItem<ui16>(item, storage)));
    }
}

Y_FORCE_INLINE bool TDqHashCombineTupleLayout::Equals(const void* left, const void* right) const {
    for (const auto& item : Items) {
        if (item.Storage == EStorage::Unboxed) {
            const auto& lhs = *reinterpret_cast<const NUdf::TUnboxedValuePod*>(
                static_cast<const char*>(left) + item.Offset);
            const auto& rhs = *reinterpret_cast<const NUdf::TUnboxedValuePod*>(
                static_cast<const char*>(right) + item.Offset);
            Y_ABORT_UNLESS(item.DataSlot, "DqHashCombine key must have a data slot");
            if (item.Optional && (!lhs || !rhs)) {
                if (bool(lhs) != bool(rhs)) {
                    return false;
                }
                continue;
            }
            if (NUdf::CompareValues(*item.DataSlot, lhs, rhs)) {
                return false;
            }
            continue;
        }

        if (item.Optional) {
            const bool lhsPresent = IsPresent(left, item);
            const bool rhsPresent = IsPresent(right, item);
            if (lhsPresent != rhsPresent) {
                return false;
            }
            if (!lhsPresent) {
                continue;
            }
        }
        if (!NDqHashCombineLayoutPrivate::EqualNative(*item.DataSlot, left, right, item.Offset)) {
            return false;
        }
    }
    return true;
}

Y_FORCE_INLINE bool TDqHashCombineTupleLayout::EqualsLogical(
    const void* packed, TArrayRef<const NUdf::TUnboxedValuePod> logical) const
{
    Y_ABORT_UNLESS(logical.size() == Items.size());
    for (const auto& item : Items) {
        const auto& value = logical[item.LogicalIndex];
        if (item.Storage == EStorage::Unboxed) {
            const auto& stored = *reinterpret_cast<const NUdf::TUnboxedValuePod*>(
                static_cast<const char*>(packed) + item.Offset);
            Y_ABORT_UNLESS(item.DataSlot, "DqHashCombine key must have a data slot");
            if (item.Optional && (!stored || !value)) {
                if (bool(stored) != bool(value)) {
                    return false;
                }
                continue;
            }
            if (NUdf::CompareValues(*item.DataSlot, stored, value)) {
                return false;
            }
            continue;
        }

        if (item.Optional) {
            const bool storedPresent = IsPresent(packed, item);
            const bool valuePresent = value.HasValue();
            if (storedPresent != valuePresent) {
                return false;
            }
            if (!storedPresent) {
                continue;
            }
        }
        if (!NDqHashCombineLayoutPrivate::EqualNativeLogical(*item.DataSlot, packed, item.Offset, value)) {
            return false;
        }
    }
    return true;
}

} // namespace NKikimr::NMiniKQL
