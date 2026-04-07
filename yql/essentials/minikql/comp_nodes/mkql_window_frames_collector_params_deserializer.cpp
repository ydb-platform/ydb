#include "mkql_window_frames_collector_params_deserializer.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/core/sql_types/window_direction.h>
#include <yql/essentials/public/udf/udf_data_type.h>

namespace NKikimr::NMiniKQL {

namespace {

constexpr TStringBuf KeyMin = "Min";
constexpr TStringBuf KeyMax = "Max";
constexpr TStringBuf KeyRangeIntervals = "RangeIntervals";
constexpr TStringBuf KeyRowIntervals = "RowIntervals";
constexpr TStringBuf KeyRangeIncrementals = "RangeIncrementals";
constexpr TStringBuf KeyRowIncrementals = "RowIncrementals";
constexpr TStringBuf KeySortOrder = "SortOrder";
constexpr TStringBuf KeyBounds = "Bounds";
constexpr TStringBuf KeySortColumnName = "SortColumnName";
constexpr TStringBuf KeyDirection = "Direction";
constexpr TStringBuf KeyNumber = "Number";

TRuntimeNode GetMember(const TStructLiteral* structLit, TStringBuf name) {
    auto index = structLit->GetType()->FindMemberIndex(name);
    MKQL_ENSURE(index, "Member '" << name << "' not found");
    return structLit->GetValue(*index);
}

const TTupleLiteral* GetTuple(const TRuntimeNode& node) {
    return AS_VALUE(TTupleLiteral, node);
}

TString GetString(const TRuntimeNode& node) {
    return TString(AS_VALUE(TDataLiteral, node)->AsValue().AsStringRef());
}

template <typename T>
T GetValue(const TRuntimeNode& node) {
    return AS_VALUE(TDataLiteral, node)->AsValue().Get<T>();
}

// Get the data slot from a NumberAndDirection node.
// Returns Nothing if the bound is unbounded (void type).
TMaybe<NUdf::EDataSlot> GetDataSlotFromBound(const TRuntimeNode& boundNode) {
    auto structLit = AS_VALUE(TStructLiteral, boundNode);
    auto numberNode = GetMember(structLit, KeyNumber);
    MKQL_ENSURE(numberNode.GetStaticType(), "Static type expected.");
    if (numberNode.GetStaticType()->IsVoid()) {
        return Nothing();
    }
    auto dataType = AS_TYPE(TDataType, numberNode.GetStaticType());
    return *dataType->GetDataSlot();
}

template <typename TCallback>
auto DispatchByDataSlot(TMaybe<NUdf::EDataSlot> slot, TCallback&& callback) {
    if (!slot.Defined()) {
        return callback.template operator()<void>();
    }

    switch (*slot) {
        case NUdf::EDataSlot::Int8:
            return callback.template operator()<i8>();
        case NUdf::EDataSlot::Uint8:
            return callback.template operator()<ui8>();
        case NUdf::EDataSlot::Int16:
            return callback.template operator()<i16>();
        case NUdf::EDataSlot::Uint16:
            return callback.template operator()<ui16>();
        case NUdf::EDataSlot::Int32:
            return callback.template operator()<i32>();
        case NUdf::EDataSlot::Uint32:
            return callback.template operator()<ui32>();
        case NUdf::EDataSlot::Int64:
            return callback.template operator()<i64>();
        case NUdf::EDataSlot::Uint64:
            return callback.template operator()<ui64>();
        case NUdf::EDataSlot::Float:
            return callback.template operator()<float>();
        case NUdf::EDataSlot::Double:
            return callback.template operator()<double>();
        case NUdf::EDataSlot::Interval64:
            return callback.template operator()<NUdf::TDataType<NUdf::TInterval64>::TLayout>();
        case NUdf::EDataSlot::Interval:
            return callback.template operator()<NUdf::TDataType<NUdf::TInterval>::TLayout>();
        default:
            ythrow yexception() << "Unsupported data slot for range type: " << *slot;
    }
}

TVariantBound DeserializeBoundAsVariant(const TRuntimeNode& boundNode) {
    auto structLit = AS_VALUE(TStructLiteral, boundNode);

    EDirection direction;
    MKQL_ENSURE(NYql::NWindow::TryParseDirectionFromString(GetString(GetMember(structLit, KeyDirection)), direction), "Unknown direction");

    auto numberNode = GetMember(structLit, KeyNumber);
    MKQL_ENSURE(numberNode.GetStaticType(), "Static type expected.");

    TMaybe<NUdf::EDataSlot> slot = GetDataSlotFromBound(boundNode);

    return DispatchByDataSlot(slot, [&]<typename TRangeType>() -> TVariantBound {
        // Void means unbounded.
        if constexpr (std::is_void_v<TRangeType>) {
            return TVariantBound::Inf(direction);
        } else {
            TRangeType value = GetValue<TRangeType>(numberNode);
            return TVariantBound(TRangeVariant(value), direction);
        }
    });
}

// Deserialize a WindowFrame (with Min and Max bounds) into variant-based representation
TWindowFrame<TVariantBound> DeserializeWindowFrameAsVariant(const TRuntimeNode& frameNode) {
    auto structLit = AS_VALUE(TStructLiteral, frameNode);

    TVariantBound minBound = DeserializeBoundAsVariant(GetMember(structLit, KeyMin));
    TVariantBound maxBound = DeserializeBoundAsVariant(GetMember(structLit, KeyMax));

    return {std::move(minBound), std::move(maxBound)};
}

template <typename T>
TNumberAndDirection<T> DeserializeNumberAndDirection(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);

    EDirection direction;
    MKQL_ENSURE(NYql::NWindow::TryParseDirectionFromString(GetString(GetMember(structLit, KeyDirection)), direction), "Unknown direction");

    auto boundLiteral = GetMember(structLit, KeyNumber);
    MKQL_ENSURE(boundLiteral.GetStaticType(), "Static type expected.");
    if (boundLiteral.GetStaticType()->IsVoid()) {
        return TNumberAndDirection<T>::Inf(direction);
    } else {
        return TNumberAndDirection<T>(GetValue<T>(boundLiteral), direction);
    }
}

template <typename T>
TWindowFrame<TNumberAndDirection<T>> DeserializeWindowFrame(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    return {DeserializeNumberAndDirection<T>(GetMember(structLit, KeyMin)),
            DeserializeNumberAndDirection<T>(GetMember(structLit, KeyMax))};
}

TVariantBounds DeserializeBoundsAsVariantImpl(const TRuntimeNode& boundsNode) {
    auto structLit = AS_VALUE(TStructLiteral, boundsNode);

    // No deduplication is allowed here. We must add as much bounds as provided by |node|.
    TVariantBounds bounds;

    // Deserialize range intervals
    auto rangeIntervalsTuple = GetTuple(GetMember(structLit, KeyRangeIntervals));
    for (ui32 i = 0; i < rangeIntervalsTuple->GetValuesCount(); ++i) {
        bounds.AddRange(DeserializeWindowFrameAsVariant(rangeIntervalsTuple->GetValue(i)));
    }

    // Row intervals don't need variant - pass through as-is
    auto rowIntervalsTuple = GetTuple(GetMember(structLit, KeyRowIntervals));
    for (ui32 i = 0; i < rowIntervalsTuple->GetValuesCount(); ++i) {
        bounds.AddRow(DeserializeWindowFrame<ui64>(rowIntervalsTuple->GetValue(i)));
    }

    // Deserialize range incrementals
    auto rangeIncrementalsTuple = GetTuple(GetMember(structLit, KeyRangeIncrementals));
    for (ui32 i = 0; i < rangeIncrementalsTuple->GetValuesCount(); ++i) {
        bounds.AddRangeIncremental(DeserializeBoundAsVariant(rangeIncrementalsTuple->GetValue(i)));
    }

    // Row incrementals don't need variant - pass through as-is
    auto rowIncrementalsTuple = GetTuple(GetMember(structLit, KeyRowIncrementals));
    for (ui32 i = 0; i < rowIncrementalsTuple->GetValuesCount(); ++i) {
        bounds.AddRowIncremental(DeserializeNumberAndDirection<ui64>(rowIncrementalsTuple->GetValue(i)));
    }

    return bounds;
}

TVariantBounds DeserializeBoundsAsVariant(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    auto boundsNode = GetMember(structLit, KeyBounds);
    return DeserializeBoundsAsVariantImpl(boundsNode);
}

} // anonymous namespace

ESortOrder DeserializeSortOrder(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    ESortOrder sortOrder;
    MKQL_ENSURE(TryParseSortOrderFromString(GetString(GetMember(structLit, KeySortOrder)), sortOrder), "Unknown sort order");
    return sortOrder;
}

TString DeserializeSortColumnName(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    return GetString(GetMember(structLit, KeySortColumnName));
}

template <typename TStreamElement>
TComparatorBounds<TStreamElement> DeserializeBounds(const TRuntimeNode& node, ESortOrder sortOrder) {
    TVariantBounds variantBounds = DeserializeBoundsAsVariant(node);
    return ConvertBoundsToComparators<TStreamElement>(variantBounds, sortOrder);
}

template TComparatorBounds<i8> DeserializeBounds<i8>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<ui8> DeserializeBounds<ui8>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<i16> DeserializeBounds<i16>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<ui16> DeserializeBounds<ui16>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<i32> DeserializeBounds<i32>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<ui32> DeserializeBounds<ui32>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<i64> DeserializeBounds<i64>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<ui64> DeserializeBounds<ui64>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<float> DeserializeBounds<float>(const TRuntimeNode&, ESortOrder);
template TComparatorBounds<double> DeserializeBounds<double>(const TRuntimeNode&, ESortOrder);

bool AnyRangeProvided(const TRuntimeNode& node) {
    auto boundsLit = AS_VALUE(TStructLiteral, GetMember(AS_VALUE(TStructLiteral, node), KeyBounds));
    auto rangeIntervals = GetTuple(GetMember(boundsLit, KeyRangeIntervals))->GetValuesCount();
    auto rangeIncrementals = GetTuple(GetMember(boundsLit, KeyRangeIncrementals))->GetValuesCount();

    return rangeIntervals > 0 || rangeIncrementals > 0;
}

} // namespace NKikimr::NMiniKQL
