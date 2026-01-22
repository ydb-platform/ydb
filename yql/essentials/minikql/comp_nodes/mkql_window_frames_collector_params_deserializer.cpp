#include "mkql_window_frames_collector_params_deserializer.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/core/sql_types/window_direction.h>

namespace NKikimr::NMiniKQL {

using namespace NYql::NWindow;
using NYql::ESortOrder;

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
constexpr TStringBuf KeyUnbounded = "Unbounded";
constexpr TStringBuf KeyBounded = "Bounded";

TRuntimeNode GetMember(const TStructLiteral* structLit, TStringBuf name) {
    auto index = structLit->GetType()->FindMemberIndex(name);
    MKQL_ENSURE(index, "Member '" << name << "' not found");
    return structLit->GetValue(*index);
}

const TListLiteral* GetList(const TRuntimeNode& node) {
    return node.GetStaticType()->IsEmptyList() ? nullptr : AS_VALUE(TListLiteral, node);
}

TString GetString(const TRuntimeNode& node) {
    return TString(AS_VALUE(TDataLiteral, node)->AsValue().AsStringRef());
}

template <typename T>
T GetValue(const TRuntimeNode& node) {
    return AS_VALUE(TDataLiteral, node)->AsValue().Get<T>();
}

template <typename T>
TNumberAndDirection<T> DeserializeNumberAndDirection(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);

    EDirection direction;
    MKQL_ENSURE(NYql::NWindow::TryParseDirectionFromString(GetString(GetMember(structLit, KeyDirection)), direction), "Unknown direction");

    auto variantLit = AS_VALUE(TVariantLiteral, GetMember(structLit, KeyNumber));
    auto alternatives = AS_TYPE(TStructType, variantLit->GetType()->GetUnderlyingType());
    auto unboundedIndex = alternatives->FindMemberIndex(KeyUnbounded);
    MKQL_ENSURE(unboundedIndex, "Unbounded not found");

    return variantLit->GetIndex() == *unboundedIndex
               ? TNumberAndDirection<T>(typename TNumberAndDirection<T>::TUnbounded{}, direction)
               : TNumberAndDirection<T>(GetValue<T>(variantLit->GetItem()), direction);
}

template <typename T>
TWindowFrame<TNumberAndDirection<T>> DeserializeWindowFrame(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    return {DeserializeNumberAndDirection<T>(GetMember(structLit, KeyMin)),
            DeserializeNumberAndDirection<T>(GetMember(structLit, KeyMax))};
}

template <typename T, typename Deserializer>
TVector<T> DeserializeList(const TRuntimeNode& node, Deserializer deserializer) {
    auto listLit = GetList(node);
    if (!listLit) {
        return {};
    }

    TVector<T> result;
    result.reserve(listLit->GetItemsCount());
    for (ui32 i = 0; i < listLit->GetItemsCount(); ++i) {
        result.push_back(deserializer(listLit->GetItems()[i]));
    }
    return result;
}

template <typename TRangeType>
TCoreWinFrameCollectorBounds<TRangeType> DeserializeBoundsImpl(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    // No deduplication is allowed here. We must add as much bounds as provided by |node|.
    TCoreWinFrameCollectorBounds<TRangeType> bounds(/*dedup=*/false);

    for (auto& frame : DeserializeList<TWindowFrame<TNumberAndDirection<TRangeType>>>(
             GetMember(structLit, KeyRangeIntervals), DeserializeWindowFrame<TRangeType>)) {
        bounds.AddRange(std::move(frame));
    }
    for (auto& frame : DeserializeList<TInputRowWindowFrame>(
             GetMember(structLit, KeyRowIntervals), DeserializeWindowFrame<ui64>)) {
        bounds.AddRow(std::move(frame));
    }
    for (auto& delta : DeserializeList<TNumberAndDirection<TRangeType>>(
             GetMember(structLit, KeyRangeIncrementals), DeserializeNumberAndDirection<TRangeType>)) {
        bounds.AddRangeIncremental(std::move(delta));
    }
    for (auto& delta : DeserializeList<TInputRow>(
             GetMember(structLit, KeyRowIncrementals), DeserializeNumberAndDirection<ui64>)) {
        bounds.AddRowIncremental(std::move(delta));
    }

    return bounds;
}

TDataType* ExtractTypeFromNumberAndDirection(TType* type) {
    auto structType = AS_TYPE(TStructType, type);
    auto numberIndex = structType->FindMemberIndex(KeyNumber);
    MKQL_ENSURE(numberIndex, "Number not found");

    auto alternatives = AS_TYPE(TStructType, AS_TYPE(TVariantType, structType->GetMemberType(*numberIndex))->GetUnderlyingType());
    auto boundedIndex = alternatives->FindMemberIndex(KeyBounded);
    MKQL_ENSURE(boundedIndex, "Bounded not found");

    return AS_TYPE(TDataType, alternatives->GetMemberType(*boundedIndex));
}

TDataType* ExtractTypeFromWindowFrame(TType* type) {
    auto structType = AS_TYPE(TStructType, type);
    auto minIndex = structType->FindMemberIndex(KeyMin);
    MKQL_ENSURE(minIndex, "Min not found");
    return ExtractTypeFromNumberAndDirection(structType->GetMemberType(*minIndex));
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

template <typename TRangeType>
TCoreWinFrameCollectorBounds<TRangeType> DeserializeBounds(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    return DeserializeBoundsImpl<TRangeType>(GetMember(structLit, KeyBounds));
}

TDataType* ExtractRangeDataTypeFromWindowAggregatorParams(const TRuntimeNode& node) {
    auto boundsLit = AS_VALUE(TStructLiteral, GetMember(AS_VALUE(TStructLiteral, node), KeyBounds));

    auto extractFromList = [&](TStringBuf key, auto extractor) -> TDataType* {
        if (auto listLit = GetList(GetMember(boundsLit, key))) {
            return extractor(listLit->GetType()->GetItemType());
        }
        return nullptr;
    };

    auto intervalsType = extractFromList(KeyRangeIntervals, ExtractTypeFromWindowFrame);
    auto incrementalsType = extractFromList(KeyRangeIncrementals, ExtractTypeFromNumberAndDirection);

    if (intervalsType && incrementalsType) {
        MKQL_ENSURE(intervalsType->IsSameType(*incrementalsType), "RangeIntervals and RangeIncrementals type mismatch");
    }
    return intervalsType ? intervalsType : incrementalsType;
}

template TCoreWinFrameCollectorBounds<i8> DeserializeBounds<i8>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<ui8> DeserializeBounds<ui8>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<i16> DeserializeBounds<i16>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<ui16> DeserializeBounds<ui16>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<i32> DeserializeBounds<i32>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<ui32> DeserializeBounds<ui32>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<i64> DeserializeBounds<i64>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<ui64> DeserializeBounds<ui64>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<float> DeserializeBounds<float>(const TRuntimeNode&);
template TCoreWinFrameCollectorBounds<double> DeserializeBounds<double>(const TRuntimeNode&);

} // namespace NKikimr::NMiniKQL
