#include "mkql_window_frames_collector_params_deserializer.h"

#include <yql/essentials/minikql/comp_nodes/mkql_window_range_pg_caller.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_window_comparator_bounds.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins_datetime.h>
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
constexpr TStringBuf KeyDirection = "Direction";
constexpr TStringBuf KeyNumber = "Number";
constexpr TStringBuf KeySortedColumn = "SortedColumn";
constexpr TStringBuf KeyFiniteValue = "FiniteValue";
constexpr TStringBuf KeyProcId = "ProcId";

struct TCurrentRowTag {};
struct TInfTag {};

struct TPgFiniteBound {
    TRuntimeNode Node;
    ui32 ProcId;
};

template <auto TScaler, typename T>
struct TPromoteToRangeType {
    using type = T;
};

template <typename T>
    requires(std::is_integral_v<T>)
struct TPromoteToRangeType<TPlainNumericTag{}, T> {
    using TUnsigned = std::make_unsigned_t<T>;
    using type = std::conditional_t<(sizeof(TUnsigned) <= 2), ui32, TUnsigned>;
};

template <>
struct TPromoteToRangeType<TPlainNumericTag{}, NYql::NDecimal::TInt128> {
    using type = NYql::NDecimal::TInt128;
};

using TBlackboxTypeData = TBlackboxTypeData<TComputationContext, NUdf::TUnboxedValue>;
using TRangeBound = TRangeBound<TComputationContext, NUdf::TUnboxedValue>;
using TColumnTypeWithScale = TColumnTypeWithScale<TComputationContext, NUdf::TUnboxedValue>;
using TRangeTypeWithScale = TRangeTypeWithScale<TComputationContext, NUdf::TUnboxedValue>;

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

std::variant<TCurrentRowTag, TInfTag, TDataType*> GetDataTypeFromBound(const TRuntimeNode& boundNode) {
    auto structLit = AS_VALUE(TStructLiteral, boundNode);
    auto numberNode = GetMember(structLit, KeyNumber);
    MKQL_ENSURE(numberNode.GetStaticType(), "Static type expected.");
    TType* type = numberNode.GetStaticType();
    if (type->IsTagged()) {
        auto tag = AS_TYPE(TTaggedType, type)->GetTag();
        if (tag == "zero") {
            return TCurrentRowTag{};
        }
        if (tag == "inf") {
            return TInfTag{};
        } else {
            ythrow yexception() << "Unknown tag for window frame range bound: " << tag;
        }
    }
    if (type->IsStruct()) {
        auto innerStruct = AS_VALUE(TStructLiteral, numberNode);
        numberNode = GetMember(innerStruct, KeyFiniteValue);
        type = numberNode.GetStaticType();
        auto dataType = AS_TYPE(TDataType, type);
        return dataType;
    }
    ythrow yexception() << "Expected tagged or struct type for window frame range bound";
}

std::variant<TCurrentRowTag, TInfTag, TPgFiniteBound> GetPgBound(const TRuntimeNode& boundNode) {
    auto structLit = AS_VALUE(TStructLiteral, boundNode);
    auto numberNode = GetMember(structLit, KeyNumber);
    MKQL_ENSURE(numberNode.GetStaticType(), "Static type expected.");
    TType* type = numberNode.GetStaticType();
    if (type->IsTagged()) {
        auto tag = AS_TYPE(TTaggedType, type)->GetTag();
        if (tag == "zero") {
            return TCurrentRowTag{};
        } else if (tag == "inf") {
            return TInfTag{};
        }
        MKQL_ENSURE(false, "Unknown tag: " << tag);
    }
    MKQL_ENSURE(type->IsStruct(), "Expected struct type for PG bound");
    auto innerStruct = AS_VALUE(TStructLiteral, numberNode);
    auto finiteValueNode = GetMember(innerStruct, KeyFiniteValue);
    auto procIdIndex = innerStruct->GetType()->FindMemberIndex(KeyProcId);
    MKQL_ENSURE(procIdIndex, "ProcId is required for PG bounds");
    ui32 procId = GetValue<ui32>(GetMember(innerStruct, KeyProcId));
    return TPgFiniteBound{finiteValueNode, procId};
}

template <bool IsRangeBound, typename TCallback>
auto DispatchByDataSlot(std::variant<TCurrentRowTag, TInfTag, TDataType*> slot, TCallback&& callback) {
    if constexpr (IsRangeBound) {
        if (std::holds_alternative<TInfTag>(slot)) {
            return callback.template operator()<nullptr, TInfTag, void>();
        } else if (std::holds_alternative<TCurrentRowTag>(slot)) {
            return callback.template operator()<nullptr, TCurrentRowTag, void>();
        }
    } else {
        MKQL_ENSURE(std::holds_alternative<TDataType*>(slot), "Slot must be defined");
    }

    using TScaledInterval = TScaledDateType<NUdf::TInterval>;
    using TScaledInterval64 = TScaledDateType<NUdf::TInterval64>;

    switch (*std::get<TDataType*>(slot)->GetDataSlot()) {
        case NUdf::EDataSlot::Int8:
            return callback.template operator()<TPlainNumericTag{}, i8, TNoScaledType<ui32>>();
        case NUdf::EDataSlot::Uint8:
            return callback.template operator()<TPlainNumericTag{}, ui8, TNoScaledType<ui32>>();
        case NUdf::EDataSlot::Int16:
            return callback.template operator()<TPlainNumericTag{}, i16, TNoScaledType<ui32>>();
        case NUdf::EDataSlot::Uint16:
            return callback.template operator()<TPlainNumericTag{}, ui16, TNoScaledType<ui32>>();
        case NUdf::EDataSlot::Int32:
            return callback.template operator()<TPlainNumericTag{}, i32, TNoScaledType<ui32>>();
        case NUdf::EDataSlot::Uint32:
            return callback.template operator()<TPlainNumericTag{}, ui32, TNoScaledType<ui32>>();
        case NUdf::EDataSlot::Int64:
            return callback.template operator()<TPlainNumericTag{}, i64, TNoScaledType<ui64>>();
        case NUdf::EDataSlot::Uint64:
            return callback.template operator()<TPlainNumericTag{}, ui64, TNoScaledType<ui64>>();
        case NUdf::EDataSlot::Float:
            return callback.template operator()<TPlainNumericTag{}, float, TNoScaledType<float>>();
        case NUdf::EDataSlot::Double:
            return callback.template operator()<TPlainNumericTag{}, double, TNoScaledType<double>>();
        case NUdf::EDataSlot::Interval:
            return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TInterval>>, NUdf::TDataType<NUdf::TInterval>::TLayout, TScaledInterval>();
        case NUdf::EDataSlot::Interval64:
            return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>, NUdf::TDataType<NUdf::TInterval64>::TLayout, TScaledInterval64>();
        case NUdf::EDataSlot::Decimal: {
            const auto precision = TMaybe<ui8>(static_cast<TDataDecimalType*>(std::get<TDataType*>(slot))->GetParams().first);
            return callback.template operator()<TPlainNumericTag{}, NYql::NDecimal::TInt128, TNoScaledType<NYql::NDecimal::TInt128>>(precision);
        }
        default:
            break;
    }

    if constexpr (!IsRangeBound) {
        switch (*std::get<TDataType*>(slot)->GetDataSlot()) {
            case NUdf::EDataSlot::Date:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TDate>>, NUdf::TDataType<NUdf::TDate>::TLayout, TScaledInterval>();
            case NUdf::EDataSlot::Datetime:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TDatetime>>, NUdf::TDataType<NUdf::TDatetime>::TLayout, TScaledInterval>();
            case NUdf::EDataSlot::Timestamp:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTimestamp>>, NUdf::TDataType<NUdf::TTimestamp>::TLayout, TScaledInterval>();
            case NUdf::EDataSlot::TzDate:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTzDate>>, NUdf::TDataType<NUdf::TTzDate>::TLayout, TScaledInterval>();
            case NUdf::EDataSlot::TzDatetime:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>, NUdf::TDataType<NUdf::TTzDatetime>::TLayout, TScaledInterval>();
            case NUdf::EDataSlot::TzTimestamp:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>, NUdf::TDataType<NUdf::TTzTimestamp>::TLayout, TScaledInterval>();
            case NUdf::EDataSlot::Date32:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TDate32>>, NUdf::TDataType<NUdf::TDate32>::TLayout, TScaledInterval64>();
            case NUdf::EDataSlot::Datetime64:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TDatetime64>>, NUdf::TDataType<NUdf::TDatetime64>::TLayout, TScaledInterval64>();
            case NUdf::EDataSlot::Timestamp64:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>, NUdf::TDataType<NUdf::TTimestamp64>::TLayout, TScaledInterval64>();
            case NUdf::EDataSlot::TzDate32:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTzDate32>>, NUdf::TDataType<NUdf::TTzDate32>::TLayout, TScaledInterval64>();
            case NUdf::EDataSlot::TzDatetime64:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>, NUdf::TDataType<NUdf::TTzDatetime64>::TLayout, TScaledInterval64>();
            case NUdf::EDataSlot::TzTimestamp64:
                return callback.template operator()<ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>, NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout, TScaledInterval64>();
            default:
                break;
        }
    }

    ythrow yexception() << "Unsupported data slot: " << *std::get<TDataType*>(slot)->GetDataSlot();
}

TUnboxedValueVariantBound DeserializeBoundAsVariant(const TRuntimeNode& boundNode, const TStructType* streamType, std::vector<IComputationNode*>& dependentNodes, TNodeExtractor nodeExtractor, ui32& ctxIndex) {
    auto structLit = AS_VALUE(TStructLiteral, boundNode);

    EDirection direction;
    MKQL_ENSURE(NYql::NWindow::TryParseDirectionFromString(GetString(GetMember(structLit, KeyDirection)), direction), "Unknown direction");

    auto numberNode = GetMember(structLit, KeyNumber);
    MKQL_ENSURE(numberNode.GetStaticType(), "Static type expected.");
    auto sortedColumnNode = GetMember(structLit, KeySortedColumn);
    TString sortedColumn = GetString(sortedColumnNode);
    ui32 memberIndex = streamType->GetMemberIndex(sortedColumn);
    auto columnType = streamType->GetMemberType(memberIndex);
    if (columnType->IsOptional()) {
        columnType = AS_TYPE(TOptionalType, columnType)->GetItemType();
    }

    if (columnType->IsPg()) {
        auto pgBound = GetPgBound(boundNode);
        return std::visit(TOverloaded{
                              [&](TInfTag) -> TUnboxedValueVariantBound {
                                  return TUnboxedValueVariantBound::Inf(direction);
                              },
                              [&](TCurrentRowTag) -> TUnboxedValueVariantBound {
                                  TBlackboxTypeData::TPtr blackbox = new TPgWindowRangeCaller(AS_TYPE(TPgType, columnType), TPgWindowRangeCaller::TCurrentRowTag{}, ctxIndex++);
                                  return TUnboxedValueVariantBound(
                                      TRangeBound(TNoScaledType<TBlackboxTypeData::TPtr>{.Value = std::move(blackbox)}, TNoScaledType<TBlackboxTypeData::TPtr>{.Value = nullptr}, memberIndex), direction);
                              },
                              [&](TPgFiniteBound& bound) -> TUnboxedValueVariantBound {
                                  MKQL_ENSURE(bound.Node.GetStaticType()->IsPg(), "Expected pg type");
                                  auto* computationNode = dependentNodes.emplace_back(nodeExtractor(bound.Node));
                                  TBlackboxTypeData::TPtr blackbox = new TPgWindowRangeCaller(AS_TYPE(TPgType, columnType), std::make_tuple(bound.ProcId, computationNode, AS_TYPE(TPgType, bound.Node.GetStaticType())), ctxIndex++);
                                  return TUnboxedValueVariantBound(
                                      TRangeBound(TNoScaledType<TBlackboxTypeData::TPtr>{.Value = std::move(blackbox)}, TNoScaledType<TBlackboxTypeData::TPtr>{.Value = nullptr}, memberIndex), direction);
                              }}, pgBound);
    }

    auto type = GetDataTypeFromBound(boundNode);
    MKQL_ENSURE(columnType->IsData() && AS_TYPE(TDataType, columnType)->GetDataSlot().Defined(), "Column type must be data slot");
    auto visitColumnLambda = [&]<auto TColumnScaler, typename TColumnType, typename TZeroBoundType>(TMaybe<ui8> precision = {})
        -> std::pair<TColumnTypeWithScale, TRangeTypeWithScale> {
        if constexpr (std::is_same_v<TColumnType, NYql::NDecimal::TInt128>) {
            return std::make_pair(TWithScale<TColumnScaler, TColumnType>{.Value = TColumnType{0}, .Precision = *precision},
                                  TZeroBoundType{.Value = {}, .Precision = *precision});
        } else {
            return std::make_pair(TWithScale<TColumnScaler, TColumnType>{.Value = TColumnType{0}},
                                  TZeroBoundType{.Value = {}});
        }
    };
    auto [column, scaledZeroBound] = DispatchByDataSlot</*IsRangeBound=*/false>(AS_TYPE(TDataType, columnType),
                                                                                visitColumnLambda);

    auto finiteValueNode = numberNode;
    if (numberNode.GetStaticType()->IsStruct()) {
        auto innerStruct = AS_VALUE(TStructLiteral, numberNode);
        finiteValueNode = GetMember(innerStruct, KeyFiniteValue);
    }

    auto bound = DispatchByDataSlot</*IsRangeBound=*/true>(type, [&]<auto TRangeScaler, typename TRangeType, typename TUnused>(TMaybe<ui8> precision = {}) -> TUnboxedValueVariantBound {
        if constexpr (std::is_same_v<TInfTag, TRangeType>) {
            return TUnboxedValueVariantBound::Inf(direction);
        } else if constexpr (std::is_same_v<TCurrentRowTag, TRangeType>) {
            return TUnboxedValueVariantBound(TRangeBound(std::move(scaledZeroBound), std::move(column), memberIndex), direction);
        } else {
            using TPromoted = typename TPromoteToRangeType<TRangeScaler, TRangeType>::type;
            MKQL_ENSURE(GetValue<TRangeType>(finiteValueNode) >= 0, "Range value must be non-negative");
            auto value = static_cast<TPromoted>(GetValue<TRangeType>(finiteValueNode));
            if constexpr (std::is_same_v<TRangeType, NYql::NDecimal::TInt128>) {
                TRangeTypeWithScale bound = TWithScale<TRangeScaler, TPromoted>{.Value = value, .Precision = *precision};
                return TUnboxedValueVariantBound(TRangeBound(std::move(bound), std::move(column), memberIndex), direction);
            } else {
                TRangeTypeWithScale bound = TWithScale<TRangeScaler, TPromoted>{.Value = value};
                return TUnboxedValueVariantBound(TRangeBound(std::move(bound), std::move(column), memberIndex), direction);
            }
        }
    });
    return bound;
}

// Deserialize a WindowFrame (with Min and Max bounds) into variant-based representation
TWindowFrame<TUnboxedValueVariantBound> DeserializeWindowFrameAsVariant(const TRuntimeNode& frameNode, const TStructType* streamType, std::vector<IComputationNode*>& dependentNodes, TNodeExtractor nodeExtractor, ui32& ctxIndex) {
    auto structLit = AS_VALUE(TStructLiteral, frameNode);

    TUnboxedValueVariantBound minBound = DeserializeBoundAsVariant(GetMember(structLit, KeyMin), streamType, dependentNodes, nodeExtractor, ctxIndex);
    TUnboxedValueVariantBound maxBound = DeserializeBoundAsVariant(GetMember(structLit, KeyMax), streamType, dependentNodes, nodeExtractor, ctxIndex);

    return {std::move(minBound), std::move(maxBound)};
}

template <typename T>
TNumberAndDirection<T> DeserializeNumberAndDirection(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);

    EDirection direction;
    MKQL_ENSURE(NYql::NWindow::TryParseDirectionFromString(GetString(GetMember(structLit, KeyDirection)), direction), "Unknown direction");

    auto boundLiteral = GetMember(structLit, KeyNumber);
    MKQL_ENSURE(boundLiteral.GetStaticType(), "Static type expected.");
    TType* type = boundLiteral.GetStaticType();
    if (type->IsTagged()) {
        auto tag = AS_TYPE(TTaggedType, type)->GetTag();
        MKQL_ENSURE(tag != "zero", "Zero bound must already be normalized");
        if (tag == "inf") {
            return TNumberAndDirection<T>::Inf(direction);
        }
        ythrow yexception() << "Unknown tag for window frame range bound: " << tag;
    }
    if (type->IsStruct()) {
        auto innerStruct = AS_VALUE(TStructLiteral, boundLiteral);
        boundLiteral = GetMember(innerStruct, KeyFiniteValue);
    }
    return TNumberAndDirection<T>(GetValue<T>(boundLiteral), direction);
}

template <typename T>
TWindowFrame<TNumberAndDirection<T>> DeserializeWindowFrame(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    return {DeserializeNumberAndDirection<T>(GetMember(structLit, KeyMin)),
            DeserializeNumberAndDirection<T>(GetMember(structLit, KeyMax))};
}

std::pair<TUnboxedValueVariantBounds, std::vector<IComputationNode*>> DeserializeBoundsAsVariantImpl(const TRuntimeNode& boundsNode, const TStructType* streamType, TNodeExtractor nodeExtractor, ui32& ctxIndex) {
    auto structLit = AS_VALUE(TStructLiteral, boundsNode);

    // No deduplication is allowed here. We must add as much bounds as provided by |node|.
    TUnboxedValueVariantBounds bounds;
    std::vector<IComputationNode*> dependentNodes;
    // Deserialize range intervals
    auto rangeIntervalsTuple = GetTuple(GetMember(structLit, KeyRangeIntervals));
    for (ui32 i = 0; i < rangeIntervalsTuple->GetValuesCount(); ++i) {
        bounds.AddRange(DeserializeWindowFrameAsVariant(rangeIntervalsTuple->GetValue(i), streamType, dependentNodes, nodeExtractor, ctxIndex));
    }

    // Row intervals don't need variant - pass through as-is
    auto rowIntervalsTuple = GetTuple(GetMember(structLit, KeyRowIntervals));
    for (ui32 i = 0; i < rowIntervalsTuple->GetValuesCount(); ++i) {
        bounds.AddRow(DeserializeWindowFrame<ui64>(rowIntervalsTuple->GetValue(i)));
    }

    // Deserialize range incrementals
    auto rangeIncrementalsTuple = GetTuple(GetMember(structLit, KeyRangeIncrementals));
    for (ui32 i = 0; i < rangeIncrementalsTuple->GetValuesCount(); ++i) {
        bounds.AddRangeIncremental(DeserializeBoundAsVariant(rangeIncrementalsTuple->GetValue(i), streamType, dependentNodes, nodeExtractor, ctxIndex));
    }

    // Row incrementals don't need variant - pass through as-is
    auto rowIncrementalsTuple = GetTuple(GetMember(structLit, KeyRowIncrementals));
    for (ui32 i = 0; i < rowIncrementalsTuple->GetValuesCount(); ++i) {
        bounds.AddRowIncremental(DeserializeNumberAndDirection<ui64>(rowIncrementalsTuple->GetValue(i)));
    }

    return {std::move(bounds), std::move(dependentNodes)};
}

} // anonymous namespace

std::pair<TUnboxedValueVariantBounds, std::vector<IComputationNode*>> DeserializeBoundsAsVariant(const TRuntimeNode& node, const TStructType* streamType, TNodeExtractor nodeExtractor, ui32& ctxIndex) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    auto boundsNode = GetMember(structLit, KeyBounds);
    return DeserializeBoundsAsVariantImpl(boundsNode, streamType, nodeExtractor, ctxIndex);
}

ESortOrder DeserializeSortOrder(const TRuntimeNode& node) {
    auto structLit = AS_VALUE(TStructLiteral, node);
    ESortOrder sortOrder;
    MKQL_ENSURE(TryParseSortOrderFromString(GetString(GetMember(structLit, KeySortOrder)), sortOrder), "Unknown sort order");
    return sortOrder;
}

bool AnyRangeProvided(const TRuntimeNode& node) {
    auto boundsLit = AS_VALUE(TStructLiteral, GetMember(AS_VALUE(TStructLiteral, node), KeyBounds));
    auto rangeIntervals = GetTuple(GetMember(boundsLit, KeyRangeIntervals))->GetValuesCount();
    auto rangeIncrementals = GetTuple(GetMember(boundsLit, KeyRangeIncrementals))->GetValuesCount();

    return rangeIntervals > 0 || rangeIncrementals > 0;
}

} // namespace NKikimr::NMiniKQL
