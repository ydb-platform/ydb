#include "mkql_block_agg_minmax.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <arrow/scalar.h>

namespace NKikimr {
namespace NMiniKQL {

template <bool IsMin, typename T>
T UpdateMinMax(T x, T y) {
    if constexpr (IsMin) {
        return x < y ? x : y;
    } else {
        return x > y ? x : y;
    }
}

template <typename TIn, typename TInScalar, bool IsMin>
class TMinMaxBlockAggregatorNullableOrScalar : public TBlockAggregatorBase {
public:
    struct TState {
        TIn Value_;
        bool IsValid_ = false;

        TState() {
            if constexpr (IsMin) {
                Value_ = std::numeric_limits<TIn>::max();
            } else {
                Value_ = std::numeric_limits<TIn>::min();
            }
        }
    };

    TMinMaxBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(sizeof(TState), filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    void InitState(void* state) final {
        new(state) TState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Value_ = datum.scalar_as<TInScalar>().value;
                typedState->IsValid_ = true;
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            auto len = array->length;
            auto count = len - array->GetNullCount();
            if (!count) {
                return;
            }

            if (!filtered) {
                typedState->IsValid_ = true;
                TIn value = typedState->Value_;
                if (array->GetNullCount() == 0) {
                    for (int64_t i = 0; i < len; ++i) {
                        value = UpdateMinMax<IsMin>(value, ptr[i]);
                    }
                } else {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        value = UpdateMinMax<IsMin>(value, TIn((ptr[i] & mask) | (value & ~mask)));
                    }
                }

                typedState->Value_ = value;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);

                TIn value = typedState->Value_;
                if (array->GetNullCount() == 0) {
                    typedState->IsValid_ = true;
                    for (int64_t i = 0; i < len; ++i) {
                        TIn filterMask = (((*filterBitmap++) & 1) ^ 1) - TIn(1);
                        value = UpdateMinMax<IsMin>(value, TIn((ptr[i] & filterMask) | (value & ~filterMask)));
                    }
                } else {
                    ui64 count = 0;
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        TIn filterMask = (((*filterBitmap++) & 1) ^ 1) - TIn(1);
                        mask &= filterMask;
                        value = UpdateMinMax<IsMin>(value, TIn((ptr[i] & mask) | (value & ~mask)));
                        count += mask & 1;
                    }

                    typedState->IsValid_ = typedState->IsValid_ || count > 0;
                }

                typedState->Value_ = value;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        if (!typedState->IsValid_) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(typedState->Value_);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TIn, typename TInScalar, bool IsMin>
class TMinMaxBlockAggregator: public TBlockAggregatorBase {
public:
    struct TState {
        TIn Value_;
        TState() {
            if constexpr (IsMin) {
                Value_ = std::numeric_limits<TIn>::max();
            } else {
                Value_ = std::numeric_limits<TIn>::min();
            }
        }
    };

    TMinMaxBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(sizeof(TState), filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    void InitState(void* state) final {
        new(state) TState;
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        MKQL_ENSURE(datum.is_array(), "Expected array");
        const auto& array = datum.array();
        auto ptr = array->GetValues<TIn>(1);
        auto len = array->length;
        MKQL_ENSURE(array->GetNullCount() == 0, "Expected no nulls");
        MKQL_ENSURE(len > 0, "Expected at least one value");
        if (!filtered) {
            TIn value = typedState->Value_;
            for (int64_t i = 0; i < len; ++i) {
                value = UpdateMinMax<IsMin>(value, ptr[i]);
            }

            typedState->Value_ = value;
        } else {
            const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
            const auto& filterArray = filterDatum.array();
            MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
            const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);

            TIn value = typedState->Value_;
            for (int64_t i = 0; i < len; ++i) {
                ui64 fullIndex = i + array->offset;
                TIn filterMask = (((*filterBitmap++) & 1) ^ 1) - TIn(1);
                value = UpdateMinMax<IsMin>(value, TIn((ptr[i] & filterMask) | (value & ~filterMask)));
            }

            typedState->Value_ = value;
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        return NUdf::TUnboxedValuePod(typedState->Value_);
    }

private:
    const ui32 ArgColumn_;
};

template <bool IsMin>
class TBlockMinMaxFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       const THolderFactory& holderFactory) const final {
       Y_UNUSED(holderFactory);
       auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(argsColumns[0]));
       auto argType = blockType->GetItemType();
       bool isOptional;
       auto dataType = UnpackOptionalData(argType, isOptional);
       if (blockType->GetShape() == TBlockType::EShape::Scalar || isOptional) {
           switch (*dataType->GetDataSlot()) {
           case NUdf::EDataSlot::Int8:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<i8, arrow::Int8Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Bool:
           case NUdf::EDataSlot::Uint8:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<ui8, arrow::UInt8Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int16:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<i16, arrow::Int16Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint16:
           case NUdf::EDataSlot::Date:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<ui16, arrow::UInt16Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int32:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<i32, arrow::Int32Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint32:
           case NUdf::EDataSlot::Datetime:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<ui32, arrow::UInt32Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int64:
           case NUdf::EDataSlot::Interval:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<i64, arrow::Int64Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint64:
           case NUdf::EDataSlot::Timestamp:
               return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<ui64, arrow::UInt64Scalar, IsMin>>(filterColumn, argsColumns[0]);
           default:
               throw yexception() << "Unsupported MIN/MAX input type";
           }
       } else {
           switch (*dataType->GetDataSlot()) {
           case NUdf::EDataSlot::Int8:
               return std::make_unique<TMinMaxBlockAggregator<i8, arrow::Int8Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint8:
           case NUdf::EDataSlot::Bool:
               return std::make_unique<TMinMaxBlockAggregator<ui8, arrow::UInt8Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int16:
               return std::make_unique<TMinMaxBlockAggregator<i16, arrow::Int16Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint16:
           case NUdf::EDataSlot::Date:
               return std::make_unique<TMinMaxBlockAggregator<ui16, arrow::UInt16Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int32:
               return std::make_unique<TMinMaxBlockAggregator<i32, arrow::Int32Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint32:
           case NUdf::EDataSlot::Datetime:
               return std::make_unique<TMinMaxBlockAggregator<ui32, arrow::UInt32Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int64:
           case NUdf::EDataSlot::Interval:
               return std::make_unique<TMinMaxBlockAggregator<i64, arrow::Int64Scalar, IsMin>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint64:
           case NUdf::EDataSlot::Timestamp:
               return std::make_unique<TMinMaxBlockAggregator<ui64, arrow::UInt64Scalar, IsMin>>(filterColumn, argsColumns[0]);
           default:
               throw yexception() << "Unsupported MIN/MAX input type";
           }
       }
   }
};

std::unique_ptr<IBlockAggregatorFactory> MakeBlockMinFactory() {
    return std::make_unique<TBlockMinMaxFactory<true>>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockMaxFactory() {
    return std::make_unique<TBlockMinMaxFactory<false>>();
}
 
}
}
