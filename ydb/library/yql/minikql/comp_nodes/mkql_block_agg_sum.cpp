#include "mkql_block_agg_sum.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <arrow/scalar.h>

namespace NKikimr {
namespace NMiniKQL {

template <typename TIn, typename TSum, typename TInScalar>
class TSumBlockAggregatorNullableOrScalar : public TBlockAggregatorBase {
public:
    TSumBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns, ui64 batchLength) final {
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                Sum_ += batchLength * datum.scalar_as<TInScalar>().value;
                IsValid_ = true;
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            auto len = array->length;
            auto count = len - array->GetNullCount();
            if (!count) {
                return;
            }

            IsValid_ = true;
            TSum sum = Sum_;
            if (array->GetNullCount() == 0) {
                for (int64_t i = 0; i < len; ++i) {
                    sum += ptr[i];
                }
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                for (int64_t i = 0; i < len; ++i) {
                    ui64 fullIndex = i + array->offset;
                    // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                    TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                    sum += (ptr[i] & mask);
                }
            }

            Sum_ = sum;
        }
    }

    NUdf::TUnboxedValue Finish() final {
        if (!IsValid_) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(Sum_);
    }

private:
    const ui32 ArgColumn_;
    TSum Sum_ = 0;
    bool IsValid_ = false;
};

template <typename TIn, typename TSum, typename TInScalar>
class TSumBlockAggregator : public TBlockAggregatorBase {
public:
    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns, ui64 batchLength) final {
        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        MKQL_ENSURE(datum.is_array(), "Expected array");
        const auto& array = datum.array();
        auto ptr = array->GetValues<TIn>(1);
        auto len = array->length;
        MKQL_ENSURE(array->GetNullCount() == 0, "Expected no nulls");
        MKQL_ENSURE(len > 0, "Expected at least one value");

        TSum sum = Sum_;
        for (int64_t i = 0; i < len; ++i) {
            sum += ptr[i];
        }

        Sum_ = sum;
    }

    NUdf::TUnboxedValue Finish() final {
        return NUdf::TUnboxedValuePod(Sum_);
    }

private:
    const ui32 ArgColumn_;
    TSum Sum_ = 0;
};

template <typename TIn, typename TInScalar>
class TAvgBlockAggregator : public TBlockAggregatorBase {
public:
    TAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, const THolderFactory& holderFactory)
        : TBlockAggregatorBase(filterColumn)
        , ArgColumn_(argColumn)
        , HolderFactory_(holderFactory)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns, ui64 batchLength) final {
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                Sum_ += double(batchLength * datum.scalar_as<TInScalar>().value);
                Count_ += batchLength;
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            auto len = array->length;
            auto count = len - array->GetNullCount();
            if (!count) {
                return;
            }

            Count_ += count;
            double sum = Sum_;
            if (array->GetNullCount() == 0) {
                for (int64_t i = 0; i < len; ++i) {
                    sum += double(ptr[i]);
                }
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                for (int64_t i = 0; i < len; ++i) {
                    ui64 fullIndex = i + array->offset;
                    // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                    TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                    sum += double(ptr[i] & mask);
                }
            }

            Sum_ = sum;
        }
    }

    NUdf::TUnboxedValue Finish() final {
        if (!Count_) {
            return NUdf::TUnboxedValuePod();
        }

        NUdf::TUnboxedValue* items;
        auto arr = HolderFactory_.CreateDirectArrayHolder(2, items);
        items[0] = NUdf::TUnboxedValuePod(Sum_);
        items[1] = NUdf::TUnboxedValuePod(Count_);
        return arr;
    }

private:
    const ui32 ArgColumn_;
    const THolderFactory& HolderFactory_;
    double Sum_ = 0;
    ui64 Count_ = 0;
};

class TBlockSumFactory : public IBlockAggregatorFactory {
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
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i8, i64, arrow::Int8Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint8:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui8, ui64, arrow::UInt8Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int16:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i16, i64, arrow::Int16Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint16:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui16, ui64, arrow::UInt16Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int32:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i32, i64, arrow::Int32Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint32:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui32, ui64, arrow::UInt32Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int64:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i64, i64, arrow::Int64Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint64:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui64, ui64, arrow::UInt64Scalar>>(filterColumn, argsColumns[0]);
           default:
               throw yexception() << "Unsupported SUM input type";
           }
       } else {
           switch (*dataType->GetDataSlot()) {
           case NUdf::EDataSlot::Int8:
               return std::make_unique<TSumBlockAggregator<i8, i64, arrow::Int8Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint8:
               return std::make_unique<TSumBlockAggregator<ui8, ui64, arrow::UInt8Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int16:
               return std::make_unique<TSumBlockAggregator<i16, i64, arrow::Int16Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint16:
               return std::make_unique<TSumBlockAggregator<ui16, ui64, arrow::UInt16Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int32:
               return std::make_unique<TSumBlockAggregator<i32, i64, arrow::Int32Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint32:
               return std::make_unique<TSumBlockAggregator<ui32, ui64, arrow::UInt32Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Int64:
               return std::make_unique<TSumBlockAggregator<i64, i64, arrow::Int64Scalar>>(filterColumn, argsColumns[0]);
           case NUdf::EDataSlot::Uint64:
               return std::make_unique<TSumBlockAggregator<ui64, ui64, arrow::UInt64Scalar>>(filterColumn, argsColumns[0]);
           default:
               throw yexception() << "Unsupported SUM input type";
           }
       }
   }
};

class TBlockAvgFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       const THolderFactory& holderFactory) const final {
       auto argType = AS_TYPE(TBlockType, tupleType->GetElementType(argsColumns[0]))->GetItemType();
       bool isOptional;
       auto dataType = UnpackOptionalData(argType, isOptional);
       switch (*dataType->GetDataSlot()) {
       case NUdf::EDataSlot::Int8:
           return std::make_unique<TAvgBlockAggregator<i8, arrow::Int8Scalar>>(filterColumn, argsColumns[0], holderFactory);
       case NUdf::EDataSlot::Uint8:
           return std::make_unique<TAvgBlockAggregator<ui8, arrow::UInt8Scalar>>(filterColumn, argsColumns[0], holderFactory);
       case NUdf::EDataSlot::Int16:
           return std::make_unique<TAvgBlockAggregator<i16, arrow::Int16Scalar>>(filterColumn, argsColumns[0], holderFactory);
       case NUdf::EDataSlot::Uint16:
           return std::make_unique<TAvgBlockAggregator<ui16, arrow::UInt16Scalar>>(filterColumn, argsColumns[0], holderFactory);
       case NUdf::EDataSlot::Int32:
           return std::make_unique<TAvgBlockAggregator<i32, arrow::Int32Scalar>>(filterColumn, argsColumns[0], holderFactory);
       case NUdf::EDataSlot::Uint32:
           return std::make_unique<TAvgBlockAggregator<ui32, arrow::UInt32Scalar>>(filterColumn, argsColumns[0], holderFactory);
       case NUdf::EDataSlot::Int64:
           return std::make_unique<TAvgBlockAggregator<i64, arrow::Int64Scalar>>(filterColumn, argsColumns[0], holderFactory);
       case NUdf::EDataSlot::Uint64:
           return std::make_unique<TAvgBlockAggregator<ui64, arrow::UInt64Scalar>>(filterColumn, argsColumns[0], holderFactory);
       default:
           throw yexception() << "Unsupported AVG input type";
       }
   }
};

std::unique_ptr<IBlockAggregatorFactory> MakeBlockSumFactory() {
    return std::make_unique<TBlockSumFactory>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockAvgFactory() {
    return std::make_unique<TBlockAvgFactory>();
}
 
}
}
