#include "mkql_block_agg_sum.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/scalar.h>

namespace NKikimr {
namespace NMiniKQL {

template <typename TIn, typename TState, typename TInScalar>
class TSumBlockAggregator : public TBlockAggregatorBase {
public:
    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns, ui64 batchLength) final {
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                State_ += batchLength * datum.scalar_as<TInScalar>().value;
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
            TState state = State_;
            if (array->GetNullCount() == 0) {
                for (int64_t i = 0; i < len; ++i) {
                    state += ptr[i];
                }
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                for (int64_t i = 0; i < len; ++i) {
                    ui64 fullIndex = i + array->offset;
                    // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                    TState mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TState(1);
                    state += ptr[i] & mask;
                }
            }

            State_ = state;
        }
    }

    NUdf::TUnboxedValue Finish() final {
        if (!Count_) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(State_);
    }

private:
    const ui32 ArgColumn_;
    TState State_ = 0;
    ui64 Count_ = 0;
};

class TBlockSumFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns) const final {
       auto argType = AS_TYPE(TBlockType, tupleType->GetElementType(argsColumns[0]))->GetItemType();
       bool isOptional;
       auto dataType = UnpackOptionalData(argType, isOptional);
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
};

std::unique_ptr<IBlockAggregatorFactory> MakeBlockSumFactory() {
    return std::make_unique<TBlockSumFactory>();
}
 
}
}
