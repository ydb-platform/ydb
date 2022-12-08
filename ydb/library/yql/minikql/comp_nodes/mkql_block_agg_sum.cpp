#include "mkql_block_agg_sum.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <arrow/scalar.h>
#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregatorNullableOrScalar : public TBlockAggregatorBase {
public:
    struct TState {
        TSum Sum_ = 0;
        ui8 IsValid_ = 0;
    };

    class TColumnBuilder : public IAggColumnBuilder {
    public:
        TColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& dataType, TComputationContext& ctx)
            : Builder_(dataType, &ctx.ArrowMemoryPool)
            , Ctx_(ctx)
        {
            ARROW_OK(Builder_.Reserve(size));
        }

        void Add(const void* state) final {
            auto typedState = static_cast<const TState*>(state);
            if (typedState->IsValid_) {
                Builder_.UnsafeAppend(typedState->Sum_);
            } else {
                Builder_.UnsafeAppendNull();
            }
        }

        NUdf::TUnboxedValue Build() final {
            std::shared_ptr<arrow::ArrayData> result;
            ARROW_OK(Builder_.FinishInternal(&result));
            return Ctx_.HolderFactory.CreateArrowBlock(result);
        }

    private:
        TBuilder Builder_;
        TComputationContext& Ctx_;
    };

    TSumBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBlockAggregatorBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void InitState(void* state) final {
        new(state) TState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Sum_ += (filtered ? *filtered : batchLength) * datum.scalar_as<TInScalar>().value;
                typedState->IsValid_ = 1;
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
                typedState->IsValid_ = 1;
                TSum sum = typedState->Sum_;
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

                typedState->Sum_ = sum;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                auto filterBitmap = filterArray->template GetValues<uint8_t>(1, 0);
                TSum sum = typedState->Sum_;
                if (array->GetNullCount() == 0) {
                    typedState->IsValid_ = 1;
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn filterMask = (((filterBitmap[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        sum += ptr[i] & filterMask;
                    }
                } else {
                    ui64 count = 0;
                    auto nullBitmapPtr = array->template GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        TIn filterMask = (((filterBitmap[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        mask &= filterMask;
                        sum += (ptr[i] & mask);
                        count += mask & 1;
                    }

                    typedState->IsValid_ |= count ? 1 : 0; 
                }

                typedState->Sum_ = sum;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        if (!typedState->IsValid_) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(typedState->Sum_);
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Sum_ += datum.scalar_as<TInScalar>().value;
                typedState->IsValid_ = 1;
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            if (array->GetNullCount() == 0) {
                typedState->IsValid_ = 1;
                typedState->Sum_ += ptr[row];
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                ui64 fullIndex = row + array->offset;
                // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                typedState->Sum_ += (ptr[row] & mask);
                typedState->IsValid_ |= mask & 1;
            }
        }
    }

    std::unique_ptr<IAggColumnBuilder> MakeBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregator : public TBlockAggregatorBase {
public:
    struct TState {
        TSum Sum_ = 0;
    };

    class TColumnBuilder : public IAggColumnBuilder {
    public:
        TColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& dataType, TComputationContext& ctx)
            : Builder_(dataType, &ctx.ArrowMemoryPool)
            , Ctx_(ctx)
        {
            ARROW_OK(Builder_.Reserve(size));
        }

        void Add(const void* state) final {
            auto typedState = static_cast<const TState*>(state);
            Builder_.UnsafeAppend(typedState->Sum_);
        }

        NUdf::TUnboxedValue Build() final {
            std::shared_ptr<arrow::ArrayData> result;
            ARROW_OK(Builder_.FinishInternal(&result));
            return Ctx_.HolderFactory.CreateArrowBlock(result);
        }

    private:
        TBuilder Builder_;
        TComputationContext& Ctx_;
    };

    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBlockAggregatorBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void InitState(void* state) final {
        new(state) TState();
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

        TSum sum = typedState->Sum_;
        if (!filtered) {
            for (int64_t i = 0; i < len; ++i) {
                sum += ptr[i];
            }
        } else {
            const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
            const auto& filterArray = filterDatum.array();
            MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
            auto filterBitmap = filterArray->template GetValues<uint8_t>(1, 0);
            for (int64_t i = 0; i < len; ++i) {
                ui64 fullIndex = i + array->offset;
                // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                TIn filterMask = (((filterBitmap[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                sum += ptr[i] & filterMask;
            }
        }

        typedState->Sum_ = sum;
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        return NUdf::TUnboxedValuePod(typedState->Sum_);
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        const auto& array = datum.array();
        auto ptr = array->GetValues<TIn>(1);
        typedState->Sum_ += ptr[row];
    }

    std::unique_ptr<IAggColumnBuilder> MakeBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TInScalar>
class TAvgBlockAggregator : public TBlockAggregatorBase {
public:
    struct TState {
        double Sum_ = 0;
        ui64 Count_ = 0;
    };

    TAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBlockAggregatorBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void InitState(void* state) final {
        new(state) TState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Sum_ += double((filtered ? *filtered : batchLength) * datum.scalar_as<TInScalar>().value);
                typedState->Count_ += batchLength;
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
                typedState->Count_ += count;
                double sum = typedState->Sum_;
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

                typedState->Sum_ = sum;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                auto filterBitmap = filterArray->template GetValues<uint8_t>(1, 0);

                double sum = typedState->Sum_;
                ui64 count = typedState->Count_;
                if (array->GetNullCount() == 0) {
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn filterMask = (((filterBitmap[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        sum += double(ptr[i] & filterMask);
                        count += filterMask & 1;
                    }
                } else {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        TIn filterMask = (((filterBitmap[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        mask &= filterMask;
                        sum += double(ptr[i] & mask);
                        count += mask & 1;
                    }
                }

                typedState->Sum_ = sum;
                typedState->Count_ = count;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        if (!typedState->Count_) {
            return NUdf::TUnboxedValuePod();
        }

        NUdf::TUnboxedValue* items;
        auto arr = Ctx_.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = NUdf::TUnboxedValuePod(typedState->Sum_);
        items[1] = NUdf::TUnboxedValuePod(typedState->Count_);
        return arr;
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Sum_ += double(datum.scalar_as<TInScalar>().value);
                typedState->Count_ += 1;
            }
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<TIn>(1);
            if (array->GetNullCount() == 0) {
                typedState->Sum_ += double(ptr[row]);
                typedState->Count_ += 1;
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                ui64 fullIndex = row + array->offset;
                // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                typedState->Sum_ += double(ptr[row] & mask);
                typedState->Count_ += mask & 1;
            }
        }
    }

    std::unique_ptr<IAggColumnBuilder> MakeBuilder(ui64 size) final {
        Y_UNUSED(size);
        MKQL_ENSURE(false, "TODO: support of tuples");
    }

private:
    const ui32 ArgColumn_;
};

class TBlockSumFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       TComputationContext& ctx) const final {
       auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(argsColumns[0]));
       auto argType = blockType->GetItemType();
       bool isOptional;
       auto dataType = UnpackOptionalData(argType, isOptional);
       if (blockType->GetShape() == TBlockType::EShape::Scalar || isOptional) {
           switch (*dataType->GetDataSlot()) {
           case NUdf::EDataSlot::Int8:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i8, i64, arrow::Int64Builder, arrow::Int8Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint8:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui8, ui64, arrow::UInt64Builder, arrow::UInt8Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
           case NUdf::EDataSlot::Int16:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i16, i64, arrow::Int64Builder, arrow::Int16Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint16:
           case NUdf::EDataSlot::Date:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui16, ui64, arrow::UInt64Builder, arrow::UInt16Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
           case NUdf::EDataSlot::Int32:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i32, i64, arrow::Int64Builder, arrow::Int32Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint32:
           case NUdf::EDataSlot::Datetime:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui32, ui64, arrow::UInt64Builder, arrow::UInt32Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
           case NUdf::EDataSlot::Int64:
           case NUdf::EDataSlot::Interval:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<i64, i64, arrow::Int64Builder, arrow::Int64Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint64:
           case NUdf::EDataSlot::Timestamp:
               return std::make_unique<TSumBlockAggregatorNullableOrScalar<ui64, ui64, arrow::UInt64Builder, arrow::UInt64Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
           default:
               throw yexception() << "Unsupported SUM input type";
           }
       } else {
           switch (*dataType->GetDataSlot()) {
           case NUdf::EDataSlot::Int8:
               return std::make_unique<TSumBlockAggregator<i8, i64, arrow::Int64Builder, arrow::Int8Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint8:
               return std::make_unique<TSumBlockAggregator<ui8, ui64, arrow::UInt64Builder, arrow::UInt8Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
           case NUdf::EDataSlot::Int16:
               return std::make_unique<TSumBlockAggregator<i16, i64, arrow::Int64Builder, arrow::Int16Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint16:
           case NUdf::EDataSlot::Date:
               return std::make_unique<TSumBlockAggregator<ui16, ui64, arrow::UInt64Builder, arrow::UInt16Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
           case NUdf::EDataSlot::Int32:
               return std::make_unique<TSumBlockAggregator<i32, i64, arrow::Int64Builder, arrow::Int32Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint32:
           case NUdf::EDataSlot::Datetime:
               return std::make_unique<TSumBlockAggregator<ui32, ui64, arrow::UInt64Builder, arrow::UInt32Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
           case NUdf::EDataSlot::Int64:
           case NUdf::EDataSlot::Interval:
               return std::make_unique<TSumBlockAggregator<i64, i64, arrow::Int64Builder, arrow::Int64Scalar>>(filterColumn, argsColumns[0], arrow::int64(), ctx);
           case NUdf::EDataSlot::Uint64:
           case NUdf::EDataSlot::Timestamp:
               return std::make_unique<TSumBlockAggregator<ui64, ui64, arrow::UInt64Builder, arrow::UInt64Scalar>>(filterColumn, argsColumns[0], arrow::uint64(), ctx);
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
       TComputationContext& ctx) const final {
       auto argType = AS_TYPE(TBlockType, tupleType->GetElementType(argsColumns[0]))->GetItemType();
       bool isOptional;
       auto dataType = UnpackOptionalData(argType, isOptional);
       switch (*dataType->GetDataSlot()) {
       case NUdf::EDataSlot::Int8:
           return std::make_unique<TAvgBlockAggregator<i8, arrow::Int8Scalar>>(filterColumn, argsColumns[0], ctx);
       case NUdf::EDataSlot::Uint8:
           return std::make_unique<TAvgBlockAggregator<ui8, arrow::UInt8Scalar>>(filterColumn, argsColumns[0], ctx);
       case NUdf::EDataSlot::Int16:
           return std::make_unique<TAvgBlockAggregator<i16, arrow::Int16Scalar>>(filterColumn, argsColumns[0], ctx);
       case NUdf::EDataSlot::Uint16:
       case NUdf::EDataSlot::Date:
           return std::make_unique<TAvgBlockAggregator<ui16, arrow::UInt16Scalar>>(filterColumn, argsColumns[0], ctx);
       case NUdf::EDataSlot::Int32:
           return std::make_unique<TAvgBlockAggregator<i32, arrow::Int32Scalar>>(filterColumn, argsColumns[0], ctx);
       case NUdf::EDataSlot::Uint32:
       case NUdf::EDataSlot::Datetime:
           return std::make_unique<TAvgBlockAggregator<ui32, arrow::UInt32Scalar>>(filterColumn, argsColumns[0], ctx);
       case NUdf::EDataSlot::Int64:
       case NUdf::EDataSlot::Interval:
           return std::make_unique<TAvgBlockAggregator<i64, arrow::Int64Scalar>>(filterColumn, argsColumns[0], ctx);
       case NUdf::EDataSlot::Uint64:
       case NUdf::EDataSlot::Timestamp:
           return std::make_unique<TAvgBlockAggregator<ui64, arrow::UInt64Scalar>>(filterColumn, argsColumns[0], ctx);
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
