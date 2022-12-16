#include "mkql_block_agg_minmax.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <arrow/scalar.h>
#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsMin, typename T>
T UpdateMinMax(T x, T y) {
    if constexpr (IsMin) {
        return x < y ? x : y;
    } else {
        return x > y ? x : y;
    }
}

template <typename TTag, typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregatorNullableOrScalar;

template <typename TTag, typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregator;

template <typename TIn, bool IsMin>
struct TState {
    TIn Value_;
    ui8 IsValid_ = 0;

    TState() {
        if constexpr (IsMin) {
            Value_ = std::numeric_limits<TIn>::max();
        } else {
            Value_ = std::numeric_limits<TIn>::min();
        }
    }
};

template <typename TIn, bool IsMin>
struct TSimpleState {
    TIn Value_;

    TSimpleState() {
        if constexpr (IsMin) {
            Value_ = std::numeric_limits<TIn>::max();
        } else {
            Value_ = std::numeric_limits<TIn>::min();
        }
    }
};

template <typename TIn, bool IsMin, typename TBuilder>
class TColumnBuilder : public IAggColumnBuilder {
public:
    TColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& dataType, TComputationContext& ctx)
        : Builder_(dataType, &ctx.ArrowMemoryPool)
        , Ctx_(ctx)
    {
        ARROW_OK(Builder_.Reserve(size));
    }

    void Add(const void* state) final {
        auto typedState = static_cast<const TState<TIn, IsMin>*>(state);
        if (typedState->IsValid_) {
            Builder_.UnsafeAppend(typedState->Value_);
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

template <typename TIn, bool IsMin, typename TBuilder>
class TSimpleColumnBuilder : public IAggColumnBuilder {
public:
    TSimpleColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& dataType, TComputationContext& ctx)
        : Builder_(dataType, &ctx.ArrowMemoryPool)
        , Ctx_(ctx)
    {
        ARROW_OK(Builder_.Reserve(size));
    }

    void Add(const void* state) final {
        auto typedState = static_cast<const TSimpleState<TIn, IsMin>*>(state);
        Builder_.UnsafeAppend(typedState->Value_);
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

template <typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregatorNullableOrScalar<TCombineAllTag, TIn, TInScalar, TBuilder, IsMin> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TMinMaxBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TState<TIn, IsMin>), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(builderDataType);
    }

    void InitState(void* state) final {
        new(state) TState<TIn, IsMin>();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState<TIn, IsMin>*>(state);
        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Value_ = datum.scalar_as<TInScalar>().value;
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
                        TIn mask = -TIn((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
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
                    typedState->IsValid_ = 1;
                    for (int64_t i = 0; i < len; ++i) {
                        TIn filterMask = -TIn(filterBitmap[i]);
                        value = UpdateMinMax<IsMin>(value, TIn((ptr[i] & filterMask) | (value & ~filterMask)));
                    }
                } else {
                    ui64 count = 0;
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn mask = -TIn((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                        TIn filterMask = -TIn(filterBitmap[i]);
                        mask &= filterMask;
                        value = UpdateMinMax<IsMin>(value, TIn((ptr[i] & mask) | (value & ~mask)));
                        count += mask & 1;
                    }

                    typedState->IsValid_ |= count ? 1 : 0;
                }

                typedState->Value_ = value;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState<TIn, IsMin>*>(state);
        if (!typedState->IsValid_) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(typedState->Value_);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TIn, typename TInScalar, bool IsMin>
void PushValueToState(TState<TIn, IsMin>* typedState, const arrow::Datum& datum, ui64 row) {
    if (datum.is_scalar()) {
        if (datum.scalar()->is_valid) {
            typedState->Value_ = datum.scalar_as<TInScalar>().value;
            typedState->IsValid_ = 1;
        }
    } else {
        const auto& array = datum.array();
        auto ptr = array->GetValues<TIn>(1);
        if (array->GetNullCount() == 0) {
            typedState->IsValid_ = 1;
            typedState->Value_ = UpdateMinMax<IsMin>(typedState->Value_, ptr[row]);
        } else {
            auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
            ui64 fullIndex = row + array->offset;
            // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
            TIn mask = -TIn((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
            typedState->Value_ = UpdateMinMax<IsMin>(typedState->Value_, TIn((ptr[row] & mask) | (typedState->Value_ & ~mask)));
            typedState->IsValid_ |= mask & 1;
        }
    }
}

template <typename TIn, bool IsMin>
void PushValueToSimpleState(TSimpleState<TIn, IsMin>* typedState, const arrow::Datum& datum, ui64 row) {
    const auto& array = datum.array();
    auto ptr = array->GetValues<TIn>(1);
    typedState->Value_ = UpdateMinMax<IsMin>(typedState->Value_, ptr[row]);
}

template <typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregatorNullableOrScalar<TCombineKeysTag, TIn, TInScalar, TBuilder, IsMin> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TMinMaxBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TState<TIn, IsMin>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState<TIn, IsMin>();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TState<TIn, IsMin>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<TIn, TInScalar, IsMin>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder<TIn, IsMin, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregatorNullableOrScalar<TFinalizeKeysTag, TIn, TInScalar, TBuilder, IsMin> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TMinMaxBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TState<TIn, IsMin>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void LoadState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState<TIn, IsMin>();
        UpdateState(state, columns, row);
    }

    void UpdateState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TState<TIn, IsMin>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<TIn, TInScalar, IsMin>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder<TIn, IsMin, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregator<TCombineAllTag, TIn, TInScalar, TBuilder, IsMin> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TMinMaxBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSimpleState<TIn, IsMin>), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(builderDataType);
    }

    void InitState(void* state) final {
        new(state) TSimpleState<TIn, IsMin>;
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TSimpleState<TIn, IsMin>*>(state);
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
                TIn filterMask = -TIn(filterBitmap[i]);
                value = UpdateMinMax<IsMin>(value, TIn((ptr[i] & filterMask) | (value & ~filterMask)));
            }

            typedState->Value_ = value;
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TSimpleState<TIn, IsMin>*>(state);
        return NUdf::TUnboxedValuePod(typedState->Value_);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregator<TCombineKeysTag, TIn, TInScalar, TBuilder, IsMin> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TMinMaxBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSimpleState<TIn, IsMin>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TSimpleState<TIn, IsMin>();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TSimpleState<TIn, IsMin>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToSimpleState<TIn, IsMin>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TSimpleColumnBuilder<TIn, IsMin, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TMinMaxBlockAggregator<TFinalizeKeysTag, TIn, TInScalar, TBuilder, IsMin> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TMinMaxBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSimpleState<TIn, IsMin>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void LoadState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TSimpleState<TIn, IsMin>();
        UpdateState(state, columns, row);
    }

    void UpdateState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TSimpleState<TIn, IsMin>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToSimpleState<TIn, IsMin>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TSimpleColumnBuilder<TIn, IsMin, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TTag, typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TPreparedMinMaxBlockAggregatorNullableOrScalar : public TTag::TPreparedAggregator {
public:
    TPreparedMinMaxBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType)
        : FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TMinMaxBlockAggregatorNullableOrScalar<TTag, TIn, TInScalar, TBuilder, IsMin>>(FilterColumn_, ArgColumn_, BuilderDataType_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TTag, typename TIn, typename TInScalar, typename TBuilder, bool IsMin>
class TPreparedMinMaxBlockAggregator : public TTag::TPreparedAggregator {
public:
    TPreparedMinMaxBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType)
        : FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TMinMaxBlockAggregator<TTag, TIn, TInScalar, TBuilder, IsMin>>(FilterColumn_, ArgColumn_, BuilderDataType_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TTag, bool IsMin>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareMinMax(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn) {
    auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn));
    auto argType = blockType->GetItemType();
    bool isOptional;
    auto dataType = UnpackOptionalData(argType, isOptional);
    if (blockType->GetShape() == TBlockType::EShape::Scalar || isOptional) {
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, i8, arrow::Int8Scalar, arrow::Int8Builder, IsMin>>(filterColumn, argColumn, arrow::int8());
        case NUdf::EDataSlot::Bool:
        case NUdf::EDataSlot::Uint8:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, ui8, arrow::UInt8Scalar, arrow::UInt8Builder, IsMin>>(filterColumn, argColumn, arrow::uint8());
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, i16, arrow::Int16Scalar, arrow::Int16Builder, IsMin>>(filterColumn, argColumn, arrow::int16());
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, ui16, arrow::UInt16Scalar, arrow::UInt16Builder, IsMin>>(filterColumn, argColumn, arrow::uint16());
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, i32, arrow::Int32Scalar, arrow::Int32Builder, IsMin>>(filterColumn, argColumn, arrow::int32());
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, ui32, arrow::UInt32Scalar, arrow::UInt32Builder, IsMin>>(filterColumn, argColumn, arrow::uint32());
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, i64, arrow::Int64Scalar, arrow::Int64Builder, IsMin>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TPreparedMinMaxBlockAggregatorNullableOrScalar<TTag, ui64, arrow::UInt64Scalar, arrow::UInt64Builder, IsMin>>(filterColumn, argColumn, arrow::uint64());
        default:
            throw yexception() << "Unsupported MIN/MAX input type";
        }
    }
    else {
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, i8, arrow::Int8Scalar, arrow::Int8Builder, IsMin>>(filterColumn, argColumn, arrow::int8());
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Bool:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, ui8, arrow::UInt8Scalar, arrow::UInt8Builder, IsMin>>(filterColumn, argColumn, arrow::uint8());
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, i16, arrow::Int16Scalar, arrow::Int16Builder, IsMin>>(filterColumn, argColumn, arrow::int16());
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, ui16, arrow::UInt16Scalar, arrow::UInt16Builder, IsMin>>(filterColumn, argColumn, arrow::uint16());
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, i32, arrow::Int32Scalar, arrow::Int32Builder, IsMin>>(filterColumn, argColumn, arrow::int32());
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, ui32, arrow::UInt32Scalar, arrow::UInt32Builder, IsMin>>(filterColumn, argColumn, arrow::uint32());
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, i64, arrow::Int64Scalar, arrow::Int64Builder, IsMin>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            return std::make_unique<TPreparedMinMaxBlockAggregator<TTag, ui64, arrow::UInt64Scalar, arrow::UInt64Builder, IsMin>>(filterColumn, argColumn, arrow::uint64());
        default:
            throw yexception() << "Unsupported MIN/MAX input type";
        }
    }
}

template <bool IsMin>
class TBlockMinMaxFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareMinMax<TCombineAllTag, IsMin>(tupleType, filterColumn, argsColumns[0]);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareMinMax<TCombineKeysTag, IsMin>(tupleType, filterColumn, argsColumns[0]);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareMinMax<TFinalizeKeysTag, IsMin>(tupleType, std::optional<ui32>(), argsColumns[0]);
    }
};

}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockMinFactory() {
    return std::make_unique<TBlockMinMaxFactory<true>>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockMaxFactory() {
    return std::make_unique<TBlockMinMaxFactory<false>>();
}
 
}
}
