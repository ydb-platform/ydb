#include "mkql_block_agg_sum.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <arrow/scalar.h>
#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename TSum>
struct TSumState {
    TSum Sum_ = 0;
    ui8 IsValid_ = 0;
};

template <typename TSum>
struct TSumSimpleState {
    TSum Sum_ = 0;
};

struct TAvgState {
    double Sum_ = 0;
    ui64 Count_ = 0;
};

template <typename TSum, typename TBuilder>
class TSumColumnBuilder : public IAggColumnBuilder {
public:
    TSumColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& dataType, TComputationContext& ctx)
        : Builder_(dataType, &ctx.ArrowMemoryPool)
        , Ctx_(ctx)
    {
        ARROW_OK(Builder_.Reserve(size));
    }

    void Add(const void* state) final {
        auto typedState = static_cast<const TSumState<TSum>*>(state);
        if (typedState->IsValid_) {
            Builder_.UnsafeAppend(typedState->Sum_);
        }
        else {
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

template <typename TSum, typename TBuilder>
class TSimpleSumColumnBuilder : public IAggColumnBuilder {
public:
    TSimpleSumColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& dataType, TComputationContext& ctx)
        : Builder_(dataType, &ctx.ArrowMemoryPool)
        , Ctx_(ctx)
    {
        ARROW_OK(Builder_.Reserve(size));
    }

    void Add(const void* state) final {
        auto typedState = static_cast<const TSumSimpleState<TSum>*>(state);
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

class TAvgStateColumnBuilder : public IAggColumnBuilder {
public:
    TAvgStateColumnBuilder(ui64 size, const std::shared_ptr<arrow::DataType>& arrowType, TComputationContext& ctx)
        : ArrowType_(arrowType)
        , Ctx_(ctx)
        , NullBitmapBuilder_(&ctx.ArrowMemoryPool)
        , SumBuilder_(arrow::float64(), &ctx.ArrowMemoryPool)
        , CountBuilder_(arrow::uint64(), &ctx.ArrowMemoryPool)
    {
        ARROW_OK(NullBitmapBuilder_.Reserve(size));
        ARROW_OK(SumBuilder_.Reserve(size));
        ARROW_OK(CountBuilder_.Reserve(size));
    }

    void Add(const void* state) final {
        auto typedState = static_cast<const TAvgState*>(state);
        if (typedState->Count_) {
            NullBitmapBuilder_.UnsafeAppend(true);
            SumBuilder_.UnsafeAppend(typedState->Sum_);
            CountBuilder_.UnsafeAppend(typedState->Count_);
        } else {
            NullBitmapBuilder_.UnsafeAppend(false);
            SumBuilder_.UnsafeAppendNull();
            CountBuilder_.UnsafeAppendNull();
        }
    }

    NUdf::TUnboxedValue Build() final {
        std::shared_ptr<arrow::ArrayData> sumResult;
        std::shared_ptr<arrow::ArrayData> countResult;
        ARROW_OK(SumBuilder_.FinishInternal(&sumResult));
        ARROW_OK(CountBuilder_.FinishInternal(&countResult));
        std::shared_ptr<arrow::Buffer> nullBitmap;
        auto length = NullBitmapBuilder_.length();
        auto nullCount = NullBitmapBuilder_.false_count();
        ARROW_OK(NullBitmapBuilder_.Finish(&nullBitmap));

        auto arrayData = arrow::ArrayData::Make(ArrowType_, length, { nullBitmap }, nullCount, 0);
        arrayData->child_data.push_back(sumResult);
        arrayData->child_data.push_back(countResult);
        return Ctx_.HolderFactory.CreateArrowBlock(arrow::Datum(arrayData));
    }

private:
    const std::shared_ptr<arrow::DataType> ArrowType_;
    TComputationContext& Ctx_;
    arrow::TypedBufferBuilder<bool> NullBitmapBuilder_;
    arrow::DoubleBuilder SumBuilder_;
    arrow::UInt64Builder CountBuilder_;
};

class TAvgResultColumnBuilder : public IAggColumnBuilder {
public:
    TAvgResultColumnBuilder(ui64 size, TComputationContext& ctx)
        : Ctx_(ctx)
        , Builder_(arrow::float64(), &ctx.ArrowMemoryPool)
    {
        ARROW_OK(Builder_.Reserve(size));
    }

    void Add(const void* state) final {
        auto typedState = static_cast<const TAvgState*>(state);
        if (typedState->Count_) {
            Builder_.UnsafeAppend(typedState->Sum_ / typedState->Count_);
        } else {
            Builder_.UnsafeAppendNull();
        }
    }

    NUdf::TUnboxedValue Build() final {
        std::shared_ptr<arrow::ArrayData> result;
        ARROW_OK(Builder_.FinishInternal(&result));
        return Ctx_.HolderFactory.CreateArrowBlock(arrow::Datum(result));
    }

private:
    TComputationContext& Ctx_;
    arrow::DoubleBuilder Builder_;
};

template <typename TTag, typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregatorNullableOrScalar;

template <typename TTag, typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregator;

template <typename TTag, typename TIn, typename TInScalar>
class TAvgBlockAggregator;

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregatorNullableOrScalar<TCombineAllTag, TIn, TSum, TBuilder, TInScalar> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TSumBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSumState<TSum>), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(builderDataType);
    }

    void InitState(void* state) final {
        new(state) TSumState<TSum>();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TSumState<TSum>*>(state);
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
                const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);
                TSum sum = typedState->Sum_;
                if (array->GetNullCount() == 0) {
                    typedState->IsValid_ = 1;
                    for (int64_t i = 0; i < len; ++i) {
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn filterMask = -TIn(filterBitmap[i]);
                        sum += ptr[i] & filterMask;
                    }
                } else {
                    ui64 count = 0;
                    auto nullBitmapPtr = array->template GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn mask = (((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1) ^ 1) - TIn(1);
                        TIn filterMask = -TIn(filterBitmap[i]);
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
        auto typedState = static_cast<const TSumState<TSum>*>(state);
        if (!typedState->IsValid_) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(typedState->Sum_);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TIn, typename TSum, typename TInScalar>
void PushValueToState(TSumState<TSum>* typedState, const arrow::Datum& datum, ui64 row) {
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

template <typename TIn, typename TSum>
void PushValueToSimpleState(TSumSimpleState<TSum>* typedState, const arrow::Datum& datum, ui64 row) {
    const auto& array = datum.array();
    auto ptr = array->GetValues<TIn>(1);
    typedState->Sum_ += ptr[row];
}

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregatorNullableOrScalar<TCombineKeysTag, TIn, TSum, TBuilder, TInScalar> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TSumBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSumState<TSum>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TSumState<TSum>();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TSumState<TSum>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<TIn, TSum, TInScalar>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TSumColumnBuilder<TSum, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregatorNullableOrScalar<TFinalizeKeysTag, TIn, TSum, TBuilder, TInScalar> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TSumBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSumState<TSum>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void LoadState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TSumState<TSum>();
        UpdateState(state, columns, row);
    }

    void UpdateState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TSumState<TSum>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState<TIn, TSum, TInScalar>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TSumColumnBuilder<TSum, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregator<TCombineAllTag, TIn, TSum, TBuilder, TInScalar> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSumSimpleState<TSum>), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(builderDataType);
    }

    void InitState(void* state) final {
        new(state) TSumSimpleState<TSum>();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TSumSimpleState<TSum>*>(state);
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
            const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);
            for (int64_t i = 0; i < len; ++i) {
                // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                TIn filterMask = -TIn(filterBitmap[i]);
                sum += ptr[i] & filterMask;
            }
        }

        typedState->Sum_ = sum;
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TSumSimpleState<TSum>*>(state);
        return NUdf::TUnboxedValuePod(typedState->Sum_);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregator<TCombineKeysTag, TIn, TSum, TBuilder, TInScalar> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSumSimpleState<TSum>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TSumSimpleState<TSum>();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TSumSimpleState<TSum>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToSimpleState<TIn, TSum>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TSimpleSumColumnBuilder<TSum, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TSumBlockAggregator<TFinalizeKeysTag, TIn, TSum, TBuilder, TInScalar> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TSumSimpleState<TSum>), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void LoadState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TSumSimpleState<TSum>();
        UpdateState(state, columns, row);
    }

    void UpdateState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TSumSimpleState<TSum>*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToSimpleState<TIn, TSum>(typedState, datum, row);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TSimpleSumColumnBuilder<TSum, TBuilder>>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TIn, typename TInScalar>
class TAvgBlockAggregator<TCombineAllTag, TIn, TInScalar> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TAvgState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
        Y_UNUSED(builderDataType);
    }

    void InitState(void* state) final {
        new(state) TAvgState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TAvgState*>(state);
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
                const ui8* filterBitmap = filterArray->template GetValues<uint8_t>(1);

                double sum = typedState->Sum_;
                ui64 count = typedState->Count_;
                if (array->GetNullCount() == 0) {
                    for (int64_t i = 0; i < len; ++i) {
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn filterMask = -TIn(filterBitmap[i]);
                        sum += double(ptr[i] & filterMask);
                        count += filterMask & 1;
                    }
                } else {
                    auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                    for (int64_t i = 0; i < len; ++i) {
                        ui64 fullIndex = i + array->offset;
                        // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                        TIn mask = -TIn((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                        TIn filterMask = -TIn(filterBitmap[i]);
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
        auto typedState = static_cast<const TAvgState*>(state);
        if (!typedState->Count_) {
            return NUdf::TUnboxedValuePod();
        }

        NUdf::TUnboxedValue* items;
        auto arr = Ctx_.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = NUdf::TUnboxedValuePod(typedState->Sum_);
        items[1] = NUdf::TUnboxedValuePod(typedState->Count_);
        return arr;
    }

private:
    ui32 ArgColumn_;
};

template <typename TIn, typename TInScalar>
class TAvgBlockAggregator<TCombineKeysTag, TIn, TInScalar> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType, TComputationContext& ctx)
        : TBase(sizeof(TAvgState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TAvgState();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TAvgState*>(state);
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

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TAvgStateColumnBuilder>(size, BuilderDataType_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

class TAvgBlockAggregatorOverState : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TAvgBlockAggregatorOverState(ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TAvgState), {}, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void LoadState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TAvgState();
        UpdateState(state, columns, row);
    }

    void UpdateState(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        auto typedState = static_cast<TAvgState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(*datum.scalar());

                typedState->Sum_ += arrow::internal::checked_cast<const arrow::DoubleScalar&>(*structScalar.value[0]).value;
                typedState->Count_ += arrow::internal::checked_cast<const arrow::UInt64Scalar&>(*structScalar.value[1]).value;
            }
        } else {
            const auto& array = datum.array();
            auto sumPtr = array->child_data[0]->GetValues<double>(1);
            auto countPtr = array->child_data[1]->GetValues<ui64>(1);
            if (array->GetNullCount() == 0) {
                typedState->Sum_ += sumPtr[row];
                typedState->Count_ += countPtr[row];
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                ui64 fullIndex = row + array->offset;
                // bit 1 -> mask 0xFF..FF, bit 0 -> mask 0x00..00
                auto bit = (nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1;
                ui64 mask = -ui64(bit);
                typedState->Sum_ += sumPtr[row] * bit;
                typedState->Count_ += mask & countPtr[row];
            }
        }
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TAvgResultColumnBuilder>(size, Ctx_);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TTag, typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TPreparedSumBlockAggregatorNullableOrScalar : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedSumBlockAggregatorNullableOrScalar(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType)
        : TBase(sizeof(TSumState<TSum>))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TSumBlockAggregatorNullableOrScalar<TTag, TIn, TSum, TBuilder, TInScalar>>(FilterColumn_, ArgColumn_, BuilderDataType_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TTag, typename TIn, typename TSum, typename TBuilder, typename TInScalar>
class TPreparedSumBlockAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedSumBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType)
        : TBase(sizeof(TSumSimpleState<TSum>))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TSumBlockAggregator<TTag, TIn, TSum, TBuilder, TInScalar>>(FilterColumn_, ArgColumn_, BuilderDataType_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareSum(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn) {
    auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn));
    auto argType = blockType->GetItemType();
    bool isOptional;
    auto dataType = UnpackOptionalData(argType, isOptional);
    if (blockType->GetShape() == TBlockType::EShape::Scalar || isOptional) {
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, i8, i64, arrow::Int64Builder, arrow::Int8Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint8:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, ui8, ui64, arrow::UInt64Builder, arrow::UInt8Scalar>>(filterColumn, argColumn, arrow::uint64());
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, i16, i64, arrow::Int64Builder, arrow::Int16Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint16:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, ui16, ui64, arrow::UInt64Builder, arrow::UInt16Scalar>>(filterColumn, argColumn, arrow::uint64());
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, i32, i64, arrow::Int64Builder, arrow::Int32Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint32:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, ui32, ui64, arrow::UInt64Builder, arrow::UInt32Scalar>>(filterColumn, argColumn, arrow::uint64());
        case NUdf::EDataSlot::Int64:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, i64, i64, arrow::Int64Builder, arrow::Int64Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint64:
            return std::make_unique<TPreparedSumBlockAggregatorNullableOrScalar<TTag, ui64, ui64, arrow::UInt64Builder, arrow::UInt64Scalar>>(filterColumn, argColumn, arrow::uint64());
        default:
            throw yexception() << "Unsupported SUM input type";
        }
    } else {
        switch (*dataType->GetDataSlot()) {
        case NUdf::EDataSlot::Int8:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, i8, i64, arrow::Int64Builder, arrow::Int8Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint8:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, ui8, ui64, arrow::UInt64Builder, arrow::UInt8Scalar>>(filterColumn, argColumn, arrow::uint64());
        case NUdf::EDataSlot::Int16:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, i16, i64, arrow::Int64Builder, arrow::Int16Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint16:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, ui16, ui64, arrow::UInt64Builder, arrow::UInt16Scalar>>(filterColumn, argColumn, arrow::uint64());
        case NUdf::EDataSlot::Int32:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, i32, i64, arrow::Int64Builder, arrow::Int32Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint32:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, ui32, ui64, arrow::UInt64Builder, arrow::UInt32Scalar>>(filterColumn, argColumn, arrow::uint64());
        case NUdf::EDataSlot::Int64:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, i64, i64, arrow::Int64Builder, arrow::Int64Scalar>>(filterColumn, argColumn, arrow::int64());
        case NUdf::EDataSlot::Uint64:
            return std::make_unique<TPreparedSumBlockAggregator<TTag, ui64, ui64, arrow::UInt64Builder, arrow::UInt64Scalar>>(filterColumn, argColumn, arrow::uint64());
        default:
            throw yexception() << "Unsupported SUM input type";
        }
    }
}

class TBlockSumFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareSum<TCombineAllTag>(tupleType, filterColumn, argsColumns[0]);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareSum<TCombineKeysTag>(tupleType, filterColumn, argsColumns[0]);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(env);
        return PrepareSum<TFinalizeKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0]);
    }
};

template <typename TTag, typename TIn, typename TInScalar>
class TPreparedAvgBlockAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedAvgBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn,
        const std::shared_ptr<arrow::DataType>& builderDataType)
        : TBase(sizeof(TAvgState))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
        , BuilderDataType_(builderDataType)
    {}
    
    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TAvgBlockAggregator<TTag, TIn, TInScalar>>(FilterColumn_, ArgColumn_, BuilderDataType_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
    const std::shared_ptr<arrow::DataType> BuilderDataType_;
};

class TPreparedAvgBlockAggregatorOverState : public TFinalizeKeysTag::TPreparedAggregator {
public:
    using TBase = TFinalizeKeysTag::TPreparedAggregator;

    TPreparedAvgBlockAggregatorOverState(ui32 argColumn)
        : TBase(sizeof(TAvgState))
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TFinalizeKeysTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TAvgBlockAggregatorOverState>(ArgColumn_, ctx);
    }

private:
    const ui32 ArgColumn_;
};

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareAvg(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env);

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareAvgOverInput(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    auto doubleType = TDataType::Create(NUdf::TDataType<double>::Id, env);
    auto ui64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, env);
    TVector<TType*> tupleElements = { doubleType, ui64Type };
    auto avgRetType = TTupleType::Create(2, tupleElements.data(), env);
    std::shared_ptr<arrow::DataType> builderDataType;
    MKQL_ENSURE(ConvertArrowType(avgRetType, builderDataType), "Unsupported builder type");

    auto argType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn))->GetItemType();
    bool isOptional;
    auto dataType = UnpackOptionalData(argType, isOptional);
    switch (*dataType->GetDataSlot()) {
    case NUdf::EDataSlot::Int8:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i8, arrow::Int8Scalar>>(filterColumn, argColumn, builderDataType);
    case NUdf::EDataSlot::Uint8:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui8, arrow::UInt8Scalar>>(filterColumn, argColumn, builderDataType);
    case NUdf::EDataSlot::Int16:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i16, arrow::Int16Scalar>>(filterColumn, argColumn, builderDataType);
    case NUdf::EDataSlot::Uint16:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui16, arrow::UInt16Scalar>>(filterColumn, argColumn, builderDataType);
    case NUdf::EDataSlot::Int32:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i32, arrow::Int32Scalar>>(filterColumn, argColumn, builderDataType);
    case NUdf::EDataSlot::Uint32:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui32, arrow::UInt32Scalar>>(filterColumn, argColumn, builderDataType);
    case NUdf::EDataSlot::Int64:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, i64, arrow::Int64Scalar>>(filterColumn, argColumn, builderDataType);
    case NUdf::EDataSlot::Uint64:
        return std::make_unique<TPreparedAvgBlockAggregator<TTag, ui64, arrow::UInt64Scalar>>(filterColumn, argColumn, builderDataType);
    default:
        throw yexception() << "Unsupported AVG input type";
    }
}

template <>
std::unique_ptr<typename TCombineAllTag::TPreparedAggregator> PrepareAvg<TCombineAllTag>(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    return PrepareAvgOverInput<TCombineAllTag>(tupleType, filterColumn, argColumn, env);
}

template <>
std::unique_ptr<typename TCombineKeysTag::TPreparedAggregator> PrepareAvg<TCombineKeysTag>(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    return PrepareAvgOverInput<TCombineKeysTag>(tupleType, filterColumn, argColumn, env);
}

template <>
std::unique_ptr<typename TFinalizeKeysTag::TPreparedAggregator> PrepareAvg<TFinalizeKeysTag>(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn, const TTypeEnvironment& env) {
    Y_UNUSED(tupleType);
    Y_UNUSED(filterColumn);
    Y_UNUSED(env);
    return std::make_unique<TPreparedAvgBlockAggregatorOverState>(argColumn);
}

class TBlockAvgFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        return PrepareAvg<TCombineAllTag>(tupleType, filterColumn, argsColumns[0], env);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        return PrepareAvg<TCombineKeysTag>(tupleType, filterColumn, argsColumns[0], env);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        return PrepareAvg<TFinalizeKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0], env);
    }
};

}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockSumFactory() {
    return std::make_unique<TBlockSumFactory>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockAvgFactory() {
    return std::make_unique<TBlockAvgFactory>();
}

}
}
