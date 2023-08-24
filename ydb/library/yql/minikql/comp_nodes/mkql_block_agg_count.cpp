#include "mkql_block_agg_count.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TState {
    ui64 Count_ = 0;
};

class TColumnBuilder : public IAggColumnBuilder {
public:
    TColumnBuilder(ui64 size, TComputationContext& ctx)
        : Builder_(TTypeInfoHelper(), arrow::uint64(), ctx.ArrowMemoryPool, size)
        , Ctx_(ctx)
    {
    }

    void Add(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        Builder_.Add(TBlockItem(typedState->Count_));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_.Build(true));
    }

private:
    NYql::NUdf::TFixedSizeArrayBuilder<ui64, false> Builder_;
    TComputationContext& Ctx_;
};

template <typename TTag>
class TCountAllAggregator;

template <typename TTag>
class TCountAggregator;

template <>
class TCountAllAggregator<TCombineAllTag> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TCountAllAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
    {
        Y_UNUSED(argColumn);
    }

    void InitState(void* state) final {
        new(state) TState();
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TState>::value);
        Y_UNUSED(state);
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        Y_UNUSED(columns);
        if (filtered) {
            typedState->Count_ += *filtered;
        }
        else {
            typedState->Count_ += batchLength;
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        return NUdf::TUnboxedValuePod(typedState->Count_);
    }
};

template <>
class TCountAllAggregator<TCombineKeysTag> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TCountAllAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
    {
        Y_UNUSED(argColumn);
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateKey(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TState>::value);
        Y_UNUSED(state);
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        Y_UNUSED(columns);
        Y_UNUSED(row);
        auto typedState = static_cast<TState*>(state);
        typedState->Count_ += 1;
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, Ctx_);
    }
};

template <>
class TCountAllAggregator<TFinalizeKeysTag> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TCountAllAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateState(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TState>::value);
        Y_UNUSED(state);
    }

    void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            MKQL_ENSURE(datum.scalar()->is_valid, "Expected not null");
            typedState->Count_ += datum.scalar_as<arrow::UInt64Scalar>().value;
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<ui64>(1);
            MKQL_ENSURE(array->GetNullCount() == 0, "Expected not null");
            typedState->Count_ += ptr[row];
        }
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, Ctx_);
    }

private:
    const ui32 ArgColumn_;
};

template <>
class TCountAggregator<TCombineAllTag> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TCountAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void InitState(void* state) final {
        new(state) TState();
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TState>::value);
        Y_UNUSED(state);
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Count_ += filtered ? *filtered : batchLength;
            }
        } else {
            const auto& array = datum.array();
            if (!filtered) {
                typedState->Count_ += array->length - array->GetNullCount();
            } else if (array->GetNullCount() == array->length) {
                // all nulls
                return;
            } else if (array->GetNullCount() == 0) {
                // no nulls
                typedState->Count_ += *filtered;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                // intersect masks from nulls and filter column
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                const ui8* filterBitmap = filterArray->GetValues<uint8_t>(1);
                auto state = typedState->Count_;
                for (ui32 i = 0; i < array->length; ++i) {
                    ui64 fullIndex = i + array->offset;
                    auto bit1 = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                    auto bit2 = filterBitmap[i];
                    state += bit1 & bit2;
                }

                typedState->Count_ = state;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        return NUdf::TUnboxedValuePod(typedState->Count_);
    }

private:
    const ui32 ArgColumn_;
};

template <>
class TCountAggregator<TCombineKeysTag> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TCountAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateKey(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TState>::value);
        Y_UNUSED(state);
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TState*>(state);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                typedState->Count_ += 1;
            }
        } else {
            const auto& array = datum.array();
            if (array->GetNullCount() == 0) {
                typedState->Count_ += 1;
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                auto fullIndex = row + array->offset;
                auto bit = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                typedState->Count_ += bit;
            }
        }
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, Ctx_);
    }

private:
    const ui32 ArgColumn_;
};

template <>
class TCountAggregator<TFinalizeKeysTag> : public TCountAllAggregator<TFinalizeKeysTag>
{
public:
    using TBase = TCountAllAggregator<TFinalizeKeysTag>;

    TCountAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(filterColumn, argColumn, ctx)
    {}
};

template <typename TTag>
class TPreparedCountAll : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedCountAll(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TState))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TCountAllAggregator<TTag>>(FilterColumn_, ArgColumn_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
};

template <typename TTag>
class TPreparedCount : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedCount(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TState))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TCountAggregator<TTag>>(FilterColumn_, ArgColumn_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
};

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareCountAll(std::optional<ui32> filterColumn, ui32 argColumn) {
    return std::make_unique<TPreparedCountAll<TTag>>(filterColumn, argColumn);
}

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareCount(std::optional<ui32> filterColumn, ui32 argColumn) {
    return std::make_unique<TPreparedCount<TTag>>(filterColumn, argColumn);
}

class TBlockCountAllFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(argsColumns);
        Y_UNUSED(env);
        return PrepareCountAll<TCombineAllTag>(filterColumn, 0);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(argsColumns);
        Y_UNUSED(env);
        return PrepareCountAll<TCombineKeysTag>(std::optional<ui32>(), 0);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env,
        TType* returnType) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(argsColumns);
        Y_UNUSED(env);
        Y_UNUSED(returnType);
        return PrepareCountAll<TFinalizeKeysTag>(std::optional<ui32>(), argsColumns[0]);
    }
};

class TBlockCountFactory : public IBlockAggregatorFactory {
public:
    std::unique_ptr<TCombineAllTag::TPreparedAggregator> PrepareCombineAll(
        TTupleType* tupleType,
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(env);
        return PrepareCount<TCombineAllTag>(filterColumn, argsColumns[0]);
    }

    std::unique_ptr<TCombineKeysTag::TPreparedAggregator> PrepareCombineKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(argsColumns);
        Y_UNUSED(env);
        return PrepareCount<TCombineKeysTag>(std::optional<ui32>(), argsColumns[0]);
    }

    std::unique_ptr<TFinalizeKeysTag::TPreparedAggregator> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env,
        TType* returnType) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(argsColumns);
        Y_UNUSED(env);
        Y_UNUSED(returnType);
        return PrepareCount<TFinalizeKeysTag>(std::optional<ui32>(), argsColumns[0]);
    }
};

}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockCountAllFactory() {
    return std::make_unique<TBlockCountAllFactory>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockCountFactory() {
    return std::make_unique<TBlockCountFactory>();
}
 
}
}
