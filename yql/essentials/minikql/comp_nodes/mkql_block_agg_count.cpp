#include "mkql_block_agg_count.h"

#include <yql/essentials/minikql/arrow/arrow_defs.h>

#include <yql/essentials/minikql/computation/mkql_block_builder.h>

namespace NKikimr::NMiniKQL {

namespace {

struct TState {
    ui64 Count = 0;
};

class TColumnBuilder: public IAggColumnBuilder {
public:
    TColumnBuilder(ui64 size, TComputationContext& ctx)
        : Builder_(TTypeInfoHelper(), arrow::uint64(), ctx.ArrowMemoryPool, size)
        , Ctx_(ctx)
    {
    }

    void Add(const void* state) final {
        static_assert(std::is_trivially_copyable<TState>::value);
        const auto stateValue = ReadUnaligned<TState>(state);
        Builder_.Add(TBlockItem(stateValue.Count));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_.Build(true), Ctx_.RuntimeSettings.DatumValidation.Get());
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
class TCountAllAggregator<TCombineAllTag>: public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TCountAllAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
    {
        Y_UNUSED(argColumn);
    }

    void InitState(void* state) final {
        new (state) TState();
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TState>::value);
        Y_UNUSED(state);
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        Y_UNUSED(columns);
        if (filtered) {
            typedState->Count += *filtered;
        } else {
            typedState->Count += batchLength;
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        return NUdf::TUnboxedValuePod(typedState->Count);
    }
};

template <>
class TCountAllAggregator<TCombineKeysTag>: public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TCountAllAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
    {
        Y_UNUSED(argColumn);
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TState stateToReturn;
        UpdateKey(&stateToReturn, batchNum, columns, row);
        static_assert(std::is_trivially_copyable<TState>::value);
        WriteUnaligned<TState>(state, stateToReturn);
    }

    void DestroyState(void* state) noexcept final {
        static_assert(std::is_trivially_destructible<TState>::value);
        Y_UNUSED(state);
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        Y_UNUSED(columns);
        Y_UNUSED(row);
        static_assert(std::is_trivially_copyable<TState>::value);
        auto stateValue = ReadUnaligned<TState>(state);
        stateValue.Count += 1;
        WriteUnaligned<TState>(state, stateValue);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, Ctx_);
    }
};

template <>
class TCountAllAggregator<TFinalizeKeysTag>: public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TCountAllAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        TState stateToReturn;
        UpdateState(&stateToReturn, batchNum, columns, row);
        static_assert(std::is_trivially_copyable<TState>::value);
        WriteUnaligned<TState>(state, stateToReturn);
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
            typedState->Count += datum.scalar_as<arrow::UInt64Scalar>().value;
        } else {
            const auto& array = datum.array();
            auto ptr = array->GetValues<ui64>(1);
            MKQL_ENSURE(array->GetNullCount() == 0, "Expected not null");
            typedState->Count += ptr[row];
        }
    }

    void SerializeState(void* state, NUdf::TOutputBuffer& buffer) final {
        auto typedState = static_cast<TState*>(state);
        buffer.PushNumber(typedState->Count);
    }

    void DeserializeState(void* state, NUdf::TInputBuffer& buffer) final {
        auto typedState = static_cast<TState*>(state);
        buffer.PopNumber(typedState->Count);
    }

    void DeserializeAndUpdateState(void* state, NUdf::TInputBuffer& buffer) final {
        auto typedState = static_cast<TState*>(state);

        TState deserializedState;
        buffer.PopNumber(deserializedState.Count);
        typedState->Count += deserializedState.Count;
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, Ctx_);
    }

private:
    const ui32 ArgColumn_;
};

template <>
class TCountAggregator<TCombineAllTag>: public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TCountAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void InitState(void* state) final {
        new (state) TState();
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
                typedState->Count += filtered ? *filtered : batchLength;
            }
        } else {
            const auto& array = datum.array();
            if (!filtered) {
                typedState->Count += array->length - array->GetNullCount();
            } else if (array->GetNullCount() == array->length) {
                // all nulls
                return;
            } else if (array->GetNullCount() == 0) {
                // no nulls
                typedState->Count += *filtered;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                // intersect masks from nulls and filter column
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                const ui8* filterBitmap = filterArray->GetValues<uint8_t>(1);
                auto state = typedState->Count;
                for (ui32 i = 0; i < array->length; ++i) {
                    ui64 fullIndex = i + array->offset;
                    auto bit1 = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                    auto bit2 = filterBitmap[i];
                    state += bit1 & bit2;
                }

                typedState->Count = state;
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        return NUdf::TUnboxedValuePod(typedState->Count);
    }

private:
    const ui32 ArgColumn_;
};

template <>
class TCountAggregator<TCombineKeysTag>: public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TCountAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TState), filterColumn, ctx)
        , ArgColumn_(argColumn)
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new (state) TState();
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
                typedState->Count += 1;
            }
        } else {
            const auto& array = datum.array();
            if (array->GetNullCount() == 0) {
                typedState->Count += 1;
            } else {
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                auto fullIndex = row + array->offset;
                auto bit = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                typedState->Count += bit;
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
class TCountAggregator<TFinalizeKeysTag>: public TCountAllAggregator<TFinalizeKeysTag> {
public:
    using TBase = TCountAllAggregator<TFinalizeKeysTag>;

    TCountAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(filterColumn, argColumn, ctx)
    {
    }
};

template <typename TTag>
class TPreparedCountAll: public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedCountAll(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TState))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TCountAllAggregator<TTag>>(FilterColumn_, ArgColumn_, ctx);
    }

private:
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
};

template <typename TTag>
class TPreparedCount: public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedCount(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TState))
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {
    }

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

class TBlockCountAllFactory: public IBlockAggregatorFactory {
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
        TType* returnType,
        ui32 hint) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(argsColumns);
        Y_UNUSED(env);
        Y_UNUSED(returnType);
        Y_UNUSED(hint);
        return PrepareCountAll<TFinalizeKeysTag>(std::optional<ui32>(), argsColumns[0]);
    }
};

class TBlockCountFactory: public IBlockAggregatorFactory {
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
        TType* returnType,
        ui32 hint) const final {
        Y_UNUSED(tupleType);
        Y_UNUSED(argsColumns);
        Y_UNUSED(env);
        Y_UNUSED(returnType);
        Y_UNUSED(hint);
        return PrepareCount<TFinalizeKeysTag>(std::optional<ui32>(), argsColumns[0]);
    }
};

} // namespace

std::unique_ptr<IBlockAggregatorFactory> MakeBlockCountAllFactory() {
    return std::make_unique<TBlockCountAllFactory>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockCountFactory() {
    return std::make_unique<TBlockCountFactory>();
}

} // namespace NKikimr::NMiniKQL
