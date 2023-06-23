#include "mkql_block_agg_some.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

using TGenericState = NUdf::TUnboxedValuePod;

void PushValueToState(TGenericState* typedState, const arrow::Datum& datum, ui64 row, IBlockReader& reader,
    IBlockItemConverter& converter, TComputationContext& ctx)
{
    if (datum.is_scalar()) {
        if (datum.scalar()->is_valid) {
            auto item = reader.GetScalarItem(*datum.scalar());
            *typedState = converter.MakeValue(item, ctx.HolderFactory);
        }
    } else {
        const auto& array = datum.array();
        TBlockItem curr = reader.GetItem(*array, row);
        if (curr) {
            *typedState = converter.MakeValue(curr, ctx.HolderFactory);
        }
    }
}

class TGenericColumnBuilder : public IAggColumnBuilder {
public:
    TGenericColumnBuilder(ui64 size, TType* columnType, TComputationContext& ctx)
        : Builder_(MakeArrayBuilder(TTypeInfoHelper(), columnType, ctx.ArrowMemoryPool, size, &ctx.Builder->GetPgBuilder()))
        , Ctx_(ctx)
    {
    }

    void Add(const void* state) final {
        Builder_->Add(*static_cast<const TGenericState*>(state));
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_->Build(true));
    }

private:
    const std::unique_ptr<IArrayBuilder> Builder_;
    TComputationContext& Ctx_;
};

template<typename TTag>
class TSomeBlockGenericAggregator;

template<>
class TSomeBlockGenericAggregator<TCombineAllTag> : public TCombineAllTag::TBase {
public:
    using TBase = TCombineAllTag::TBase;

    TSomeBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Reader_(MakeBlockReader(TTypeInfoHelper(), type))
        , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), type, ctx.Builder->GetPgBuilder()))
    {
    }

    void InitState(void* state) final {
        new(state) TGenericState();
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        TGenericState& typedState = *static_cast<TGenericState*>(state);
        if (typedState) {
            return;
        }

        Y_UNUSED(batchLength);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();

        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                auto item = Reader_->GetScalarItem(*datum.scalar());
                typedState = Converter_->MakeValue(item, Ctx_.HolderFactory);
            }
        } else {
            const auto& array = datum.array();
            auto len = array->length;

            const ui8* filterBitmap = nullptr;
            if (filtered) {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                filterBitmap = filterArray->template GetValues<uint8_t>(1);
            }

            for (auto i = 0; i < len; ++i) {
                TBlockItem curr = Reader_->GetItem(*array, i);
                if (curr && (!filterBitmap || filterBitmap[i])) {
                    typedState = Converter_->MakeValue(curr, Ctx_.HolderFactory);
                    break;
                }
            }
        }
    }

    NUdf::TUnboxedValue FinishOne(const void *state) final {
        auto typedState = *static_cast<const TGenericState *>(state);
        return typedState;
    }

private:
    const ui32 ArgColumn_;
    const std::unique_ptr<IBlockReader> Reader_;
    const std::unique_ptr<IBlockItemConverter> Converter_;
};

template<>
class TSomeBlockGenericAggregator<TCombineKeysTag> : public TCombineKeysTag::TBase {
public:
    using TBase = TCombineKeysTag::TBase;

    TSomeBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
        , Reader_(MakeBlockReader(TTypeInfoHelper(), type))
        , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), type, ctx.Builder->GetPgBuilder()))
    {
    }

    void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TGenericState();
        UpdateKey(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TGenericState*>(state);
        if (*typedState) {
            return;
        }

        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState(typedState, datum, row, *Reader_, *Converter_, Ctx_);
    }

    std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
        return std::make_unique<TGenericColumnBuilder>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const Type_;
    const std::unique_ptr<IBlockReader> Reader_;
    const std::unique_ptr<IBlockItemConverter> Converter_;
};

template<>
class TSomeBlockGenericAggregator<TFinalizeKeysTag> : public TFinalizeKeysTag::TBase {
public:
    using TBase = TFinalizeKeysTag::TBase;

    TSomeBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
        : TBase(sizeof(TGenericState), filterColumn, ctx)
        , ArgColumn_(argColumn)
        , Type_(type)
        , Reader_(MakeBlockReader(TTypeInfoHelper(), type))
        , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), type, ctx.Builder->GetPgBuilder()))
    {
    }

    void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TGenericState();
        UpdateState(state, batchNum, columns, row);
    }

    void DestroyState(void* state) noexcept final {
        auto typedState = static_cast<TGenericState*>(state);
        typedState->DeleteUnreferenced();
        *typedState = TGenericState();
    }

    void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(batchNum);
        auto typedState = static_cast<TGenericState*>(state);
        if (*typedState) {
            return;
        }

        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        PushValueToState(typedState, datum, row, *Reader_, *Converter_, Ctx_);
    }

    std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
        return std::make_unique<TGenericColumnBuilder>(size, Type_, Ctx_);
    }

private:
    const ui32 ArgColumn_;
    TType* const Type_;
    const std::unique_ptr<IBlockReader> Reader_;
    const std::unique_ptr<IBlockItemConverter> Converter_;
};

template <typename TTag>
class TPreparedSomeBlockGenericAggregator : public TTag::TPreparedAggregator {
public:
    using TBase = typename TTag::TPreparedAggregator;

    TPreparedSomeBlockGenericAggregator(TType* type, std::optional<ui32> filterColumn, ui32 argColumn)
        : TBase(sizeof(TGenericState))
        , Type_(type)
        , FilterColumn_(filterColumn)
        , ArgColumn_(argColumn)
    {}

    std::unique_ptr<typename TTag::TAggregator> Make(TComputationContext& ctx) const final {
        return std::make_unique<TSomeBlockGenericAggregator<TTag>>(Type_, FilterColumn_, ArgColumn_, ctx);
    }

private:
    TType* const Type_;
    const std::optional<ui32> FilterColumn_;
    const ui32 ArgColumn_;
};

template <typename TTag>
std::unique_ptr<typename TTag::TPreparedAggregator> PrepareSome(TTupleType* tupleType, std::optional<ui32> filterColumn, ui32 argColumn) {
    const auto blockType = AS_TYPE(TBlockType, tupleType->GetElementType(argColumn));
    const auto argType = blockType->GetItemType();
    return std::make_unique<TPreparedSomeBlockGenericAggregator<TTag>>(argType, filterColumn, argColumn);
}

class TBlockSomeFactory : public IBlockAggregatorFactory {
   std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       const TTypeEnvironment& env) const override {
        Y_UNUSED(env);
        return PrepareSome<TCombineAllTag>(tupleType, filterColumn, argsColumns[0]);
    }

   std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
       TTupleType* tupleType,
       const std::vector<ui32>& argsColumns,
       const TTypeEnvironment& env) const override {
        Y_UNUSED(env);
        return PrepareSome<TCombineKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0]);
    }

    std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
        TTupleType* tupleType,
        const std::vector<ui32>& argsColumns,
        const TTypeEnvironment& env,
        TType* returnType) const override {
        Y_UNUSED(env);
        Y_UNUSED(returnType);
        return PrepareSome<TFinalizeKeysTag>(tupleType, std::optional<ui32>(), argsColumns[0]);
    }
};

}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockSomeFactory() {
    return std::make_unique<TBlockSomeFactory>();
}

}
}
