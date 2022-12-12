#include "mkql_block_agg_count.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

class TCountAllBlockAggregator : public TBlockAggregatorBase {
public:
    struct TState {
        ui64 Count_ = 0;
    };

    class TColumnBuilder : public IAggColumnBuilder {
    public:
        TColumnBuilder(ui64 size, TComputationContext& ctx)
            : Builder_(arrow::uint64(), &ctx.ArrowMemoryPool)
            , Ctx_(ctx)
        {
            ARROW_OK(Builder_.Reserve(size));
        }

        void Add(const void* state) final {
            auto typedState = static_cast<const TState*>(state);
            Builder_.UnsafeAppend(typedState->Count_);
        }

        NUdf::TUnboxedValue Build() final {
            std::shared_ptr<arrow::ArrayData> result;
            ARROW_OK(Builder_.FinishInternal(&result));
            return Ctx_.HolderFactory.CreateArrowBlock(result);
        }

    private:
        arrow::UInt64Builder Builder_;
        TComputationContext& Ctx_;
    };

    TCountAllBlockAggregator(std::optional<ui32> filterColumn, TComputationContext& ctx)
        : TBlockAggregatorBase(sizeof(TState), filterColumn, ctx)
    {
    }

    void InitState(void* state) final {
        new(state) TState();
    }

    void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        auto typedState = static_cast<TState*>(state);
        Y_UNUSED(columns);
        if (filtered) {
           typedState->Count_ += *filtered;
        } else {
           typedState->Count_ += batchLength;
        }
    }

    NUdf::TUnboxedValue FinishOne(const void* state) final {
        auto typedState = static_cast<const TState*>(state);
        return NUdf::TUnboxedValuePod(typedState->Count_);
    }

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        Y_UNUSED(columns);
        Y_UNUSED(row);
        auto typedState = static_cast<TState*>(state);
        typedState->Count_ += 1;
    }

    std::unique_ptr<IAggColumnBuilder> MakeBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, Ctx_);
    }
};

class TCountBlockAggregator : public TBlockAggregatorBase {
public:
    struct TState {
        ui64 Count_ = 0;
    };

    class TColumnBuilder : public IAggColumnBuilder {
    public:
        TColumnBuilder(ui64 size, TComputationContext& ctx)
            : Builder_(arrow::uint64(), &ctx.ArrowMemoryPool)
            , Ctx_(ctx)
        {
            ARROW_OK(Builder_.Reserve(size));
        }

        void Add(const void* state) final {
            auto typedState = static_cast<const TState*>(state);
            Builder_.UnsafeAppend(typedState->Count_);
        }

        NUdf::TUnboxedValue Build() final {
            std::shared_ptr<arrow::ArrayData> result;
            ARROW_OK(Builder_.FinishInternal(&result));
            return Ctx_.HolderFactory.CreateArrowBlock(result);
        }

    private:
        arrow::UInt64Builder Builder_;
        TComputationContext& Ctx_;
    };

    TCountBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn, TComputationContext& ctx)
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

    void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
        new(state) TState();
        UpdateKey(state, columns, row);
    }

    void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) final {
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

    std::unique_ptr<IAggColumnBuilder> MakeBuilder(ui64 size) final {
        return std::make_unique<TColumnBuilder>(size, Ctx_);
    }

private:
    const ui32 ArgColumn_;
};

class TBlockCountAllFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       TComputationContext& ctx) const final {
       Y_UNUSED(tupleType);
       Y_UNUSED(argsColumns);
       return std::make_unique<TCountAllBlockAggregator>(filterColumn, ctx);
   }
};

class TBlockCountFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       TComputationContext& ctx) const final {
       Y_UNUSED(tupleType);
       return std::make_unique<TCountBlockAggregator>(filterColumn, argsColumns[0], ctx);
   }
};

std::unique_ptr<IBlockAggregatorFactory> MakeBlockCountAllFactory() {
    return std::make_unique<TBlockCountAllFactory>();
}

std::unique_ptr<IBlockAggregatorFactory> MakeBlockCountFactory() {
    return std::make_unique<TBlockCountFactory>();
}
 
}
}
