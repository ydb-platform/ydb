#include "mkql_block_agg_count.h"

namespace NKikimr {
namespace NMiniKQL {

class TCountAllBlockAggregator : public TBlockAggregatorBase {
public:
    struct TState {
        ui64 Count_ = 0;
    };

    TCountAllBlockAggregator(std::optional<ui32> filterColumn)
        : TBlockAggregatorBase(sizeof(TState), filterColumn)
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
};

class TCountBlockAggregator : public TBlockAggregatorBase {
public:
    struct TState {
        ui64 Count_ = 0;
    };

    TCountBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(sizeof(TState), filterColumn)
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
                auto filterBitmap = filterArray->GetValues<uint8_t>(1, 0);
                auto state = typedState->Count_;
                for (ui32 i = 0; i < array->length; ++i) {
                    ui64 fullIndex = i + array->offset;
                    auto bit1 = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                    auto bit2 = ((filterBitmap[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
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

class TBlockCountAllFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       const THolderFactory& holderFactory) const final {
       Y_UNUSED(tupleType);
       Y_UNUSED(argsColumns);
       Y_UNUSED(holderFactory);
       return std::make_unique<TCountAllBlockAggregator>(filterColumn);
   }
};

class TBlockCountFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       const THolderFactory& holderFactory) const final {
       Y_UNUSED(tupleType);
       Y_UNUSED(holderFactory);
       return std::make_unique<TCountBlockAggregator>(filterColumn, argsColumns[0]);
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
