#include "mkql_block_agg_count.h"

namespace NKikimr {
namespace NMiniKQL {

class TCountAllBlockAggregator : public TBlockAggregatorBase {
public:
    TCountAllBlockAggregator(std::optional<ui32> filterColumn)
        : TBlockAggregatorBase(filterColumn)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        Y_UNUSED(columns);
        if (filtered) {
           State_ += *filtered;
        } else {
           State_ += batchLength;
        }
    }

    NUdf::TUnboxedValue Finish() final {
        return NUdf::TUnboxedValuePod(State_);
    }

private:
    ui64 State_ = 0;
};

class TCountBlockAggregator : public TBlockAggregatorBase {
public:
    TCountBlockAggregator(std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                State_ += filtered ? *filtered : batchLength;
            }
        } else {
            const auto& array = datum.array();
            if (!filtered) {
                State_ += array->length - array->GetNullCount();
            } else if (array->GetNullCount() == array->length) {
                // all nulls
                return;
            } else if (array->GetNullCount() == 0) {
                // no nulls
                State_ += *filtered;
            } else {
                const auto& filterDatum = TArrowBlock::From(columns[*FilterColumn_]).GetDatum();
                // intersect masks from nulls and filter column
                const auto& filterArray = filterDatum.array();
                MKQL_ENSURE(filterArray->GetNullCount() == 0, "Expected non-nullable bool column");
                auto nullBitmapPtr = array->GetValues<uint8_t>(0, 0);
                auto filterBitmap = filterArray->GetValues<uint8_t>(1, 0);
                auto state = State_;
                for (ui32 i = 0; i < array->length; ++i) {
                    ui64 fullIndex = i + array->offset;
                    auto bit1 = ((nullBitmapPtr[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                    auto bit2 = ((filterBitmap[fullIndex >> 3] >> (fullIndex & 0x07)) & 1);
                    state += bit1 & bit2;
                }

                State_ = state;
            }
        }
    }

    NUdf::TUnboxedValue Finish() final {
        return NUdf::TUnboxedValuePod(State_);
    }

private:
    const ui32 ArgColumn_;
    ui64 State_ = 0;
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
