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
        Y_ENSURE(!filtered);
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                State_ += batchLength;
            }
        } else {
            const auto& array = datum.array();
            State_ += array->length - array->GetNullCount();
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
