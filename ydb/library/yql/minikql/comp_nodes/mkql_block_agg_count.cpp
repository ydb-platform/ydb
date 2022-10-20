#include "mkql_block_agg_count.h"

namespace NKikimr {
namespace NMiniKQL {

class TCountAllBlockAggregator : public TBlockAggregatorBase {
public:
    TCountAllBlockAggregator(ui32 countColumn, std::optional<ui32> filterColumn)
        : TBlockAggregatorBase(countColumn, filterColumn)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns) final {
        State_ += GetBatchLength(columns);
    }

    NUdf::TUnboxedValue Finish() final {
        return NUdf::TUnboxedValuePod(State_);
    }

private:
    ui64 State_ = 0;
};

class TCountBlockAggregator : public TBlockAggregatorBase {
public:
    TCountBlockAggregator(ui32 countColumn, std::optional<ui32> filterColumn, ui32 argColumn)
        : TBlockAggregatorBase(countColumn, filterColumn)
        , ArgColumn_(argColumn)
    {
    }

    void AddMany(const NUdf::TUnboxedValue* columns) final {
        const auto& datum = TArrowBlock::From(columns[ArgColumn_]).GetDatum();
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                State_ += GetBatchLength(columns);
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
       ui32 countColumn,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns) const final {
       Y_UNUSED(tupleType);
       Y_UNUSED(argsColumns);
       return std::make_unique<TCountAllBlockAggregator>(countColumn, filterColumn);
   }
};

class TBlockCountFactory : public IBlockAggregatorFactory {
public:
   std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       ui32 countColumn,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns) const final {
       Y_UNUSED(tupleType);
       return std::make_unique<TCountBlockAggregator>(countColumn, filterColumn, argsColumns[0]);
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
