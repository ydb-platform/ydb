#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

class IBlockAggregator {
public:
    virtual ~IBlockAggregator() = default;

    virtual void AddMany(const NUdf::TUnboxedValue* columns) = 0;

    virtual NUdf::TUnboxedValue Finish() = 0;
};

class TBlockAggregatorBase : public IBlockAggregator {
public:
    TBlockAggregatorBase(ui32 countColumn, std::optional<ui32> filterColumn)
        : CountColumn_(countColumn)
        , FilterColumn_(filterColumn)
    {
    }

protected:
    ui64 GetBatchLength(const NUdf::TUnboxedValue* columns) const {
        return TArrowBlock::From(columns[CountColumn_]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    }

protected:
    const ui32 CountColumn_;
    const std::optional<ui32> FilterColumn_;
};

std::unique_ptr<IBlockAggregator> MakeBlockAggregator(
    TStringBuf name,
    TTupleType* tupleType,
    ui32 countColumn,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns);

class IBlockAggregatorFactory {
public:
   virtual ~IBlockAggregatorFactory() = default;

   virtual std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       ui32 countColumn,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns) const = 0;
};

}
}
