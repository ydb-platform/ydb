#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

class IBlockAggregator {
public:
    virtual ~IBlockAggregator() = default;

    virtual void AddMany(const NUdf::TUnboxedValue* columns, ui64 batchLength) = 0;

    virtual NUdf::TUnboxedValue Finish() = 0;
};

class TBlockAggregatorBase : public IBlockAggregator {
public:
    TBlockAggregatorBase(std::optional<ui32> filterColumn)
        : FilterColumn_(filterColumn)
    {
    }

protected:
    const std::optional<ui32> FilterColumn_;
};

class THolderFactory;

std::unique_ptr<IBlockAggregator> MakeBlockAggregator(
    TStringBuf name,
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const THolderFactory& holderFactory);

class IBlockAggregatorFactory {
public:
   virtual ~IBlockAggregatorFactory() = default;

   virtual std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       const THolderFactory& holderFactory) const = 0;
};

}
}
