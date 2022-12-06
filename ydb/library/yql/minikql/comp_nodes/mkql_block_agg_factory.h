#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

class IBlockAggregator {
public:
    virtual ~IBlockAggregator() = default;

    virtual void InitState(void* state) = 0;

    virtual void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) = 0;

    virtual NUdf::TUnboxedValue FinishOne(const void* state) = 0;

    const ui32 StateSize;

    explicit IBlockAggregator(ui32 stateSize)
        : StateSize(stateSize)
    {}
};

class TBlockAggregatorBase : public IBlockAggregator {
public:
    TBlockAggregatorBase(ui32 stateSize, std::optional<ui32> filterColumn)
        : IBlockAggregator(stateSize)
        , FilterColumn_(filterColumn)
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
