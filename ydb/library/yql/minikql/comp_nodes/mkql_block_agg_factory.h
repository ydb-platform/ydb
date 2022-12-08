#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

class IAggColumnBuilder {
public:
    virtual ~IAggColumnBuilder() = default;

    virtual void Add(const void* state) = 0;

    virtual NUdf::TUnboxedValue Build() = 0;
};

class IBlockAggregator {
public:
    virtual ~IBlockAggregator() = default;

    virtual void InitState(void* state) = 0;

    virtual void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) = 0;

    virtual NUdf::TUnboxedValue FinishOne(const void* state) = 0;

    virtual void InitKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) = 0;

    virtual void UpdateKey(void* state, const NUdf::TUnboxedValue* columns, ui64 row) = 0;

    virtual std::unique_ptr<IAggColumnBuilder> MakeBuilder(ui64 size) = 0;

    const ui32 StateSize;

    explicit IBlockAggregator(ui32 stateSize)
        : StateSize(stateSize)
    {}
};

class TBlockAggregatorBase : public IBlockAggregator {
public:
    TBlockAggregatorBase(ui32 stateSize, std::optional<ui32> filterColumn, TComputationContext& ctx)
        : IBlockAggregator(stateSize)
        , FilterColumn_(filterColumn)
        , Ctx_(ctx)
    {
    }

protected:
    const std::optional<ui32> FilterColumn_;
    TComputationContext& Ctx_;
};

class THolderFactory;

std::unique_ptr<IBlockAggregator> MakeBlockAggregator(
    TStringBuf name,
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    TComputationContext& ctx);

class IBlockAggregatorFactory {
public:
   virtual ~IBlockAggregatorFactory() = default;

   virtual std::unique_ptr<IBlockAggregator> Make(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       TComputationContext& ctx) const = 0;
};

}
}
