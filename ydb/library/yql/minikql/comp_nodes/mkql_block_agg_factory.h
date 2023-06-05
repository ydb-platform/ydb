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

class IBlockAggregatorBase {
public:
    virtual ~IBlockAggregatorBase() = default;

    const ui32 StateSize;

    explicit IBlockAggregatorBase(ui32 stateSize)
        : StateSize(stateSize)
    {}

    virtual void DestroyState(void* state) noexcept = 0;
};


class IBlockAggregatorCombineAll : public IBlockAggregatorBase {
public:
    virtual void InitState(void* state) = 0;

    virtual void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) = 0;

    virtual NUdf::TUnboxedValue FinishOne(const void* state) = 0;

    explicit IBlockAggregatorCombineAll(ui32 stateSize)
        : IBlockAggregatorBase(stateSize)
    {}
};

class IBlockAggregatorCombineKeys : public IBlockAggregatorBase {
public:
    virtual void InitKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) = 0;

    virtual void UpdateKey(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) = 0;

    virtual std::unique_ptr<IAggColumnBuilder> MakeStateBuilder(ui64 size) = 0;

    explicit IBlockAggregatorCombineKeys(ui32 stateSize)
        : IBlockAggregatorBase(stateSize)
    {}
};

class IBlockAggregatorFinalizeKeys : public IBlockAggregatorBase {
public:
    virtual void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) = 0;

    virtual void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) = 0;

    virtual std::unique_ptr<IAggColumnBuilder> MakeResultBuilder(ui64 size) = 0;

    explicit IBlockAggregatorFinalizeKeys(ui32 stateSize)
        : IBlockAggregatorBase(stateSize)
    {}
};

template <typename TBase>
class TBlockAggregatorBase : public TBase {
public:
    TBlockAggregatorBase(ui32 stateSize, std::optional<ui32> filterColumn, TComputationContext& ctx)
        : TBase(stateSize)
        , FilterColumn_(filterColumn)
        , Ctx_(ctx)
    {
    }

protected:
    const std::optional<ui32> FilterColumn_;
    TComputationContext& Ctx_;
};

template <typename T>
class IPreparedBlockAggregator {
public:
    virtual ~IPreparedBlockAggregator() = default;

    virtual std::unique_ptr<T> Make(TComputationContext& ctx) const = 0;

    const ui32 StateSize;

    explicit IPreparedBlockAggregator(ui32 stateSize)
        : StateSize(stateSize)
    {}
};

class IBlockAggregatorFactory {
public:
   virtual ~IBlockAggregatorFactory() = default;

   virtual std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
       TTupleType* tupleType,
       std::optional<ui32> filterColumn,
       const std::vector<ui32>& argsColumns,
       const TTypeEnvironment& env) const = 0;

   virtual std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
       TTupleType* tupleType,
       const std::vector<ui32>& argsColumns,
       const TTypeEnvironment& env) const = 0;

   virtual std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
       TTupleType* tupleType,
       const std::vector<ui32>& argsColumns,
       const TTypeEnvironment& env,
       TType* returnType) const = 0;
};

const IBlockAggregatorFactory& GetBlockAggregatorFactory(TStringBuf name);

struct TCombineAllTag {
    using TAggregator = IBlockAggregatorCombineAll;
    using TPreparedAggregator = IPreparedBlockAggregator<TAggregator>;
    using TBase = TBlockAggregatorBase<TAggregator>;
};

struct TCombineKeysTag {
    using TAggregator = IBlockAggregatorCombineKeys;
    using TPreparedAggregator = IPreparedBlockAggregator<TAggregator>;
    using TBase = TBlockAggregatorBase<TAggregator>;
};

struct TFinalizeKeysTag {
    using TAggregator = IBlockAggregatorFinalizeKeys;
    using TPreparedAggregator = IPreparedBlockAggregator<TAggregator>;
    using TBase = TBlockAggregatorBase<TAggregator>;
};

}
}
