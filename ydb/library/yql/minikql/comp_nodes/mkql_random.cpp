#include "mkql_random.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <util/random/mersenne.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TRandomMTResource : public TComputationValue<TRandomMTResource> {
public:
    TRandomMTResource(TMemoryUsageInfo* memInfo, ui64 seed)
        : TComputationValue(memInfo)
        , Gen(seed)
    {
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef(RandomMTResource);
    }

    void* GetResource() override {
        return &Gen;
    }

    TMersenne<ui64> Gen;
};

class TNewMTRandWrapper : public TMutableComputationNode<TNewMTRandWrapper> {
    typedef TMutableComputationNode<TNewMTRandWrapper> TBaseComputation;
public:
    TNewMTRandWrapper(TComputationMutables& mutables, IComputationNode* seed)
        : TBaseComputation(mutables)
        , Seed(seed)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const ui64 seedValue = Seed->GetValue(compCtx).Get<ui64>();
        return compCtx.HolderFactory.Create<TRandomMTResource>(seedValue);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Seed);
    }

    IComputationNode* const Seed;
};

class TNextMTRandWrapper : public TMutableComputationNode<TNextMTRandWrapper> {
    typedef TMutableComputationNode<TNextMTRandWrapper> TBaseComputation;
public:
    TNextMTRandWrapper(TComputationMutables& mutables, IComputationNode* rand)
        : TBaseComputation(mutables)
        , Rand(rand)
        , ResPair(mutables)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& compCtx) const {
        auto rand = Rand->GetValue(compCtx);
        Y_DEBUG_ABORT_UNLESS(rand.GetResourceTag() == NUdf::TStringRef(RandomMTResource));
        NUdf::TUnboxedValue *items = nullptr;
        const auto tuple = ResPair.NewArray(compCtx, 2, items);
        items[0] = NUdf::TUnboxedValuePod(static_cast<TMersenne<ui64>*>(rand.GetResource())->GenRand());
        items[1] = std::move(rand);
        return tuple;
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Rand);
    }

    IComputationNode* const Rand;
    const TContainerCacheOnContext ResPair;
};

template <ERandom Rnd>
class TRandomWrapper : public TMutableComputationNode<TRandomWrapper<Rnd>> {
    typedef TMutableComputationNode<TRandomWrapper<Rnd>> TBaseComputation;
public:
    TRandomWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables)
        , DependentNodes(dependentNodes)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        switch (Rnd) {
        case ERandom::Double:
            return NUdf::TUnboxedValuePod(ctx.RandomProvider.GenRandReal2());
        case ERandom::Number:
            return NUdf::TUnboxedValuePod(ctx.RandomProvider.GenRand64());
        case ERandom::Uuid: {
            auto uuid = ctx.RandomProvider.GenUuid4();
            return MakeString(NUdf::TStringRef((const char*)&uuid, sizeof(uuid)));
        }
        }

        Y_ABORT("Unexpected");
    }

private:
    void RegisterDependencies() const final {
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(), std::bind(&TRandomWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector DependentNodes;
};

}

IComputationNode* WrapNewMTRand(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    TDataType* dataType = AS_TYPE(TDataType, callable.GetInput(0));
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<ui64>::Id,
        "Expected ui64");

    auto data = LocateNode(ctx.NodeLocator, callable, 0);
    return new TNewMTRandWrapper(ctx.Mutables, data);
}

IComputationNode* WrapNextMTRand(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    AS_TYPE(TResourceType, callable.GetInput(0));

    auto rand = LocateNode(ctx.NodeLocator, callable, 0);
    return new TNextMTRandWrapper(ctx.Mutables, rand);
}

template <ERandom Rnd>
IComputationNode* WrapRandom(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i] = LocateNode(ctx.NodeLocator, callable, i);
    }

    return new TRandomWrapper<Rnd>(ctx.Mutables, std::move(dependentNodes));
}

template
IComputationNode* WrapRandom<ERandom::Double>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

template
IComputationNode* WrapRandom<ERandom::Number>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

template
IComputationNode* WrapRandom<ERandom::Uuid>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
