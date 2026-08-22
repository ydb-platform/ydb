#include "mkql_random.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <util/random/mersenne.h>

namespace NKikimr::NMiniKQL {

namespace {

class TRandomMTResource: public TComputationValue<TRandomMTResource> {
public:
    TRandomMTResource(TMemoryUsageInfo* memInfo, ui64 seed)
        : TComputationValue(memInfo)
        , Gen_(seed)
    {
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef(RandomMTResource);
    }

    void* GetResource() override {
        return &Gen_;
    }

    TMersenne<ui64> Gen_;
};

class TNewMTRandWrapper: public TMutableComputationNode<TNewMTRandWrapper> {
    using TBaseComputation = TMutableComputationNode<TNewMTRandWrapper>;

public:
    TNewMTRandWrapper(TComputationMutables& mutables, IComputationNode* seed)
        : TBaseComputation(mutables)
        , Seed_(seed)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const ui64 seedValue = Seed_->GetValue(compCtx).Get<ui64>();
        return compCtx.HolderFactory.Create<TRandomMTResource>(seedValue);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Seed_);
    }

    IComputationNode* const Seed_;
};

class TNextMTRandWrapper: public TMutableComputationNode<TNextMTRandWrapper> {
    using TBaseComputation = TMutableComputationNode<TNextMTRandWrapper>;

public:
    TNextMTRandWrapper(TComputationMutables& mutables, IComputationNode* rand)
        : TBaseComputation(mutables)
        , Rand_(rand)
        , ResPair_(mutables)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& compCtx) const {
        auto rand = Rand_->GetValue(compCtx);
        Y_DEBUG_ABORT_UNLESS(rand.GetResourceTag() == NUdf::TStringRef(RandomMTResource));
        NUdf::TUnboxedValue* items = nullptr;
        const auto tuple = ResPair_.NewArray(compCtx, 2, items);
        items[0] = NUdf::TUnboxedValuePod(static_cast<TMersenne<ui64>*>(rand.GetResource())->GenRand());
        items[1] = std::move(rand);
        return tuple;
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Rand_);
    }

    IComputationNode* const Rand_;
    const TContainerCacheOnContext ResPair_;
};

template <ERandom Rnd>
class TRandomWrapper: public TMutableComputationNode<TRandomWrapper<Rnd>> {
    using TBaseComputation = TMutableComputationNode<TRandomWrapper<Rnd>>;

public:
    TRandomWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables)
        , DependentNodes_(dependentNodes)
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
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TRandomWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector DependentNodes_;
};

} // namespace

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

template IComputationNode* WrapRandom<ERandom::Double>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

template IComputationNode* WrapRandom<ERandom::Number>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

template IComputationNode* WrapRandom<ERandom::Uuid>(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
