#include "mkql_dynamic_variant.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename TDerived>
class TDynamicVariantBaseWrapper : public TMutableComputationNode<TDerived> {
public:
    using TBase = TMutableComputationNode<TDerived>;

    TDynamicVariantBaseWrapper(TComputationMutables& mutables, IComputationNode* item,
        IComputationNode* index)
        : TBase(mutables)
        , Item(item)
        , Index(index)
    {}

private:
    void RegisterDependencies() const final {
        this->DependsOn(Item);
        this->DependsOn(Index);
    }

protected:
    IComputationNode* const Item;
    IComputationNode* const Index;
};

class TDynamicVariantTupleWrapper : public TDynamicVariantBaseWrapper<TDynamicVariantTupleWrapper> {
public:
    using TBase = TDynamicVariantBaseWrapper<TDynamicVariantTupleWrapper>;

    TDynamicVariantTupleWrapper(TComputationMutables& mutables, IComputationNode* item,
        IComputationNode* index, TVariantType* varType)
        : TBase(mutables, item, index)
        , AltCounts(varType->GetAlternativesCount())
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto indexValue = Index->GetValue(ctx);
        if (!indexValue || indexValue.Get<ui32>() >= AltCounts) {
            return {};
        }

        NUdf::TUnboxedValuePod item = Item->GetValue(ctx).Release();
        NUdf::TUnboxedValuePod var = ctx.HolderFactory.CreateVariantHolder(item, indexValue.Get<ui32>());
        return var.MakeOptional();
    }

private:
    const ui32 AltCounts;
};

class TDynamicVariantStructWrapper : public TDynamicVariantBaseWrapper<TDynamicVariantStructWrapper> {
public:
    using TBase = TDynamicVariantBaseWrapper<TDynamicVariantStructWrapper>;

    TDynamicVariantStructWrapper(TComputationMutables& mutables, IComputationNode* item,
        IComputationNode* index, TVariantType* varType)
        : TBase(mutables, item, index)
        , Fields(MakeFields(varType))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto indexValue = Index->GetValue(ctx);
        if (!indexValue) {
            return {};
        }

        TStringBuf indexStr = indexValue.AsStringRef();
        auto ptr = Fields.FindPtr(indexStr);
        if (!ptr) {
            return {};
        }

        NUdf::TUnboxedValuePod item = Item->GetValue(ctx).Release();
        NUdf::TUnboxedValuePod var = ctx.HolderFactory.CreateVariantHolder(item, *ptr);
        return var.MakeOptional();
    }

private:
    static THashMap<TStringBuf, ui32> MakeFields(TVariantType* varType) {
        THashMap<TStringBuf, ui32> res;
        auto structType = AS_TYPE(TStructType, varType->GetUnderlyingType());
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            res[structType->GetMemberName(i)] = i;
        }

        return res;
    }

private:
    const THashMap<TStringBuf, ui32> Fields;
};

}

IComputationNode* WrapDynamicVariant(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 arguments");
    const auto item = LocateNode(ctx.NodeLocator, callable, 0);
    const auto index = LocateNode(ctx.NodeLocator, callable, 1);
    const auto varTypeNode = callable.GetInput(2);
    MKQL_ENSURE(varTypeNode.IsImmediate() && varTypeNode.GetStaticType()->IsType(), "Expected immediate type");
    const auto varType = AS_TYPE(TVariantType, static_cast<TType*>(varTypeNode.GetNode()));

    if (varType->GetUnderlyingType()->IsTuple()) {
        return new TDynamicVariantTupleWrapper(ctx.Mutables, item, index, varType);
    } else {
        return new TDynamicVariantStructWrapper(ctx.Mutables, item, index, varType);
    }

    return nullptr;
}

}
}
