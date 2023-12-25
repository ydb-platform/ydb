#include "mkql_contains.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TContainsWrapper : public TMutableCodegeneratorNode<TContainsWrapper> {
    typedef TMutableCodegeneratorNode<TContainsWrapper> TBaseComputation;
public:
    TContainsWrapper(TComputationMutables& mutables, IComputationNode* dict, IComputationNode* key)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Dict(dict)
        , Key(key)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        return NUdf::TUnboxedValuePod(Dict->GetValue(compCtx).Contains(Key->GetValue(compCtx)));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto dict = GetNodeValue(Dict, ctx, block);

        const auto keyp = *Stateless || ctx.AlwaysInline ?
            new AllocaInst(valueType, 0U, "key", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(valueType, 0U, "key", block);
        GetNodeValue(keyp, Key, ctx, block);
        const auto cont = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Contains>(Type::getInt1Ty(context), dict, ctx.Codegen, block, keyp);

        ValueUnRef(Key->GetRepresentation(), keyp, ctx, block);
        if (Dict->IsTemporaryValue())
            CleanupBoxed(dict, ctx, block);
        return MakeBoolean(cont, context, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(Dict);
        DependsOn(Key);
    }

    IComputationNode* const Dict;
    IComputationNode* const Key;
};

}

IComputationNode* WrapContains(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto dict = LocateNode(ctx.NodeLocator, callable, 0);
    const auto key = LocateNode(ctx.NodeLocator, callable, 1);
    return new TContainsWrapper(ctx.Mutables, dict, key);
}

}
}
