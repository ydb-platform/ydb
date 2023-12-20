#include "mkql_lookup.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TLookupWrapper : public TMutableCodegeneratorPtrNode<TLookupWrapper> {
    typedef TMutableCodegeneratorPtrNode<TLookupWrapper> TBaseComputation;
public:
    TLookupWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* dict, IComputationNode* key)
        : TBaseComputation(mutables, kind)
        , Dict(dict)
        , Key(key)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        return Dict->GetValue(ctx).Lookup(Key->GetValue(ctx));
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        const auto dict = GetNodeValue(Dict, ctx, block);

        GetNodeValue(pointer, Key, ctx, block);
        const auto keyp = new LoadInst(Type::getInt128Ty(ctx.Codegen.GetContext()), pointer, "key", block);

        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Lookup>(pointer, dict, ctx.Codegen, block, pointer);
        ValueUnRef(Key->GetRepresentation(), keyp, ctx, block);
        if (Dict->IsTemporaryValue())
            CleanupBoxed(dict, ctx, block);
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

IComputationNode* WrapLookup(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto dict = LocateNode(ctx.NodeLocator, callable, 0);
    const auto key = LocateNode(ctx.NodeLocator, callable, 1);
    return new TLookupWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), dict, key);
}

}
}
