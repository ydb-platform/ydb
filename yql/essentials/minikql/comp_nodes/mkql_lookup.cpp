#include "mkql_lookup.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TLookupWrapper: public TMutableCodegeneratorPtrNode<TLookupWrapper> {
    using TBaseComputation = TMutableCodegeneratorPtrNode<TLookupWrapper>;

public:
    TLookupWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* dict, IComputationNode* key)
        : TBaseComputation(mutables, kind)
        , Dict_(dict)
        , Key_(key)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        return Dict_->GetValue(ctx).Lookup(Key_->GetValue(ctx));
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const override {
        const auto dict = GetNodeValue(Dict_, ctx, block);

        GetNodeValue(pointer, Key_, ctx, block);
        const auto keyp = new LoadInst(Type::getInt128Ty(ctx.Codegen.GetContext()), pointer, "key", block);

        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Lookup>(pointer, dict, ctx.Codegen, block, pointer);
        ValueUnRef(Key_->GetRepresentation(), keyp, ctx, block);
        if (Dict_->IsTemporaryValue()) {
            CleanupBoxed(dict, ctx, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(Dict_);
        DependsOn(Key_);
    }

    IComputationNode* const Dict_;
    IComputationNode* const Key_;
};

} // namespace

IComputationNode* WrapLookup(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto dict = LocateNode(ctx.NodeLocator, callable, 0);
    const auto key = LocateNode(ctx.NodeLocator, callable, 1);
    return new TLookupWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), dict, key);
}

} // namespace NKikimr::NMiniKQL
