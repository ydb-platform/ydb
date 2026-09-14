#include "mkql_contains.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TContainsWrapper: public TMutableCodegeneratorNode<TContainsWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TContainsWrapper>;

public:
    TContainsWrapper(TComputationMutables& mutables, IComputationNode* dict, IComputationNode* key)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Dict_(dict)
        , Key_(key)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        return NUdf::TUnboxedValuePod(Dict_->GetValue(compCtx).Contains(Key_->GetValue(compCtx)));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto dict = GetNodeValue(Dict_, ctx, block);

        const auto keyp = *Stateless_ || ctx.AlwaysInline ? new AllocaInst(valueType, 0U, "key", &ctx.Func->getEntryBlock().back()) : new AllocaInst(valueType, 0U, "key", block);
        GetNodeValue(keyp, Key_, ctx, block);
        const auto cont = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Contains>(Type::getInt1Ty(context), dict, ctx.Codegen, block, keyp);

        ValueUnRef(Key_->GetRepresentation(), keyp, ctx, block);
        if (Dict_->IsTemporaryValue()) {
            CleanupBoxed(dict, ctx, block);
        }
        return MakeBoolean(cont, context, block);
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

IComputationNode* WrapContains(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto dict = LocateNode(ctx.NodeLocator, callable, 0);
    const auto key = LocateNode(ctx.NodeLocator, callable, 1);
    return new TContainsWrapper(ctx.Mutables, dict, key);
}

} // namespace NKikimr::NMiniKQL
