#include "mkql_aggrcount.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TAggrCountInitWrapper: public TDecoratorCodegeneratorNode<TAggrCountInitWrapper> {
    using TBaseComputation = TDecoratorCodegeneratorNode<TAggrCountInitWrapper>;

public:
    explicit TAggrCountInitWrapper(IComputationNode* value)
        : TBaseComputation(value)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        return NUdf::TUnboxedValuePod(ui64(value ? 1ULL : 0ULL));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* value, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto check = IsExists(value, block, context);
        if (Node_->IsTemporaryValue()) {
            ValueCleanup(Node_->GetRepresentation(), value, ctx, block);
        }
        return MakeBoolean(check, context, block);
    }
#endif
};

class TAggrCountUpdateWrapper: public TDecoratorCodegeneratorNode<TAggrCountUpdateWrapper> {
    using TBaseComputation = TDecoratorCodegeneratorNode<TAggrCountUpdateWrapper>;

public:
    explicit TAggrCountUpdateWrapper(IComputationNode* state)
        : TBaseComputation(state)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        return NUdf::TUnboxedValuePod(value.Get<ui64>() + 1U);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext&, Value* value, BasicBlock*& block) const override {
        return BinaryOperator::CreateAdd(value, ConstantInt::get(value->getType(), 1), "incr", block);
    }
#endif
};

class TAggrCountIfUpdateWrapper: public TMutableCodegeneratorNode<TAggrCountIfUpdateWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TAggrCountIfUpdateWrapper>;

public:
    TAggrCountIfUpdateWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* state)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Arg_(value)
        , State_(state)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto state = State_->GetValue(compCtx);
        return Arg_->GetValue(compCtx) ? NUdf::TUnboxedValuePod(state.Get<ui64>() + 1U) : state.Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto state = GetNodeValue(State_, ctx, block);
        const auto value = GetNodeValue(Arg_, ctx, block);
        const auto check = IsExists(value, block, context);
        if (Arg_->IsTemporaryValue()) {
            ValueCleanup(Arg_->GetRepresentation(), value, ctx, block);
        }
        const auto zext = new ZExtInst(check, state->getType(), "zext", block);
        const auto incr = BinaryOperator::CreateAdd(state, zext, "incr", block);
        return incr;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(Arg_);
        DependsOn(State_);
    }

    IComputationNode* const Arg_;
    IComputationNode* const State_;
};

} // namespace

IComputationNode* WrapAggrCountInit(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    if (callable.GetInput(0).GetStaticType()->IsOptional()) {
        return new TAggrCountInitWrapper(LocateNode(ctx.NodeLocator, callable, 0));
    } else {
        return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod(ui64(1ULL)));
    }
}

IComputationNode* WrapAggrCountUpdate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    MKQL_ENSURE(AS_TYPE(TDataType, callable.GetInput(1))->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64 type");
    if (callable.GetInput(0).GetStaticType()->IsOptional()) {
        return new TAggrCountIfUpdateWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), LocateNode(ctx.NodeLocator, callable, 1));
    } else {
        return new TAggrCountUpdateWrapper(LocateNode(ctx.NodeLocator, callable, 1));
    }
}

} // namespace NKikimr::NMiniKQL
