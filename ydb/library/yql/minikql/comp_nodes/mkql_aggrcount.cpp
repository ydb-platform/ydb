#include "mkql_aggrcount.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TAggrCountInitWrapper : public TDecoratorCodegeneratorNode<TAggrCountInitWrapper> {
    typedef TDecoratorCodegeneratorNode<TAggrCountInitWrapper> TBaseComputation;
public:
    TAggrCountInitWrapper(IComputationNode* value)
        : TBaseComputation(value)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        return NUdf::TUnboxedValuePod(ui64(value ? 1ULL : 0ULL));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* value, BasicBlock*& block) const {
        const auto check = IsExists(value, block);
        if (Node->IsTemporaryValue())
            ValueCleanup(Node->GetRepresentation(), value, ctx, block);
        return MakeBoolean(check, ctx.Codegen.GetContext(), block);
    }
#endif
};

class TAggrCountUpdateWrapper : public TDecoratorCodegeneratorNode<TAggrCountUpdateWrapper> {
    typedef TDecoratorCodegeneratorNode<TAggrCountUpdateWrapper> TBaseComputation;
public:
    TAggrCountUpdateWrapper(IComputationNode* state)
        : TBaseComputation(state)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        return NUdf::TUnboxedValuePod(value.Get<ui64>() + 1U);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext&, Value* value, BasicBlock*& block) const {
        return BinaryOperator::CreateAdd(value, ConstantInt::get(value->getType(), 1), "incr", block);
    }
#endif
};

class TAggrCountIfUpdateWrapper : public TMutableCodegeneratorNode<TAggrCountIfUpdateWrapper> {
    typedef TMutableCodegeneratorNode<TAggrCountIfUpdateWrapper> TBaseComputation;
public:
    TAggrCountIfUpdateWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* state)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Arg(value)
        , State(state)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto state = State->GetValue(compCtx);
        return Arg->GetValue(compCtx) ? NUdf::TUnboxedValuePod(state.Get<ui64>() + 1U) : state.Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        const auto state = GetNodeValue(State, ctx, block);
        const auto value = GetNodeValue(Arg, ctx, block);
        const auto check = IsExists(value, block);
        if (Arg->IsTemporaryValue())
            ValueCleanup(Arg->GetRepresentation(), value, ctx, block);
        const auto zext = new ZExtInst(check, state->getType(), "zext", block);
        const auto incr = BinaryOperator::CreateAdd(state, zext, "incr", block);
        return incr;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(Arg);
        DependsOn(State);
    }

    IComputationNode* const Arg;
    IComputationNode* const State;
};

}

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

}
}
