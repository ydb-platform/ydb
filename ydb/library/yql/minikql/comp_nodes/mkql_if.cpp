#include "mkql_if.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsOptional>
class TIfWrapper : public TMutableCodegeneratorNode<TIfWrapper<IsOptional>> {
using TBaseComputation = TMutableCodegeneratorNode<TIfWrapper<IsOptional>>;
public:
    TIfWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* predicate, IComputationNode* thenBranch, IComputationNode* elseBranch)
        : TBaseComputation(mutables, kind)
        , Predicate(predicate)
        , ThenBranch(thenBranch)
        , ElseBranch(elseBranch)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& predicate = Predicate->GetValue(ctx);
        if (IsOptional && !predicate) {
            return NUdf::TUnboxedValuePod();
        }

        return (predicate.Get<bool>() ? ThenBranch : ElseBranch)->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto then = BasicBlock::Create(context, "then", ctx.Func);
        const auto elsb = BasicBlock::Create(context, "else", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto value = GetNodeValue(Predicate, ctx, block);
        const auto result = PHINode::Create(value->getType(), IsOptional ? 3U : 2U, "result", done);

        if (IsOptional) {
            result->addIncoming(value, block);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            BranchInst::Create(done, good, IsEmpty(value, block), block);

            block = good;
        }

        const auto cast = CastInst::Create(Instruction::Trunc, value, Type::getInt1Ty(context), "bool", block);

        BranchInst::Create(then, elsb, cast, block);

        {
            block = then;
            const auto left = GetNodeValue(ThenBranch, ctx, block);
            result->addIncoming(left, block);
            BranchInst::Create(done, block);
        }

        {
            block = elsb;
            const auto right = GetNodeValue(ElseBranch, ctx, block);
            result->addIncoming(right, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Predicate);
        this->DependsOn(ThenBranch);
        this->DependsOn(ElseBranch);
    }

    IComputationNode* const Predicate;
    IComputationNode* const ThenBranch;
    IComputationNode* const ElseBranch;
};

template<bool IsOptional>
class TFlowIfWrapper : public TStatefulFlowCodegeneratorNode<TFlowIfWrapper<IsOptional>> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TFlowIfWrapper<IsOptional>>;
public:
    TFlowIfWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* predicate, IComputationNode* thenBranch, IComputationNode* elseBranch)
        : TBaseComputation(mutables, nullptr, kind)
        , Predicate(predicate)
        , ThenBranch(thenBranch)
        , ElseBranch(elseBranch)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = Predicate->GetValue(ctx);
        }

        if (IsOptional && !state) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        }

        return (state.Get<bool>() ? ThenBranch : ElseBranch)->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto then = BasicBlock::Create(context, "then", ctx.Func);
        const auto elsb = BasicBlock::Create(context, "else", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(init, test, IsInvalid(statePtr, block), block);

        block = init;

        GetNodeValue(statePtr, Predicate, ctx, block);

        BranchInst::Create(test, block);

        block = test;

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto result = PHINode::Create(valueType, IsOptional ? 3U : 2U, "result", done);

        if (IsOptional) {
            result->addIncoming(state, block);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            BranchInst::Create(done, good, IsEmpty(state, block), block);

            block = good;
        }

        const auto cast = CastInst::Create(Instruction::Trunc, state, Type::getInt1Ty(context), "bool", block);

        BranchInst::Create(then, elsb, cast, block);

        {
            block = then;
            const auto left = GetNodeValue(ThenBranch, ctx, block);
            result->addIncoming(left, block);
            BranchInst::Create(done, block);
        }

        {
            block = elsb;
            const auto right = GetNodeValue(ElseBranch, ctx, block);
            result->addIncoming(right, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOnBoth(ThenBranch, ElseBranch))
            this->DependsOn(flow, Predicate);
    }

    IComputationNode* const Predicate;
    IComputationNode* const ThenBranch;
    IComputationNode* const ElseBranch;
};

class TWideIfWrapper : public TStatefulWideFlowCodegeneratorNode<TWideIfWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideIfWrapper>;
public:
    TWideIfWrapper(TComputationMutables& mutables, IComputationNode* predicate, IComputationWideFlowNode* thenBranch, IComputationWideFlowNode* elseBranch)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Embedded)
        , Predicate(predicate)
        , ThenBranch(thenBranch)
        , ElseBranch(elseBranch)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = Predicate->GetValue(ctx);
        }

        return (state.Get<bool>() ? ThenBranch : ElseBranch)->FetchValues(ctx, output);
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto then = BasicBlock::Create(context, "then", ctx.Func);
        const auto elsb = BasicBlock::Create(context, "else", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(init, test, IsInvalid(statePtr, block), block);

        block = init;

        GetNodeValue(statePtr, Predicate, ctx, block);

        BranchInst::Create(test, block);

        block = test;

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto result = PHINode::Create(Type::getInt32Ty(context), 2, "result", done);

        const auto cast = CastInst::Create(Instruction::Trunc, state, Type::getInt1Ty(context), "bool", block);

        BranchInst::Create(then, elsb, cast, block);

        block = then;

        const auto left = GetNodeValues(ThenBranch, ctx, block);
        result->addIncoming(left.first, block);
        BranchInst::Create(done, block);

        block = elsb;

        const auto right = GetNodeValues(ElseBranch, ctx, block);
        result->addIncoming(right.first, block);
        BranchInst::Create(done, block);

        block = done;

        MKQL_ENSURE(left.second.size() == right.second.size(), "Expected same width of flows.");
        TGettersList getters;
        getters.reserve(left.second.size());
        const auto index = static_cast<const IComputationNode*>(this)->GetIndex();
        size_t idx = 0U;
        std::generate_n(std::back_inserter(getters), right.second.size(), [&]() {
            const auto i = idx++;
            return [index, lget = left.second[i], rget = right.second[i]](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();

                const auto then = BasicBlock::Create(context, "then", ctx.Func);
                const auto elsb = BasicBlock::Create(context, "elsb", ctx.Func);
                const auto done = BasicBlock::Create(context, "done", ctx.Func);
                const auto valueType = Type::getInt128Ty(context);
                const auto result = PHINode::Create(valueType, 2, "result", done);

                const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), index)}, "state_ptr", block);
                const auto state = new LoadInst(valueType, statePtr, "state", block);
                const auto trunc = CastInst::Create(Instruction::Trunc, state, Type::getInt1Ty(context), "trunc", block);

                BranchInst::Create(then, elsb, trunc, block);

                block = then;
                result->addIncoming(lget(ctx, block), block);
                BranchInst::Create(done, block);

                block = elsb;
                result->addIncoming(rget(ctx, block), block);
                BranchInst::Create(done, block);

                block = done;
                return result;
            };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOnBoth(ThenBranch, ElseBranch))
            DependsOn(flow, Predicate);
    }

    IComputationNode* const Predicate;
    IComputationWideFlowNode* const ThenBranch;
    IComputationWideFlowNode* const ElseBranch;
};

}

IComputationNode* WrapIf(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");

    bool isOptional;
    const auto predicateType = UnpackOptionalData(callable.GetInput(0), isOptional);
    MKQL_ENSURE(predicateType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool or optional of bool.");

    const auto predicate = LocateNode(ctx.NodeLocator, callable, 0);
    const auto thenBranch = LocateNode(ctx.NodeLocator, callable, 1);
    const auto elseBranch = LocateNode(ctx.NodeLocator, callable, 2);
    const auto type = callable.GetType()->GetReturnType();

    if (type->IsFlow()) {
        const auto thenWide = dynamic_cast<IComputationWideFlowNode*>(thenBranch);
        const auto elseWide = dynamic_cast<IComputationWideFlowNode*>(elseBranch);
        if (thenWide && elseWide && !isOptional)
            return new TWideIfWrapper(ctx.Mutables, predicate, thenWide, elseWide);
        else if (!thenWide && !elseWide) {
            if (isOptional)
                return new TFlowIfWrapper<true>(ctx.Mutables, GetValueRepresentation(type), predicate, thenBranch, elseBranch);
            else
                return new TFlowIfWrapper<false>(ctx.Mutables, GetValueRepresentation(type), predicate, thenBranch, elseBranch);
        }
    } else {
        if (isOptional) {
            return new TIfWrapper<true>(ctx.Mutables, GetValueRepresentation(type), predicate, thenBranch, elseBranch);
        } else {
            return new TIfWrapper<false>(ctx.Mutables, GetValueRepresentation(type), predicate, thenBranch, elseBranch);
        }
    }

    THROW yexception() << "Wrong signature.";
}

}
}
