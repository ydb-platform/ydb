#include "mkql_wide_filter.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/cast.h>


namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class TBaseWideFilterWrapper {
protected:
    TBaseWideFilterWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : Flow(flow)
        , Items(std::move(items))
        , Predicate(predicate)
        , FilterByField(GetPasstroughtMap({Predicate}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {}

    NYql::NUdf::TUnboxedValue** GetFields(TComputationContext& ctx) const {
        return ctx.WideFields.data() + WideFieldsIndex;
    }

    void PrepareArguments(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = GetFields(ctx);

        for (auto i = 0U; i < Items.size(); ++i) {
            if (Predicate == Items[i] || Items[i]->GetDependencesCount() > 0U)
                fields[i] = &Items[i]->RefValue(ctx);
            else
                fields[i] = output[i];
        }
    }

    void FillOutputs(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = GetFields(ctx);

        for (auto i = 0U; i < Items.size(); ++i)
            if (const auto out = output[i])
                if (Predicate == Items[i] || Items[i]->GetDependencesCount() > 0U)
                    *out = *fields[i];
    }
#ifndef MKQL_DISABLE_CODEGEN
    template<bool ReplaceOriginalGetter = true>
    Value* GenGetPredicate(const TCodegenContext& ctx,
        std::conditional_t<ReplaceOriginalGetter, ICodegeneratorInlineWideNode::TGettersList, const ICodegeneratorInlineWideNode::TGettersList>& getters,
        BasicBlock*& block) const {
        if (FilterByField)
            return CastInst::Create(Instruction::Trunc, getters[*FilterByField](ctx, block), Type::getInt1Ty(ctx.Codegen.GetContext()), "predicate", block);

        for (auto i = 0U; i < Items.size(); ++i)
            if (Predicate == Items[i] || Items[i]->GetDependencesCount() > 0U) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getters[i](ctx, block));
                if constexpr (ReplaceOriginalGetter)
                    getters[i] = [node=Items[i]](const TCodegenContext& ctx, BasicBlock*& block){ return GetNodeValue(node, ctx, block); };
            }

        const auto pred = GetNodeValue(Predicate, ctx, block);
        return CastInst::Create(Instruction::Trunc, pred, Type::getInt1Ty(ctx.Codegen.GetContext()), "predicate", block);
    }
#endif
    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    IComputationNode* const Predicate;

    std::optional<size_t> FilterByField;

    const ui32 WideFieldsIndex;
};

class TWideFilterWrapper : public TStatelessWideFlowCodegeneratorNode<TWideFilterWrapper>,  public TBaseWideFilterWrapper {
using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TWideFilterWrapper>;
public:
    TWideFilterWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(flow)
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = GetFields(ctx);

        while (true) {
            PrepareArguments(ctx, output);

            if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
                return result;

            if (Predicate->GetValue(ctx).Get<bool>()) {
                FillOutputs(ctx, output);
                return EFetchResult::One;
            }
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);

        BranchInst::Create(loop, block);

        block = loop;

        auto status = GetNodeValues(Flow, ctx, block);

        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, status.first, ConstantInt::get(status.first->getType(), 0), "good", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        BranchInst::Create(work, pass, good, block);

        block = work;

        const auto predicate = GenGetPredicate(ctx, status.second, block);

        BranchInst::Create(pass, loop, predicate, block);

        block = pass;
        return status;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideFilterWrapper::Own, flow, std::placeholders::_1));
            DependsOn(flow, Predicate);
        }
    }
};

class TWideFilterWithLimitWrapper : public TStatefulWideFlowCodegeneratorNode<TWideFilterWithLimitWrapper>,  public TBaseWideFilterWrapper {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideFilterWithLimitWrapper>;
public:
    TWideFilterWithLimitWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* limit,
            TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
        , Limit(limit)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = Limit->GetValue(ctx);
        } else if (!state.Get<ui64>()) {
            return EFetchResult::Finish;
        }

        auto **fields = GetFields(ctx);
        while (true) {
            PrepareArguments(ctx, output);

            if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
                return result;

            if (Predicate->GetValue(ctx).Get<bool>()) {
                FillOutputs(ctx, output);

                auto todo = state.Get<ui64>();
                state = NUdf::TUnboxedValuePod(--todo);
                return EFetchResult::One;
            }
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 3U, "result", exit);

        BranchInst::Create(test, init, IsValid(statePtr, block), block);

        block = init;

        GetNodeValue(statePtr, Limit, ctx, block);
        BranchInst::Create(test, block);

        block = test;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto done = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state, GetFalse(context), "done", block);
        result->addIncoming(ConstantInt::get(resultType, -1), block);

        BranchInst::Create(exit, loop, done, block);

        block = loop;

        auto status = GetNodeValues(Flow, ctx, block);
        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, status.first, ConstantInt::get(status.first->getType(), 0), "good", block);

        result->addIncoming(status.first, block);

        BranchInst::Create(work, exit, good, block);

        block = work;

        const auto predicate = GenGetPredicate(ctx, status.second, block);

        BranchInst::Create(pass, loop, predicate, block);

        block = pass;

        const auto decr = BinaryOperator::CreateSub(state, ConstantInt::get(state->getType(), 1ULL), "decr", block);
        new StoreInst(decr, statePtr, block);

        result->addIncoming(status.first, block);

        BranchInst::Create(exit, block);

        block = exit;
        return {result, std::move(status.second)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Limit);
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideFilterWithLimitWrapper::Own, flow, std::placeholders::_1));
            DependsOn(flow, Predicate);
        }
    }

    IComputationNode* const Limit;
};

template<bool Inclusive>
class TWideTakeWhileWrapper : public TStatefulWideFlowCodegeneratorNode<TWideTakeWhileWrapper<Inclusive>>,  public TBaseWideFilterWrapper {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideTakeWhileWrapper<Inclusive>>;
public:
     TWideTakeWhileWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items,
            IComputationNode* predicate)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.IsInvalid()) {
            return EFetchResult::Finish;
        }

        PrepareArguments(ctx, output);

        auto **fields = GetFields(ctx);

        if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
            return result;

        const bool predicate = Predicate->GetValue(ctx).Get<bool>();
        if (!predicate)
            state = NUdf::TUnboxedValuePod();

        if (Inclusive || predicate) {
            FillOutputs(ctx, output);
            return EFetchResult::One;
        }

        return EFetchResult::Finish;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto resultType = Type::getInt32Ty(context);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(resultType, 4U, "result", done);
        result->addIncoming(ConstantInt::get(resultType, static_cast<i32>(EFetchResult::Finish)), block);

        const auto state = new LoadInst(Type::getInt128Ty(context), statePtr, "state", block);
        const auto finished = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state, GetTrue(context), "finished", block);

        BranchInst::Create(done, work, IsValid(statePtr, block), block);

        block = work;
        auto status = GetNodeValues(Flow, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, status.first, ConstantInt::get(resultType, 0), "special", block);
        result->addIncoming(status.first, block);
        BranchInst::Create(done, test, special, block);

        block = test;

        const auto predicate = GenGetPredicate(ctx, status.second, block);
        result->addIncoming(status.first, block);
        BranchInst::Create(done, stop, predicate, block);

        block = stop;

        new StoreInst(GetEmpty(context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, static_cast<i32>(Inclusive ? EFetchResult::One: EFetchResult::Finish)), block);

        BranchInst::Create(done, block);

        block = done;
        return {result, std::move(status.second)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideTakeWhileWrapper::Own, flow, std::placeholders::_1));
            TWideTakeWhileWrapper::DependsOn(flow, Predicate);
        }
    }
};

template<bool Inclusive>
class TWideSkipWhileWrapper : public TStatefulWideFlowCodegeneratorNode<TWideSkipWhileWrapper<Inclusive>>,  public TBaseWideFilterWrapper {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideSkipWhileWrapper<Inclusive>>;
public:
     TWideSkipWhileWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.IsInvalid()) {
            return Flow->FetchValues(ctx, output);
        }

        auto **fields = GetFields(ctx);

        do {
            PrepareArguments(ctx, output);
            if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
                return result;
        } while (Predicate->GetValue(ctx).Get<bool>());

        state = NUdf::TUnboxedValuePod();

        if constexpr (Inclusive)
            return Flow->FetchValues(ctx, output);
        else {
            FillOutputs(ctx, output);
            return EFetchResult::One;
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto resultType = Type::getInt32Ty(context);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(work, block);

        block = work;

        const auto status = GetNodeValues(Flow, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, status.first, ConstantInt::get(resultType, 0), "special", block);
        const auto passtrought = BinaryOperator::CreateOr(special, IsValid(statePtr, block), "passtrought", block);
        BranchInst::Create(done, test, passtrought, block);

        block = test;

        const auto predicate = GenGetPredicate<false>(ctx, status.second, block);
        BranchInst::Create(work, stop, predicate, block);

        block = stop;

        new StoreInst(GetEmpty(context), statePtr, block);

        BranchInst::Create(Inclusive ? work : done, block);

        block = done;
        return status;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideSkipWhileWrapper::Own, flow, std::placeholders::_1));
            TWideSkipWhileWrapper::DependsOn(flow, Predicate);
        }
    }
};

template<bool TakeOrSkip, bool Inclusive>
IComputationNode* WrapWideWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    MKQL_ENSURE(callable.GetInputsCount() == width + 2U, "Expected 3 or more args.");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    MKQL_ENSURE(callable.GetInputsCount() == width + 2U, "Wrong signature");
    const auto predicate = LocateNode(ctx.NodeLocator, callable, callable.GetInputsCount() - 1U);
    TComputationExternalNodePtrVector args;
    args.reserve(width);
    ui32 index = 0U;
    std::generate_n(std::back_inserter(args), width, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        if constexpr (TakeOrSkip) {
            return new TWideTakeWhileWrapper<Inclusive>(ctx.Mutables, wide, std::move(args), predicate);
        } else {
            return new TWideSkipWhileWrapper<Inclusive>(ctx.Mutables, wide, std::move(args), predicate);
        }
    }

    THROW yexception() << "Expected wide flow.";
}

}

IComputationNode* WrapWideFilter(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    MKQL_ENSURE(callable.GetInputsCount() == width + 2U || callable.GetInputsCount() == width + 3U, "Expected 3 or more args.");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    const auto predicate = LocateNode(ctx.NodeLocator, callable, width + 1U);
    TComputationExternalNodePtrVector args;
    args.reserve(width);
    ui32 index = 0U;
    std::generate_n(std::back_inserter(args), width, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        if (const auto last = callable.GetInputsCount() - 1U; last == width + 1U) {
            return new TWideFilterWrapper(ctx.Mutables, wide, std::move(args), predicate);
        } else {
            const auto limit = LocateNode(ctx.NodeLocator, callable, last);
            return new TWideFilterWithLimitWrapper(ctx.Mutables, wide, limit, std::move(args), predicate);
        }
    }

    THROW yexception() << "Expected wide flow.";
}

IComputationNode* WrapWideTakeWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideWhile<true, false>(callable, ctx);
}

IComputationNode* WrapWideSkipWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideWhile<false, false>(callable, ctx);
}

IComputationNode* WrapWideTakeWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideWhile<true, true>(callable, ctx);
}

IComputationNode* WrapWideSkipWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideWhile<false, true>(callable, ctx);
}


}
}
