#include "mkql_wide_filter.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_simple_codegen.h>
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
        , FilterByField(GetPasstroughtMap(TComputationNodePtrVector{Predicate}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {}

    NYql::NUdf::TUnboxedValue** GetFields(TComputationContext& ctx) const {
        return ctx.WideFields.data() + WideFieldsIndex;
    }

    NUdf::TUnboxedValue*const* PrepareArguments(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = GetFields(ctx);

        for (auto i = 0U; i < Items.size(); ++i) {
            if (Predicate == Items[i] || Items[i]->GetDependencesCount() > 0U)
                fields[i] = &Items[i]->RefValue(ctx);
            else
                fields[i] = output[i];
        }

        return fields;
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

class TWideFilterWrapper : public TSimpleStatelessWideFlowCodegeneratorNode<TWideFilterWrapper>,  public TBaseWideFilterWrapper {
using TBaseComputation = TSimpleStatelessWideFlowCodegeneratorNode<TWideFilterWrapper>;
public:
    TWideFilterWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(flow, items.size(), items.size())
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    NUdf::TUnboxedValue*const* PrepareInput(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        return PrepareArguments(ctx, output);
    }

    TMaybeFetchResult DoProcess(TComputationContext& ctx, TMaybeFetchResult fetchRes, NUdf::TUnboxedValue*const* output) const {
        if (fetchRes.Get() == EFetchResult::One) {
            if (Predicate->GetValue(ctx).Get<bool>()) {
                FillOutputs(ctx, output);
                return EFetchResult::One;
            }
            return TMaybeFetchResult::None();
        }
        return fetchRes;
    }

#ifndef MKQL_DISABLE_CODEGEN
    typename TBaseComputation::TGenerateResult GenFetchProcess(const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const override {
        auto &context = ctx.Codegen.GetContext();
        auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        auto check = BasicBlock::Create(context, "check", ctx.Func);
        auto decr = BasicBlock::Create(context, "decr", ctx.Func);
        auto maybeResultVal = PHINode::Create(TMaybeFetchResult::LLVMType(context), 4, "maybe_res", pass);
        
        auto [fetchResVal, fetchGetters] = fetchGenerator(ctx, block);
        auto passCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, ConstantInt::get(fetchResVal->getType(), static_cast<i32>(EFetchResult::One)), fetchResVal, "not_one", block);
        maybeResultVal->addIncoming(TMaybeFetchResult::LLVMFromFetchResult(fetchResVal, "fetch_res_ext", block), block);
        BranchInst::Create(pass, check, passCond, block);

        block = check;
        auto predicateCond = GenGetPredicate<false>(ctx, fetchGetters, block);
        maybeResultVal->addIncoming(TMaybeFetchResult::None().LLVMConst(context), block);
        BranchInst::Create(decr, pass, predicateCond, block);

        block = decr;
        maybeResultVal->addIncoming(TMaybeFetchResult(EFetchResult::One).LLVMConst(context), block);
        BranchInst::Create(pass, block);

        block = pass;

        return {maybeResultVal, std::move(fetchGetters)};
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

class TWideFilterWithLimitWrapper : public TSimpleStatefulWideFlowCodegeneratorNode<TWideFilterWithLimitWrapper>,  public TBaseWideFilterWrapper {
using TBaseComputation = TSimpleStatefulWideFlowCodegeneratorNode<TWideFilterWithLimitWrapper>;
public:
    TWideFilterWithLimitWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* limit,
            TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, items.size(), items.size())
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
        , Limit(limit)
    {}

    void InitState(NUdf::TUnboxedValue& cntToTake, TComputationContext& ctx) const {
        cntToTake = Limit->GetValue(ctx);
    }

    NUdf::TUnboxedValue*const* PrepareInput(NUdf::TUnboxedValue& cntToTake, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        return cntToTake.Get<ui64>() ? PrepareArguments(ctx, output) : nullptr;
    }

    TMaybeFetchResult DoProcess(NUdf::TUnboxedValue& cntToTake, TComputationContext& ctx, TMaybeFetchResult fetchRes, NUdf::TUnboxedValue*const* output) const {
        if (fetchRes.Empty()) {
            return EFetchResult::Finish;
        } else if (fetchRes.Get() == EFetchResult::One) {
            if (Predicate->GetValue(ctx).Get<bool>()) {
                FillOutputs(ctx, output);
                cntToTake = NUdf::TUnboxedValuePod(cntToTake.Get<ui64>() - 1);
                return EFetchResult::One;
            }
            return TMaybeFetchResult::None();
        }
        return fetchRes;
    }

#ifndef MKQL_DISABLE_CODEGEN
    typename TBaseComputation::TGenerateResult GenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const override {
        auto &context = ctx.Codegen.GetContext();
        auto fetch = BasicBlock::Create(context, "fetch", ctx.Func);
        auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        auto check = BasicBlock::Create(context, "check", ctx.Func);
        auto decr = BasicBlock::Create(context, "decr", ctx.Func);
        auto maybeResultVal = PHINode::Create(TMaybeFetchResult::LLVMType(context), 4, "maybe_res", pass);

        auto stateVal = new LoadInst(statePtrVal->getType()->getPointerElementType(), statePtrVal, "state", block);
        auto needFetchCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, GetFalse(context), stateVal, "need_fetch", block);
        maybeResultVal->addIncoming(TMaybeFetchResult(EFetchResult::Finish).LLVMConst(context), block);
        BranchInst::Create(fetch, pass, needFetchCond, block);
        
        block = fetch;
        auto [fetchResVal, fetchGetters] = fetchGenerator(ctx, block);
        auto passCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, ConstantInt::get(fetchResVal->getType(), static_cast<i32>(EFetchResult::One)), fetchResVal, "not_one", block);
        maybeResultVal->addIncoming(TMaybeFetchResult::LLVMFromFetchResult(fetchResVal, "fetch_res_ext", block), block);
        BranchInst::Create(pass, check, passCond, block);

        block = check;
        auto predicateCond = GenGetPredicate<false>(ctx, fetchGetters, block);
        maybeResultVal->addIncoming(TMaybeFetchResult::None().LLVMConst(context), block);
        BranchInst::Create(decr, pass, predicateCond, block);

        block = decr;
        auto newStateVal = BinaryOperator::CreateSub(stateVal, ConstantInt::get(stateVal->getType(), 1), "new_state", block);
        new StoreInst(newStateVal, statePtrVal, block);
        maybeResultVal->addIncoming(TMaybeFetchResult(EFetchResult::One).LLVMConst(context), block);
        BranchInst::Create(pass, block);

        block = pass;

        return {maybeResultVal, std::move(fetchGetters)};
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
class TWideTakeWhileWrapper : public TSimpleStatefulWideFlowCodegeneratorNode<TWideTakeWhileWrapper<Inclusive>>,  public TBaseWideFilterWrapper {
using TBaseComputation = TSimpleStatefulWideFlowCodegeneratorNode<TWideTakeWhileWrapper<Inclusive>>;
public:
     TWideTakeWhileWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items,
            IComputationNode* predicate)
        : TBaseComputation(mutables, flow, items.size(), items.size())
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    void InitState(NUdf::TUnboxedValue& stop, TComputationContext& ) const {
        stop = NUdf::TUnboxedValuePod(false);
    }

    NUdf::TUnboxedValue*const* PrepareInput(NUdf::TUnboxedValue& stop, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        return stop.Get<bool>() ? nullptr : PrepareArguments(ctx, output);
    }

    TMaybeFetchResult DoProcess(NUdf::TUnboxedValue& stop, TComputationContext& ctx, TMaybeFetchResult fetchRes, NUdf::TUnboxedValue*const* output) const {
        if (fetchRes.Empty()) {
            return EFetchResult::Finish;
        } else if (fetchRes.Get() == EFetchResult::One) {
            const bool predicate = Predicate->GetValue(ctx).Get<bool>();
            if (!predicate) {
                stop = NUdf::TUnboxedValuePod(true);
            }
            if (Inclusive || predicate) {
                FillOutputs(ctx, output);
                return EFetchResult::One;
            }
            return EFetchResult::Finish;
        }
        return fetchRes;
    }

#ifndef MKQL_DISABLE_CODEGEN
    typename TBaseComputation::TGenerateResult GenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const override {
        auto &context = ctx.Codegen.GetContext();
        auto fetch = BasicBlock::Create(context, "fetch", ctx.Func);
        auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        auto check = BasicBlock::Create(context, "check", ctx.Func);
        auto maybeResultVal = PHINode::Create(TMaybeFetchResult::LLVMType(context), 3, "maybe_res", pass);

        auto stateVal = new LoadInst(statePtrVal->getType()->getPointerElementType(), statePtrVal, "state", block);
        auto needFetchCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, GetTrue(context), stateVal, "need_fetch", block);
        maybeResultVal->addIncoming(TMaybeFetchResult(EFetchResult::Finish).LLVMConst(context), block);
        BranchInst::Create(fetch, pass, needFetchCond, block);
        
        block = fetch;
        auto [fetchResVal, fetchGetters] = fetchGenerator(ctx, block);
        auto passCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, ConstantInt::get(fetchResVal->getType(), static_cast<i32>(EFetchResult::One)), fetchResVal, "not_one", block);
        maybeResultVal->addIncoming(TMaybeFetchResult::LLVMFromFetchResult(fetchResVal, "fetch_res_ext", block), block);
        BranchInst::Create(pass, check, passCond, block);

        block = check;
        auto predicateCond = GenGetPredicate<false>(ctx, fetchGetters, block);
        auto newStateVal = SelectInst::Create(predicateCond, GetFalse(context), GetTrue(context), "new_state", block);
        new StoreInst(newStateVal, statePtrVal, block);
        auto retOneCond = Inclusive ? ConstantInt::getTrue(context) : predicateCond;
        auto retStatusVal = SelectInst::Create(retOneCond, TMaybeFetchResult(EFetchResult::One).LLVMConst(context), TMaybeFetchResult(EFetchResult::Finish).LLVMConst(context), "ret_status", block);
        maybeResultVal->addIncoming(retStatusVal, block);
        BranchInst::Create(pass, block);

        block = pass;

        return {maybeResultVal, std::move(fetchGetters)};
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
class TWideSkipWhileWrapper : public TSimpleStatefulWideFlowCodegeneratorNode<TWideSkipWhileWrapper<Inclusive>>,  public TBaseWideFilterWrapper {
using TBaseComputation = TSimpleStatefulWideFlowCodegeneratorNode<TWideSkipWhileWrapper<Inclusive>>;
public:
     TWideSkipWhileWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, items.size(), items.size())
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    void InitState(NUdf::TUnboxedValue& start, TComputationContext& ) const {
        start = NUdf::TUnboxedValuePod(false);
    }

    NUdf::TUnboxedValue*const* PrepareInput(NUdf::TUnboxedValue& start, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        return start.Get<bool>() ? output : PrepareArguments(ctx, output);
    }

    TMaybeFetchResult DoProcess(NUdf::TUnboxedValue& start, TComputationContext& ctx, TMaybeFetchResult fetchRes, NUdf::TUnboxedValue*const* output) const {
        if (!start.Get<bool>() && fetchRes.Get() == EFetchResult::One) {
            const bool predicate = Predicate->GetValue(ctx).Get<bool>();
            if (!predicate) {
                start = NUdf::TUnboxedValuePod(true);
            }
            if (!Inclusive && !predicate) {
                FillOutputs(ctx, output);
                return EFetchResult::One;
            }
            return TMaybeFetchResult::None();
        }
        return fetchRes;
    }

#ifndef MKQL_DISABLE_CODEGEN
    typename TBaseComputation::TGenerateResult GenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        auto check = BasicBlock::Create(context, "check", ctx.Func);
        auto save = BasicBlock::Create(context, "save", ctx.Func);
        auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        auto maybeResultVal = PHINode::Create(TMaybeFetchResult::LLVMType(context), 3, "maybe_res", pass);

        auto [fetchResVal, fetchGetters] = fetchGenerator(ctx, block);
        auto stateVal = new LoadInst(statePtrVal->getType()->getPointerElementType(), statePtrVal, "state", block);
        auto needCheckCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, GetTrue(context), stateVal, "need_check", block);
        auto oneCond = CmpInst::Create(Instruction::ICmp, CmpInst::ICMP_EQ, ConstantInt::get(fetchResVal->getType(), static_cast<i32>(EFetchResult::One)), fetchResVal, "one", block);
        auto willCheckCond = BinaryOperator::Create(Instruction::And, needCheckCond, oneCond, "will_check", block);
        maybeResultVal->addIncoming(TMaybeFetchResult::LLVMFromFetchResult(fetchResVal, "fetch_res_ext", block), block);
        BranchInst::Create(check, pass, willCheckCond, block);

        block = check;
        auto predicateCond = GenGetPredicate<false>(ctx, fetchGetters, block);
        maybeResultVal->addIncoming(TMaybeFetchResult::None().LLVMConst(context), block);
        BranchInst::Create(pass, save, predicateCond, block);

        block = save;
        new StoreInst(GetTrue(context), statePtrVal, block);
        maybeResultVal->addIncoming((Inclusive ? TMaybeFetchResult::None() : TMaybeFetchResult(EFetchResult::One)).LLVMConst(context), block);
        BranchInst::Create(pass, block);

        block = pass;
        
        return {maybeResultVal, std::move(fetchGetters)};
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
