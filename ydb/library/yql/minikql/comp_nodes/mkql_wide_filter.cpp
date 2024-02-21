#include "mkql_wide_filter.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen_impl.h>
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

    bool ApplyPredicate(TComputationContext& ctx, NUdf::TUnboxedValue*const* values) const {
        auto **fields = GetFields(ctx);
        PrepareArguments(ctx, values);
        for (size_t idx = 0; idx < Items.size(); idx++) {
            *fields[idx] = *values[idx];
        }
        return Predicate->GetValue(ctx).Get<bool>();
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

class TWideFilterWithLimitWrapper : public TSimpleStatefulWideFlowCodegeneratorNode<TWideFilterWithLimitWrapper, ui64>,  public TBaseWideFilterWrapper {
using TBaseComputation = TSimpleStatefulWideFlowCodegeneratorNode<TWideFilterWithLimitWrapper, ui64>;
public:
    TWideFilterWithLimitWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* limit,
            TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
        , Limit(limit)
    {}

    void InitState(ui64& limit, TComputationContext& ctx) const {
        limit = Limit->GetValue(ctx).Get<ui64>();
    }

    EProcessResult DoProcess(ui64& limit, TComputationContext& ctx, EFetchResult fetchRes, NUdf::TUnboxedValue*const* values) const {
        if (limit == 0) {
            return EProcessResult::Finish;
        }
        if (fetchRes == EFetchResult::One) {
            if (ApplyPredicate(ctx, values)) {
                FillOutputs(ctx, values);
                limit--;
                return EProcessResult::One;
            }
            return EProcessResult::Fetch;
        }
        return static_cast<EProcessResult>(fetchRes);
    }

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
class TWideTakeWhileWrapper : public TSimpleStatefulWideFlowCodegeneratorNode<TWideTakeWhileWrapper<Inclusive>, bool>,  public TBaseWideFilterWrapper {
using TBaseComputation = TSimpleStatefulWideFlowCodegeneratorNode<TWideTakeWhileWrapper<Inclusive>, bool>;
public:
     TWideTakeWhileWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items,
            IComputationNode* predicate)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    void InitState(bool& stop, TComputationContext& ctx) const {
        stop = false;
    }

    TBaseComputation::EProcessResult DoProcess(bool& stop, TComputationContext& ctx, EFetchResult fetchRes, NUdf::TUnboxedValue*const* values) const {
        if (stop) {
            return TBaseComputation::EProcessResult::Finish;
        }
        if (fetchRes == EFetchResult::One) {
            const bool predicate = ApplyPredicate(ctx, values);
            if (!predicate) {
                stop = true;
            }
            if (Inclusive || predicate) {
                FillOutputs(ctx, values);
                return TBaseComputation::EProcessResult::One;
            }
            return TBaseComputation::EProcessResult::Finish;
        }
        return static_cast<TBaseComputation::EProcessResult>(fetchRes);
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideTakeWhileWrapper::Own, flow, std::placeholders::_1));
            TWideTakeWhileWrapper::DependsOn(flow, Predicate);
        }
    }
};

template<bool Inclusive>
class TWideSkipWhileWrapper : public TSimpleStatefulWideFlowCodegeneratorNode<TWideSkipWhileWrapper<Inclusive>, bool>,  public TBaseWideFilterWrapper {
using TBaseComputation = TSimpleStatefulWideFlowCodegeneratorNode<TWideSkipWhileWrapper<Inclusive>, bool>;
public:
     TWideSkipWhileWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , TBaseWideFilterWrapper(mutables, flow, std::move(items), predicate)
    {}

    void InitState(bool& start, TComputationContext& ctx) const {
        start = false;
    }

    TBaseComputation::EProcessResult DoProcess(bool& start, TComputationContext& ctx, EFetchResult fetchRes, NUdf::TUnboxedValue*const* values) const {
        if (!start && fetchRes == EFetchResult::One) {
            const bool predicate = ApplyPredicate(ctx, values);
            if (!predicate) {
                start = true;
            }
            if (!Inclusive && !predicate) {
                FillOutputs(ctx, values);
                return TBaseComputation::EProcessResult::One;
            }
            return TBaseComputation::EProcessResult::Fetch;
        }
        return static_cast<TBaseComputation::EProcessResult>(fetchRes);
    }

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
