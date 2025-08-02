#include "mkql_wide_map.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/cast.h>

namespace NKikimr::NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class TWideMapFlowWrapper : public TStatelessWideFlowCodegeneratorNode<TWideMapFlowWrapper> {
using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TWideMapFlowWrapper>;
public:
    TWideMapFlowWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& newItems)
        : TBaseComputation(flow)
        , Flow(flow)
        , Items(std::move(items))
        , NewItems(std::move(newItems))
        , PasstroughtMap(GetPasstroughtMapOneToOne(Items, NewItems))
        , ReversePasstroughtMap(GetPasstroughtMapOneToOne(NewItems, Items))
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {}

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        for (auto i = 0U; i < Items.size(); ++i)
            if (const auto& map = PasstroughtMap[i]; map && !Items[i]->GetDependencesCount()) {
                if (const auto out = output[*map])
                    fields[i] = out;
            } else
                fields[i] = &Items[i]->RefValue(ctx);

        if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
            return result;

        for (auto i = 0U; i < NewItems.size(); ++i) {
            if (const auto out = output[i]) {
                if (const auto& map = ReversePasstroughtMap[i]) {
                    if (const auto from = *map; !Items[from]->GetDependencesCount()) {
                        if (const auto first = *PasstroughtMap[from]; first != i)
                            *out = *output[first];
                        continue;
                    }
                }

                *out = NewItems[i]->GetValue(ctx);
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto result = GetNodeValues(Flow, ctx, block);

        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, result.first, ConstantInt::get(result.first->getType(), 0), "good", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        BranchInst::Create(work, pass, good, block);

        block = work;

        for (auto i = 0U; i < Items.size(); ++i)
            if (Items[i]->GetDependencesCount() > 0U || !PasstroughtMap[i])
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, result.second[i](ctx, block));

        BranchInst::Create(pass, block);

        block = pass;

        TGettersList getters;
        getters.reserve(NewItems.size());
        for (auto i = 0U; i < NewItems.size(); ++i) {
            if (const auto map = ReversePasstroughtMap[i])
                getters.emplace_back(result.second[*map]);
            else
                getters.emplace_back([node=NewItems[i]](const TCodegenContext& ctx, BasicBlock*& block){ return GetNodeValue(node, ctx, block); });
        };
        return {result.first, std::move(getters)};

    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideMapFlowWrapper::Own, flow, std::placeholders::_1));
            std::for_each(NewItems.cbegin(), NewItems.cend(), std::bind(&TWideMapFlowWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    const TComputationNodePtrVector NewItems;
    const TPasstroughtMap PasstroughtMap, ReversePasstroughtMap;

    const ui32 WideFieldsIndex;
};

class TWideMapStreamWrapper: public TMutableComputationNode<TWideMapStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TWideMapStreamWrapper>;

public:
    TWideMapStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables)
        , Stream(stream)
        , Items(std::move(items))
        , NewItems(std::move(newItems))
        , PasstroughtMap(GetPasstroughtMapOneToOne(Items, NewItems))
        , ReversePasstroughtMap(GetPasstroughtMapOneToOne(NewItems, Items))
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {
    }

    NYql::NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(
                    ctx,
                     ctx.HolderFactory,
                     Stream->GetValue(ctx),
                     Items,
                     NewItems,
                     PasstroughtMap,
                     ReversePasstroughtMap);
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo,
                     TComputationContext& compCtx,
                     const THolderFactory& holderFactory,
                     NYql::NUdf::TUnboxedValue&& stream,
                     const TComputationExternalNodePtrVector& items,
                     const TComputationNodePtrVector& newItems,
                     TPassthroughSpan passtroughtMap,
                     TPassthroughSpan reversePasstroughtMap)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , HolderFactory(holderFactory)
            , Stream(std::move(stream))
            , Items(items)
            , NewItems(newItems)
            , PasstroughtMap(std::move(passtroughtMap))
            , ReversePasstroughtMap(std::move(reversePasstroughtMap))
        {
            State.resize(Items.size());
            Y_UNUSED(HolderFactory);
        }

        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) final {
            Y_UNUSED(width);
            if (const auto result = Stream.WideFetch(State.data(), State.size()); NUdf::EFetchStatus::Ok != result) {
                return result;
            }

            for (auto i = 0U; i < Items.size(); ++i) {
                if (const auto& map = PasstroughtMap[i]; map && !Items[i]->GetDependencesCount()) {
                    output[*map] = State[i];
                } else {
                    Items[i]->RefValue(CompCtx) = State[i];
                }
            }

            for (auto i = 0U; i < NewItems.size(); ++i) {
                if (const auto& map = ReversePasstroughtMap[i]) {
                    if (const auto from = *map; !Items[from]->GetDependencesCount()) {
                        if (const auto first = *PasstroughtMap[from]; first != i) {
                            output[i] = output[first];
                        }
                        continue;
                    }
                }

                output[i] = NewItems[i]->GetValue(CompCtx);
            }
            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx;
        const THolderFactory& HolderFactory;
        NUdf::TUnboxedValue Stream;
        const TComputationExternalNodePtrVector& Items;
        const TComputationNodePtrVector& NewItems;

        const TPassthroughSpan PasstroughtMap;
        const TPassthroughSpan ReversePasstroughtMap;
        TUnboxedValueVector State;
    };

    void RegisterDependencies() const final {
        Stream->AddDependence(this);
        std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideMapStreamWrapper::Own, this, std::placeholders::_1));
        std::for_each(NewItems.cbegin(), NewItems.cend(), std::bind(&TWideMapStreamWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Stream;
    const TComputationExternalNodePtrVector Items;
    const TComputationNodePtrVector NewItems;
    const TPasstroughtMap PasstroughtMap;
    const TPasstroughtMap ReversePasstroughtMap;

    const ui32 WideFieldsIndex;
};
}

IComputationNode* WrapWideMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 0U, "Expected argument.");
    MKQL_ENSURE(callable.GetInput(0U).GetStaticType()->IsFlow() || callable.GetInput(0U).GetStaticType()->IsStream(),
                "Expected stream or flow for input.");

    const auto inputWidth = GetWideComponentsCount(callable.GetInput(0U).GetStaticType());
    const auto outputWidth = GetWideComponentsCount(callable.GetType()->GetReturnType());

    if (callable.GetInput(0U).GetStaticType()->IsFlow()) {
        MKQL_ENSURE(callable.GetType()->GetReturnType()->IsFlow(), "Expected flow return type.");
    } else {
        MKQL_ENSURE(callable.GetType()->GetReturnType()->IsStream(), "Expected stream return type.");
    }

    MKQL_ENSURE(callable.GetInputsCount() == inputWidth + outputWidth + 1U, "Wrong signature.");

    const auto flowOrStream = LocateNode(ctx.NodeLocator, callable, 0U);
    TComputationNodePtrVector newItems(outputWidth, nullptr);
    ui32 index = inputWidth;
    std::generate(newItems.begin(), newItems.end(), [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

    TComputationExternalNodePtrVector args(inputWidth, nullptr);
    index = 0U;
    std::generate(args.begin(), args.end(), [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

    if (const auto flow = dynamic_cast<IComputationWideFlowNode*>(flowOrStream)) {
        return new TWideMapFlowWrapper(ctx.Mutables, flow, std::move(args), std::move(newItems));
    } else {
        auto* stream = flowOrStream;
        return new TWideMapStreamWrapper(ctx.Mutables, stream, std::move(args), std::move(newItems));
    }

    THROW yexception() << "Expected wide flow.";
}

}
