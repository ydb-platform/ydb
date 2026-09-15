#include "mkql_wide_map.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/cast.h>

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using NYql::EnsureDynamicCast;
#endif

namespace {

class TWideMapFlowWrapper: public TStatelessWideFlowCodegeneratorNode<TWideMapFlowWrapper> {
    using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TWideMapFlowWrapper>;

public:
    TWideMapFlowWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& newItems)
        : TBaseComputation(flow)
        , Flow_(flow)
        , Items_(std::move(items))
        , NewItems_(std::move(newItems))
        , PasstroughtMap_(GetPasstroughtMapOneToOne(Items_, NewItems_))
        , ReversePasstroughtMap_(GetPasstroughtMapOneToOne(NewItems_, Items_))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Items_.size()))
    {
    }

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        for (auto i = 0U; i < Items_.size(); ++i) {
            if (const auto& map = PasstroughtMap_[i]; map && !Items_[i]->GetDependentsCount()) {
                if (const auto out = output[*map]) {
                    fields[i] = out;
                }
            } else {
                fields[i] = &Items_[i]->RefValue(ctx);
            }
        }

        if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result) {
            return result;
        }

        for (auto i = 0U; i < NewItems_.size(); ++i) {
            if (const auto out = output[i]) {
                if (const auto& map = ReversePasstroughtMap_[i]) {
                    if (const auto from = *map; !Items_[from]->GetDependentsCount()) {
                        if (const auto first = *PasstroughtMap_[from]; first != i) {
                            *out = *output[first];
                        }
                        continue;
                    }
                }

                *out = NewItems_[i]->GetValue(ctx);
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto result = GetNodeValues(Flow_, ctx, block);

        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, result.first, ConstantInt::get(result.first->getType(), 0), "good", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        BranchInst::Create(work, pass, good, block);

        block = work;

        for (auto i = 0U; i < Items_.size(); ++i) {
            if (Items_[i]->GetDependentsCount() > 0U || !PasstroughtMap_[i]) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Items_[i])->CreateSetValue(ctx, block, result.second[i](ctx, block));
            }
        }

        BranchInst::Create(pass, block);

        block = pass;

        TGettersList getters;
        getters.reserve(NewItems_.size());
        for (auto i = 0U; i < NewItems_.size(); ++i) {
            if (const auto map = ReversePasstroughtMap_[i]) {
                getters.emplace_back(result.second[*map]);
            } else {
                getters.emplace_back([node = NewItems_[i]](const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(node, ctx, block); });
            }
        };
        return {result.first, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow_)) {
            std::for_each(Items_.cbegin(), Items_.cend(), std::bind(&TWideMapFlowWrapper::Own, flow, std::placeholders::_1));
            std::for_each(NewItems_.cbegin(), NewItems_.cend(), std::bind(&TWideMapFlowWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow_;
    const TComputationExternalNodePtrVector Items_;
    const TComputationNodePtrVector NewItems_;
    const TPasstroughtMap PasstroughtMap_, ReversePasstroughtMap_;

    const ui32 WideFieldsIndex_;
};

class TWideMapStreamWrapper: public TMutableComputationNode<TWideMapStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TWideMapStreamWrapper>;

public:
    TWideMapStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , Items_(std::move(items))
        , NewItems_(std::move(newItems))
        , PasstroughtMap_(GetPasstroughtMapOneToOne(Items_, NewItems_))
        , ReversePasstroughtMap_(GetPasstroughtMapOneToOne(NewItems_, Items_))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Items_.size()))
    {
    }

    NYql::NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(
            ctx,
            ctx.HolderFactory,
            Stream_->GetValue(ctx),
            Items_,
            NewItems_,
            PasstroughtMap_,
            ReversePasstroughtMap_);
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
            , CompCtx_(compCtx)
            , HolderFactory_(holderFactory)
            , Stream_(std::move(stream))
            , Items_(items)
            , NewItems_(newItems)
            , PasstroughtMap_(std::move(passtroughtMap))
            , ReversePasstroughtMap_(std::move(reversePasstroughtMap))
        {
            State_.resize(Items_.size());
            Y_UNUSED(HolderFactory_);
        }

        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) final {
            Y_UNUSED(width);
            if (const auto result = Stream_.WideFetch(State_.data(), State_.size()); NUdf::EFetchStatus::Ok != result) {
                return result;
            }

            for (auto i = 0U; i < Items_.size(); ++i) {
                if (const auto& map = PasstroughtMap_[i]; map && !Items_[i]->GetDependentsCount()) {
                    output[*map] = State_[i];
                } else {
                    Items_[i]->RefValue(CompCtx_) = State_[i];
                }
            }

            for (auto i = 0U; i < NewItems_.size(); ++i) {
                if (const auto& map = ReversePasstroughtMap_[i]) {
                    if (const auto from = *map; !Items_[from]->GetDependentsCount()) {
                        if (const auto first = *PasstroughtMap_[from]; first != i) {
                            output[i] = output[first];
                        }
                        continue;
                    }
                }

                output[i] = NewItems_[i]->GetValue(CompCtx_);
            }
            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx_;
        const THolderFactory& HolderFactory_;
        NUdf::TUnboxedValue Stream_;
        const TComputationExternalNodePtrVector& Items_;
        const TComputationNodePtrVector& NewItems_;

        const TPassthroughSpan PasstroughtMap_;
        const TPassthroughSpan ReversePasstroughtMap_;
        TUnboxedValueVector State_;
    };

    void RegisterDependencies() const final {
        Stream_->AddDependent(this);
        std::for_each(Items_.cbegin(), Items_.cend(), std::bind(&TWideMapStreamWrapper::Own, this, std::placeholders::_1));
        std::for_each(NewItems_.cbegin(), NewItems_.cend(), std::bind(&TWideMapStreamWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Stream_;
    const TComputationExternalNodePtrVector Items_;
    const TComputationNodePtrVector NewItems_;
    const TPasstroughtMap PasstroughtMap_;
    const TPasstroughtMap ReversePasstroughtMap_;

    const ui32 WideFieldsIndex_;
};
} // namespace

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

} // namespace NKikimr::NMiniKQL
