#include "mkql_narrow_map.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/cast.h>

namespace NKikimr::NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class TNarrowMapWrapper : public TStatelessFlowCodegeneratorNode<TNarrowMapWrapper> {
using TBaseComputation = TStatelessFlowCodegeneratorNode<TNarrowMapWrapper>;
public:
    TNarrowMapWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* newItem)
        : TBaseComputation(flow, kind)
        , Flow(flow)
        , Items(std::move(items))
        , NewItem(newItem)
        , PasstroughItem(GetPasstroughtMap(TComputationNodePtrVector{NewItem}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        for (auto i = 0U; i < Items.size(); ++i) {
            if (NewItem == Items[i] || Items[i]->GetDependencesCount() > 0U)
                fields[i] = &Items[i]->RefValue(ctx);
        }

        switch (const auto result = Flow->FetchValues(ctx, fields)) {
            case EFetchResult::Finish:
                return NUdf::TUnboxedValuePod::MakeFinish();
            case EFetchResult::Yield:
                return NUdf::TUnboxedValuePod::MakeYield();
            case EFetchResult::One:
                return NewItem->GetValue(ctx).Release();
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto getres = GetNodeValues(Flow, ctx, block);

        const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(getres.first->getType(), 0), "yield", block);
        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, getres.first, ConstantInt::get(getres.first->getType(), 0), "good", block);

        const auto outres = SelectInst::Create(yield, GetYield(context), GetFinish(context), "outres", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto result = PHINode::Create(outres->getType(), 2, "result", pass);
        result->addIncoming(outres, block);

        BranchInst::Create(work, pass, good, block);

        block = work;

        if (const auto passtrough = PasstroughItem) {
            result->addIncoming(getres.second[*passtrough](ctx, block), block);
        } else {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Items[i]->GetDependencesCount() > 0U)
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));

            result->addIncoming(GetNodeValue(NewItem, ctx, block), block);
        }

        BranchInst::Create(pass, block);

        block = pass;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TNarrowMapWrapper::Own, flow, std::placeholders::_1));
            DependsOn(flow, NewItem);
        }
    }

    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    IComputationNode* const NewItem;

    const std::optional<size_t> PasstroughItem;
    const ui32 WideFieldsIndex;
};

}

IComputationNode* WrapNarrowMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 1U, "Expected two or more args.");
    const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    MKQL_ENSURE(callable.GetInputsCount() == width + 2U, "Wrong signature.");
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto newItem = LocateNode(ctx.NodeLocator, callable, callable.GetInputsCount() - 1U);

        TComputationExternalNodePtrVector args(width, nullptr);
        ui32 index = 0U;
        std::generate(args.begin(), args.end(), [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });
        return new TNarrowMapWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), wide, std::move(args), newItem);
    }

    THROW yexception() << "Expected wide flow.";
}

}
