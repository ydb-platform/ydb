#include "mkql_expand_map.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TExpandMapWrapper: public TStatelessWideFlowCodegeneratorNode<TExpandMapWrapper> {
    using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TExpandMapWrapper>;

public:
    TExpandMapWrapper(IComputationNode* flow, IComputationExternalNode* item, TComputationNodePtrVector&& newItems)
        : TBaseComputation(flow)
        , Flow(flow)
        , Item(item)
        , NewItems(std::move(newItems))
    {
    }

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
            return item.IsYield() ? EFetchResult::Yield : EFetchResult::Finish;
        } else {
            Item->SetValue(ctx, std::move(item));
        }

        for (const auto item : NewItems) {
            if (const auto out = *output++) {
                *out = item->GetValue(ctx);
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto item = GetNodeValue(Flow, ctx, block);

        const auto resultType = Type::getInt32Ty(context);
        const auto outres = SelectInst::Create(IsYield(item, block, context), ConstantInt::get(resultType, 0), ConstantInt::get(resultType, -1), "outres", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto result = PHINode::Create(outres->getType(), 2, "result", pass);

        result->addIncoming(outres, block);

        BranchInst::Create(pass, work, IsSpecial(item, block, context), block);

        block = work;
        codegenItem->CreateSetValue(ctx, block, item);

        result->addIncoming(ConstantInt::get(resultType, 1), block);

        BranchInst::Create(pass, block);

        block = pass;

        TGettersList getters;
        getters.reserve(NewItems.size());
        std::transform(NewItems.cbegin(), NewItems.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, Item);
            std::for_each(NewItems.cbegin(), NewItems.cend(), std::bind(&TExpandMapWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    const TComputationNodePtrVector NewItems;
};

} // namespace

IComputationNode* WrapExpandMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    MKQL_ENSURE(callable.GetInputsCount() == width + 2U, "Expected two or more args.");
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);

    TComputationNodePtrVector newItems(width, nullptr);
    ui32 index = 1U;
    std::generate(newItems.begin(), newItems.end(), [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1U);
    return new TExpandMapWrapper(flow, itemArg, std::move(newItems));
}

} // namespace NKikimr::NMiniKQL
