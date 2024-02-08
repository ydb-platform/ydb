#include "mkql_ifpresent.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsMultiOptional>
class TIfPresentWrapper : public TMutableCodegeneratorNode<TIfPresentWrapper<IsMultiOptional>> {
using TBaseComputation = TMutableCodegeneratorNode<TIfPresentWrapper<IsMultiOptional>>;
public:
    TIfPresentWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* optional, IComputationExternalNode* item, IComputationNode* presentBranch,
        IComputationNode* missingBranch)
        : TBaseComputation(mutables, kind)
        , Optional(optional)
        , Item(item)
        , PresentBranch(presentBranch)
        , MissingBranch(missingBranch)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (const auto& previous = Item->GetValue(ctx); previous.IsInvalid()) {
            const auto optional = Optional->GetValue(ctx);
            if (optional)
                Item->SetValue(ctx, optional.GetOptionalValueIf<IsMultiOptional>());

            return (optional ? PresentBranch : MissingBranch)->GetValue(ctx).Release();
        } else {
            return (previous ? PresentBranch : MissingBranch)->GetValue(ctx).Release();
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");
        const auto previous = codegenItem->CreateGetValue(ctx, block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto pres = BasicBlock::Create(context, "pres", ctx.Func);
        const auto miss = BasicBlock::Create(context, "miss", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(previous->getType(), 2, "result", done);

        const auto choise = SwitchInst::Create(previous, fast, 2U, block);
        choise->addCase(GetEmpty(context), miss);
        choise->addCase(GetInvalid(context), slow);

        block = slow;

        const auto value = GetNodeValue(Optional, ctx, block);
        BranchInst::Create(pres, miss, IsExists(value, block), block);

        block = pres;
        codegenItem->CreateSetValue(ctx, block, IsMultiOptional ? GetOptionalValue(context, value, block) : value);
        BranchInst::Create(fast, block);

        block = fast;
        const auto left = GetNodeValue(PresentBranch, ctx, block);
        result->addIncoming(left, block);
        BranchInst::Create(done, block);

        block = miss;
        const auto right = GetNodeValue(MissingBranch, ctx, block);
        result->addIncoming(right, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Optional);
        this->DependsOn(MissingBranch);
        Optional->AddDependence(Item);
        this->Own(Item);
        this->DependsOn(PresentBranch);
    }

    IComputationNode* const Optional;
    IComputationExternalNode* const Item;
    IComputationNode* const PresentBranch;
    IComputationNode* const MissingBranch;
};

template<bool IsMultiOptional>
class TFlowIfPresentWrapper : public TStatelessFlowCodegeneratorNode<TFlowIfPresentWrapper<IsMultiOptional>> {
using TBaseComputation = TStatelessFlowCodegeneratorNode<TFlowIfPresentWrapper<IsMultiOptional>>;
public:
    TFlowIfPresentWrapper(EValueRepresentation kind, IComputationNode* optional, IComputationExternalNode* item, IComputationNode* presentBranch,
        IComputationNode* missingBranch)
        : TBaseComputation(nullptr, kind)
        , Optional(optional)
        , Item(item)
        , PresentBranch(presentBranch)
        , MissingBranch(missingBranch)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (const auto& previous = Item->GetValue(ctx); previous.IsInvalid()) {
            const auto optional = Optional->GetValue(ctx);
            if (optional)
                Item->SetValue(ctx, optional.GetOptionalValueIf<IsMultiOptional>());

            return (optional ? PresentBranch : MissingBranch)->GetValue(ctx).Release();
        } else {
            return (previous ? PresentBranch : MissingBranch)->GetValue(ctx).Release();
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");
        const auto previous = codegenItem->CreateGetValue(ctx, block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto pres = BasicBlock::Create(context, "pres", ctx.Func);
        const auto miss = BasicBlock::Create(context, "miss", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(previous->getType(), 2, "result", done);

        const auto choise = SwitchInst::Create(previous, fast, 2U, block);
        choise->addCase(GetEmpty(context), miss);
        choise->addCase(GetInvalid(context), slow);

        block = slow;

        const auto value = GetNodeValue(Optional, ctx, block);
        BranchInst::Create(pres, miss, IsExists(value, block), block);

        block = pres;
        codegenItem->CreateSetValue(ctx, block, IsMultiOptional ? GetOptionalValue(context, value, block) : value);
        BranchInst::Create(fast, block);

        block = fast;
        const auto left = GetNodeValue(PresentBranch, ctx, block);
        result->addIncoming(left, block);
        BranchInst::Create(done, block);

        block = miss;
        const auto right = GetNodeValue(MissingBranch, ctx, block);
        result->addIncoming(right, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOnBoth(PresentBranch, MissingBranch)) {
            this->DependsOn(flow, Optional);
            this->Own(flow, Item);
        }
        Optional->AddDependence(Item);
    }

    IComputationNode* const Optional;
    IComputationExternalNode* const Item;
    IComputationNode* const PresentBranch;
    IComputationNode* const MissingBranch;
};

template<bool IsMultiOptional>
class TWideIfPresentWrapper : public TStatelessWideFlowCodegeneratorNode<TWideIfPresentWrapper<IsMultiOptional>> {
using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TWideIfPresentWrapper<IsMultiOptional>>;
public:
    TWideIfPresentWrapper(IComputationNode* optional, IComputationExternalNode* item, IComputationWideFlowNode* presentBranch,
        IComputationWideFlowNode* missingBranch)
        : TBaseComputation(nullptr)
        , Optional(optional)
        , Item(item)
        , PresentBranch(presentBranch)
        , MissingBranch(missingBranch)
    {}

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (const auto& previous = Item->GetValue(ctx); previous.IsInvalid()) {
            const auto optional = Optional->GetValue(ctx);
            if (optional)
                Item->SetValue(ctx, optional.GetOptionalValueIf<IsMultiOptional>());

            return (optional ? PresentBranch : MissingBranch)->FetchValues(ctx, output);
        } else {
            return (previous ? PresentBranch : MissingBranch)->FetchValues(ctx, output);
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");
        const auto previous = codegenItem->CreateGetValue(ctx, block);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto pres = BasicBlock::Create(context, "pres", ctx.Func);
        const auto miss = BasicBlock::Create(context, "miss", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(Type::getInt32Ty(context), 2, "result", done);

        const auto choise = SwitchInst::Create(previous, pres, 2U, block);
        choise->addCase(GetEmpty(context), miss);
        choise->addCase(GetInvalid(context), init);

        block = init;

        const auto value = GetNodeValue(Optional, ctx, block);
        BranchInst::Create(good, miss, IsExists(value, block), block);

        block = good;

        codegenItem->CreateSetValue(ctx, block, IsMultiOptional ? GetOptionalValue(context, value, block) : value);

        BranchInst::Create(pres, block);

        block = pres;
        const auto left = GetNodeValues(PresentBranch, ctx, block);
        result->addIncoming(left.first, block);
        BranchInst::Create(done, block);

        block = miss;
        const auto right = GetNodeValues(MissingBranch, ctx, block);
        result->addIncoming(right.first, block);
        BranchInst::Create(done, block);

        block = done;

        MKQL_ENSURE(left.second.size() == right.second.size(), "Expected same width of flows.");
        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(left.second.size());
        size_t idx = 0U;
        std::generate_n(std::back_inserter(getters), right.second.size(), [&]() {
            const auto i = idx++;
            return [codegenItem, lget = left.second[i], rget = right.second[i]](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();

                const auto pres = BasicBlock::Create(context, "pres", ctx.Func);
                const auto miss = BasicBlock::Create(context, "miss", ctx.Func);
                const auto done = BasicBlock::Create(context, "done", ctx.Func);

                const auto current = codegenItem->CreateGetValue(ctx, block);
                const auto result = PHINode::Create(current->getType(), 2, "result", done);

                const auto choise = SwitchInst::Create(current, pres, 2U, block);
                choise->addCase(GetEmpty(context), miss);
                choise->addCase(GetInvalid(context), miss);

                block = pres;
                result->addIncoming(lget(ctx, block), block);
                BranchInst::Create(done, block);

                block = miss;
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
        if (const auto flow = this->FlowDependsOnBoth(PresentBranch, MissingBranch)) {
            this->DependsOn(flow, Optional);
            this->Own(flow, Item);
        }
        Optional->AddDependence(Item);
    }

    IComputationNode* const Optional;
    IComputationExternalNode* const Item;
    IComputationWideFlowNode* const PresentBranch;
    IComputationWideFlowNode* const MissingBranch;
};

}

IComputationNode* WrapIfPresent(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

    const auto optional = LocateNode(ctx.NodeLocator, callable, 0);
    const auto presentBranch = LocateNode(ctx.NodeLocator, callable, 2);
    const auto missingBranch = LocateNode(ctx.NodeLocator, callable, 3);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto innerType = AS_TYPE(TOptionalType, callable.GetInput(0U).GetStaticType())->GetItemType();
    const bool multiOptional = innerType->IsOptional() || innerType->IsPg();
    if (const auto type = callable.GetType()->GetReturnType(); type->IsFlow()) {
        const auto presWide = dynamic_cast<IComputationWideFlowNode*>(presentBranch);
        const auto missWide = dynamic_cast<IComputationWideFlowNode*>(missingBranch);

        if (presWide && missWide) {
            if (multiOptional)
                return new TWideIfPresentWrapper<true>(optional, itemArg, presWide, missWide);
            else
                return new TWideIfPresentWrapper<false>(optional, itemArg, presWide, missWide);
        } else if (!presWide && !missWide) {
            if (multiOptional)
                return new TFlowIfPresentWrapper<true>(GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
            else
                return new TFlowIfPresentWrapper<false>(GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
        }
    } else if (multiOptional) {
        return new TIfPresentWrapper<true>(ctx.Mutables, GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
    } else {
        return new TIfPresentWrapper<false>(ctx.Mutables, GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
    }

    THROW yexception() << "Wrong signature.";
}

}
}
