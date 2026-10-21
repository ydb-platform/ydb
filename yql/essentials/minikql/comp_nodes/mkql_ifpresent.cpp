#include "mkql_ifpresent.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

template <bool IsMultiOptional>
class TIfPresentWrapper: public TMutableCodegeneratorNode<TIfPresentWrapper<IsMultiOptional>> {
    using TBaseComputation = TMutableCodegeneratorNode<TIfPresentWrapper<IsMultiOptional>>;

public:
    TIfPresentWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* optional, IComputationExternalNode* item, IComputationNode* presentBranch,
                      IComputationNode* missingBranch)
        : TBaseComputation(mutables, kind)
        , Optional_(optional)
        , Item_(item)
        , PresentBranch_(presentBranch)
        , MissingBranch_(missingBranch)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (const auto& previous = Item_->GetValue(ctx); previous.IsInvalid()) {
            const auto optional = Optional_->GetValue(ctx);
            if (optional) {
                Item_->SetValue(ctx, optional.GetOptionalValueIf<IsMultiOptional>());
            }

            return (optional ? PresentBranch_ : MissingBranch_)->GetValue(ctx).Release();
        } else {
            return (previous ? PresentBranch_ : MissingBranch_)->GetValue(ctx).Release();
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
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

        const auto value = GetNodeValue(Optional_, ctx, block);
        BranchInst::Create(pres, miss, IsExists(value, block, context), block);

        block = pres;
        codegenItem->CreateSetValue(ctx, block, IsMultiOptional ? GetOptionalValue(context, value, block) : value);
        BranchInst::Create(fast, block);

        block = fast;
        const auto left = GetNodeValue(PresentBranch_, ctx, block);
        result->addIncoming(left, block);
        BranchInst::Create(done, block);

        block = miss;
        const auto right = GetNodeValue(MissingBranch_, ctx, block);
        result->addIncoming(right, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Optional_);
        this->DependsOn(MissingBranch_);
        Optional_->AddDependent(Item_);
        this->Own(Item_);
        this->DependsOn(PresentBranch_);
    }

    IComputationNode* const Optional_;
    IComputationExternalNode* const Item_;
    IComputationNode* const PresentBranch_;
    IComputationNode* const MissingBranch_;
};

template <bool IsMultiOptional>
class TFlowIfPresentWrapper: public TStatelessFlowCodegeneratorNode<TFlowIfPresentWrapper<IsMultiOptional>> {
    using TBaseComputation = TStatelessFlowCodegeneratorNode<TFlowIfPresentWrapper<IsMultiOptional>>;

public:
    TFlowIfPresentWrapper(EValueRepresentation kind, IComputationNode* optional, IComputationExternalNode* item, IComputationNode* presentBranch,
                          IComputationNode* missingBranch)
        : TBaseComputation(/*source=*/nullptr, kind)
        , Optional_(optional)
        , Item_(item)
        , PresentBranch_(presentBranch)
        , MissingBranch_(missingBranch)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (const auto& previous = Item_->GetValue(ctx); previous.IsInvalid()) {
            const auto optional = Optional_->GetValue(ctx);
            if (optional) {
                Item_->SetValue(ctx, optional.GetOptionalValueIf<IsMultiOptional>());
            }

            return (optional ? PresentBranch_ : MissingBranch_)->GetValue(ctx).Release();
        } else {
            return (previous ? PresentBranch_ : MissingBranch_)->GetValue(ctx).Release();
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
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

        const auto value = GetNodeValue(Optional_, ctx, block);
        BranchInst::Create(pres, miss, IsExists(value, block, context), block);

        block = pres;
        codegenItem->CreateSetValue(ctx, block, IsMultiOptional ? GetOptionalValue(context, value, block) : value);
        BranchInst::Create(fast, block);

        block = fast;
        const auto left = GetNodeValue(PresentBranch_, ctx, block);
        result->addIncoming(left, block);
        BranchInst::Create(done, block);

        block = miss;
        const auto right = GetNodeValue(MissingBranch_, ctx, block);
        result->addIncoming(right, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOnBoth(PresentBranch_, MissingBranch_)) {
            this->DependsOn(flow, Optional_);
            this->Own(flow, Item_);
        }
        Optional_->AddDependent(Item_);
    }

    IComputationNode* const Optional_;
    IComputationExternalNode* const Item_;
    IComputationNode* const PresentBranch_;
    IComputationNode* const MissingBranch_;
};

template <bool IsMultiOptional>
class TWideIfPresentWrapper: public TStatelessWideFlowCodegeneratorNode<TWideIfPresentWrapper<IsMultiOptional>> {
    using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TWideIfPresentWrapper<IsMultiOptional>>;

public:
    TWideIfPresentWrapper(IComputationNode* optional, IComputationExternalNode* item, IComputationWideFlowNode* presentBranch,
                          IComputationWideFlowNode* missingBranch)
        : TBaseComputation(nullptr)
        , Optional_(optional)
        , Item_(item)
        , PresentBranch_(presentBranch)
        , MissingBranch_(missingBranch)
    {
    }

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (const auto& previous = Item_->GetValue(ctx); previous.IsInvalid()) {
            const auto optional = Optional_->GetValue(ctx);
            if (optional) {
                Item_->SetValue(ctx, optional.GetOptionalValueIf<IsMultiOptional>());
            }

            return (optional ? PresentBranch_ : MissingBranch_)->FetchValues(ctx, output);
        } else {
            return (previous ? PresentBranch_ : MissingBranch_)->FetchValues(ctx, output);
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
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

        const auto value = GetNodeValue(Optional_, ctx, block);
        BranchInst::Create(good, miss, IsExists(value, block, context), block);

        block = good;

        codegenItem->CreateSetValue(ctx, block, IsMultiOptional ? GetOptionalValue(context, value, block) : value);

        BranchInst::Create(pres, block);

        block = pres;
        const auto left = GetNodeValues(PresentBranch_, ctx, block);
        result->addIncoming(left.first, block);
        BranchInst::Create(done, block);

        block = miss;
        const auto right = GetNodeValues(MissingBranch_, ctx, block);
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
        if (const auto flow = this->FlowDependsOnBoth(PresentBranch_, MissingBranch_)) {
            this->DependsOn(flow, Optional_);
            this->Own(flow, Item_);
        }
        Optional_->AddDependent(Item_);
    }

    IComputationNode* const Optional_;
    IComputationExternalNode* const Item_;
    IComputationWideFlowNode* const PresentBranch_;
    IComputationWideFlowNode* const MissingBranch_;
};

} // namespace

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
            if (multiOptional) {
                return new TWideIfPresentWrapper<true>(optional, itemArg, presWide, missWide);
            } else {
                return new TWideIfPresentWrapper<false>(optional, itemArg, presWide, missWide);
            }
        } else if (!presWide && !missWide) {
            if (multiOptional) {
                return new TFlowIfPresentWrapper<true>(GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
            } else {
                return new TFlowIfPresentWrapper<false>(GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
            }
        }
    } else if (multiOptional) {
        return new TIfPresentWrapper<true>(ctx.Mutables, GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
    } else {
        return new TIfPresentWrapper<false>(ctx.Mutables, GetValueRepresentation(type), optional, itemArg, presentBranch, missingBranch);
    }

    THROW yexception() << "Wrong signature.";
}

} // namespace NKikimr::NMiniKQL
