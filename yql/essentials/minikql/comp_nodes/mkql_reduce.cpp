#include "mkql_reduce.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

template <bool IsStream>
class TReduceWrapper: public TMutableCodegeneratorRootNode<TReduceWrapper<IsStream>> {
    using TBaseComputation = TMutableCodegeneratorRootNode<TReduceWrapper<IsStream>>;

public:
    TReduceWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* list, IComputationExternalNode* item, IComputationExternalNode* state1,
                   IComputationNode* newState1, IComputationNode* newState2,
                   IComputationNode* initialState1, IComputationExternalNode* itemState2, IComputationExternalNode* state3,
                   IComputationNode* newState3, IComputationNode* initialState3)
        : TBaseComputation(mutables, kind)
        , List_(list)
        , Item_(item)
        , State1_(state1)
        , NewState1_(newState1)
        , NewState2_(newState2)
        , InitialState1_(initialState1)
        , ItemState2_(itemState2)
        , State3_(state3)
        , NewState3_(newState3)
        , InitialState3_(initialState3)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        State1_->SetValue(compCtx, InitialState1_->GetValue(compCtx));
        State3_->SetValue(compCtx, InitialState3_->GetValue(compCtx));

        TThresher<IsStream>::DoForEachItem(List_->GetValue(compCtx),
                                           [this, &compCtx](NUdf::TUnboxedValue&& item) {
                                               Item_->SetValue(compCtx, std::move(item));
                                               State1_->SetValue(compCtx, NewState1_->GetValue(compCtx));
                                           });

        ItemState2_->SetValue(compCtx, NewState2_->GetValue(compCtx));
        return NewState3_->GetValue(compCtx).Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
        const auto codegenState1 = dynamic_cast<ICodegeneratorExternalNode*>(State1_);
        const auto codegenItemState2 = dynamic_cast<ICodegeneratorExternalNode*>(ItemState2_);
        const auto codegenState3 = dynamic_cast<ICodegeneratorExternalNode*>(State3_);

        MKQL_ENSURE(codegenState1, "State1 must be codegenerator node.");
        MKQL_ENSURE(codegenState3, "State3 must be codegenerator node.");
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");
        MKQL_ENSURE(codegenItemState2, "ItemState2 must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);

        const auto init1 = GetNodeValue(InitialState1_, ctx, block);

        codegenState1->CreateSetValue(ctx, block, init1);

        const auto init3 = GetNodeValue(InitialState3_, ctx, block);

        codegenState3->CreateSetValue(ctx, block, init3);

        const auto list = GetNodeValue(List_, ctx, block);

        const auto itemPtr = *this->Stateless_ || ctx.AlwaysInline ? new AllocaInst(valueType, 0U, "item_ptr", &ctx.Func->getEntryBlock().back()) : new AllocaInst(valueType, 0U, "item_ptr", block);
        new StoreInst(ConstantInt::get(valueType, 0), itemPtr, block);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        if constexpr (IsStream) {
            BranchInst::Create(loop, block);
            block = loop;

            const auto status = CallBoxedValueFetch(list, ctx, block, itemPtr);

            const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block);

            BranchInst::Create(done, good, icmp, block);
            block = good;

            codegenItem->CreateSetValue(ctx, block, itemPtr);

            const auto newState1 = GetNodeValue(NewState1_, ctx, block);

            codegenState1->CreateSetValue(ctx, block, newState1);

            BranchInst::Create(loop, block);

            block = done;
        } else {
            const auto iterPtr = *this->Stateless_ || ctx.AlwaysInline ? new AllocaInst(valueType, 0U, "iter_ptr", &ctx.Func->getEntryBlock().back()) : new AllocaInst(valueType, 0U, "iter_ptr", block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(iterPtr, list, ctx.Codegen, block);
            const auto iter = new LoadInst(valueType, iterPtr, "iter", block);

            BranchInst::Create(loop, block);
            block = loop;

            const auto status = CallBoxedValueNext(iter, ctx, block, itemPtr);
            const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::getFalse(context), "cond", block);

            BranchInst::Create(done, good, icmp, block);
            block = good;

            codegenItem->CreateSetValue(ctx, block, itemPtr);

            const auto newState1 = GetNodeValue(NewState1_, ctx, block);

            codegenState1->CreateSetValue(ctx, block, newState1);

            BranchInst::Create(loop, block);

            block = done;
            UnRefBoxed(iter, ctx, block);
        }

        const auto newState2 = GetNodeValue(NewState2_, ctx, block);

        codegenItemState2->CreateSetValue(ctx, block, newState2);

        const auto newState3 = GetNodeValue(NewState3_, ctx, block);

        return newState3;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(List_);
        this->DependsOn(InitialState1_);
        this->DependsOn(InitialState3_);
        this->DependsOn(NewState1_);
        this->DependsOn(NewState2_);
        this->DependsOn(NewState3_);
        this->Own(Item_);
        this->Own(State1_);
        this->Own(ItemState2_);
        this->Own(State3_);
    }

    IComputationNode* const List_;
    IComputationExternalNode* const Item_;
    IComputationExternalNode* const State1_;
    IComputationNode* const NewState1_;
    IComputationNode* const NewState2_;
    IComputationNode* const InitialState1_;
    IComputationExternalNode* const ItemState2_;
    IComputationExternalNode* const State3_;
    IComputationNode* const NewState3_;
    IComputationNode* const InitialState3_;
};

} // namespace

IComputationNode* WrapReduce(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 10, "Expected 10 args");

    auto list = LocateNode(ctx.NodeLocator, callable, 0);
    auto initialState1 = LocateNode(ctx.NodeLocator, callable, 1);
    auto initialState3 = LocateNode(ctx.NodeLocator, callable, 2);
    auto newState1 = LocateNode(ctx.NodeLocator, callable, 5);
    auto newState2 = LocateNode(ctx.NodeLocator, callable, 6);
    auto newState3 = LocateNode(ctx.NodeLocator, callable, 9);
    auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 3);
    auto state1NodeArg = LocateExternalNode(ctx.NodeLocator, callable, 4);
    auto itemState2Arg = LocateExternalNode(ctx.NodeLocator, callable, 7);
    auto state3NodeArg = LocateExternalNode(ctx.NodeLocator, callable, 8);
    const auto kind = GetValueRepresentation(callable.GetType()->GetReturnType());
    if (callable.GetInput(0).GetStaticType()->IsStream()) {
        return new TReduceWrapper<true>(ctx.Mutables, kind, list, itemArg, state1NodeArg, newState1, newState2,
                                        initialState1, itemState2Arg, state3NodeArg, newState3, initialState3);
    } else {
        return new TReduceWrapper<false>(ctx.Mutables, kind, list, itemArg, state1NodeArg, newState1, newState2,
                                         initialState1, itemState2Arg, state3NodeArg, newState3, initialState3);
    }
}

} // namespace NKikimr::NMiniKQL
