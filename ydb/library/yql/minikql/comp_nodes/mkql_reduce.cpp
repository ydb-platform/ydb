#include "mkql_reduce.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsStream>
class TReduceWrapper : public TMutableCodegeneratorRootNode<TReduceWrapper<IsStream>> {
    typedef TMutableCodegeneratorRootNode<TReduceWrapper<IsStream>> TBaseComputation;
public:
    TReduceWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* list, IComputationExternalNode* item, IComputationExternalNode* state1,
        IComputationNode* newState1, IComputationNode* newState2,
        IComputationNode* initialState1, IComputationExternalNode* itemState2, IComputationExternalNode* state3,
        IComputationNode* newState3, IComputationNode* initialState3)
        : TBaseComputation(mutables, kind)
        , List(list)
        , Item(item)
        , State1(state1)
        , NewState1(newState1)
        , NewState2(newState2)
        , InitialState1(initialState1)
        , ItemState2(itemState2)
        , State3(state3)
        , NewState3(newState3)
        , InitialState3(initialState3)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        State1->SetValue(compCtx, InitialState1->GetValue(compCtx));
        State3->SetValue(compCtx, InitialState3->GetValue(compCtx));

        TThresher<IsStream>::DoForEachItem(List->GetValue(compCtx),
            [this, &compCtx] (NUdf::TUnboxedValue&& item) {
                Item->SetValue(compCtx, std::move(item));
                State1->SetValue(compCtx, NewState1->GetValue(compCtx));
            }
        );

        ItemState2->SetValue(compCtx, NewState2->GetValue(compCtx));
        return NewState3->GetValue(compCtx).Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto &context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        const auto codegenState1 = dynamic_cast<ICodegeneratorExternalNode*>(State1);
        const auto codegenItemState2 = dynamic_cast<ICodegeneratorExternalNode*>(ItemState2);
        const auto codegenState3 = dynamic_cast<ICodegeneratorExternalNode*>(State3);

        MKQL_ENSURE(codegenState1, "State1 must be codegenerator node.");
        MKQL_ENSURE(codegenState3, "State3 must be codegenerator node.");
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");
        MKQL_ENSURE(codegenItemState2, "ItemState2 must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);

        const auto init1 = GetNodeValue(InitialState1, ctx, block);

        codegenState1->CreateSetValue(ctx, block, init1);

        const auto init3 = GetNodeValue(InitialState3, ctx, block);

        codegenState3->CreateSetValue(ctx, block, init3);

        const auto list = GetNodeValue(List, ctx, block);

        const auto itemPtr = *this->Stateless || ctx.AlwaysInline ?
            new AllocaInst(valueType, 0U, "item_ptr", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(valueType, 0U, "item_ptr", block);
        new StoreInst(ConstantInt::get(valueType, 0), itemPtr, block);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        if constexpr (IsStream) {
            BranchInst::Create(loop, block);
            block = loop;

            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(Type::getInt32Ty(context), list, ctx.Codegen, block, itemPtr);

            const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block);

            BranchInst::Create(done, good, icmp, block);
            block = good;

            codegenItem->CreateSetValue(ctx, block, itemPtr);

            const auto newState1 = GetNodeValue(NewState1, ctx, block);

            codegenState1->CreateSetValue(ctx, block, newState1);

            BranchInst::Create(loop, block);

            block = done;
        } else {
            const auto iterPtr = *this->Stateless || ctx.AlwaysInline ?
                new AllocaInst(valueType, 0U, "iter_ptr", &ctx.Func->getEntryBlock().back()):
                new AllocaInst(valueType, 0U, "iter_ptr", block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(iterPtr, list, ctx.Codegen, block);
            const auto iter = new LoadInst(valueType, iterPtr, "iter", block);

            BranchInst::Create(loop, block);
            block = loop;

            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), iter, ctx.Codegen, block, itemPtr);
            const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::getFalse(context), "cond", block);

            BranchInst::Create(done, good, icmp, block);
            block = good;

            codegenItem->CreateSetValue(ctx, block, itemPtr);

            const auto newState1 = GetNodeValue(NewState1, ctx, block);

            codegenState1->CreateSetValue(ctx, block, newState1);

            BranchInst::Create(loop, block);

            block = done;
            UnRefBoxed(iter, ctx, block);
        }

        const auto newState2 = GetNodeValue(NewState2, ctx, block);

        codegenItemState2->CreateSetValue(ctx, block, newState2);

        const auto newState3 = GetNodeValue(NewState3, ctx, block);

        return newState3;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(List);
        this->DependsOn(InitialState1);
        this->DependsOn(InitialState3);
        this->DependsOn(NewState1);
        this->DependsOn(NewState2);
        this->DependsOn(NewState3);
        this->Own(Item);
        this->Own(State1);
        this->Own(ItemState2);
        this->Own(State3);
    }

    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationExternalNode* const State1;
    IComputationNode* const NewState1;
    IComputationNode* const NewState2;
    IComputationNode* const InitialState1;
    IComputationExternalNode* const ItemState2;
    IComputationExternalNode* const State3;
    IComputationNode* const NewState3;
    IComputationNode* const InitialState3;
};

}

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

}
}
