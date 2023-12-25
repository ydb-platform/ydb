#include "mkql_fold.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TFoldWrapper : public TMutableCodegeneratorRootNode<TFoldWrapper> {
    typedef TMutableCodegeneratorRootNode<TFoldWrapper> TBaseComputation;
public:
    TFoldWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* list, IComputationExternalNode* item, IComputationExternalNode* state,
        IComputationNode* newState, IComputationNode* initialState)
        : TBaseComputation(mutables, kind)
        , List(list)
        , Item(item)
        , State(state)
        , NewState(newState)
        , InitialState(initialState)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        State->SetValue(compCtx, InitialState->GetValue(compCtx));

        TThresher<false>::DoForEachItem(List->GetValue(compCtx),
            [this, &compCtx] (NUdf::TUnboxedValue&& item) {
                Item->SetValue(compCtx, std::move(item));
                State->SetValue(compCtx, NewState->GetValue(compCtx));
            }
        );

        return State->GetValue(compCtx).Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    llvm::Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto &context = ctx.Codegen.GetContext();

        const auto codegenState = dynamic_cast<ICodegeneratorExternalNode*>(State);
        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);

        MKQL_ENSURE(codegenState, "State must be codegenerator node.");
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrType = PointerType::getUnqual(valueType);

        const auto init = GetNodeValue(InitialState, ctx, block);

        codegenState->CreateSetValue(ctx, block, init);

        const auto list = GetNodeValue(List, ctx, block);

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, list, ctx.Codegen, block);

        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(slow, fast, null, block);

        {
            block = fast;
            const auto sizeType = Type::getInt64Ty(context);
            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(sizeType, list, ctx.Codegen, block);

            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto index = PHINode::Create(sizeType, 2U, "index", loop);
            index->addIncoming(ConstantInt::get(sizeType, 0), block);

            BranchInst::Create(loop, block);
            block = loop;

            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, index, size, "more", block);

            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            BranchInst::Create(good, done, more, block);

            block = good;
            const auto itemPtr = GetElementPtrInst::CreateInBounds(valueType, elements, {index}, "item_ptr", block);
            const auto item = new LoadInst(valueType, itemPtr, "item", block);

            codegenItem->CreateSetValue(ctx, block, item);

            const auto newState = GetNodeValue(NewState, ctx, block);

            codegenState->CreateSetValue(ctx, block, newState);

            const auto next = BinaryOperator::CreateAdd(index, ConstantInt::get(sizeType, 1), "next", block);
            index->addIncoming(next, block);

            BranchInst::Create(loop, block);
        }

        {
            block = slow;

            const auto iterPtr = *Stateless || ctx.AlwaysInline ?
                new AllocaInst(valueType, 0U, "iter_ptr", &ctx.Func->getEntryBlock().back()):
                new AllocaInst(valueType, 0U, "iter_ptr", block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(iterPtr, list, ctx.Codegen, block);
            const auto iter = new LoadInst(valueType, iterPtr, "iter", block);

            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);

            BranchInst::Create(loop, block);
            block = loop;

            const auto itemPtr = codegenItem->CreateRefValue(ctx, block);
            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), iter, ctx.Codegen, block, itemPtr);

            BranchInst::Create(good, stop, status, block);
            block = good;

            const auto newState = GetNodeValue(NewState, ctx, block);

            codegenState->CreateSetValue(ctx, block, newState);

            BranchInst::Create(loop, block);

            block = stop;
            UnRefBoxed(iter, ctx, block);
            BranchInst::Create(done, block);
        }

        block = done;
        if (List->IsTemporaryValue())
            CleanupBoxed(list, ctx, block);
        return codegenState->CreateGetValue(ctx, block);
    }
#endif

private:
    void RegisterDependencies() const final {
        this->DependsOn(List);
        this->DependsOn(InitialState);
        this->Own(Item);
        this->Own(State);
        this->DependsOn(NewState);
    }

    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationExternalNode* const State;
    IComputationNode* const NewState;
    IComputationNode* const InitialState;
};

}

IComputationNode* WrapFold(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsList(), "Expected List");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initialState = LocateNode(ctx.NodeLocator, callable, 1);
    const auto newState = LocateNode(ctx.NodeLocator, callable, 4);
    const auto item = LocateExternalNode(ctx.NodeLocator, callable, 2);
    const auto state = LocateExternalNode(ctx.NodeLocator, callable, 3);

    const auto kind = GetValueRepresentation(callable.GetType()->GetReturnType());

    return new TFoldWrapper(ctx.Mutables, kind, list, item, state, newState, initialState);
}

}
}
