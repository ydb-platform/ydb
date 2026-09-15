#include "mkql_fold1.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE

namespace NKikimr::NMiniKQL {

namespace {

class TFold1Wrapper: public TMutableCodegeneratorRootNode<TFold1Wrapper> {
    using TBaseComputation = TMutableCodegeneratorRootNode<TFold1Wrapper>;

public:
    TFold1Wrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* list, IComputationExternalNode* item, IComputationExternalNode* state,
                  IComputationNode* newState, IComputationNode* initialState)
        : TBaseComputation(mutables, kind)
        , List_(list)
        , Item_(item)
        , State_(state)
        , NewState_(newState)
        , InitialState_(initialState)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        ui64 length = 0ULL;

        TThresher<false>::DoForEachItem(List_->GetValue(compCtx),
                                        [this, &length, &compCtx](NUdf::TUnboxedValue&& item) {
                                            Item_->SetValue(compCtx, std::move(item));
                                            State_->SetValue(compCtx, (length++ ? NewState_ : InitialState_)->GetValue(compCtx));
                                        });

        return length ? State_->GetValue(compCtx).Release().MakeOptional() : NUdf::TUnboxedValuePod();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenState = dynamic_cast<ICodegeneratorExternalNode*>(State_);
        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);

        MKQL_ENSURE(codegenState, "State must be codegenerator node.");
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrType = PointerType::getUnqual(valueType);

        const auto list = GetNodeValue(List_, ctx, block);

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, list, ctx.Codegen, block);

        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto result = PHINode::Create(valueType, 3, "result", exit);

        BranchInst::Create(slow, fast, null, block);

        {
            block = fast;
            const auto sizeType = Type::getInt64Ty(context);
            const auto nil = ConstantInt::get(sizeType, 0);
            const auto one = ConstantInt::get(sizeType, 1);

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(sizeType, list, ctx.Codegen, block);

            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto index = PHINode::Create(sizeType, 2, "index", loop);

            const auto more1 = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, nil, size, "more1", block);
            result->addIncoming(ConstantInt::get(valueType, 0), block);

            BranchInst::Create(next, exit, more1, block);

            block = next;

            const auto item1Ptr = GetElementPtrInst::CreateInBounds(valueType, elements, {nil}, "item1_ptr", block);
            const auto item1 = new LoadInst(valueType, item1Ptr, "item1", block);
            codegenItem->CreateSetValue(ctx, block, item1);

            const auto init = GetNodeValue(InitialState_, ctx, block);

            codegenState->CreateSetValue(ctx, block, init);

            index->addIncoming(one, block);

            BranchInst::Create(loop, block);
            block = loop;

            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, index, size, "more", block);

            BranchInst::Create(good, done, more, block);

            block = good;

            const auto itemPtr = GetElementPtrInst::CreateInBounds(valueType, elements, {index}, "item_ptr", block);
            const auto item = new LoadInst(valueType, itemPtr, "item", block);

            codegenItem->CreateSetValue(ctx, block, item);

            const auto newState = GetNodeValue(NewState_, ctx, block);

            codegenState->CreateSetValue(ctx, block, newState);

            const auto plus = BinaryOperator::CreateAdd(index, one, "plus", block);
            index->addIncoming(plus, block);

            BranchInst::Create(loop, block);

            block = done;

            const auto res = codegenState->CreateGetValue(ctx, block);
            const auto opt = MakeOptional(context, res, block);
            result->addIncoming(opt, block);
            BranchInst::Create(exit, block);
        }

        {
            block = slow;

            const auto iterPtr = *Stateless_ || ctx.AlwaysInline ? new AllocaInst(valueType, 0U, "iter_ptr", &ctx.Func->getEntryBlock().back()) : new AllocaInst(valueType, 0U, "iter_ptr", block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(iterPtr, list, ctx.Codegen, block);
            const auto iter = new LoadInst(valueType, iterPtr, "iter", block);

            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto step = PHINode::Create(valueType, 2, "step", stop);

            const auto [status1, item1Ptr] = RefValueWithCallResult(codegenItem, ctx, block, [&](Value* itemPtr) {
                return CallBoxedValueNext(iter, ctx, block, itemPtr);
            });

            step->addIncoming(ConstantInt::get(valueType, 0), block);
            BranchInst::Create(next, stop, status1, block);

            block = next;

            const auto init = GetNodeValue(InitialState_, ctx, block);

            codegenState->CreateSetValue(ctx, block, init);

            BranchInst::Create(loop, block);
            block = loop;

            const auto [status, itemPtr] = RefValueWithCallResult(codegenItem, ctx, block, [&](Value* itemPtr) {
                return CallBoxedValueNext(iter, ctx, block, itemPtr);
            });

            BranchInst::Create(good, done, status, block);
            block = good;

            const auto newState = GetNodeValue(NewState_, ctx, block);

            codegenState->CreateSetValue(ctx, block, newState);

            BranchInst::Create(loop, block);

            block = done;

            const auto res = codegenState->CreateGetValue(ctx, block);
            const auto opt = MakeOptional(context, res, block);
            step->addIncoming(opt, block);
            BranchInst::Create(stop, block);

            block = stop;

            UnRefBoxed(iter, ctx, block);
            result->addIncoming(step, block);
            BranchInst::Create(exit, block);
        }

        block = exit;
        if (List_->IsTemporaryValue()) {
            CleanupBoxed(list, ctx, block);
        }
        return result;
    }
#endif

private:
    void RegisterDependencies() const final {
        this->DependsOn(List_);
        this->DependsOn(InitialState_);
        this->Own(Item_);
        this->Own(State_);
        this->DependsOn(NewState_);
    }

    IComputationNode* const List_;
    IComputationExternalNode* const Item_;
    IComputationExternalNode* const State_;
    IComputationNode* const NewState_;
    IComputationNode* const InitialState_;
};

} // namespace

IComputationNode* WrapFold1(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");
    MKQL_ENSURE(callable.GetInput(0).GetStaticType()->IsList(), "Expected List");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initialState = LocateNode(ctx.NodeLocator, callable, 2);
    const auto newState = LocateNode(ctx.NodeLocator, callable, 4);
    const auto item = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto state = LocateExternalNode(ctx.NodeLocator, callable, 3);

    const auto kind = GetValueRepresentation(callable.GetType()->GetReturnType());

    return new TFold1Wrapper(ctx.Mutables, kind, list, item, state, newState, initialState);
}

} // namespace NKikimr::NMiniKQL
