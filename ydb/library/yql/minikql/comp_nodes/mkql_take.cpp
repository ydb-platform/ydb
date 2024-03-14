#include "mkql_take.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TTakeFlowWrapper : public TStatefulFlowCodegeneratorNode<TTakeFlowWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TTakeFlowWrapper>;
public:
     TTakeFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationNode* count)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded), Flow(flow), Count(count)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = Count->GetValue(ctx);
        }

        if (auto count = state.Get<ui64>()) {
            const auto item = Flow->GetValue(ctx);
            if (!(item.IsSpecial())) {
                state = NUdf::TUnboxedValuePod(--count);
            }
            return item;
        }

        return NUdf::TUnboxedValuePod::MakeFinish();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(load->getType(), 2U, "state", main);
        state->addIncoming(load, block);

        BranchInst::Create(init, main, IsInvalid(load, block), block);

        block = init;

        GetNodeValue(statePtr, Count, ctx, block);
        const auto save = new LoadInst(valueType, statePtr, "save", block);
        state->addIncoming(save, block);
        BranchInst::Create(main, block);

        block = main;

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", done);
        result->addIncoming(GetFinish(context), block);

        const auto trunc = GetterFor<ui64>(state, context, block);

        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, trunc, ConstantInt::get(trunc->getType(), 0ULL), "plus", block);

        BranchInst::Create(work, done, plus, block);

        block = work;
        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(item, block);
        BranchInst::Create(done, good, IsSpecial(item, block), block);

        block = good;
        result->addIncoming(item, block);

        const auto decr = BinaryOperator::CreateSub(trunc, ConstantInt::get(trunc->getType(), 1ULL), "decr", block);
        new StoreInst(SetterFor<ui64>(decr, context, block), statePtr, block);

        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow))
            DependsOn(flow, Count);
    }

    IComputationNode* const Flow;
    IComputationNode* const Count;
};

class TWideTakeWrapper : public TStatefulWideFlowCodegeneratorNode<TWideTakeWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideTakeWrapper>;
public:
     TWideTakeWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded), Flow(flow), Count(count)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = Count->GetValue(ctx);
        }

        if (auto count = state.Get<ui64>()) {
            if (const auto result = Flow->FetchValues(ctx, output); EFetchResult::One == result) {
                state = NUdf::TUnboxedValuePod(--count);
                return EFetchResult::One;
            } else {
                return result;
            }
        }

        return EFetchResult::Finish;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(load->getType(), 2U, "state", main);
        state->addIncoming(load, block);

        BranchInst::Create(init, main, IsInvalid(load, block), block);

        block = init;

        GetNodeValue(statePtr, Count, ctx, block);
        const auto save = new LoadInst(valueType, statePtr, "save", block);
        state->addIncoming(save, block);
        BranchInst::Create(main, block);

        block = main;

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 3U, "result", done);
        result->addIncoming(ConstantInt::get(resultType, static_cast<i32>(EFetchResult::Finish)), block);

        const auto trunc = GetterFor<ui64>(state, context, block);

        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, trunc, ConstantInt::get(trunc->getType(), 0ULL), "plus", block);

        BranchInst::Create(work, done, plus, block);

        block = work;
        const auto getres = GetNodeValues(Flow, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), 0), "special", block);
        result->addIncoming(getres.first, block);
        BranchInst::Create(done, good, special, block);

        block = good;

        const auto decr = BinaryOperator::CreateSub(trunc, ConstantInt::get(trunc->getType(), 1ULL), "decr", block);
        new StoreInst(SetterFor<ui64>(decr, context, block), statePtr, block);
        result->addIncoming(getres.first, block);
        BranchInst::Create(done, block);

        block = done;
        return {result, std::move(getres.second)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow))
            DependsOn(flow, Count);
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
};

class TTakeStreamWrapper : public TMutableComputationNode<TTakeStreamWrapper> {
    typedef TMutableComputationNode<TTakeStreamWrapper> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, ui64 count)
            : TBase(memInfo)
            , Input_(std::move(input))
            , Count_(count)
            , Index_(0)
        {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index_ >= Count_) {
                return NUdf::EFetchStatus::Finish;
            }

            const auto status = Input_.Fetch(result);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            ++Index_;
            return status;
        }

        const NUdf::TUnboxedValue Input_;
        const ui64 Count_;
        ui64 Index_;
    };

    TTakeStreamWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* count)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , List(list)
        , Count(count)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(List->GetValue(ctx), Count->GetValue(ctx).Get<ui64>());
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Count);
    }

    IComputationNode* const List;
    IComputationNode* const Count;
};

class TTakeWrapper : public TMutableCodegeneratorNode<TTakeWrapper> {
    typedef TMutableCodegeneratorNode<TTakeWrapper> TBaseComputation;
public:
    TTakeWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* count)
        : TBaseComputation(mutables, list->GetRepresentation())
        , List(list)
        , Count(count)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.TakeList(ctx.Builder, List->GetValue(ctx).Release(), Count->GetValue(ctx).Get<ui64>());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto factory = ctx.GetFactory();
        const auto builder = ctx.GetBuilder();

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::TakeList));

        const auto list = GetNodeValue(List, ctx, block);
        const auto cnt = GetNodeValue(Count, ctx, block);
        const auto count = GetterFor<ui64>(cnt, context, block);

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(list->getType(), {factory->getType(), builder->getType(), list->getType(), count->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            const auto result = CallInst::Create(funType, funcPtr, {factory, builder, list, count}, "result", block);
            return result;
        } else {
            const auto retPtr = new AllocaInst(list->getType(), 0U, "ret_ptr", block);
            new StoreInst(list, retPtr, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), builder->getType(), retPtr->getType(), count->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {factory, retPtr, builder, retPtr, count}, "", block);
            const auto result = new LoadInst(list->getType(), retPtr, "result", block);
            return result;
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Count);
    }

    IComputationNode* const List;
    IComputationNode* const Count;
};

}

IComputationNode* WrapTake(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    const auto type = callable.GetInput(0).GetStaticType();

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    if (type->IsFlow()) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow))
            return new TWideTakeWrapper(ctx.Mutables, wide, count);
        else
            return new TTakeFlowWrapper(ctx.Mutables, GetValueRepresentation(type), flow, count);
    } else if (type->IsStream()) {
        return new TTakeStreamWrapper(ctx.Mutables, flow, count);
    } else if (type->IsList()) {
        return new TTakeWrapper(ctx.Mutables, flow, count);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

}
}
