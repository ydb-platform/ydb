#include "mkql_skip.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_simple_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TSkipFlowWrapper : public TStatefulFlowCodegeneratorNode<TSkipFlowWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TSkipFlowWrapper>;
public:
     TSkipFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationNode* count)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded), Flow(flow), Count(count)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = Count->GetValue(ctx);
        }

        if (auto count = state.Get<ui64>()) {
            do {
                const auto item = Flow->GetValue(ctx);
                if (item.IsSpecial()) {
                    state = NUdf::TUnboxedValuePod(count);
                    return item;
                }
            } while (--count);

            state = NUdf::TUnboxedValuePod::Zero();
        }

        return Flow->GetValue(ctx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(valueType, 2U, "state", main);
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
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 2U, "result", done);

        const auto trunc = GetterFor<ui64>(state, context, block);

        const auto count = PHINode::Create(trunc->getType(), 2U, "count", work);
        count->addIncoming(trunc, block);

        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, trunc, ConstantInt::get(trunc->getType(), 0ULL), "plus", block);

        BranchInst::Create(work, skip, plus, block);

        block = work;
        const auto item = GetNodeValue(Flow, ctx, block);
        BranchInst::Create(pass, good, IsSpecial(item, block), block);

        block = pass;
        result->addIncoming(item, block);
        new StoreInst(SetterFor<ui64>(count, context, block), statePtr, block);
        BranchInst::Create(done, block);

        block = good;

        ValueCleanup(Flow->GetRepresentation(), item, ctx, block);

        const auto decr = BinaryOperator::CreateSub(count, ConstantInt::get(count->getType(), 1ULL), "decr", block);
        count->addIncoming(decr, block);
        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, decr, ConstantInt::get(decr->getType(), 0ULL), "next", block);
        BranchInst::Create(work, exit, next, block);

        block = exit;
        new StoreInst(SetterFor<ui64>(decr, context, block), statePtr, block);
        BranchInst::Create(skip, block);

        block = skip;
        const auto res = GetNodeValue(Flow, ctx, block);
        result->addIncoming(res, block);
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

class TWideSkipWrapper : public TSimpleStatefulWideFlowCodegeneratorNode<TWideSkipWrapper> {
using TBaseComputation = TSimpleStatefulWideFlowCodegeneratorNode<TWideSkipWrapper>;
public:
     TWideSkipWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, ui32 size)
        : TBaseComputation(mutables, flow, size, size)
        , Flow(flow)
        , Count(count)
        , StubsIndex(mutables.IncrementWideFieldsIndex(size))
    {}

    void InitState(NUdf::TUnboxedValue& cntToSkip, TComputationContext& ctx) const {
        cntToSkip = Count->GetValue(ctx);
    }

    NUdf::TUnboxedValue*const* PrepareInput(NUdf::TUnboxedValue& cntToSkip, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
         return cntToSkip.Get<ui64>() ? ctx.WideFields.data() + StubsIndex : output;
     }

    TMaybeFetchResult DoProcess(NUdf::TUnboxedValue& cntToSkip, TComputationContext&, TMaybeFetchResult fetchRes, NUdf::TUnboxedValue*const*) const {
        if (fetchRes.Get() == EFetchResult::One && cntToSkip.Get<ui64>()) {
            cntToSkip = NUdf::TUnboxedValuePod(cntToSkip.Get<ui64>() - 1);
            return TMaybeFetchResult::None();
        }
        return fetchRes;
    }

#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult GenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto decr = BasicBlock::Create(context, "decr", ctx.Func);
        const auto end = BasicBlock::Create(context, "end", ctx.Func);

        const auto fetched = fetchGenerator(ctx, block);
        const auto cntToSkipVal = GetterFor<ui64>(new LoadInst(IntegerType::getInt128Ty(context), statePtrVal, "unboxed_state", block), context, block);
        const auto needSkipCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, cntToSkipVal, ConstantInt::get(cntToSkipVal->getType(), 0), "need_skip", block);
        const auto gotOneCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, fetched.first, ConstantInt::get(fetched.first->getType(), 1), "got_one", block);
        const auto willSkipCond = BinaryOperator::Create(Instruction::And, needSkipCond, gotOneCond, "will_skip", block);
        BranchInst::Create(decr, end, willSkipCond, block);

        block = decr;
        const auto cntToSkipNewVal = BinaryOperator::CreateSub(cntToSkipVal, ConstantInt::get(cntToSkipVal->getType(), 1), "decr", block);
        new StoreInst(SetterFor<ui64>(cntToSkipNewVal, context, block), statePtrVal, block);
        BranchInst::Create(end, block);

        block = end;
        const auto result = SelectInst::Create(willSkipCond, TMaybeFetchResult::None().LLVMConst(context), TMaybeFetchResult::LLVMFromFetchResult(fetched.first, "fetch_res_ext", block), "result", block);
        return {result, fetched.second};
    }
#endif

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow))
            DependsOn(flow, Count);
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
    const ui32 StubsIndex;
};

class TSkipStreamWrapper : public TMutableComputationNode<TSkipStreamWrapper> {
    typedef TMutableComputationNode<TSkipStreamWrapper> TBaseComputation;
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
            for (;;) {
                if (Index_ >= Count_) {
                    return Input_.Fetch(result);
                }

                auto status = Input_.Fetch(result);
                if (status != NUdf::EFetchStatus::Ok) {
                    return status;
                }

                ++Index_;
            }
        }

        const NUdf::TUnboxedValue Input_;
        const ui64 Count_;
        ui64 Index_;
    };

    TSkipStreamWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* count)
        : TBaseComputation(mutables, list->GetRepresentation())
        , List(list)
        , Count(count)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(List->GetValue(ctx), Count->GetValue(ctx).Get<ui64>());
    }

    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Count);
    }

private:
    IComputationNode* const List;
    IComputationNode* const Count;
};

class TSkipWrapper : public TMutableCodegeneratorNode<TSkipWrapper> {
    typedef TMutableCodegeneratorNode<TSkipWrapper> TBaseComputation;
public:
    TSkipWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* count)
        : TBaseComputation(mutables, list->GetRepresentation())
        , List(list)
        , Count(count)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.SkipList(ctx.Builder, List->GetValue(ctx).Release(), Count->GetValue(ctx).Get<ui64>());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto factory = ctx.GetFactory();
        const auto builder = ctx.GetBuilder();

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::SkipList));

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

IComputationNode* WrapSkip(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    const auto type = callable.GetInput(0).GetStaticType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    if (type->IsFlow()) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow))
            return new TWideSkipWrapper(ctx.Mutables, wide, count, GetWideComponentsCount(AS_TYPE(TFlowType, type)));
        else
            return new TSkipFlowWrapper(ctx.Mutables, GetValueRepresentation(type), flow, count);
    } else if (type->IsStream()) {
        return new TSkipStreamWrapper(ctx.Mutables, flow, count);
    } else if (type->IsList()) {
        return new TSkipWrapper(ctx.Mutables, flow, count);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

}
}
