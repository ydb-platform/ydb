#include "mkql_skip.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TSkipFlowWrapper: public TStatefulFlowCodegeneratorNode<TSkipFlowWrapper> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TSkipFlowWrapper>;

public:
    TSkipFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationNode* count)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded)
        , Flow(flow)
        , Count(count)
    {
    }

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
        BranchInst::Create(init, main, IsInvalid(load, block, context), block);

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
        BranchInst::Create(pass, good, IsSpecial(item, block, context), block);

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
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Count);
        }
    }

    IComputationNode* const Flow;
    IComputationNode* const Count;
};

class TWideSkipWrapper: public TStatefulWideFlowCodegeneratorNode<TWideSkipWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideSkipWrapper>;

public:
    TWideSkipWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, ui32 size)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , Flow(flow)
        , Count(count)
        , StubsIndex(mutables.IncrementWideFieldsIndex(size))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            state = Count->GetValue(ctx);
        }

        if (auto count = state.Get<ui64>()) {
            do {
                if (const auto result = Flow->FetchValues(ctx, ctx.WideFields.data() + StubsIndex); EFetchResult::One != result) {
                    state = NUdf::TUnboxedValuePod(count);
                    return result;
                }
            } while (--count);

            state = NUdf::TUnboxedValuePod::Zero();
        }

        return Flow->FetchValues(ctx, output);
    }

#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(valueType, 2U, "state", main);
        state->addIncoming(load, block);
        BranchInst::Create(init, main, IsInvalid(load, block, context), block);

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

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 2U, "result", done);

        const auto trunc = GetterFor<ui64>(state, context, block);

        const auto count = PHINode::Create(trunc->getType(), 2U, "count", work);
        count->addIncoming(trunc, block);

        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, trunc, ConstantInt::get(trunc->getType(), 0ULL), "plus", block);

        BranchInst::Create(work, skip, plus, block);

        block = work;
        const auto status = GetNodeValues(Flow, ctx, block).first;
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, status, ConstantInt::get(status->getType(), 0), "special", block);
        BranchInst::Create(pass, good, special, block);

        block = pass;
        new StoreInst(SetterFor<ui64>(count, context, block), statePtr, block);
        result->addIncoming(status, block);
        BranchInst::Create(done, block);

        block = good;

        const auto decr = BinaryOperator::CreateSub(count, ConstantInt::get(count->getType(), 1ULL), "decr", block);
        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, decr, ConstantInt::get(decr->getType(), 0ULL), "next", block);
        count->addIncoming(decr, block);
        BranchInst::Create(work, exit, next, block);

        block = exit;
        new StoreInst(SetterFor<ui64>(decr, context, block), statePtr, block);
        BranchInst::Create(skip, block);

        block = skip;
        auto getres = GetNodeValues(Flow, ctx, block);
        result->addIncoming(getres.first, block);
        BranchInst::Create(done, block);

        block = done;
        return {result, std::move(getres.second)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Count);
        }
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
    const ui32 StubsIndex;
};

class TSkipStreamWrapper: public TMutableComputationNode<TSkipStreamWrapper> {
    typedef TMutableComputationNode<TSkipStreamWrapper> TBaseComputation;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, ui64 count)
            : TBase(memInfo)
            , Input_(std::move(input))
            , Count_(count)
            , Index_(0)
        {
        }

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

class TSkipWrapper: public TMutableCodegeneratorNode<TSkipWrapper> {
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

        const auto list = GetNodeValue(List, ctx, block);
        const auto cnt = GetNodeValue(Count, ctx, block);
        const auto count = GetterFor<ui64>(cnt, context, block);

        return EmitFunctionCall<&THolderFactory::SkipList>(list->getType(), {factory, builder, list, count}, ctx, block);
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

} // namespace

IComputationNode* WrapSkip(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    const auto type = callable.GetInput(0).GetStaticType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    if (type->IsFlow()) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
            return new TWideSkipWrapper(ctx.Mutables, wide, count, GetWideComponentsCount(AS_TYPE(TFlowType, type)));
        } else {
            return new TSkipFlowWrapper(ctx.Mutables, GetValueRepresentation(type), flow, count);
        }
    } else if (type->IsStream()) {
        return new TSkipStreamWrapper(ctx.Mutables, flow, count);
    } else if (type->IsList()) {
        return new TSkipWrapper(ctx.Mutables, flow, count);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

} // namespace NMiniKQL
} // namespace NKikimr
