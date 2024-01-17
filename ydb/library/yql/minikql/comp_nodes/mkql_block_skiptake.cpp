#include "mkql_block_skiptake.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TWideSkipBlocksWrapper : public TStatefulWideFlowCodegeneratorNode<TWideSkipBlocksWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideSkipBlocksWrapper>;
public:
    TWideSkipBlocksWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, ui32 size)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded), Flow(flow), Count(count), Width(size - 1U)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = Count->GetValue(ctx);
        }

        if (auto count = state.Get<ui64>()) while (true) {
            if (const auto result = Flow->FetchValues(ctx, output); EFetchResult::One != result) {
                state = NUdf::TUnboxedValuePod(count);
                return result;
            }

            if (const auto blockSize = GetBlockCount(*output[Width]); count < blockSize) {
                state = NUdf::TUnboxedValuePod::Zero();
                *output[Width] = MakeBlockCount(ctx.HolderFactory, blockSize - count);
                for (auto i = 0U; i < Width; ++i)
                    if (const auto out = output[i])
                        *out = SliceBlock(ctx.HolderFactory, *out, count);
                return EFetchResult::One;
            } else
                count -= blockSize;
        }

        return Flow->FetchValues(ctx, output);
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto indexType = Type::getInt64Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto offsetPtr = new AllocaInst(indexType, 0U, "offset_ptr", atTop);
        const auto sizePtr = new AllocaInst(indexType, 0U, "size_ptr", atTop);

        const auto sliceFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideSkipBlocksWrapper::SliceBlock));
        const auto sliceType = FunctionType::get(valueType, {ctx.GetFactory()->getType(), valueType, indexType}, false);
        const auto slicePtr = CastInst::Create(Instruction::IntToPtr, sliceFunc, PointerType::getUnqual(sliceType), "slice", atTop);

        const auto name = "GetBlockCount";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&GetBlockCount));
        const auto getCountType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(indexType, { valueType }, false):
            FunctionType::get(indexType, { ptrValueType }, false);
        const auto getCount = ctx.Codegen.GetModule().getOrInsertFunction(name, getCountType);

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
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 2U, "result", done);

        const auto trunc = GetterFor<ui64>(state, context, block);

        const auto count = PHINode::Create(trunc->getType(), 2U, "count", work);
        count->addIncoming(trunc, block);

        BranchInst::Create(work, block);

        block = work;

        const auto getres = GetNodeValues(Flow, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);
        BranchInst::Create(pass, good, special, block);

        block = good;

        const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, count, ConstantInt::get(indexType, 0), "more", block);
        BranchInst::Create(test, pass, more, block);


        block = test;
        const auto height = CallInst::Create(getCount, { WrapArgumentForWindows(getres.second.back()(ctx, block), ctx, block) }, "height", block);
        const auto part = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, count, height, "part", block);
        const auto decr = BinaryOperator::CreateSub(count, height, "decr", block);
        count->addIncoming(decr, block);

        BranchInst::Create(over, work, part, block);

        block = over;

        const auto tail = BinaryOperator::CreateSub(height, count, "tail", block);
        new StoreInst(count, offsetPtr, block);
        new StoreInst(tail, sizePtr, block);
        new StoreInst(GetFalse(context), statePtr, block);

        result->addIncoming(getres.first, block);

        BranchInst::Create(done, block);

        block = pass;

        new StoreInst(ConstantInt::get(indexType, 0), offsetPtr, block);
        new StoreInst(ConstantInt::get(indexType, 0), sizePtr, block);
        new StoreInst(SetterFor<ui64>(count, context, block), statePtr, block);
        result->addIncoming(getres.first, block);

        BranchInst::Create(done, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList getters(getres.second.size());
        getters.back() = [sizePtr, indexType, valueType, getSize = getres.second.back()](const TCodegenContext& ctx, BasicBlock*& block) {
            auto& context = ctx.Codegen.GetContext();
            const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
            const auto calc = BasicBlock::Create(context, "calc", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            const auto height = PHINode::Create(valueType, 2U, "state", exit);


            const auto count = new LoadInst(indexType, sizePtr, "count", block);
            const auto work = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, count, ConstantInt::get(indexType, 0), "work", block);

            BranchInst::Create(calc, pass, work, block);

            block = calc;

            const auto makeCountFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&MakeBlockCount));
            const auto makeCountType = FunctionType::get(valueType, {ctx.GetFactory()->getType(), indexType}, false);
            const auto makeCountPtr = CastInst::Create(Instruction::IntToPtr, makeCountFunc, PointerType::getUnqual(makeCountType), "make_count_func", block);
            const auto slice = CallInst::Create(makeCountType, makeCountPtr, {ctx.GetFactory(), count}, "slice", block);

            height->addIncoming(slice, block);
            BranchInst::Create(exit, block);

            block = pass;

            const auto size = getSize(ctx, block);
            height->addIncoming(size, block);
            BranchInst::Create(exit, block);

            block = exit;
            return height;
        };
        for (auto idx = 0U; idx < Width; ++idx) {
            getters[idx] = [offsetPtr, indexType, valueType, sliceType, slicePtr, getBlock = getres.second[idx]](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();

                const auto calc = BasicBlock::Create(context, "calc", ctx.Func);
                const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

                const auto output = PHINode::Create(valueType, 2U, "output", exit);

                const auto offset = new LoadInst(indexType, offsetPtr, "offset", block);
                const auto work = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, offset, ConstantInt::get(indexType, 0), "work", block);

                const auto value = getBlock(ctx, block);
                output->addIncoming(value, block);

                BranchInst::Create(calc, exit, work, block);

                block = calc;

                const auto slice = CallInst::Create(sliceType, slicePtr, {ctx.GetFactory(), value, offset}, "slice", block);

                output->addIncoming(slice, block);
                BranchInst::Create(exit, block);

                block = exit;
                return output;
            };
        }

        return {result, std::move(getters)};
    }
#endif
private:
    static NUdf::TUnboxedValuePod SliceBlock(const THolderFactory& holderFactory, NUdf::TUnboxedValuePod block, const uint64_t offset) {
        NUdf::TUnboxedValue b(block);
        auto& datum = TArrowBlock::From(b).GetDatum();
        return datum.is_scalar() ? b.Release() : holderFactory.CreateArrowBlock(DeepSlice(datum.array(), offset, datum.array()->length - offset));
    }

    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Count);
        }
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
    const ui32 Width;
};

class TWideTakeBlocksWrapper : public TStatefulWideFlowCodegeneratorNode<TWideTakeBlocksWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideTakeBlocksWrapper>;
public:
    TWideTakeBlocksWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, ui32 size)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded), Flow(flow), Count(count), Width(size - 1U)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = Count->GetValue(ctx);
        }

        if (const auto count = state.Get<ui64>()) {
            if (const auto result = Flow->FetchValues(ctx, output); EFetchResult::One == result) {
                if (const auto blockSize = GetBlockCount(*output[Width]); count < blockSize) {
                    state = NUdf::TUnboxedValuePod::Zero();
                    *output[Width] = MakeBlockCount(ctx.HolderFactory, count);
                    for (auto i = 0U; i < Width; ++i)
                        if (const auto out = output[i])
                            *out = SliceBlock(ctx.HolderFactory, *out, count);
                } else
                    state = NUdf::TUnboxedValuePod(ui64(count - blockSize));
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

        const auto indexType = Type::getInt64Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto sizePtr = new AllocaInst(indexType, 0U, "size_ptr", atTop);
        new StoreInst(ConstantInt::get(indexType, 0), sizePtr, atTop);

        const auto sliceFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideTakeBlocksWrapper::SliceBlock));
        const auto sliceType = FunctionType::get(valueType, {ctx.GetFactory()->getType(), valueType, indexType}, false);
        const auto slicePtr = CastInst::Create(Instruction::IntToPtr, sliceFunc, PointerType::getUnqual(sliceType), "slice", atTop);

        const auto name = "GetBlockCount";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&GetBlockCount));
        const auto getCountType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(indexType, { valueType }, false):
            FunctionType::get(indexType, { ptrValueType }, false);
        const auto getCount = ctx.Codegen.GetModule().getOrInsertFunction(name, getCountType);

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
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 3U, "result", done);
        result->addIncoming(ConstantInt::get(resultType, static_cast<i32>(EFetchResult::Finish)), block);

        const auto count = GetterFor<ui64>(state, context, block);
        const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, count, ConstantInt::get(count->getType(), 0ULL), "plus", block);

        BranchInst::Create(work, done, plus, block);

        block = work;

        const auto getres = GetNodeValues(Flow, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);
        result->addIncoming(getres.first, block);

        BranchInst::Create(done, good, special, block);

        block = good;

        const auto height = CallInst::Create(getCount, { WrapArgumentForWindows(getres.second.back()(ctx, block), ctx, block) }, "height", block);
        const auto part = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, count, height, "part", block);
        const auto decr = BinaryOperator::CreateSub(count, height, "decr", block);

        const auto next = SelectInst::Create(part, ConstantInt::get(indexType, 0), decr, "next", block);
        const auto size = SelectInst::Create(part, count, ConstantInt::get(indexType, 0), "size", block);

        new StoreInst(SetterFor<ui64>(next, context, block), statePtr, block);
        new StoreInst(size, sizePtr, block);

        result->addIncoming(getres.first, block);

        BranchInst::Create(done, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList getters(getres.second.size());
        getters.back() = [sizePtr, indexType, valueType, getSize = getres.second.back()](const TCodegenContext& ctx, BasicBlock*& block) {
            auto& context = ctx.Codegen.GetContext();
            const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
            const auto calc = BasicBlock::Create(context, "calc", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            const auto height = PHINode::Create(valueType, 2U, "state", exit);

            const auto count = new LoadInst(indexType, sizePtr, "count", block);
            const auto work = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, count, ConstantInt::get(indexType, 0), "work", block);

            BranchInst::Create(calc, pass, work, block);

            block = calc;

            const auto makeCountFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&MakeBlockCount));
            const auto makeCountType = FunctionType::get(valueType, {ctx.GetFactory()->getType(), indexType}, false);
            const auto makeCountPtr = CastInst::Create(Instruction::IntToPtr, makeCountFunc, PointerType::getUnqual(makeCountType), "make_count_func", block);
            const auto slice = CallInst::Create(makeCountType, makeCountPtr, {ctx.GetFactory(), count}, "slice", block);

            height->addIncoming(slice, block);
            BranchInst::Create(exit, block);

            block = pass;

            const auto size = getSize(ctx, block);
            height->addIncoming(size, block);
            BranchInst::Create(exit, block);

            block = exit;
            return height;
        };
        for (auto idx = 0U; idx < Width; ++idx) {
            getters[idx] = [sizePtr, indexType, valueType, sliceType, slicePtr, getBlock = getres.second[idx]](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();

                const auto calc = BasicBlock::Create(context, "calc", ctx.Func);
                const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

                const auto output = PHINode::Create(valueType, 2U, "output", exit);

                const auto size = new LoadInst(indexType, sizePtr, "size", block);
                const auto work = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(indexType, 0), "work", block);

                const auto value = getBlock(ctx, block);
                output->addIncoming(value, block);

                BranchInst::Create(calc, exit, work, block);

                block = calc;

                const auto slice = CallInst::Create(sliceType, slicePtr, {ctx.GetFactory(), value, size}, "slice", block);

                output->addIncoming(slice, block);
                BranchInst::Create(exit, block);

                block = exit;
                return output;
            };
        }

        return {result, std::move(getters)};
    }
#endif
private:
    static NUdf::TUnboxedValuePod SliceBlock(const THolderFactory& holderFactory, NUdf::TUnboxedValuePod block, const uint64_t offset) {
        NUdf::TUnboxedValue b(block);
        auto& datum = TArrowBlock::From(b).GetDatum();
        return datum.is_scalar() ? b.Release() : holderFactory.CreateArrowBlock(DeepSlice(datum.array(), 0ULL, offset));
    }

    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Count);
        }
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
    const ui32 Width;
};

IComputationNode* WrapSkipTake(bool skip, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto flowWidth = GetWideComponentsCount(flowType);
    MKQL_ENSURE(flowWidth > 0, "Expected at least one column");

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
    MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    if (skip) {
        return new TWideSkipBlocksWrapper(ctx.Mutables, wideFlow, count, flowWidth);
    }
    return new TWideTakeBlocksWrapper(ctx.Mutables, wideFlow, count, flowWidth);
}

} //namespace

IComputationNode* WrapWideSkipBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    bool skip = true;
    return WrapSkipTake(skip, callable, ctx);
}

IComputationNode* WrapWideTakeBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    bool skip = false;
    return WrapSkipTake(skip, callable, ctx);
}

}
}
