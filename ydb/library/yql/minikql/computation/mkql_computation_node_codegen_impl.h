#pragma once

#include "mkql_computation_node_codegen.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

template<typename TDerived, typename TState, EValueRepresentation StateKind = EValueRepresentation::Embedded>
class TSimpleStatefulWideFlowCodegeneratorNode
        : public TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode<TDerived, TState, StateKind>> {
    using TBase = TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode>;

protected:
    TSimpleStatefulWideFlowCodegeneratorNode(TComputationMutables& mutables, IComputationWideFlowNode* source, ui32 inWidth, ui32 outWidth)
            : TBase(mutables, source, StateKind), SourceFlow(source), InWidth(inWidth), OutWidth(outWidth)
    {}

    enum class EProcessResult : i32 {
        Finish = -1,
        Yield = 0,
        One = 1,
        Again = std::numeric_limits<i32>::max()
    };

private:
    void InitStateWrapper(NUdf::TUnboxedValue &state, TComputationContext &ctx) const {
        if constexpr (StateKind == EValueRepresentation::Embedded) {
            static_cast<const TDerived *>(this)->InitState(*static_cast<TState *>(state.GetRawPtr()), ctx);
        } else {
            static_cast<const TDerived *>(this)->InitState(state, ctx);
        }
    }

    EProcessResult DoProcessWrapper(NUdf::TUnboxedValue &state, TComputationContext& ctx, EFetchResult fetchRes, NUdf::TUnboxedValuePod* inputValues, NUdf::TUnboxedValuePod* outputValues) const {
        Fill(outputValues, outputValues + OutWidth, NUdf::TUnboxedValuePod());
        TVector<NUdf::TUnboxedValue*> outputPtrsVec(OutWidth, nullptr);
        for (size_t pos = 0; pos < OutWidth; pos++) {
            outputPtrsVec[pos] = static_cast<NUdf::TUnboxedValue*>(outputValues + pos);
        }
        NUdf::TUnboxedValue*const* inputPtrs = nullptr;
        if constexpr (StateKind == EValueRepresentation::Embedded) {
            inputPtrs = static_cast<const TDerived*>(this)->PrepareInput(*static_cast<TState*>(state.GetRawPtr()), ctx, outputPtrsVec.data());
        } else {
            inputPtrs = static_cast<const TDerived*>(this)->PrepareInput(static_cast<TState*>(state.AsBoxed().Get()), ctx, outputPtrsVec.data());
        }
        if (inputPtrs) {
            for (size_t pos = 0; pos < InWidth; pos++) {
                if (auto in = inputPtrs[pos]) {
                    *in = inputValues[pos];
                }
            }
        }
        if constexpr (StateKind == EValueRepresentation::Embedded) {
            return static_cast<const TDerived*>(this)->DoProcess(*static_cast<TState*>(state.GetRawPtr()), ctx, fetchRes, outputPtrsVec.data());
        } else {
            return static_cast<const TDerived*>(this)->DoProcess(static_cast<TState*>(state.AsBoxed().Get()), ctx, fetchRes, outputPtrsVec.data());
        }
    }

public:
    EFetchResult DoCalculate(NUdf::TUnboxedValue &state, TComputationContext &ctx,
                             NUdf::TUnboxedValue *const *output) const {
        if (state.IsInvalid()) {
            state = NUdf::TUnboxedValuePod();
            InitStateWrapper(state, ctx);
        }
        EProcessResult res = EProcessResult::Again;
        while (res == EProcessResult::Again) {
            NUdf::TUnboxedValue*const* input = nullptr;
            if constexpr (StateKind == EValueRepresentation::Embedded) {
                input = static_cast<const TDerived*>(this)->PrepareInput(*static_cast<TState*>(state.GetRawPtr()), ctx, output);
            } else {
                input = static_cast<const TDerived*>(this)->PrepareInput(static_cast<TState*>(state.AsBoxed().Get()), ctx, output);
            }
            auto fetchRes = input ? SourceFlow->FetchValues(ctx, input) : EFetchResult::One;
            if constexpr (StateKind == EValueRepresentation::Embedded) {
                res = static_cast<const TDerived*>(this)->DoProcess(*static_cast<TState*>(state.GetRawPtr()), ctx, fetchRes, output);
            } else {
                res = static_cast<const TDerived*>(this)->DoProcess(static_cast<TState*>(state.AsBoxed().Get()), ctx, fetchRes, output);
            }
        }
        return static_cast<EFetchResult>(res);
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        // block = block;

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        BranchInst::Create(init, main, IsInvalid(load, block), block);

        block = init;

        const auto thisType = StructType::get(context)->getPointerTo();
        const auto initFuncType = FunctionType::get(Type::getVoidTy(context), {thisType, statePtr->getType(), ctx.Ctx->getType()}, false);
        const auto initFuncRawVal = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSimpleStatefulWideFlowCodegeneratorNode::InitStateWrapper));
        const auto initFuncVal = CastInst::Create(Instruction::IntToPtr, initFuncRawVal, PointerType::getUnqual(initFuncType), "init_func", block);
        const auto thisRawVal = ConstantInt::get(Type::getInt64Ty(context), reinterpret_cast<ui64>(this));
        const auto thisVal = CastInst::Create(Instruction::IntToPtr, thisRawVal, thisType, "this", block);
        CallInst::Create(initFuncType, initFuncVal, {thisVal, statePtr, ctx.Ctx}, "", block);
        BranchInst::Create(main, block);

        block = main;

        BranchInst::Create(loop, block);

        block = loop;

        const auto [fetchResVal, getters] = GetNodeValues(SourceFlow, ctx, block);
        const auto inputValues = new AllocaInst(valueType, 0, ConstantInt::get(Type::getInt64Ty(context), InWidth), "inputValues", &ctx.Func->getEntryBlock().back());
        for (size_t pos = 0; pos < InWidth; pos++) {
            const auto offset = GetElementPtrInst::CreateInBounds(valueType, inputValues, {ConstantInt::get(Type::getInt64Ty(context), pos)}, "offset", block);
            new StoreInst(getters[pos](ctx, block), offset, block);
        }
        const auto outputValues = new AllocaInst(valueType, 0, ConstantInt::get(Type::getInt64Ty(context), OutWidth), "outputValues", &ctx.Func->getEntryBlock().back());
        for (size_t pos = 0; pos < OutWidth; pos++) {
            const auto offset = GetElementPtrInst::CreateInBounds(valueType, outputValues, {ConstantInt::get(Type::getInt64Ty(context), pos)}, "offset", block);
            new StoreInst(ConstantInt::get(valueType, 0), offset, block);
        }
        const auto processFuncType = FunctionType::get(Type::getInt32Ty(context), {thisType, statePtr->getType(), ctx.Ctx->getType(), Type::getInt32Ty(context), inputValues->getType(), outputValues->getType()}, false);
        const auto processFuncRawVal = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSimpleStatefulWideFlowCodegeneratorNode::DoProcessWrapper));
        const auto thisRawVal2 = ConstantInt::get(Type::getInt64Ty(context), reinterpret_cast<ui64>(this));
        const auto thisVal2 = CastInst::Create(Instruction::IntToPtr, thisRawVal2, thisType, "this", block);
        const auto processFuncVal = CastInst::Create(Instruction::IntToPtr, processFuncRawVal, PointerType::getUnqual(processFuncType), "process_func", block);
        const auto processResVal = CallInst::Create(processFuncType, processFuncVal, {thisVal2, statePtr, ctx.Ctx, fetchResVal, inputValues, outputValues}, "process_res", block);
        ICodegeneratorInlineWideNode::TGettersList new_getters;
        new_getters.reserve(OutWidth);
        for (size_t pos = 0; pos < OutWidth; pos++) {
            new_getters.push_back([pos, outputValues, valueType] (const TCodegenContext& ctx, BasicBlock*& block) -> Value* {
                const auto offset = GetElementPtrInst::CreateInBounds(valueType, outputValues, {ConstantInt::get(Type::getInt64Ty(ctx.Codegen.GetContext()), pos)}, "offset", block);
                const auto value = new LoadInst(valueType, offset, "value", block);
                return value;
            });
        }
        const auto brk = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, processResVal, ConstantInt::get(processResVal->getType(), static_cast<i32>(EProcessResult::Again)), "brk", block);
        BranchInst::Create(done, loop, brk, block);

        block = done;
        return {processResVal, std::move(new_getters)};
    }
#endif

private:
    IComputationWideFlowNode* const SourceFlow;
    const ui32 InWidth, OutWidth;
};

}
}