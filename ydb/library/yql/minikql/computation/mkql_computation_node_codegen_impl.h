#pragma once

#include "mkql_computation_node_codegen.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

template<typename TDerived, typename TState>
class TSimpleStatefulWideFlowCodegeneratorNode
        : public TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode<TDerived, TState>> {
    using TBase = TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode>;

protected:
    TSimpleStatefulWideFlowCodegeneratorNode(TComputationMutables& mutables, IComputationWideFlowNode* source, EValueRepresentation stateKind)
            : TBase(mutables, source, stateKind), SourceFlow(source)
    {}

    enum class EProcessResult : i32 {
        Finish = -1,
        Yield = 0,
        One = 1,
        Fetch = std::numeric_limits<i32>::max()
    };

private:
    void InitStateWrapper(NUdf::TUnboxedValue &state, TComputationContext &ctx) const {
        static_cast<const TDerived*>(this)->InitState(*static_cast<TState*>(state.GetRawPtr()), ctx);
    }

    EProcessResult DoProcessWrapper(NUdf::TUnboxedValue &state, TComputationContext& ctx, EFetchResult fetchRes, NUdf::TUnboxedValuePod* values, size_t width) const {
        TVector<NUdf::TUnboxedValue> valuesVec(width, NUdf::TUnboxedValuePod());
        for (size_t pos = 0; pos < width; pos++) {
            valuesVec[pos] = values[pos];
        }
        TVector<NUdf::TUnboxedValue*> valuePtrsVec(width, nullptr);
        for (size_t pos = 0; pos < width; pos++) {
            valuePtrsVec[pos] = valuesVec.data() + pos;
        }
        auto res = static_cast<const TDerived*>(this)->DoProcess(*static_cast<TState*>(state.GetRawPtr()), ctx, fetchRes, valuePtrsVec.data());
        for (size_t pos = 0; pos < width; pos++) {
            values[pos] = valuesVec[pos].Release();
        }
        return res;
    }

public:
    EFetchResult DoCalculate(NUdf::TUnboxedValue &state, TComputationContext &ctx,
                             NUdf::TUnboxedValue *const *output) const {
        if (state.IsInvalid()) {
            state = NUdf::TUnboxedValuePod();
            InitStateWrapper(state, ctx);
        }
        EProcessResult res = EProcessResult::Fetch;
        while (res == EProcessResult::Fetch) {
            auto fetchRes = SourceFlow->FetchValues(ctx, output);
            res = static_cast<const TDerived*>(this)->DoProcess(*static_cast<TState*>(state.GetRawPtr()), ctx, fetchRes, output);
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
        const auto values = new AllocaInst(valueType, 0, ConstantInt::get(Type::getInt64Ty(context), getters.size()), "values", &ctx.Func->getEntryBlock().back());
        for (size_t pos = 0; pos < getters.size(); pos++) {
            const auto offset = GetElementPtrInst::CreateInBounds(valueType, values, {ConstantInt::get(Type::getInt64Ty(context), pos)}, "offset", block);
            new StoreInst(getters[pos](ctx, block), offset, block);
        }
        const auto processFuncType = FunctionType::get(Type::getInt32Ty(context), {thisType, statePtr->getType(), ctx.Ctx->getType(), Type::getInt32Ty(context), values->getType(), Type::getInt64Ty(context)}, false);
        const auto processFuncRawVal = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSimpleStatefulWideFlowCodegeneratorNode::DoProcessWrapper));
        const auto thisRawVal2 = ConstantInt::get(Type::getInt64Ty(context), reinterpret_cast<ui64>(this));
        const auto thisVal2 = CastInst::Create(Instruction::IntToPtr, thisRawVal2, thisType, "this", block);
        const auto processFuncVal = CastInst::Create(Instruction::IntToPtr, processFuncRawVal, PointerType::getUnqual(processFuncType), "process_func", block);
        const auto processResVal = CallInst::Create(processFuncType, processFuncVal, {thisVal2, statePtr, ctx.Ctx, fetchResVal, values, ConstantInt::get(Type::getInt64Ty(context), getters.size())}, "process_res", block);
        ICodegeneratorInlineWideNode::TGettersList new_getters;
        new_getters.reserve(getters.size());
        for (size_t pos = 0; pos < getters.size(); pos++) {
            new_getters.push_back([pos, values] (const TCodegenContext& ctx, BasicBlock*& block) -> Value* {
                const auto offset = GetElementPtrInst::CreateInBounds(values->getType()->getElementType(), values, {ConstantInt::get(Type::getInt64Ty(ctx.Codegen.GetContext()), pos)}, "offset", block);
                const auto value = new LoadInst(offset, "value", block);
                return value;
            });
        }
        const auto brk = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, processResVal, ConstantInt::get(processResVal->getType(), static_cast<i32>(EProcessResult::Fetch)), "brk", block);
        BranchInst::Create(done, loop, brk, block);

        block = done;
        return {processResVal, std::move(new_getters)};
    }
#endif

private:
    IComputationWideFlowNode* const SourceFlow;
};

}
}