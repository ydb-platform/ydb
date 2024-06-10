#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include "mkql_simple_codegen.h"

namespace NKikimr {
namespace NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
ICodegeneratorInlineWideNode::TGenerateResult TSimpleWideFlowCodegeneratorNodeLLVMBase::DoGenGetValuesBase(const NKikimr::NMiniKQL::TCodegenContext& ctx, llvm::Value* statePtrVal, llvm::BasicBlock*& genToBlock) const  {
    // init stuff (mainly in global entry block)

    auto& context = ctx.Codegen.GetContext();

    const auto valueType = Type::getInt128Ty(context);
    const auto init = BasicBlock::Create(context, "init", ctx.Func);
    const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
    const auto loopFetch = BasicBlock::Create(context, "loop_fetch", ctx.Func);
    const auto loopCalc = BasicBlock::Create(context, "loop_calc", ctx.Func);
    const auto loopTail = BasicBlock::Create(context, "loop_tail", ctx.Func);
    const auto done = BasicBlock::Create(context, "done", ctx.Func);
    const auto entryPos = &ctx.Func->getEntryBlock().back();

    const bool hasState = statePtrVal != nullptr;

    const auto thisType = StructType::get(context)->getPointerTo();
    const auto thisRawVal = ConstantInt::get(Type::getInt64Ty(context), PtrTable.ThisPtr);
    const auto thisVal = CastInst::Create(Instruction::IntToPtr, thisRawVal, thisType, "this", entryPos);
    const auto valuePtrType = PointerType::getUnqual(valueType);
    const auto valuePtrsPtrType = PointerType::getUnqual(valuePtrType);
    const auto statePtrType = hasState ? statePtrVal->getType() : nullptr;
    const auto ctxType = ctx.Ctx->getType();
    const auto i32Type = Type::getInt32Ty(context);
    const auto valueNullptrVal = ConstantPointerNull::get(valuePtrType);
    const auto valuePtrNullptrVal = ConstantPointerNull::get(valuePtrsPtrType);
    const auto oneVal = ConstantInt::get(i32Type, static_cast<i32>(EFetchResult::One));
    const auto maybeResType = TMaybeFetchResult::LLVMType(context);
    const auto noneVal = TMaybeFetchResult::None().LLVMConst(context);

    const auto outputArrayVal = new AllocaInst(valueType, 0, ConstantInt::get(i32Type, OutWidth), "output_array", entryPos);
    const auto outputPtrsVal = new AllocaInst(valuePtrType, 0, ConstantInt::get(Type::getInt64Ty(context), OutWidth), "output_ptrs", entryPos);
    for (ui32 pos = 0; pos < OutWidth; pos++) {
        const auto posVal = ConstantInt::get(i32Type, pos);
        const auto arrayPtrVal = GetElementPtrInst::CreateInBounds(valueType, outputArrayVal, {posVal}, "array_ptr", entryPos);
        const auto ptrsPtrVal = GetElementPtrInst::CreateInBounds(valuePtrType, outputPtrsVal, {posVal}, "ptrs_ptr", entryPos);
        new StoreInst(arrayPtrVal, ptrsPtrVal, &ctx.Func->getEntryBlock().back());
    }

    auto block = genToBlock; // >>> start of main code chunk

    const auto stateVal = hasState ? new LoadInst(valueType, statePtrVal, "state", block) : nullptr;
    BranchInst::Create(init, loop, hasState ? IsInvalid(stateVal, block) : ConstantInt::get(Type::getInt1Ty(context), 0), block);

    block = init; // state initialization block:

    if (hasState) {
        const auto initFuncType = FunctionType::get(Type::getVoidTy(context), {thisType, statePtrType, ctxType}, false);
        const auto initFuncRawVal = ConstantInt::get(Type::getInt64Ty(context), PtrTable.InitStateMethPtr);
        const auto initFuncVal = CastInst::Create(Instruction::IntToPtr, initFuncRawVal, PointerType::getUnqual(initFuncType), "init_func", block);
        CallInst::Create(initFuncType, initFuncVal, {thisVal, statePtrVal, ctx.Ctx}, "", block);
    }
    BranchInst::Create(loop, block);

    block = loop; // loop head block: (prepare inputs and decide whether to calculate row or not)

    const auto generated = DispatchGenFetchProcess(statePtrVal, ctx, std::bind_front(GetNodeValues, SourceFlow), block);
    auto processResVal = generated.first;
    if (processResVal == nullptr) {
        const auto prepareFuncType = hasState
            ? FunctionType::get(valuePtrsPtrType, {thisType, statePtrType, ctxType, valuePtrsPtrType}, false)
            : FunctionType::get(valuePtrsPtrType, {thisType, ctxType, valuePtrsPtrType}, false);
        const auto prepareFuncRawVal = ConstantInt::get(Type::getInt64Ty(context), PtrTable.PrepareInputMethPtr);
        const auto prepareFuncVal = CastInst::Create(Instruction::IntToPtr, prepareFuncRawVal, PointerType::getUnqual(prepareFuncType), "prepare_func", block);
        const auto inputPtrsVal = hasState
            ? CallInst::Create(prepareFuncType, prepareFuncVal, {thisVal, statePtrVal, ctx.Ctx, outputPtrsVal}, "input_ptrs", block)
            : CallInst::Create(prepareFuncType, prepareFuncVal, {thisVal, ctx.Ctx, outputPtrsVal}, "input_ptrs", block);
        const auto skipFetchCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, inputPtrsVal, valuePtrNullptrVal, "skip_fetch", block);
        BranchInst::Create(loopTail, loopFetch, skipFetchCond, block);

        block = loopFetch; // loop fetch chunk:

        const auto [fetchResVal, getters] = GetNodeValues(SourceFlow, ctx, block);
        const auto fetchResExtVal = new ZExtInst(fetchResVal, maybeResType, "res_ext", block);
        const auto skipCalcCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, fetchResVal, oneVal, "skip_calc", block);
        const auto fetchSourceBlock = block;
        BranchInst::Create(loopTail, loopCalc, skipCalcCond, block);

        block = loopCalc; // loop calc chunk: (calculate needed values in the row)

        for (ui32 pos = 0; pos < InWidth; pos++) {
            const auto stor = BasicBlock::Create(context, "stor", ctx.Func);
            const auto cont = BasicBlock::Create(context, "cont", ctx.Func);

            auto innerBlock = block; // >>> start of inner chunk (calculates and stores the value if needed)

            const auto posVal = ConstantInt::get(i32Type, pos);
            const auto inputPtrPtrVal = GetElementPtrInst::CreateInBounds(valuePtrType, inputPtrsVal, {posVal}, "input_ptr_ptr", innerBlock);
            const auto inputPtrVal = new LoadInst(valuePtrType, inputPtrPtrVal, "input_ptr", innerBlock);
            const auto isNullCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, inputPtrVal, valueNullptrVal, "is_null", innerBlock);
            BranchInst::Create(cont, stor, isNullCond, innerBlock);

            innerBlock = stor; // calculate & store chunk:

            new StoreInst(getters[pos](ctx, innerBlock), inputPtrVal, innerBlock);
            BranchInst::Create(cont, innerBlock);

            innerBlock = cont; // skip input value block:

            /* nothing here yet */

            block = innerBlock; // <<< end of inner chunk
        }
        const auto calcSourceBlock = block;
        BranchInst::Create(loopTail, block);

        block = loopTail; // loop tail block: (process row)

        const auto maybeFetchResVal = PHINode::Create(maybeResType, 2, "fetch_res", block);
        maybeFetchResVal->addIncoming(noneVal, loop);
        maybeFetchResVal->addIncoming(fetchResExtVal, fetchSourceBlock);
        maybeFetchResVal->addIncoming(fetchResExtVal, calcSourceBlock);
        const auto processFuncType = hasState 
            ? FunctionType::get(maybeResType, {thisType, statePtrType, ctxType, maybeResType, valuePtrsPtrType}, false)
            : FunctionType::get(maybeResType, {thisType, ctxType, maybeResType, valuePtrsPtrType}, false);
        const auto processFuncRawVal = ConstantInt::get(Type::getInt64Ty(context), PtrTable.DoProcessMethPtr);
        const auto processFuncVal = CastInst::Create(Instruction::IntToPtr, processFuncRawVal, PointerType::getUnqual(processFuncType), "process_func", block);
        processResVal = hasState
            ? CallInst::Create(processFuncType, processFuncVal, {thisVal, statePtrVal, ctx.Ctx, maybeFetchResVal, outputPtrsVal}, "process_res", block)
            : CallInst::Create(processFuncType, processFuncVal, {thisVal, ctx.Ctx, maybeFetchResVal, outputPtrsVal}, "process_res", block);
    } else {
        BranchInst::Create(loopFetch, loopFetch);
        BranchInst::Create(loopCalc, loopCalc);
        BranchInst::Create(loopTail, loopTail);
    }
    const auto brkCond = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, processResVal, noneVal, "brk", block);
    BranchInst::Create(done, loop, brkCond, block);

    block = done; // finalization block:

    const auto processResTruncVal = new TruncInst(processResVal, i32Type, "res_trunc", block);

    genToBlock = block; // <<< end of main code chunk

    if (generated.first) {
        return {processResTruncVal, generated.second};
    }

    ICodegeneratorInlineWideNode::TGettersList new_getters;
    new_getters.reserve(OutWidth);
    for (size_t pos = 0; pos < OutWidth; pos++) {
        new_getters.push_back([pos, outputArrayVal, i32Type, valueType] (const TCodegenContext&, BasicBlock*& block) -> Value* {
            const auto posVal = ConstantInt::get(i32Type, pos);
            const auto arrayPtrVal = GetElementPtrInst::CreateInBounds(valueType, outputArrayVal, {posVal}, "array_ptr", block);
            const auto valueVal = new LoadInst(valueType, arrayPtrVal, "value", block);
            return valueVal;
        });
    }
    return {processResTruncVal, std::move(new_getters)};
}

#endif

}
}