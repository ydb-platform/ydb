#include "mkql_block_compress.h"

#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl_codegen.h> // Y_IGNORE
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TCompressWithScalarBitmap : public TStatefulWideFlowCodegeneratorNode<TCompressWithScalarBitmap> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TCompressWithScalarBitmap>;
public:
    TCompressWithScalarBitmap(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, ui32 width)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , Flow_(flow)
        , BitmapIndex_(bitmapIndex)
        , Width_(width)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Width_))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsFinish())
            return EFetchResult::Finish;

        const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
        NUdf::TUnboxedValue bitmap;
        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            fields[i] = i == BitmapIndex_ ? &bitmap : output[outIndex++];
        }

        if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result)
            return result;

        const bool bitmapValue = GetBitmapScalarValue(bitmap) & 1;
        state = bitmapValue ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod::MakeFinish();
        return bitmapValue ? EFetchResult::One : EFetchResult::Finish;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto bitmapType = Type::getInt8Ty(context);

        const auto name = "GetBitmapScalarValue";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&GetBitmapScalarValue));
        const auto getBitmapType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(bitmapType, { valueType }, false):
            FunctionType::get(bitmapType, { ptrValueType }, false);
        const auto getBitmap = ctx.Codegen.GetModule().getOrInsertFunction(name, getBitmapType);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(statusType, 3U, "result", over);
        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        BranchInst::Create(over, work, IsFinish(statePtr, block), block);

        block = work;

        const auto getres = GetNodeValues(Flow_, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        result->addIncoming(getres.first, block);

        BranchInst::Create(over, test, special, block);

        block = test;

        const auto bitmapValue = getres.second[BitmapIndex_](ctx, block);
        const auto bitmap = CallInst::Create(getBitmap, { WrapArgumentForWindows(bitmapValue, ctx, block) }, "bitmap", block);
        const auto one = ConstantInt::get(bitmapType, 1);
        const auto band = BinaryOperator::CreateAnd(bitmap, one, "band", block);
        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, band, one, "good", block);

        const auto state = SelectInst::Create(good, GetEmpty(context), GetFinish(context), "state", block);
        new StoreInst(state, statePtr, block);

        const auto status = SelectInst::Create(good, ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), "status", block);
        result->addIncoming(status, block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(getres.second.size() - 1U);
        for (ui32 i = 0, j = 0; i < getres.second.size(); ++i) {
            if (i != BitmapIndex_)
                getters[j++] = std::move(getres.second[i]);
        }
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode *const Flow_;
    const ui32 BitmapIndex_;
    const ui32 Width_;
    const ui32 WideFieldsIndex_;
};

class TCompressScalars : public TStatelessWideFlowCodegeneratorNode<TCompressScalars> {
using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TCompressScalars>;
public:
    TCompressScalars(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, ui32 width)
        : TBaseComputation(flow)
        , Flow_(flow)
        , BitmapIndex_(bitmapIndex)
        , Width_(width)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Width_))
    {
    }

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
        NUdf::TUnboxedValue bitmap;
        for (ui32 i = 0, outIndex = 0; i < Width_; ++i) {
            fields[i] = i == BitmapIndex_ ? &bitmap : output[outIndex++];
        }

        for (;;) {
            if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result)
                return result;

            if (const auto popCount = GetBitmapPopCountCount(bitmap)) {
                if (const auto out = output[Width_ - 2])
                    *out = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(popCount)));
                break;
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto sizeType = Type::getInt64Ty(context);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto sizePtr = new AllocaInst(valueType, 0U, "size_ptr", atTop);
        new StoreInst(ConstantInt::get(valueType, 0), sizePtr, atTop);

        const auto name = "GetBitmapPopCountCount";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&GetBitmapPopCountCount));
        const auto getPopCountType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(sizeType, { valueType }, false):
            FunctionType::get(sizeType, { ptrValueType }, false);
        const auto getPopCount = ctx.Codegen.GetModule().getOrInsertFunction(name, getPopCountType);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto fill = BasicBlock::Create(context, "fill", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        BranchInst::Create(loop, block);
        block = loop;

        const auto getres = GetNodeValues(Flow_, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        const auto result = PHINode::Create(statusType, 2U, "result", over);
        result->addIncoming(getres.first, block);

        BranchInst::Create(over, work, special, block);

        block = work;

        const auto bitmapValue = getres.second[BitmapIndex_](ctx, block);
        const auto pops = CallInst::Create(getPopCount, { WrapArgumentForWindows(bitmapValue, ctx, block) }, "pops", block);
        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, pops, ConstantInt::get(sizeType, 0), "good", block);

        BranchInst::Create(fill, loop, good, block);

        block = fill;

        const auto makeCountFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&MakeBlockCount));
        const auto makeCountType = FunctionType::get(valueType, {ctx.GetFactory()->getType(), pops->getType()}, false);
        const auto makeCountPtr = CastInst::Create(Instruction::IntToPtr, makeCountFunc, PointerType::getUnqual(makeCountType), "make_count_func", block);
        const auto slice = CallInst::Create(makeCountType, makeCountPtr, {ctx.GetFactory(), pops}, "slice", block);
        new StoreInst(slice, sizePtr, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(getres.second.size() - 1U);
        for (ui32 i = 0, j = 0; i < getters.size(); ++i) {
            if (i != BitmapIndex_)
                getters[j++] = std::move(getres.second[i]);
        }
        getters.back() = [sizePtr, valueType](const TCodegenContext&, BasicBlock*& block) {
            return new LoadInst(valueType, sizePtr, "count", block);
        };
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode *const Flow_;
    const ui32 BitmapIndex_;
    const ui32 Width_;
    const ui32 WideFieldsIndex_;
};

size_t GetBitmapPopCount(const std::shared_ptr<arrow::ArrayData>& arr) {
    size_t len = (size_t)arr->length;
    MKQL_ENSURE(arr->GetNullCount() == 0, "Bitmap block should not have nulls");
    const ui8* src = arr->GetValues<ui8>(1);
    return GetSparseBitmapPopCount(src, len);
}

class TCompressBlocks : public TStatefulWideFlowCodegeneratorNode<TCompressBlocks> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TCompressBlocks>;
public:
    TCompressBlocks(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 bitmapIndex, TVector<TBlockType*>&& types)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , BitmapIndex_(bitmapIndex)
        , Types_(std::move(types))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Types_.size() + 2U))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& s = GetState(state, ctx);

        const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
        for (auto i = 0U, j = 0U; i <= Types_.size() + 1U; ++i) {
            if (BitmapIndex_ !=  i)
                fields[i] =  &s.Values[j++];
        }

        NUdf::TUnboxedValue bitmap;
        fields[BitmapIndex_] = &bitmap;

        if (!s.Count) {
            do if (!s.InputSize_) {
                s.ClearValues();
                switch (Flow_->FetchValues(ctx, fields)) {
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                    case EFetchResult::Finish:
                        s.IsFinished_ = true;
                        break;
                    case EFetchResult::One:
                        switch (s.Check(bitmap.Release())) {
                            case TState::EStep::Copy:
                                for (ui32 i = 0; i < s.Values.size(); ++i) {
                                    if (const auto out = output[i]) {
                                        *out = s.Values[i];
                                    }
                                }
                                return EFetchResult::One;
                            case TState::EStep::Skip:
                                continue;
                            case TState::EStep::Pass:
                                break;
                        }
                        break;
                }
            } while (!s.IsFinished_ && s.Sparse());

            if (s.OutputPos_)
                s.FlushBuffers(ctx.HolderFactory);
            else
                return EFetchResult::Finish;
        }

        const auto sliceSize = s.Slice();
        for (size_t i = 0; i <= Types_.size(); ++i) {
            if (const auto out = output[i]) {
                *out = s.Get(sliceSize, ctx.HolderFactory, i);
            }
        }

        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto width = Types_.size() + 1U;

        const auto valueType = Type::getInt128Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto arrayType = ArrayType::get(valueType, width);
        const auto ptrValuesType = PointerType::getUnqual(arrayType);

        TLLVMFieldsStructureState stateFields(context, width);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Get));
        const auto getType = FunctionType::get(valueType, {statePtrType, indexType, ctx.GetFactory()->getType(), indexType}, false);
        const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", atTop);

        const auto heightPtr = new AllocaInst(indexType, 0U, "height_ptr", atTop);
        const auto stateOnStack = new AllocaInst(statePtrType, 0U, "state_on_stack", atTop);

        new StoreInst(ConstantInt::get(indexType, 0), heightPtr, atTop);
        new StoreInst(ConstantPointerNull::get(statePtrType), stateOnStack, atTop);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto read = BasicBlock::Create(context, "read", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto save = BasicBlock::Create(context, "save", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto tail = BasicBlock::Create(context, "tail", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto fill = BasicBlock::Create(context, "fill", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TCompressBlocks::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetCount() }, "count_ptr", block);
        const auto inputSizePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetInputSize() }, "input_size_ptr", block);

        BranchInst::Create(loop, block);

        block = loop;

        const auto count = new LoadInst(indexType, countPtr, "count", block);

        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, ConstantInt::get(indexType, 0), "next", block);

        BranchInst::Create(more, fill, next, block);

        block = more;

        const auto inputSize = new LoadInst(indexType, inputSizePtr, "input_size", block);
        const auto zero = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, inputSize, ConstantInt::get(indexType, 0), "zero", block);

        BranchInst::Create(read, work, zero, block);

        block = read;

        const auto clearFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::ClearValues));
        const auto clearType = FunctionType::get(Type::getVoidTy(context), {statePtrType}, false);
        const auto clearPtr = CastInst::Create(Instruction::IntToPtr, clearFunc, PointerType::getUnqual(clearType), "clear", block);
        CallInst::Create(clearType, clearPtr, {stateArg}, "", block);

        const auto getres = GetNodeValues(Flow_, ctx, block);

        new StoreInst(ConstantInt::get(indexType, 0), heightPtr, block);
        const auto result = PHINode::Create(statusType, 4U, "result", over);
        result->addIncoming(getres.first, block);

        const auto way = SwitchInst::Create(getres.first, good, 2U, block);
        way->addCase(ConstantInt::get(statusType, i32(EFetchResult::Finish)), stop);
        way->addCase(ConstantInt::get(statusType, i32(EFetchResult::Yield)), over);

        block = stop;

        const auto finishPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetIsFinished() }, "finish_ptr", block);
        new StoreInst(ConstantInt::getTrue(context), finishPtr, block);
        BranchInst::Create(tail, block);

        block = good;

        const auto bitmap = getres.second[BitmapIndex_](ctx, block);
        const auto bitmapArg = WrapArgumentForWindows(bitmap, ctx, block);

        const auto stepType = Type::getInt8Ty(context);
        const auto checkFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Check));
        const auto checkType = FunctionType::get(stepType, {statePtrType, bitmapArg->getType()}, false);
        const auto checkPtr = CastInst::Create(Instruction::IntToPtr, checkFunc, PointerType::getUnqual(checkType), "check_func", block);
        const auto check = CallInst::Create(checkType, checkPtr, {stateArg, bitmapArg}, "check", block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        const auto step = SwitchInst::Create(check, save, 2U, block);
        step->addCase(ConstantInt::get(stepType, i8(TState::EStep::Skip)), read);
        step->addCase(ConstantInt::get(stepType, i8(TState::EStep::Copy)), over);

        block = save;

        const auto valuesPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetPointer() }, "values_ptr", block);
        const auto values = new LoadInst(ptrValuesType, valuesPtr, "values", block);
        for (size_t idx = 0U; idx <= Types_.size(); ++idx) {
            const auto pointer = GetElementPtrInst::CreateInBounds(arrayType, values, {  ConstantInt::get(indexType, 0),  ConstantInt::get(indexType, idx) }, "pointer", block);
            const auto value = getres.second[idx < BitmapIndex_ ? idx : idx + 1U](ctx, block);
            new StoreInst(value, pointer, block);
            AddRefBoxed(value, ctx, block);
        }

        BranchInst::Create(work, block);

        block = work;

        const auto sparseFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Sparse));
        const auto sparseType = FunctionType::get(Type::getInt1Ty(context), {statePtrType}, false);
        const auto sparsePtr = CastInst::Create(Instruction::IntToPtr, sparseFunc, PointerType::getUnqual(sparseType), "sparse_func", block);
        const auto sparse = CallInst::Create(sparseType, sparsePtr, {stateArg}, "sparse", block);

        BranchInst::Create(loop, tail, sparse, block);

        block = tail;

        const auto outputPosPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetOutputPos() }, "output_pos_ptr", block);
        const auto outputPos = new LoadInst(indexType, outputPosPtr, "output_pos", block);
        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, outputPos, ConstantInt::get(indexType, 0), "empty", block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        BranchInst::Create(over, done, empty, block);

        block = done;

        const auto flushFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::FlushBuffers));
        const auto flushType = FunctionType::get(Type::getVoidTy(context), {statePtrType, ctx.GetFactory()->getType()}, false);
        const auto flushPtr = CastInst::Create(Instruction::IntToPtr, flushFunc, PointerType::getUnqual(flushType), "flush_func", block);
        CallInst::Create(flushType, flushPtr, {stateArg, ctx.GetFactory()}, "", block);

        BranchInst::Create(fill, block);

        block = fill;

        const auto sliceFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Slice));
        const auto sliceType = FunctionType::get(indexType, {statePtrType}, false);
        const auto slicePtr = CastInst::Create(Instruction::IntToPtr, sliceFunc, PointerType::getUnqual(sliceType), "slice_func", block);
        const auto slice = CallInst::Create(sliceType, slicePtr, {stateArg}, "slice", block);
        new StoreInst(slice, heightPtr, block);
        new StoreInst(stateArg, stateOnStack, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(width);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, getType, getPtr, heightPtr, indexType, valueType, statePtrType, stateOnStack, getter = getres.second[idx < BitmapIndex_ ? idx : idx + 1U]](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();
                const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
                const auto call = BasicBlock::Create(context, "call", ctx.Func);
                const auto done = BasicBlock::Create(context, "done", ctx.Func);

                const auto result = PHINode::Create(valueType, 2U, "result", done);

                const auto height = new LoadInst(indexType, heightPtr, "height", block);
                const auto zero = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, height, ConstantInt::get(indexType, 0), "zero", block);

                BranchInst::Create(pass, call, zero, block);

                block = pass;

                const auto source = getter(ctx, block);
                result->addIncoming(source, block);
                BranchInst::Create(done, block);

                block = call;

                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto value = CallInst::Create(getType, getPtr, {stateArg, height, ctx.GetFactory(), ConstantInt::get(indexType, idx)}, "value", block);
                result->addIncoming(value, block);
                BranchInst::Create(done, block);

                block = done;
                return result;
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
    struct TState : public TBlockState {
        size_t InputSize_ = 0;
        size_t OutputPos_ = 0;
        bool IsFinished_ = false;

        const size_t MaxLength_;

        std::vector<std::shared_ptr<arrow::ArrayData>> Arrays_;
        std::vector<std::unique_ptr<IArrayBuilder>> Builders_;

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TBlockType*>& types)
            : TBlockState(memInfo, types.size() + 1U)
            , MaxLength_(CalcBlockLen(std::accumulate(types.cbegin(), types.cend(), 0ULL, [](size_t max, const TBlockType* type){ return std::max(max, CalcMaxBlockItemSize(type->GetItemType())); })))
            , Arrays_(types.size() + 1U)
            , Builders_(types.size())
        {
            for (ui32 i = 0; i < types.size(); ++i) {
                if (types[i]->GetShape() != TBlockType::EShape::Scalar) {
                    Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), types[i]->GetItemType(), ctx.ArrowMemoryPool, MaxLength_, &ctx.Builder->GetPgBuilder());
                }
            }
        }

        enum class EStep : i8 {
            Copy = -1,
            Skip = 0,
            Pass = 1
        };

        EStep Check(const NUdf::TUnboxedValuePod bitmapValue) {
            Y_ABORT_UNLESS(!IsFinished_);
            Y_ABORT_UNLESS(!InputSize_);
            const NUdf::TUnboxedValue b(std::move(bitmapValue));
            auto& bitmap = Arrays_.back();
            bitmap = TArrowBlock::From(b).GetDatum().array();

            if (!bitmap->length)
                return EStep::Skip;

            const auto popCount = GetBitmapPopCount(bitmap);
            if (!popCount)
                return EStep::Skip;

            if (!OutputPos_ && ui64(bitmap->length) == popCount)
                return EStep::Copy;

            return EStep::Pass;
        }

        bool Sparse() {
            auto& bitmap = Arrays_.back();
            if (!InputSize_) {
                InputSize_ = bitmap->length;
                for (size_t i = 0; i < Builders_.size(); ++i) {
                    if (Builders_[i]) {
                        Arrays_[i] = TArrowBlock::From(Values[i]).GetDatum().array();
                        Y_ABORT_UNLESS(ui64(Arrays_[i]->length) == InputSize_);
                    }
                }
            }

            size_t outputAvail = MaxLength_ - OutputPos_;
            size_t takeInputLen = 0;
            size_t takeInputPopcnt = 0;

            const auto bitmapData = bitmap->GetValues<ui8>(1);
            while (takeInputPopcnt < outputAvail && takeInputLen < InputSize_) {
                takeInputPopcnt += bitmapData[takeInputLen++];
            }
            Y_ABORT_UNLESS(takeInputLen > 0);
            for (size_t i = 0; i < Builders_.size(); ++i) {
                if (Builders_[i]) {
                    auto& arr = Arrays_[i];
                    auto& builder = Builders_[i];
                    auto slice = Chop(arr, takeInputLen);
                    builder->AddMany(*slice, takeInputPopcnt, bitmapData, takeInputLen);
                }
            }

            Chop(bitmap, takeInputLen);
            OutputPos_ += takeInputPopcnt;
            InputSize_ -= takeInputLen;
            return MaxLength_ > OutputPos_;
        }

        void FlushBuffers(const THolderFactory& holderFactory) {
            for (ui32 i = 0; i < Builders_.size(); ++i) {
                if (Builders_[i])
                    Values[i] = holderFactory.CreateArrowBlock(Builders_[i]->Build(IsFinished_));
            }

            Values.back() = MakeBlockCount(holderFactory, OutputPos_);
            OutputPos_ = 0;
            FillArrays();
        }
    };
#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureState: public TLLVMFieldsStructureBlockState {
    private:
        using TBase = TLLVMFieldsStructureBlockState;
        llvm::IntegerType*const InputSizeType;
        llvm::IntegerType*const OutputPosType;
        llvm::IntegerType*const IsFinishedType;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFieldsArray();
            result.emplace_back(InputSizeType);
            result.emplace_back(OutputPosType);
            result.emplace_back(IsFinishedType);
            return result;
        }

        llvm::Constant* GetInputSize() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields);
        }

        llvm::Constant* GetOutputPos() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields + 1);
        }

        llvm::Constant* GetIsFinished() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields + 2);
        }

        TLLVMFieldsStructureState(llvm::LLVMContext& context, size_t width)
            : TBase(context, width)
            , InputSizeType(Type::getInt64Ty(Context))
            , OutputPosType(Type::getInt64Ty(Context))
            , IsFinishedType(Type::getInt1Ty(Context))
        {}
    };
#endif
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ctx, Types_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue())
            MakeState(ctx, state);
        return *static_cast<TState*>(state.AsBoxed().Get());
    }


    IComputationWideFlowNode* const Flow_;
    const ui32 BitmapIndex_;
    const TVector<TBlockType*> Types_;
    const size_t WideFieldsIndex_;
};

} // namespace

IComputationNode* WrapBlockCompress(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    const ui32 width = wideComponents.size();
    MKQL_ENSURE(width > 1, "Expected at least two columns");

    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1U));
    const auto index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < width - 1, "Bad bitmap index");

    TVector<TBlockType*> types;
    types.reserve(width - 2U);
    bool bitmapIsScalar = false;
    bool allScalars = true;
    for (ui32 i = 0; i < width; ++i) {
        types.push_back(AS_TYPE(TBlockType, wideComponents[i]));
        const bool isScalar = types.back()->GetShape() == TBlockType::EShape::Scalar;
        if (i == width - 1) {
            MKQL_ENSURE(isScalar, "Expecting scalar block size as last column");
            bool isOptional;
            TDataType* unpacked = UnpackOptionalData(types.back()->GetItemType(), isOptional);
            auto slot = *unpacked->GetDataSlot();
            MKQL_ENSURE(!isOptional && slot == NUdf::EDataSlot::Uint64, "Expecting Uint64 as last column");
            types.pop_back();
        } else if (i == index) {
            bool isOptional;
            TDataType* unpacked = UnpackOptionalData(types.back()->GetItemType(), isOptional);
            auto slot = *unpacked->GetDataSlot();
            MKQL_ENSURE(!isOptional && slot == NUdf::EDataSlot::Bool, "Expecting Bool as bitmap column");
            bitmapIsScalar = isScalar;
            types.pop_back();
        } else {
            allScalars = allScalars && isScalar;
        }
    }

    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    if (bitmapIsScalar) {
        return new TCompressWithScalarBitmap(ctx.Mutables, wideFlow, index, width);
    } else if (allScalars) {
        return new TCompressScalars(ctx.Mutables, wideFlow, index, width);
    }

    return new TCompressBlocks(ctx.Mutables, wideFlow, index, std::move(types));
}

} // namespace NMiniKQL
} // namespace NKikimr
