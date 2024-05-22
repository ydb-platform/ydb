#include "mkql_blocks.h"

#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl_codegen.h>  // Y_IGNORE

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

#include <arrow/scalar.h>
#include <arrow/array.h>
#include <arrow/datum.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TToBlocksWrapper : public TStatelessFlowComputationNode<TToBlocksWrapper> {
public:
    explicit TToBlocksWrapper(IComputationNode* flow, TType* itemType)
        : TStatelessFlowComputationNode(flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , ItemType_(itemType)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto maxLen = CalcBlockLen(CalcMaxBlockItemSize(ItemType_));
        const auto builder = MakeArrayBuilder(TTypeInfoHelper(), ItemType_, ctx.ArrowMemoryPool, maxLen, &ctx.Builder->GetPgBuilder());

        for (size_t i = 0; i < builder->MaxLength(); ++i) {
            auto result = Flow_->GetValue(ctx);
            if (result.IsSpecial()) {
                if (i == 0) {
                    return result.Release();
                }
                break;
            }
            builder->Add(result);
        }

        return ctx.HolderFactory.CreateArrowBlock(builder->Build(true));
    }
private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }
private:
    IComputationNode* const Flow_;
    TType* ItemType_;
};

class TWideToBlocksWrapper : public TStatefulWideFlowCodegeneratorNode<TWideToBlocksWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideToBlocksWrapper>;
public:
    TWideToBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<TType*>&& types)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Types_(std::move(types))
        , MaxLength_(CalcBlockLen(std::accumulate(Types_.cbegin(), Types_.cend(), 0ULL, [](size_t max, const TType* type){ return std::max(max, CalcMaxBlockItemSize(type)); })))
        , Width_(Types_.size())
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Width_))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const {
        auto& s = GetState(state, ctx);
        const auto fields = ctx.WideFields.data() + WideFieldsIndex_;

        if (!s.Count) {
            if (!s.IsFinished_) do {
                switch (Flow_->FetchValues(ctx, fields)) {
                    case EFetchResult::One:
                        for (size_t i = 0; i < Types_.size(); ++i)
                            s.Add(s.Values[i], i);
                        continue;
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                    case EFetchResult::Finish:
                        s.IsFinished_ = true;
                        break;
                }
                break;
            } while (++s.Rows_ < MaxLength_ && s.BuilderAllocatedSize_ <= s.MaxBuilderAllocatedSize_);

            if (s.Rows_)
                s.MakeBlocks(ctx.HolderFactory);
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

        const auto valueType = Type::getInt128Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);

        TLLVMFieldsStructureState stateFields(context, Types_.size() + 1U);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto addFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Add));
        const auto addType = FunctionType::get(Type::getVoidTy(context), {statePtrType, valueType, indexType}, false);
        const auto addPtr = CastInst::Create(Instruction::IntToPtr, addFunc, PointerType::getUnqual(addType), "add", atTop);

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Get));
        const auto getType = FunctionType::get(valueType, {statePtrType, indexType, ctx.GetFactory()->getType(), indexType}, false);
        const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", atTop);

        const auto heightPtr = new AllocaInst(indexType, 0U, "height_ptr", atTop);
        const auto stateOnStack = new AllocaInst(statePtrType, 0U, "state_on_stack", atTop);

        new StoreInst(ConstantInt::get(indexType, 0), heightPtr, atTop);
        new StoreInst(ConstantPointerNull::get(statePtrType), stateOnStack, atTop);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto read = BasicBlock::Create(context, "read", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto fill = BasicBlock::Create(context, "fill", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto second_cond = BasicBlock::Create(context, "second_cond", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideToBlocksWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetCount() }, "count_ptr", block);
        const auto count = new LoadInst(indexType, countPtr, "count", block);
        const auto none = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, ConstantInt::get(indexType, 0), "none", block);

        BranchInst::Create(more, fill, none, block);

        block = more;

        const auto rowsPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetRows() }, "rows_ptr", block);
        const auto finishedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetIsFinished() }, "is_finished_ptr", block);
        const auto allocatedSizePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetBuilderAllocatedSize() }, "allocated_size_ptr", block);
        const auto maxAllocatedSizePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetMaxBuilderAllocatedSize() }, "max_allocated_size_ptr", block);
        const auto finished = new LoadInst(Type::getInt1Ty(context), finishedPtr, "finished", block);

        BranchInst::Create(skip, read, finished, block);

        block = read;

        const auto getres = GetNodeValues(Flow_, ctx, block);

        const auto way = SwitchInst::Create(getres.first, good, 2U, block);
        way->addCase(ConstantInt::get(statusType, i32(EFetchResult::Finish)), stop);
        way->addCase(ConstantInt::get(statusType, i32(EFetchResult::Yield)), over);

        const auto result = PHINode::Create(statusType, 3U, "result", over);
        result->addIncoming(getres.first, block);

        block = good;

        const auto read_rows = new LoadInst(indexType, rowsPtr, "read_rows", block);
        const auto increment = BinaryOperator::CreateAdd(read_rows, ConstantInt::get(indexType, 1), "increment", block);
        new StoreInst(increment, rowsPtr, block);

        for (size_t idx = 0U; idx < Types_.size(); ++idx) {
            const auto value = getres.second[idx](ctx, block);
            CallInst::Create(addType, addPtr, {stateArg, value, ConstantInt::get(indexType, idx)}, "", block);
            ValueCleanup(GetValueRepresentation(Types_[idx]), value, ctx, block);
        }

        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, increment, ConstantInt::get(indexType, MaxLength_), "next", block);
        BranchInst::Create(second_cond, work, next, block);

        block = second_cond;
        
        const auto read_allocated_size = new LoadInst(indexType, allocatedSizePtr, "read_allocated_size", block);
        const auto read_max_allocated_size = new LoadInst(indexType, maxAllocatedSizePtr, "read_max_allocated_size", block);
        const auto next2 = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, read_allocated_size, read_max_allocated_size, "next2", block);
        BranchInst::Create(read, work, next2, block);

        block = stop;

        new StoreInst(ConstantInt::getTrue(context), finishedPtr, block);
        BranchInst::Create(skip, block);

        block = skip;

        const auto rows = new LoadInst(indexType, rowsPtr, "rows", block);
        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, rows, ConstantInt::get(indexType, 0), "empty", block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        BranchInst::Create(over, work, empty, block);

        block = work;

        const auto makeBlockFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::MakeBlocks));
        const auto makeBlockType = FunctionType::get(indexType, {statePtrType, ctx.GetFactory()->getType()}, false);
        const auto makeBlockPtr = CastInst::Create(Instruction::IntToPtr, makeBlockFunc, PointerType::getUnqual(makeBlockType), "make_blocks_func", block);
        CallInst::Create(makeBlockType, makeBlockPtr, {stateArg, ctx.GetFactory()}, "", block);

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

        ICodegeneratorInlineWideNode::TGettersList getters(Types_.size() + 1U);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, getType, getPtr, heightPtr, indexType, statePtrType, stateOnStack](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto heightArg = new LoadInst(indexType, heightPtr, "height", block);
                return CallInst::Create(getType, getPtr, {stateArg, heightArg, ctx.GetFactory(), ConstantInt::get(indexType, idx)}, "get", block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
    struct TState : public TBlockState {
        size_t Rows_ = 0;
        bool IsFinished_ = false;
        size_t BuilderAllocatedSize_ = 0;
        size_t MaxBuilderAllocatedSize_ = 0;
        std::vector<std::unique_ptr<IArrayBuilder>> Builders_;
        static const size_t MaxAllocatedFactor_ = 4;

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types, size_t maxLength, NUdf::TUnboxedValue**const fields)
            : TBlockState(memInfo, types.size() + 1U)
            , Builders_(types.size())
        {
            for (size_t i = 0; i < types.size(); ++i) {
                fields[i] = &Values[i];
                Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), types[i], ctx.ArrowMemoryPool, maxLength, &ctx.Builder->GetPgBuilder(), &BuilderAllocatedSize_);
            }
            MaxBuilderAllocatedSize_ = MaxAllocatedFactor_ * BuilderAllocatedSize_;
        }

        void Add(const NUdf::TUnboxedValuePod value, size_t idx) {
            Builders_[idx]->Add(value);
        }

        void MakeBlocks(const THolderFactory& holderFactory) {
            Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(Rows_)));
            Rows_ = 0;
            BuilderAllocatedSize_ = 0;

            for (size_t i = 0; i < Builders_.size(); ++i) {
                if (const auto builder = Builders_[i].get()) {
                    Values[i] = holderFactory.CreateArrowBlock(builder->Build(IsFinished_));
                }
            }

            FillArrays();
        }
    };
#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureState: public TLLVMFieldsStructureBlockState {
    private:
        using TBase = TLLVMFieldsStructureBlockState;
        llvm::IntegerType*const RowsType;
        llvm::IntegerType*const IsFinishedType;
        llvm::IntegerType*const BuilderAllocatedSizeType;
        llvm::IntegerType*const MaxBuilderAllocatedSizeType;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFieldsArray();
            result.emplace_back(RowsType);
            result.emplace_back(IsFinishedType);
            result.emplace_back(BuilderAllocatedSizeType);
            result.emplace_back(MaxBuilderAllocatedSizeType);
            return result;
        }

        llvm::Constant* GetRows() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields);
        }

        llvm::Constant* GetIsFinished() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields + 1);
        }

        llvm::Constant* GetBuilderAllocatedSize() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields + 2);
        }

        llvm::Constant* GetMaxBuilderAllocatedSize() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + BaseFields + 3);
        }

        TLLVMFieldsStructureState(llvm::LLVMContext& context, size_t width)
            : TBase(context, width)
            , RowsType(Type::getInt64Ty(Context))
            , IsFinishedType(Type::getInt1Ty(Context))
            , BuilderAllocatedSizeType(Type::getInt64Ty(Context))
            , MaxBuilderAllocatedSizeType(Type::getInt64Ty(Context))
        {}
    };
#endif
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ctx, Types_, MaxLength_, ctx.WideFields.data() + WideFieldsIndex_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue())
            MakeState(ctx, state);
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* const Flow_;
    const TVector<TType*> Types_;
    const size_t MaxLength_;
    const size_t Width_;
    const size_t WideFieldsIndex_;
};

class TFromBlocksWrapper : public TStatefulFlowCodegeneratorNode<TFromBlocksWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TFromBlocksWrapper>;
public:
    TFromBlocksWrapper(TComputationMutables& mutables, IComputationNode* flow, TType* itemType)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , ItemType_(itemType)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        for (auto& s = GetState(state, ctx);;) {
            if (auto item = s.GetValue(ctx.HolderFactory); !item.IsInvalid())
                return item;

            if (const auto input = Flow_->GetValue(ctx).Release(); input.IsSpecial())
                return input;
            else
                s.Reset(input);
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto statePtrType = PointerType::getUnqual(StructType::get(context));

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto read = BasicBlock::Create(context, "read", ctx.Func);
        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(work, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFromBlocksWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(work, block);

        block = work;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::GetValue));
        const auto getType = FunctionType::get(valueType, {statePtrType, ctx.GetFactory()->getType()}, false);
        const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", block);
        const auto value = CallInst::Create(getType, getPtr, {stateArg, ctx.GetFactory() }, "value", block);

        const auto result = PHINode::Create(valueType, 2U, "result", done);
        result->addIncoming(value, block);

        BranchInst::Create(read, done, IsInvalid(value, block), block);

        block = read;

        const auto input = GetNodeValue(Flow_, ctx, block);
        result->addIncoming(input, block);

        BranchInst::Create(done, init, IsSpecial(input, block), block);

        block = init;

        const auto setFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Reset));
        const auto setType = FunctionType::get(valueType, {statePtrType, valueType}, false);
        const auto setPtr = CastInst::Create(Instruction::IntToPtr, setFunc, PointerType::getUnqual(setType), "set", block);
        CallInst::Create(setType, setPtr, {stateArg, input }, "", block);

        BranchInst::Create(work, block);

        block = done;
        return result;
    }
#endif
private:
    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        TState(TMemoryUsageInfo* memInfo, TType* itemType, const NUdf::IPgBuilder& pgBuilder)
            : TComputationValue(memInfo)
            , Reader_(MakeBlockReader(TTypeInfoHelper(), itemType))
            , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), itemType, pgBuilder))
        {
        }

        NUdf::TUnboxedValuePod GetValue(const THolderFactory& holderFactory) {
            for (;;) {
                if (Arrays_.empty()) {
                    return NUdf::TUnboxedValuePod::Invalid();
                }
                if (Index_ < ui64(Arrays_.front()->length)) {
                    break;
                }
                Index_ = 0;
                Arrays_.pop_front();
            }
            return Converter_->MakeValue(Reader_->GetItem(*Arrays_.front(), Index_++), holderFactory);
        }

        void Reset(const NUdf::TUnboxedValuePod block) {
            const NUdf::TUnboxedValue v(block);
            const auto& datum = TArrowBlock::From(v).GetDatum();
            MKQL_ENSURE(datum.is_arraylike(), "Expecting array as FromBlocks argument");
            MKQL_ENSURE(Arrays_.empty(), "Not all input is processed");
            if (datum.is_array()) {
                Arrays_.push_back(datum.array());
            } else {
                for (const auto& chunk : datum.chunks()) {
                    Arrays_.push_back(chunk->data());
                }
            }
            Index_ = 0;
        }

    private:
        const std::unique_ptr<IBlockReader> Reader_;
        const std::unique_ptr<IBlockItemConverter> Converter_;
        TDeque<std::shared_ptr<arrow::ArrayData>> Arrays_;
        size_t Index_ = 0;
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ItemType_, ctx.Builder->GetPgBuilder());
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue())
            MakeState(ctx, state);
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationNode* const Flow_;
    TType* ItemType_;
};

class TWideFromBlocksWrapper : public TStatefulWideFlowCodegeneratorNode<TWideFromBlocksWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideFromBlocksWrapper>;
public:
    TWideFromBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<TType*>&& types)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Types_(std::move(types))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Types_.size() + 1U))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
        if (s.Index_ == s.Count_) do {
            if (const auto result = Flow_->FetchValues(ctx, fields); result != EFetchResult::One)
                return result;

            s.Index_ = 0;
            s.Count_ = GetBlockCount(s.Values_.back());
        } while (!s.Count_);

        s.Current_ = s.Index_;
        ++s.Index_;
        for (size_t i = 0; i < Types_.size(); ++i)
            if (const auto out = output[i])
                *out = s.Get(ctx.HolderFactory, i);

        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto width = Types_.size();
        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto arrayType = ArrayType::get(valueType, width);
        const auto ptrValuesType = PointerType::getUnqual(ArrayType::get(valueType, width));

        TLLVMFieldsStructureState stateFields(context, width);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Get));
        const auto getType = FunctionType::get(valueType, {statePtrType, ctx.GetFactory()->getType(), indexType}, false);
        const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", &ctx.Func->getEntryBlock().back());
        const auto stateOnStack = new AllocaInst(statePtrType, 0U, "state_on_stack", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantPointerNull::get(statePtrType), stateOnStack, &ctx.Func->getEntryBlock().back());

        const auto name = "GetBlockCount";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&GetBlockCount));
        const auto getCountType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(indexType, { valueType }, false):
            FunctionType::get(indexType, { ptrValueType }, false);
        const auto getCount = ctx.Codegen.GetModule().getOrInsertFunction(name, getCountType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideFromBlocksWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetCount() }, "count_ptr", block);
        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetIndex() }, "index_ptr", block);

        const auto count = new LoadInst(indexType, countPtr, "count", block);
        const auto index = new LoadInst(indexType, indexPtr, "index", block);

        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, index, "next", block);

        BranchInst::Create(more, work, next, block);

        block = more;

        const auto clearFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::ClearValues));
        const auto clearType = FunctionType::get(Type::getVoidTy(context), {statePtrType}, false);
        const auto clearPtr = CastInst::Create(Instruction::IntToPtr, clearFunc, PointerType::getUnqual(clearType), "clear", block);
        CallInst::Create(clearType, clearPtr, {stateArg}, "", block);

        const auto getres = GetNodeValues(Flow_, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        const auto result = PHINode::Create(statusType, 2U, "result", over);
        result->addIncoming(getres.first, block);

        BranchInst::Create(over, good, special, block);

        block = good;

        const auto countValue = getres.second.back()(ctx, block);
        const auto height = CallInst::Create(getCount, { WrapArgumentForWindows(countValue, ctx, block) }, "height", block);

        new StoreInst(height, countPtr, block);
        new StoreInst(ConstantInt::get(indexType, 0), indexPtr, block);

        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, ConstantInt::get(indexType, 0), height, "empty", block);

        BranchInst::Create(more, work, empty, block);

        block = work;

        const auto current = new LoadInst(indexType, indexPtr, "current", block);
        const auto currentPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetCurrent() }, "current_ptr", block);
        new StoreInst(current, currentPtr, block);
        const auto increment = BinaryOperator::CreateAdd(current, ConstantInt::get(indexType, 1), "increment", block);
        new StoreInst(increment, indexPtr, block);
        new StoreInst(stateArg, stateOnStack, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(width);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, width, getType, getPtr, indexType, arrayType, ptrValuesType, stateType, statePtrType, stateOnStack, getBlocks = getres.second](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();
                const auto init = BasicBlock::Create(context, "init", ctx.Func);
                const auto call = BasicBlock::Create(context, "call", ctx.Func);

                TLLVMFieldsStructureState stateFields(context, width);

                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto valuesPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetPointer() }, "values_ptr", block);
                const auto values = new LoadInst(ptrValuesType, valuesPtr, "values", block);
                const auto index = ConstantInt::get(indexType, idx);
                const auto pointer = GetElementPtrInst::CreateInBounds(arrayType, values, {  ConstantInt::get(indexType, 0), index }, "pointer", block);

                BranchInst::Create(call, init, HasValue(pointer, block), block);

                block = init;

                const auto value = getBlocks[idx](ctx, block);
                new StoreInst(value, pointer, block);
                AddRefBoxed(value, ctx, block);

                BranchInst::Create(call, block);

                block = call;

                return CallInst::Create(getType, getPtr, {stateArg, ctx.GetFactory(), index}, "get", block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
    struct TState : public TComputationValue<TState> {
        size_t Count_ = 0;
        size_t Index_ = 0;
        size_t Current_ = 0;
        NUdf::TUnboxedValue* Pointer_ = nullptr;
        TUnboxedValueVector Values_;
        std::vector<std::unique_ptr<IBlockReader>> Readers_;
        std::vector<std::unique_ptr<IBlockItemConverter>> Converters_;
        const std::vector<arrow::ValueDescr> ValuesDescr_;

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types)
            : TComputationValue(memInfo)
            , Values_(types.size() + 1)
            , ValuesDescr_(ToValueDescr(types))
        {
            Pointer_ = Values_.data();

            const auto& pgBuilder = ctx.Builder->GetPgBuilder();
            for (size_t i = 0; i < types.size(); ++i) {
                const TType* blockItemType = AS_TYPE(TBlockType, types[i])->GetItemType();
                Readers_.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
                Converters_.push_back(MakeBlockItemConverter(TTypeInfoHelper(), blockItemType, pgBuilder));
            }
        }

        void ClearValues() {
            Values_.assign(Values_.size(), NUdf::TUnboxedValuePod());
        }

        NUdf::TUnboxedValuePod Get(const THolderFactory& holderFactory, size_t idx) const {
            TBlockItem item;
            const auto& datum = TArrowBlock::From(Values_[idx]).GetDatum();
            ARROW_DEBUG_CHECK_DATUM_TYPES(ValuesDescr_[idx], datum.descr());
            if (datum.is_scalar()) {
                item = Readers_[idx]->GetScalarItem(*datum.scalar());
            } else {
                MKQL_ENSURE(datum.is_array(), "Expecting array");
                item = Readers_[idx]->GetItem(*datum.array(), Current_);
            }
            return Converters_[idx]->MakeValue(item, holderFactory);
        }
    };
#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
    private:
        using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
        llvm::IntegerType*const CountType;
        llvm::IntegerType*const IndexType;
        llvm::IntegerType*const CurrentType;
        llvm::PointerType*const PointerType;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFields();
            result.emplace_back(CountType);
            result.emplace_back(IndexType);
            result.emplace_back(CurrentType);
            result.emplace_back(PointerType);
            return result;
        }

        llvm::Constant* GetCount() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
        }

        llvm::Constant* GetIndex() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 1);
        }

        llvm::Constant* GetCurrent() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 2);
        }

        llvm::Constant* GetPointer() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 3);
        }

        TLLVMFieldsStructureState(llvm::LLVMContext& context, size_t width)
            : TBase(context)
            , CountType(Type::getInt64Ty(Context))
            , IndexType(Type::getInt64Ty(Context))
            , CurrentType(Type::getInt64Ty(Context))
            , PointerType(PointerType::getUnqual(ArrayType::get(Type::getInt128Ty(Context), width)))
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
        if (!state.HasValue()) {
            MakeState(ctx, state);

            const auto s = static_cast<TState*>(state.AsBoxed().Get());
            auto**const fields = ctx.WideFields.data() + WideFieldsIndex_;
            for (size_t i = 0; i <= Types_.size(); ++i) {
                fields[i] = &s->Values_[i];
            }
            return *s;
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    IComputationWideFlowNode* const Flow_;
    const TVector<TType*> Types_;
    const size_t WideFieldsIndex_;
};

class TPrecomputedArrowNode : public IArrowKernelComputationNode {
public:
    TPrecomputedArrowNode(const arrow::Datum& datum, TStringBuf kernelName)
        : Kernel_({}, datum.type(), [datum](arrow::compute::KernelContext*, const arrow::compute::ExecBatch&, arrow::Datum* res) {
            *res = datum;
            return arrow::Status::OK();
        })
        , KernelName_(kernelName)
    {
        Kernel_.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
        Kernel_.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    }

    TStringBuf GetKernelName() const final {
        return KernelName_;
    }

    const arrow::compute::ScalarKernel& GetArrowKernel() const {
        return Kernel_;
    }

    const std::vector<arrow::ValueDescr>& GetArgsDesc() const {
        return EmptyDesc_;
    }

    const IComputationNode* GetArgument(ui32 index) const {
        Y_UNUSED(index);
        ythrow yexception() << "No input arguments";
    }
private:
    arrow::compute::ScalarKernel Kernel_;
    const TStringBuf KernelName_;
    const std::vector<arrow::ValueDescr> EmptyDesc_;
};

class TAsScalarWrapper : public TMutableCodegeneratorNode<TAsScalarWrapper> {
using TBaseComputation = TMutableCodegeneratorNode<TAsScalarWrapper>;
public:
    TAsScalarWrapper(TComputationMutables& mutables, IComputationNode* arg, TType* type)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Arg_(arg)
        , Type_(type)
    {
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(Type_, arrowType), "Unsupported type of scalar");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return AsScalar(Arg_->GetValue(ctx).Release(), ctx);
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto value = GetNodeValue(Arg_, ctx, block);

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto asScalarFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TAsScalarWrapper::AsScalar));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto asScalarType = FunctionType::get(Type::getInt128Ty(context), {self->getType(), value->getType(), ctx.Ctx->getType()}, false);
            const auto asScalarFuncPtr = CastInst::Create(Instruction::IntToPtr, asScalarFunc, PointerType::getUnqual(asScalarType), "function", block);
            return CallInst::Create(asScalarType, asScalarFuncPtr, {self, value, ctx.Ctx}, "scalar", block);
        } else {
            const auto valuePtr = new AllocaInst(value->getType(), 0U, "value", block);
            new StoreInst(value, valuePtr, block);
            const auto asScalarType = FunctionType::get(Type::getVoidTy(context), {self->getType(), valuePtr->getType(), valuePtr->getType(), ctx.Ctx->getType()}, false);
            const auto asScalarFuncPtr = CastInst::Create(Instruction::IntToPtr, asScalarFunc, PointerType::getUnqual(asScalarType), "function", block);
            CallInst::Create(asScalarType, asScalarFuncPtr, {self, valuePtr, valuePtr, ctx.Ctx}, "", block);
            return new LoadInst(value->getType(), valuePtr, "result", block);
        }
    }
#endif
private:
    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        return std::make_unique<TPrecomputedArrowNode>(DoAsScalar(Arg_->GetValue(ctx).Release(), ctx), "AsScalar");
    }

    arrow::Datum DoAsScalar(const NUdf::TUnboxedValuePod value, TComputationContext& ctx) const {
        const NUdf::TUnboxedValue v(value);
        return ConvertScalar(Type_, v, ctx.ArrowMemoryPool);
    }

    NUdf::TUnboxedValuePod AsScalar(const NUdf::TUnboxedValuePod value, TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateArrowBlock(DoAsScalar(value, ctx));
    }

    void RegisterDependencies() const final {
        DependsOn(Arg_);
    }

    IComputationNode* const Arg_;
    TType* Type_;
};

class TReplicateScalarWrapper : public TMutableCodegeneratorNode<TReplicateScalarWrapper> {
using TBaseComputation = TMutableCodegeneratorNode<TReplicateScalarWrapper>;
public:
    TReplicateScalarWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* count, TType* type)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Value_(value)
        , Count_(count)
        , Type_(type)
    {
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(Type_, arrowType), "Unsupported type of scalar");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto value = Value_->GetValue(ctx).Release();
        const auto count = Count_->GetValue(ctx).Release();
        return Replicate(value, count, ctx);
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto value = GetNodeValue(Value_, ctx, block);
        const auto count = GetNodeValue(Count_, ctx, block);

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto replicateFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TReplicateScalarWrapper::Replicate));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto replicateType = FunctionType::get(Type::getInt128Ty(context), {self->getType(), value->getType(), count->getType(), ctx.Ctx->getType()}, false);
            const auto replicateFuncPtr = CastInst::Create(Instruction::IntToPtr, replicateFunc, PointerType::getUnqual(replicateType), "function", block);
            return CallInst::Create(replicateType, replicateFuncPtr, {self, value, count, ctx.Ctx}, "replicate", block);
        } else {
            const auto valuePtr = new AllocaInst(value->getType(), 0U, "value", block);
            const auto countPtr = new AllocaInst(count->getType(), 0U, "count", block);
            new StoreInst(value, valuePtr, block);
            new StoreInst(count, countPtr, block);
            const auto replicateType = FunctionType::get(Type::getVoidTy(context), {self->getType(), valuePtr->getType(), valuePtr->getType(), countPtr->getType(), ctx.Ctx->getType()}, false);
            const auto replicateFuncPtr = CastInst::Create(Instruction::IntToPtr, replicateFunc, PointerType::getUnqual(replicateType), "function", block);
            CallInst::Create(replicateType, replicateFuncPtr, {self, valuePtr, valuePtr, countPtr, ctx.Ctx}, "", block);
            return new LoadInst(value->getType(), valuePtr, "result", block);
        }
    }
#endif
private:
    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        const auto value = Value_->GetValue(ctx).Release();
        const auto count = Count_->GetValue(ctx).Release();
        return std::make_unique<TPrecomputedArrowNode>(DoReplicate(value, count, ctx), "ReplicateScalar");
    }

    arrow::Datum DoReplicate(const NUdf::TUnboxedValuePod val, const NUdf::TUnboxedValuePod cnt, TComputationContext& ctx) const {
        const NUdf::TUnboxedValue v(val), c(cnt);
        const auto value = TArrowBlock::From(v).GetDatum().scalar();
        const ui64 count = TArrowBlock::From(c).GetDatum().scalar_as<arrow::UInt64Scalar>().value;

        const auto reader = MakeBlockReader(TTypeInfoHelper(), Type_);
        const auto builder = MakeArrayBuilder(TTypeInfoHelper(), Type_, ctx.ArrowMemoryPool, count, &ctx.Builder->GetPgBuilder());

        TBlockItem item = reader->GetScalarItem(*value);
        builder->Add(item, count);
        return builder->Build(true);
    }

    NUdf::TUnboxedValuePod Replicate(const NUdf::TUnboxedValuePod value, const NUdf::TUnboxedValuePod count, TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateArrowBlock(DoReplicate(value, count, ctx));
    }

    void RegisterDependencies() const final {
        DependsOn(Value_);
        DependsOn(Count_);
    }

    IComputationNode* const Value_;
    IComputationNode* const Count_;
    TType* Type_;
};

class TBlockExpandChunkedWrapper : public TStatefulWideFlowCodegeneratorNode<TBlockExpandChunkedWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TBlockExpandChunkedWrapper>;
public:
    TBlockExpandChunkedWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, size_t width)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Width_(width)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Width_))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& s = GetState(state, ctx);
        if (!s.Count) {
            const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
            s.ClearValues();
            if (const auto result = Flow_->FetchValues(ctx, fields); result != EFetchResult::One)
                return result;
            s.FillArrays();
        }

        const auto sliceSize = s.Slice();
        for (size_t i = 0; i < Width_; ++i) {
            if (const auto out = output[i]) {
                *out = s.Get(sliceSize, ctx.HolderFactory, i);
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto arrayType = ArrayType::get(valueType, Width_);
        const auto ptrValuesType = PointerType::getUnqual(arrayType);

        TLLVMFieldsStructureBlockState stateFields(context, Width_);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TBlockState::Get));
        const auto getType = FunctionType::get(valueType, {statePtrType, indexType, ctx.GetFactory()->getType(), indexType}, false);
        const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", atTop);

        const auto heightPtr = new AllocaInst(indexType, 0U, "height_ptr", atTop);
        const auto stateOnStack = new AllocaInst(statePtrType, 0U, "state_on_stack", atTop);

        new StoreInst(ConstantInt::get(indexType, 0), heightPtr, atTop);
        new StoreInst(ConstantPointerNull::get(statePtrType), stateOnStack, atTop);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto read = BasicBlock::Create(context, "read", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto fill = BasicBlock::Create(context, "fill", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TBlockExpandChunkedWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetCount() }, "count_ptr", block);
        const auto count = new LoadInst(indexType, countPtr, "count", block);

        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, ConstantInt::get(indexType, 0), "next", block);

        BranchInst::Create(read, fill, next, block);

        block = read;

        const auto clearFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TBlockState::ClearValues));
        const auto clearType = FunctionType::get(Type::getVoidTy(context), {statePtrType}, false);
        const auto clearPtr = CastInst::Create(Instruction::IntToPtr, clearFunc, PointerType::getUnqual(clearType), "clear", block);
        CallInst::Create(clearType, clearPtr, {stateArg}, "", block);

        const auto getres = GetNodeValues(Flow_, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        const auto result = PHINode::Create(statusType, 2U, "result", over);
        result->addIncoming(getres.first, block);

        BranchInst::Create(over, work, special, block);

        block = work;

        const auto valuesPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetPointer() }, "values_ptr", block);
        const auto values = new LoadInst(ptrValuesType, valuesPtr, "values", block);
        Value* array = UndefValue::get(arrayType);
        for (auto idx = 0U; idx < getres.second.size(); ++idx) {
            const auto value = getres.second[idx](ctx, block);
            AddRefBoxed(value, ctx, block);
            array = InsertValueInst::Create(array, value, {idx}, (TString("value_") += ToString(idx)).c_str(), block);
        }
        new StoreInst(array, values, block);

        const auto fillArraysFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TBlockState::FillArrays));
        const auto fillArraysType = FunctionType::get(Type::getVoidTy(context), {statePtrType}, false);
        const auto fillArraysPtr = CastInst::Create(Instruction::IntToPtr, fillArraysFunc, PointerType::getUnqual(fillArraysType), "fill_arrays_func", block);
        CallInst::Create(fillArraysType, fillArraysPtr, {stateArg}, "", block);

        BranchInst::Create(fill, block);

        block = fill;

        const auto sliceFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TBlockState::Slice));
        const auto sliceType = FunctionType::get(indexType, {statePtrType}, false);
        const auto slicePtr = CastInst::Create(Instruction::IntToPtr, sliceFunc, PointerType::getUnqual(sliceType), "slice_func", block);
        const auto slice = CallInst::Create(sliceType, slicePtr, {stateArg}, "slice", block);
        new StoreInst(slice, heightPtr, block);
        new StoreInst(stateArg, stateOnStack, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(Width_);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, getType, getPtr, heightPtr, indexType, statePtrType, stateOnStack](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto heightArg = new LoadInst(indexType, heightPtr, "height", block);
                return CallInst::Create(getType, getPtr, {stateArg, heightArg, ctx.GetFactory(), ConstantInt::get(indexType, idx)}, "get", block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TBlockState>(Width_);
    }

    TBlockState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);

            auto& s = *static_cast<TBlockState*>(state.AsBoxed().Get());
            const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
            for (size_t i = 0; i < Width_; ++i)
                fields[i] = &s.Values[i];
            return s;
        }
        return *static_cast<TBlockState*>(state.AsBoxed().Get());
    }

    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode* const Flow_;
    const size_t Width_;
    const size_t WideFieldsIndex_;
};

class TBlockExpandChunkedStreamWrapper : public TMutableComputationNode<TBlockExpandChunkedStreamWrapper> {
using TBaseComputation =  TMutableComputationNode<TBlockExpandChunkedStreamWrapper>;
class TExpanderState : public TComputationValue<TExpanderState> {
using TBase = TComputationValue<TExpanderState>;
public:
    TExpanderState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& stream, size_t width)
        : TBase(memInfo), HolderFactory_(ctx.HolderFactory), State_(ctx.HolderFactory.Create<TBlockState>(width)), Stream_(stream) {}

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
        auto& s = *static_cast<TBlockState*>(State_.AsBoxed().Get());
        if (!s.Count) {
            s.ClearValues();
            auto result = Stream_.WideFetch(s.Values.data(), width);
            if (NUdf::EFetchStatus::Ok != result) {
                return result;
            }
            s.FillArrays();
        }
        
        const auto sliceSize = s.Slice();
        for (size_t i = 0; i < width; ++i) {
            output[i] = s.Get(sliceSize, HolderFactory_, i);
        }
        return NUdf::EFetchStatus::Ok;
    }

private:
    const THolderFactory& HolderFactory_;
    NUdf::TUnboxedValue State_;
    NUdf::TUnboxedValue Stream_;
};
public:
    TBlockExpandChunkedStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, size_t width)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
        , Width_(width) {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TExpanderState>(ctx, std::move(Stream_->GetValue(ctx)), Width_);
    }
    void RegisterDependencies() const override {}
private:
    IComputationNode* const Stream_;
    const size_t Width_;
};

} // namespace

IComputationNode* WrapToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    return new TToBlocksWrapper(LocateNode(ctx.NodeLocator, callable, 0), flowType->GetItemType());
}

IComputationNode* WrapWideToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    TVector<TType*> items(wideComponents.begin(), wideComponents.end());
    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideToBlocksWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto blockType = AS_TYPE(TBlockType, flowType->GetItemType());
    return new TFromBlocksWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), blockType->GetItemType());
}

IComputationNode* WrapWideFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    MKQL_ENSURE(wideComponents.size() > 0, "Expected at least one column");
    TVector<TType*> items;
    for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
        items.push_back(AS_TYPE(TBlockType, wideComponents[i]));
    }

    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");
    return new TWideFromBlocksWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapAsScalar(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    return new TAsScalarWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), callable.GetInput(0).GetStaticType());
}

IComputationNode* WrapReplicateScalar(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args, got " << callable.GetInputsCount());

    const auto valueType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    MKQL_ENSURE(valueType->GetShape() == TBlockType::EShape::Scalar, "Expecting scalar as first arg");

    const auto value = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    return new TReplicateScalarWrapper(ctx.Mutables, value, count, valueType->GetItemType());
}

IComputationNode* WrapBlockExpandChunked(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());
    if (callable.GetInput(0).GetStaticType()->IsStream()) {
        const auto streamType = AS_TYPE(TStreamType, callable.GetInput(0).GetStaticType());
        const auto wideComponents = GetWideComponents(streamType);
        const auto computation = dynamic_cast<IComputationNode*>(LocateNode(ctx.NodeLocator, callable, 0));

        MKQL_ENSURE(computation != nullptr, "Expected computation node");
        return new TBlockExpandChunkedStreamWrapper(ctx.Mutables, computation, wideComponents.size());
    } else {
        const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
        const auto wideComponents = GetWideComponents(flowType);

        const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
        MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");
        return new TBlockExpandChunkedWrapper(ctx.Mutables, wideFlow, wideComponents.size());
    }
}

}
}
