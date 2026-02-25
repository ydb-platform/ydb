#include "mkql_blocks.h"

#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_impl_codegen.h> // Y_IGNORE

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <yql/essentials/core/sql_types/block.h>
#include <yql/essentials/parser/pg_wrapper/interface/arrow.h>

#include <arrow/scalar.h>
#include <arrow/array.h>
#include <arrow/datum.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TToBlocksWrapper: public TStatelessFlowComputationNode<TToBlocksWrapper> {
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

struct TWideToBlocksState: public TBlockState {
    size_t Rows_ = 0;
    bool IsFinished_ = false;
    size_t BuilderAllocatedSize_ = 0;
    size_t MaxBuilderAllocatedSize_ = 0;
    std::vector<std::unique_ptr<IArrayBuilder>> Builders_;
    static const size_t MaxAllocatedFactor_ = 4;

    TWideToBlocksState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types, size_t maxLength)
        : TBlockState(memInfo, types.size() + 1U)
        , Builders_(types.size())
    {
        for (size_t i = 0; i < types.size(); ++i) {
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

class TWideToBlocksFlowWrapper: public TStatefulWideFlowCodegeneratorNode<TWideToBlocksFlowWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideToBlocksFlowWrapper>;
    using TState = TWideToBlocksState;

public:
    TWideToBlocksFlowWrapper(TComputationMutables& mutables,
                             IComputationWideFlowNode* flow,
                             TVector<TType*>&& types)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Types_(std::move(types))
        , MaxLength_(CalcBlockLen(std::accumulate(Types_.cbegin(), Types_.cend(), 0ULL, [](size_t max, const TType* type) { return std::max(max, CalcMaxBlockItemSize(type)); })))
        , Width_(Types_.size())
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Width_))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
                             TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        auto& s = GetState(state, ctx);
        const auto fields = ctx.WideFields.data() + WideFieldsIndex_;

        if (!s.Count) {
            if (!s.IsFinished_) {
                do {
                    switch (Flow_->FetchValues(ctx, fields)) {
                        case EFetchResult::One:
                            for (size_t i = 0; i < Types_.size(); ++i) {
                                s.Add(s.Values[i], i);
                            }
                            continue;
                        case EFetchResult::Yield:
                            return EFetchResult::Yield;
                        case EFetchResult::Finish:
                            s.IsFinished_ = true;
                            break;
                    }
                    break;
                } while (++s.Rows_ < MaxLength_ && s.BuilderAllocatedSize_ <= s.MaxBuilderAllocatedSize_);
            }

            if (s.Rows_) {
                s.MakeBlocks(ctx.HolderFactory);
            } else {
                return EFetchResult::Finish;
            }
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TWideToBlocksFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetCount()}, "count_ptr", block);
        const auto count = new LoadInst(indexType, countPtr, "count", block);
        const auto none = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, ConstantInt::get(indexType, 0), "none", block);

        BranchInst::Create(more, fill, none, block);

        block = more;

        const auto rowsPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetRows()}, "rows_ptr", block);
        const auto finishedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetIsFinished()}, "is_finished_ptr", block);
        const auto allocatedSizePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetBuilderAllocatedSize()}, "allocated_size_ptr", block);
        const auto maxAllocatedSizePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetMaxBuilderAllocatedSize()}, "max_allocated_size_ptr", block);
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
            EmitFunctionCall<&TState::Add>(Type::getVoidTy(context), {stateArg, value, ConstantInt::get(indexType, idx)}, ctx, block);
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

        EmitFunctionCall<&TState::MakeBlocks>(indexType, {stateArg, ctx.GetFactory()}, ctx, block);

        BranchInst::Create(fill, block);

        block = fill;

        const auto slice = EmitFunctionCall<&TState::Slice>(indexType, {stateArg}, ctx, block);
        new StoreInst(slice, heightPtr, block);
        new StoreInst(stateArg, stateOnStack, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(Types_.size() + 1U);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, valueType, heightPtr, indexType, statePtrType, stateOnStack](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto heightArg = new LoadInst(indexType, heightPtr, "height", block);
                return EmitFunctionCall<&TState::Get>(valueType, {stateArg, heightArg, ctx.GetFactory(), ConstantInt::get(indexType, idx)}, ctx, block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureState: public TLLVMFieldsStructureBlockState {
    private:
        using TBase = TLLVMFieldsStructureBlockState;
        llvm::IntegerType* const RowsType;
        llvm::IntegerType* const IsFinishedType;
        llvm::IntegerType* const BuilderAllocatedSizeType;
        llvm::IntegerType* const MaxBuilderAllocatedSizeType;

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
        {
        }
    };
#endif
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ctx, Types_, MaxLength_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
            auto& s = *static_cast<TState*>(state.AsBoxed().Get());
            const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
            for (size_t i = 0; i < Width_; ++i) {
                fields[i] = &s.Values[i];
            }
            return s;
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* const Flow_;
    const TVector<TType*> Types_;
    const size_t MaxLength_;
    const size_t Width_;
    const size_t WideFieldsIndex_;
};

class TWideToBlocksStreamWrapper: public TMutableComputationNode<TWideToBlocksStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TWideToBlocksStreamWrapper>;
    using TState = TWideToBlocksState;

public:
    TWideToBlocksStreamWrapper(TComputationMutables& mutables,
                               IComputationNode* stream,
                               TVector<TType*>&& types)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
        , Types_(std::move(types))
        , MaxLength_(CalcBlockLen(std::accumulate(Types_.cbegin(), Types_.cend(), 0ULL, [](size_t max, const TType* type) { return std::max(max, CalcMaxBlockItemSize(type)); })))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto state = ctx.HolderFactory.Create<TState>(ctx, Types_, MaxLength_);
        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(state),
                                                      std::move(Stream_->GetValue(ctx)),
                                                      MaxLength_);
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory,
                     NUdf::TUnboxedValue&& blockState, NUdf::TUnboxedValue&& stream,
                     const size_t maxLength)
            : TBase(memInfo)
            , BlockState_(blockState)
            , Stream_(stream)
            , MaxLength_(maxLength)
            , HolderFactory_(holderFactory)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            auto& blockState = *static_cast<TState*>(BlockState_.AsBoxed().Get());
            auto* inputFields = blockState.Pointer;
            const size_t inputWidth = blockState.Values.size() - 1;

            if (!blockState.Count) {
                if (!blockState.IsFinished_) {
                    do {
                        switch (Stream_.WideFetch(inputFields, inputWidth)) {
                            case NUdf::EFetchStatus::Ok:
                                for (size_t i = 0; i < inputWidth; i++) {
                                    blockState.Add(blockState.Values[i], i);
                                }
                                continue;
                            case NUdf::EFetchStatus::Yield:
                                return NUdf::EFetchStatus::Yield;
                            case NUdf::EFetchStatus::Finish:
                                blockState.IsFinished_ = true;
                                break;
                        }
                        break;
                    } while (++blockState.Rows_ < MaxLength_ && blockState.BuilderAllocatedSize_ <= blockState.MaxBuilderAllocatedSize_);
                }
                if (blockState.Rows_) {
                    blockState.MakeBlocks(HolderFactory_);
                } else {
                    return NUdf::EFetchStatus::Finish;
                }
            }

            const auto sliceSize = blockState.Slice();
            for (size_t i = 0; i < width; i++) {
                output[i] = blockState.Get(sliceSize, HolderFactory_, i);
            }
            return NUdf::EFetchStatus::Ok;
        }

        NUdf::TUnboxedValue BlockState_;
        NUdf::TUnboxedValue Stream_;
        const size_t MaxLength_;
        const THolderFactory& HolderFactory_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const TVector<TType*> Types_;
    const size_t MaxLength_;
};

class TListToBlocksState: public TBlockState {
public:
    TListToBlocksState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types, size_t blockLengthIndex, size_t maxLength)
        : TBlockState(memInfo, types.size(), blockLengthIndex)
        , Builders_(types.size())
        , BlockLengthIndex_(blockLengthIndex)
        , MaxLength_(maxLength)
    {
        for (size_t i = 0; i < types.size(); ++i) {
            if (i == blockLengthIndex) {
                continue;
            }
            Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), types[i], ctx.ArrowMemoryPool, maxLength, &ctx.Builder->GetPgBuilder(), &BuilderAllocatedSize_);
        }
        MaxBuilderAllocatedSize_ = MaxAllocatedFactor_ * BuilderAllocatedSize_;
    }

    void AddRow(const NUdf::TUnboxedValuePod& row) {
        auto items = row.GetElements();
        size_t inputStructIdx = 0;
        for (size_t i = 0; i < Builders_.size(); i++) {
            if (i == BlockLengthIndex_) {
                continue;
            }
            Builders_[i]->Add(items[inputStructIdx++]);
        }
        Rows_++;
    }

    void MakeBlocks(const THolderFactory& holderFactory) {
        Values[BlockLengthIndex_] = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(Rows_)));
        Rows_ = 0;
        BuilderAllocatedSize_ = 0;

        for (size_t i = 0; i < Builders_.size(); ++i) {
            if (i == BlockLengthIndex_) {
                continue;
            }
            Values[i] = holderFactory.CreateArrowBlock(Builders_[i]->Build(IsFinished_));
        }
        FillArrays();
    }

    void Finish() {
        IsFinished_ = true;
    }

    bool IsNotFull() const {
        return Rows_ < MaxLength_ && BuilderAllocatedSize_ <= MaxBuilderAllocatedSize_;
    }

    bool IsFinished() const {
        return IsFinished_;
    }

    bool HasBlocks() const {
        return Count > 0;
    }

    bool IsEmpty() const {
        return Rows_ == 0;
    }

private:
    size_t Rows_ = 0;
    bool IsFinished_ = false;

    size_t BuilderAllocatedSize_ = 0;
    size_t MaxBuilderAllocatedSize_ = 0;

    std::vector<std::unique_ptr<IArrayBuilder>> Builders_;

    const size_t BlockLengthIndex_;
    const size_t MaxLength_;
    static const size_t MaxAllocatedFactor_ = 4;
};

class TListToBlocksWrapper: public TMutableComputationNode<TListToBlocksWrapper> {
    using TBaseComputation = TMutableComputationNode<TListToBlocksWrapper>;

public:
    TListToBlocksWrapper(TComputationMutables& mutables,
                         IComputationNode* list,
                         TStructType* structType)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , List_(list)
    {
        for (size_t i = 0; i < structType->GetMembersCount(); i++) {
            if (structType->GetMemberName(i) == NYql::BlockLengthColumnName) {
                BlockLengthIndex_ = i;
                Types_.push_back(nullptr);
                continue;
            }
            Types_.push_back(AS_TYPE(TBlockType, structType->GetMemberType(i))->GetItemType());
        }

        MaxLength_ = CalcBlockLen(std::accumulate(Types_.cbegin(), Types_.cend(), 0ULL, [](size_t max, const TType* type) {
            if (!type) {
                return max;
            }
            return std::max(max, CalcMaxBlockItemSize(type));
        }));
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TListToBlocksValue>(
            ctx,
            Types_,
            BlockLengthIndex_,
            List_->GetValue(ctx),
            MaxLength_);
    }

private:
    class TListToBlocksValue: public TCustomListValue {
        using TState = TListToBlocksState;

    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory, NUdf::TUnboxedValue&& blockState, NUdf::TUnboxedValue&& iter)
                : TComputationValue<TIterator>(memInfo)
                , HolderFactory_(holderFactory)
                , BlockState_(std::move(blockState))
                , Iter_(std::move(iter))
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                auto& blockState = *static_cast<TState*>(BlockState_.AsBoxed().Get());
                const size_t structSize = blockState.Values.size();

                if (!blockState.HasBlocks()) {
                    while (!blockState.IsFinished() && blockState.IsNotFull()) {
                        if (Iter_.Next(Row_)) {
                            blockState.AddRow(Row_);
                        } else {
                            blockState.Finish();
                        }
                    }
                    if (blockState.IsEmpty()) {
                        return false;
                    }
                    blockState.MakeBlocks(HolderFactory_);
                }

                NUdf::TUnboxedValue* items = nullptr;
                value = HolderFactory_.CreateDirectArrayHolder(structSize, items);

                const auto sliceSize = blockState.Slice();
                for (size_t i = 0; i < structSize; i++) {
                    items[i] = blockState.Get(sliceSize, HolderFactory_, i);
                }

                return true;
            }

        private:
            const THolderFactory& HolderFactory_;

            const NUdf::TUnboxedValue BlockState_;
            const NUdf::TUnboxedValue Iter_;

            NUdf::TUnboxedValue Row_;
        };

        TListToBlocksValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
                           const TVector<TType*>& types, ui32 blockLengthIndex, NUdf::TUnboxedValue&& list, size_t maxLength)
            : TCustomListValue(memInfo)
            , CompCtx_(ctx)
            , Types_(types)
            , BlockLengthIndex_(blockLengthIndex)
            , List_(std::move(list))
            , MaxLength_(maxLength)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            auto state = CompCtx_.HolderFactory.Create<TState>(CompCtx_, Types_, BlockLengthIndex_, MaxLength_);
            return CompCtx_.HolderFactory.Create<TIterator>(CompCtx_.HolderFactory, std::move(state), List_.GetListIterator());
        }

        bool HasListItems() const final {
            if (!HasItems_.has_value()) {
                HasItems_ = List_.HasListItems();
            }
            return *HasItems_;
        }

    private:
        TComputationContext& CompCtx_;

        const TVector<TType*>& Types_;
        size_t BlockLengthIndex_ = 0;

        NUdf::TUnboxedValue List_;
        const size_t MaxLength_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(List_);
    }

private:
    TVector<TType*> Types_;
    size_t BlockLengthIndex_ = 0;

    IComputationNode* const List_;

    size_t MaxLength_ = 0;
};

class TFromBlocksWrapper: public TStatefulFlowCodegeneratorNode<TFromBlocksWrapper> {
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
            if (auto item = s.GetValue(ctx.HolderFactory); !item.IsInvalid()) {
                return item;
            }

            if (const auto input = Flow_->GetValue(ctx); input.IsSpecial()) {
                return input;
            } else {
                s.Reset(input);
            }
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

        BranchInst::Create(make, work, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TFromBlocksWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(work, block);

        block = work;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto value = EmitFunctionCall<&TState::GetValue>(valueType, {stateArg, ctx.GetFactory()}, ctx, block);

        const auto result = PHINode::Create(valueType, 2U, "result", done);
        result->addIncoming(value, block);

        BranchInst::Create(read, done, IsInvalid(value, block, context), block);

        block = read;

        const auto input = GetNodeValue(Flow_, ctx, block);
        result->addIncoming(input, block);

        BranchInst::Create(done, init, IsSpecial(input, block, context), block);

        block = init;

        EmitFunctionCall<&TState::Reset>(valueType, {stateArg, input}, ctx, block);

        ValueCleanup(EValueRepresentation::Any, input, ctx, block);

        BranchInst::Create(work, block);

        block = done;
        return result;
    }
#endif
private:
    struct TState: public TComputationValue<TState> {
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
            const auto& datum = TArrowBlock::From(block).GetDatum();
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
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationNode* const Flow_;
    TType* ItemType_;
};

struct TWideFromBlocksState: public TComputationValue<TWideFromBlocksState> {
    size_t Count_ = 0;
    size_t Index_ = 0;
    size_t Current_ = 0;
    NUdf::TUnboxedValue* Pointer_ = nullptr;
    TUnboxedValueVector Values_;
    std::vector<std::unique_ptr<IBlockReader>> Readers_;
    std::vector<std::unique_ptr<IBlockItemConverter>> Converters_;
    const std::vector<arrow::ValueDescr> ValuesDescr_;

    TWideFromBlocksState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types)
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

class TWideFromBlocksFlowWrapper: public TStatefulWideFlowCodegeneratorNode<TWideFromBlocksFlowWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideFromBlocksFlowWrapper>;
    using TState = TWideFromBlocksState;

public:
    TWideFromBlocksFlowWrapper(TComputationMutables& mutables,
                               IComputationWideFlowNode* flow,
                               TVector<TType*>&& types)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Types_(std::move(types))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Types_.size() + 1U))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
                             TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        auto& s = GetState(state, ctx);
        const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
        if (s.Index_ == s.Count_) {
            do {
                if (const auto result = Flow_->FetchValues(ctx, fields); result != EFetchResult::One) {
                    return result;
                }

                s.Index_ = 0;
                s.Count_ = GetBlockCount(s.Values_.back());
            } while (!s.Count_);
        }

        s.Current_ = s.Index_;
        ++s.Index_;
        for (size_t i = 0; i < Types_.size(); ++i) {
            if (const auto out = output[i]) {
                *out = s.Get(ctx.HolderFactory, i);
            }
        }

        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto width = Types_.size();
        const auto valueType = Type::getInt128Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto arrayType = ArrayType::get(valueType, width);
        const auto ptrValuesType = PointerType::getUnqual(ArrayType::get(valueType, width));

        TLLVMFieldsStructureState stateFields(context, width);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto stateOnStack = new AllocaInst(statePtrType, 0U, "state_on_stack", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantPointerNull::get(statePtrType), stateOnStack, &ctx.Func->getEntryBlock().back());

        const auto name = "GetBlockCount";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&GetBlockCount));
        const auto getCountType = FunctionType::get(indexType, {valueType}, false);
        const auto getCount = ctx.Codegen.GetModule().getOrInsertFunction(name, getCountType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TWideFromBlocksFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetCount()}, "count_ptr", block);
        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetIndex()}, "index_ptr", block);

        const auto count = new LoadInst(indexType, countPtr, "count", block);
        const auto index = new LoadInst(indexType, indexPtr, "index", block);

        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, index, "next", block);

        BranchInst::Create(more, work, next, block);

        block = more;

        EmitFunctionCall<&TState::ClearValues>(Type::getVoidTy(context), {stateArg}, ctx, block);

        const auto getres = GetNodeValues(Flow_, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        const auto result = PHINode::Create(statusType, 2U, "result", over);
        result->addIncoming(getres.first, block);

        BranchInst::Create(over, good, special, block);

        block = good;

        const auto countValue = getres.second.back()(ctx, block);
        const auto height = CallInst::Create(getCount, {countValue}, "height", block);

        ValueCleanup(EValueRepresentation::Any, countValue, ctx, block);

        new StoreInst(height, countPtr, block);
        new StoreInst(ConstantInt::get(indexType, 0), indexPtr, block);

        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, ConstantInt::get(indexType, 0), height, "empty", block);

        BranchInst::Create(more, work, empty, block);

        block = work;

        const auto current = new LoadInst(indexType, indexPtr, "current", block);
        const auto currentPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetCurrent()}, "current_ptr", block);
        new StoreInst(current, currentPtr, block);
        const auto increment = BinaryOperator::CreateAdd(current, ConstantInt::get(indexType, 1), "increment", block);
        new StoreInst(increment, indexPtr, block);
        new StoreInst(stateArg, stateOnStack, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(width);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, width, valueType, indexType, arrayType, ptrValuesType, stateType, statePtrType, stateOnStack, getBlocks = getres.second](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();
                const auto init = BasicBlock::Create(context, "init", ctx.Func);
                const auto call = BasicBlock::Create(context, "call", ctx.Func);

                TLLVMFieldsStructureState stateFields(context, width);

                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto valuesPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetPointer()}, "values_ptr", block);
                const auto values = new LoadInst(ptrValuesType, valuesPtr, "values", block);
                const auto index = ConstantInt::get(indexType, idx);
                const auto pointer = GetElementPtrInst::CreateInBounds(arrayType, values, {ConstantInt::get(indexType, 0), index}, "pointer", block);

                BranchInst::Create(call, init, HasValue(pointer, block, context), block);

                block = init;

                const auto value = getBlocks[idx](ctx, block);
                new StoreInst(value, pointer, block);
                AddRefBoxed(value, ctx, block);

                BranchInst::Create(call, block);

                block = call;

                return EmitFunctionCall<&TState::Get>(valueType, {stateArg, ctx.GetFactory(), index}, ctx, block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
    private:
        using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
        llvm::IntegerType* const CountType;
        llvm::IntegerType* const IndexType;
        llvm::IntegerType* const CurrentType;
        llvm::PointerType* const PointerType;

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
        {
        }
    };
#endif
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ctx, Types_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);

            const auto s = static_cast<TState*>(state.AsBoxed().Get());
            auto** const fields = ctx.WideFields.data() + WideFieldsIndex_;
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

class TWideFromBlocksStreamWrapper: public TMutableComputationNode<TWideFromBlocksStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TWideFromBlocksStreamWrapper>;
    using TState = TWideFromBlocksState;

public:
    TWideFromBlocksStreamWrapper(TComputationMutables& mutables,
                                 IComputationNode* stream,
                                 TVector<TType*>&& types)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
        , Types_(std::move(types))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto state = ctx.HolderFactory.Create<TState>(ctx, Types_);
        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(state),
                                                      std::move(Stream_->GetValue(ctx)));
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory,
                     NUdf::TUnboxedValue&& blockState, NUdf::TUnboxedValue&& stream)
            : TBase(memInfo)
            , BlockState_(blockState)
            , Stream_(stream)
            , HolderFactory_(holderFactory)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            auto& blockState = *static_cast<TState*>(BlockState_.AsBoxed().Get());
            auto* inputFields = blockState.Pointer_;
            const size_t inputWidth = blockState.Values_.size();

            if (blockState.Index_ == blockState.Count_) {
                do {
                    if (const auto result = Stream_.WideFetch(inputFields, inputWidth); result != NUdf::EFetchStatus::Ok) {
                        return result;
                    }

                    blockState.Index_ = 0;
                    blockState.Count_ = GetBlockCount(blockState.Values_.back());
                } while (!blockState.Count_);
            }

            blockState.Current_ = blockState.Index_++;
            for (size_t i = 0; i < width; i++) {
                output[i] = blockState.Get(HolderFactory_, i);
            }

            return NUdf::EFetchStatus::Ok;
        }

        NUdf::TUnboxedValue BlockState_;
        NUdf::TUnboxedValue Stream_;
        const THolderFactory& HolderFactory_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const TVector<TType*> Types_;
};

struct TListFromBlocksState: public TComputationValue<TListFromBlocksState> {
public:
    TListFromBlocksState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types, size_t blockLengthIndex)
        : TComputationValue(memInfo)
        , HolderFactory_(ctx.HolderFactory)
        , BlockLengthIndex_(blockLengthIndex)
        , Readers_(types.size())
        , Converters_(types.size())
        , ValuesDescr_(ToValueDescr(types))
    {
        const auto& pgBuilder = ctx.Builder->GetPgBuilder();
        for (size_t i = 0; i < types.size(); ++i) {
            if (i == blockLengthIndex) {
                continue;
            }
            const TType* blockItemType = AS_TYPE(TBlockType, types[i])->GetItemType();
            Readers_[i] = MakeBlockReader(TTypeInfoHelper(), blockItemType);
            Converters_[i] = MakeBlockItemConverter(TTypeInfoHelper(), blockItemType, pgBuilder);
        }
    }

    NUdf::TUnboxedValue GetRow() {
        MKQL_ENSURE(CurrentRow_ < RowCount_, "Rows out of range");

        NUdf::TUnboxedValue* outItems = nullptr;
        auto row = HolderFactory_.CreateDirectArrayHolder(Readers_.size() - 1, outItems);

        size_t outputStructIdx = 0;
        for (size_t i = 0; i < Readers_.size(); i++) {
            if (i == BlockLengthIndex_) {
                continue;
            }

            const auto& datum = TArrowBlock::From(BlockItems_[i]).GetDatum();
            ARROW_DEBUG_CHECK_DATUM_TYPES(ValuesDescr_[i], datum.descr());

            TBlockItem item;
            if (datum.is_scalar()) {
                item = Readers_[i]->GetScalarItem(*datum.scalar());
            } else {
                MKQL_ENSURE(datum.is_array(), "Expecting array");
                item = Readers_[i]->GetItem(*datum.array(), CurrentRow_);
            }

            outItems[outputStructIdx++] = Converters_[i]->MakeValue(item, HolderFactory_);
        }

        CurrentRow_++;
        return row;
    }

    void SetBlock(NUdf::TUnboxedValue block) {
        BlockItems_ = block.GetElements();
        Block_ = std::move(block);

        CurrentRow_ = 0;
        RowCount_ = GetBlockCount(BlockItems_[BlockLengthIndex_]);
    }

    bool HasRows() const {
        return CurrentRow_ < RowCount_;
    }

private:
    const THolderFactory& HolderFactory_;

    size_t CurrentRow_ = 0;
    size_t RowCount_ = 0;

    size_t BlockLengthIndex_ = 0;

    NUdf::TUnboxedValue Block_;
    const NUdf::TUnboxedValue* BlockItems_ = nullptr;

    std::vector<std::unique_ptr<IBlockReader>> Readers_;
    std::vector<std::unique_ptr<IBlockItemConverter>> Converters_;
    const std::vector<arrow::ValueDescr> ValuesDescr_;
};

class TListFromBlocksWrapper: public TMutableComputationNode<TListFromBlocksWrapper> {
    using TBaseComputation = TMutableComputationNode<TListFromBlocksWrapper>;

public:
    TListFromBlocksWrapper(TComputationMutables& mutables,
                           IComputationNode* list,
                           TStructType* structType)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , List_(list)
    {
        for (size_t i = 0; i < structType->GetMembersCount(); i++) {
            if (structType->GetMemberName(i) == NYql::BlockLengthColumnName) {
                BlockLengthIndex_ = i;
                Types_.push_back(nullptr);
                continue;
            }
            Types_.push_back(structType->GetMemberType(i));
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TListFromBlocksValue>(
            ctx,
            Types_,
            BlockLengthIndex_,
            List_->GetValue(ctx));
    }

private:
    class TListFromBlocksValue: public TCustomListValue {
        using TState = TListFromBlocksState;

    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& blockState, NUdf::TUnboxedValue&& iter)
                : TComputationValue<TIterator>(memInfo)
                , BlockState_(std::move(blockState))
                , Iter_(std::move(iter))
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                auto& blockState = *static_cast<TState*>(BlockState_.AsBoxed().Get());
                if (!blockState.HasRows()) {
                    NUdf::TUnboxedValue block;
                    if (!Iter_.Next(block)) {
                        return false;
                    }
                    blockState.SetBlock(std::move(block));
                }

                value = blockState.GetRow();
                return true;
            }

        private:
            const NUdf::TUnboxedValue BlockState_;
            const NUdf::TUnboxedValue Iter_;
        };

        TListFromBlocksValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
                             const TVector<TType*>& types, ui32 blockLengthIndex, NUdf::TUnboxedValue&& list)
            : TCustomListValue(memInfo)
            , CompCtx_(ctx)
            , Types_(types)
            , BlockLengthIndex_(blockLengthIndex)
            , List_(std::move(list))
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            auto state = CompCtx_.HolderFactory.Create<TState>(CompCtx_, Types_, BlockLengthIndex_);
            return CompCtx_.HolderFactory.Create<TIterator>(std::move(state), List_.GetListIterator());
        }

        bool HasListItems() const final {
            if (!HasItems_.has_value()) {
                HasItems_ = List_.HasListItems();
            }
            return *HasItems_;
        }

        ui64 GetListLength() const final {
            if (!Length_.has_value()) {
                auto iter = List_.GetListIterator();

                Length_ = 0;
                NUdf::TUnboxedValue block;
                while (iter.Next(block)) {
                    auto blockLengthValue = block.GetElement(BlockLengthIndex_);
                    *Length_ += GetBlockCount(blockLengthValue);
                }
            }

            return *Length_;
        }

    private:
        TComputationContext& CompCtx_;

        const TVector<TType*>& Types_;
        size_t BlockLengthIndex_ = 0;

        NUdf::TUnboxedValue List_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(List_);
    }

private:
    TVector<TType*> Types_;
    size_t BlockLengthIndex_ = 0;

    IComputationNode* const List_;
};

class TPrecomputedArrowNode: public IArrowKernelComputationNode {
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

class TAsScalarWrapper: public TMutableCodegeneratorNode<TAsScalarWrapper> {
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
        return EmitFunctionCall<&TAsScalarWrapper::AsScalar>(Type::getInt128Ty(context), {self, value, ctx.Ctx}, ctx, block);
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

class TReplicateScalarWrapper: public TMutableCodegeneratorNode<TReplicateScalarWrapper> {
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
        return EmitFunctionCall<&TReplicateScalarWrapper::Replicate>(Type::getInt128Ty(context), {self, value, count, ctx.Ctx}, ctx, block);
    }
#endif
private:
    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        const auto value = Value_->GetValue(ctx).Release();
        const auto count = Count_->GetValue(ctx).Release();
        return std::make_unique<TPrecomputedArrowNode>(DoReplicate(value, count, ctx), "ReplicateScalar");
    }

    arrow::Datum DoReplicate(const NUdf::TUnboxedValuePod val, const NUdf::TUnboxedValuePod cnt, TComputationContext& ctx) const {
        const auto value = TArrowBlock::From(val).GetDatum().scalar();
        const ui64 count = TArrowBlock::From(cnt).GetDatum().scalar_as<arrow::UInt64Scalar>().value;

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

class TBlockExpandChunkedWrapper: public TStatefulWideFlowCodegeneratorNode<TBlockExpandChunkedWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TBlockExpandChunkedWrapper>;

public:
    TBlockExpandChunkedWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, size_t width)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Width_(width)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Width_))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        auto& s = GetState(state, ctx);
        if (!s.Count) {
            const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
            s.ClearValues();
            if (const auto result = Flow_->FetchValues(ctx, fields); result != EFetchResult::One) {
                return result;
            }
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TBlockExpandChunkedWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto countPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetCount()}, "count_ptr", block);
        const auto count = new LoadInst(indexType, countPtr, "count", block);

        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, count, ConstantInt::get(indexType, 0), "next", block);

        BranchInst::Create(read, fill, next, block);

        block = read;

        EmitFunctionCall<&TBlockState::ClearValues>(Type::getVoidTy(context), {stateArg}, ctx, block);

        const auto getres = GetNodeValues(Flow_, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        const auto result = PHINode::Create(statusType, 2U, "result", over);
        result->addIncoming(getres.first, block);

        BranchInst::Create(over, work, special, block);

        block = work;

        const auto valuesPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetPointer()}, "values_ptr", block);
        const auto values = new LoadInst(ptrValuesType, valuesPtr, "values", block);
        Value* array = UndefValue::get(arrayType);
        for (auto idx = 0U; idx < getres.second.size(); ++idx) {
            const auto value = getres.second[idx](ctx, block);
            AddRefBoxed(value, ctx, block);
            array = InsertValueInst::Create(array, value, {idx}, (TString("value_") += ToString(idx)).c_str(), block);
        }
        new StoreInst(array, values, block);

        EmitFunctionCall<&TBlockState::FillArrays>(Type::getVoidTy(context), {stateArg}, ctx, block);

        BranchInst::Create(fill, block);

        block = fill;

        const auto slice = EmitFunctionCall<&TBlockState::Slice>(indexType, {stateArg}, ctx, block);
        new StoreInst(slice, heightPtr, block);
        new StoreInst(stateArg, stateOnStack, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters(Width_);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, valueType, heightPtr, indexType, statePtrType, stateOnStack](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                const auto heightArg = new LoadInst(indexType, heightPtr, "height", block);
                return EmitFunctionCall<&TBlockState::Get>(valueType, {stateArg, heightArg, ctx.GetFactory(), ConstantInt::get(indexType, idx)}, ctx, block);
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
        if (state.IsInvalid()) {
            MakeState(ctx, state);

            auto& s = *static_cast<TBlockState*>(state.AsBoxed().Get());
            const auto fields = ctx.WideFields.data() + WideFieldsIndex_;
            for (size_t i = 0; i < Width_; ++i) {
                fields[i] = &s.Values[i];
            }
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

class TBlockExpandChunkedStreamWrapper: public TMutableComputationNode<TBlockExpandChunkedStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TBlockExpandChunkedStreamWrapper>;
    class TExpanderState: public TComputationValue<TExpanderState> {
        using TBase = TComputationValue<TExpanderState>;

    public:
        TExpanderState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& stream, size_t width)
            : TBase(memInfo)
            , HolderFactory_(ctx.HolderFactory)
            , State_(ctx.HolderFactory.Create<TBlockState>(width))
            , Stream_(stream)
        {
        }

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
        , Width_(width)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TExpanderState>(ctx, std::move(Stream_->GetValue(ctx)), Width_);
    }
    void RegisterDependencies() const override {
        DependsOn(Stream_);
    }

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

    const auto inputType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(inputType->IsStream() || inputType->IsFlow(),
                "Expected either WideStream or WideFlow as an input");
    const auto yieldsStream = callable.GetType()->GetReturnType()->IsStream();
    MKQL_ENSURE(yieldsStream == inputType->IsStream(),
                "Expected both input and output have to be either WideStream or WideFlow");

    const auto wideComponents = GetWideComponents(inputType);
    TVector<TType*> items(wideComponents.begin(), wideComponents.end());
    const auto wideFlowOrStream = LocateNode(ctx.NodeLocator, callable, 0);
    if (yieldsStream) {
        const auto wideStream = wideFlowOrStream;
        return new TWideToBlocksStreamWrapper(ctx.Mutables, wideStream, std::move(items));
    }
    // FIXME: Drop the branch below, when the time comes.
    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(wideFlowOrStream);
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideToBlocksFlowWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapListToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto inputType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(inputType->IsList(), "Expected List as an input");
    const auto outputType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(outputType->IsList(), "Expected List as an output");

    const auto inputItemType = AS_TYPE(TListType, inputType)->GetItemType();
    MKQL_ENSURE(inputItemType->IsStruct(), "Expected List of Struct as an input");
    const auto outputItemType = AS_TYPE(TListType, outputType)->GetItemType();
    MKQL_ENSURE(outputItemType->IsStruct(), "Expected List of Struct as an output");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    return new TListToBlocksWrapper(ctx.Mutables, list, AS_TYPE(TStructType, outputItemType));
}

IComputationNode* WrapFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto blockType = AS_TYPE(TBlockType, flowType->GetItemType());
    return new TFromBlocksWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), blockType->GetItemType());
}

IComputationNode* WrapWideFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto inputType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(inputType->IsStream() || inputType->IsFlow(),
                "Expected either WideStream or WideFlow as an input");
    const auto yieldsStream = callable.GetType()->GetReturnType()->IsStream();
    MKQL_ENSURE(yieldsStream == inputType->IsStream(),
                "Expected both input and output have to be either WideStream or WideFlow");

    const auto wideComponents = GetWideComponents(inputType);
    MKQL_ENSURE(wideComponents.size() > 0, "Expected at least one column");
    TVector<TType*> items;
    for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
        items.push_back(AS_TYPE(TBlockType, wideComponents[i]));
    }

    const auto wideFlowOrStream = LocateNode(ctx.NodeLocator, callable, 0);
    if (yieldsStream) {
        const auto wideStream = wideFlowOrStream;
        return new TWideFromBlocksStreamWrapper(ctx.Mutables, wideStream, std::move(items));
    }
    // FIXME: Drop the branch below, when the time comes.
    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(wideFlowOrStream);
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");
    return new TWideFromBlocksFlowWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapListFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto inputType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(inputType->IsList(), "Expected List as an input");
    const auto outputType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(outputType->IsList(), "Expected List as an output");

    const auto inputItemType = AS_TYPE(TListType, inputType)->GetItemType();
    MKQL_ENSURE(inputItemType->IsStruct(), "Expected List of Struct as an input");
    const auto outputItemType = AS_TYPE(TListType, outputType)->GetItemType();
    MKQL_ENSURE(outputItemType->IsStruct(), "Expected List of Struct as an output");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    return new TListFromBlocksWrapper(ctx.Mutables, list, AS_TYPE(TStructType, inputItemType));
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

} // namespace NMiniKQL
} // namespace NKikimr
