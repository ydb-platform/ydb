#include "mkql_blocks.h"
#include "mkql_llvm_base.h"

#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

#include <arrow/scalar.h>
#include <arrow/array.h>
#include <arrow/datum.h>

extern "C" size_t GetCount(const NYql::NUdf::TUnboxedValuePod data) {
    return NKikimr::NMiniKQL::TArrowBlock::From(data).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
}

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
        auto builder = MakeArrayBuilder(TTypeInfoHelper(), ItemType_, ctx.ArrowMemoryPool, maxLen, &ctx.Builder->GetPgBuilder());

        for (size_t i = 0; i < builder->MaxLength(); ++i) {
            auto result = Flow_->GetValue(ctx);
            if (result.IsFinish() || result.IsYield()) {
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

class TWideToBlocksWrapper : public TStatefulWideFlowBlockComputationNode<TWideToBlocksWrapper> {
public:
    TWideToBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<TType*>&& types)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, types.size() + 1)
        , Flow_(flow)
        , Types_(std::move(types))
        , Width_(Types_.size())
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        for (; s.Rows_ < s.MaxLength_; ++s.Rows_) {
            if (const auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data()); EFetchResult::One != result) {
                if (EFetchResult::Finish == result) {
                    s.IsFinished_ = true;
                }

                if (EFetchResult::Yield == result || s.Rows_ == 0) {
                    return result;
                }

                break;
            }
            for (size_t j = 0; j < Width_; ++j) {
                if (output[j] != nullptr) {
                    s.Builders_[j]->Add(s.Values_[j]);
                }
            }
        }

        for (size_t i = 0; i < Width_; ++i) {
            if (auto* out = output[i]; out != nullptr) {
                *out = ctx.HolderFactory.CreateArrowBlock(s.Builders_[i]->Build(s.IsFinished_));
            }
        }

        if (auto* out = output[Width_]; out != nullptr) {
            *out = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(s.Rows_)));
        }

        s.Rows_ = 0;
        return EFetchResult::One;
    }
private:
    struct TState : public TComputationValue<TState> {
        std::vector<NUdf::TUnboxedValue> Values_;
        std::vector<NUdf::TUnboxedValue*> ValuePointers_;
        std::vector<std::unique_ptr<IArrayBuilder>> Builders_;
        size_t MaxLength_;
        size_t Rows_ = 0;
        bool IsFinished_ = false;

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types)
            : TComputationValue(memInfo)
            , Values_(types.size())
            , ValuePointers_(types.size())
        {
            size_t maxBlockItemSize = 0;
            for (size_t i = 0; i < types.size(); ++i) {
                maxBlockItemSize = std::max(CalcMaxBlockItemSize(types[i]), maxBlockItemSize);
            }
            MaxLength_ = CalcBlockLen(maxBlockItemSize);

            for (size_t i = 0; i < types.size(); ++i) {
                ValuePointers_[i] = &Values_[i];
                Builders_.push_back(MakeArrayBuilder(TTypeInfoHelper(), types[i], ctx.ArrowMemoryPool, MaxLength_, &ctx.Builder->GetPgBuilder()));
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(ctx, Types_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const TVector<TType*> Types_;
    const size_t Width_;
};

class TFromBlocksWrapper : public TMutableComputationNode<TFromBlocksWrapper> {
public:
    TFromBlocksWrapper(TComputationMutables& mutables, IComputationNode* flow, TType* itemType)
        : TMutableComputationNode(mutables)
        , Flow_(flow)
        , ItemType_(itemType)
        , StateIndex_(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = GetState(ctx);

        for (;;) {
            auto item = state.GetValue(ctx);
            if (item) {
                return *item;
            }

            auto input = Flow_->GetValue(ctx);
            if (input.IsFinish()) {
                return NUdf::TUnboxedValue::MakeFinish();
            }
            if (input.IsYield()) {
                return NUdf::TUnboxedValue::MakeYield();
            }

            state.Reset(TArrowBlock::From(input).GetDatum());
        }
    }

private:
    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        TState(TMemoryUsageInfo* memInfo, TType* itemType, const NUdf::IPgBuilder& pgBuilder)
            : TComputationValue(memInfo)
            , Reader_(MakeBlockReader(TTypeInfoHelper(), itemType))
            , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), itemType, pgBuilder))
        {
        }

        TMaybe<NUdf::TUnboxedValuePod> GetValue(TComputationContext& ctx) {
            for (;;) {
                if (Arrays_.empty()) {
                    return {};
                }
                if (Index_ < ui64(Arrays_.front()->length)) {
                    break;
                }
                Index_ = 0;
                Arrays_.pop_front();
            }
            return Converter_->MakeValue(Reader_->GetItem(*Arrays_.front(), Index_++), ctx.HolderFactory);
        }

        void Reset(const arrow::Datum& datum) {
            MKQL_ENSURE(datum.is_arraylike(), "Expecting array as FromBlocks argument");
            MKQL_ENSURE(Arrays_.empty(), "Not all input is processed");
            if (datum.is_array()) {
                Arrays_.push_back(datum.array());
            } else {
                for (auto& chunk : datum.chunks()) {
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
        this->DependsOn(Flow_);
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex_];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>(ItemType_, ctx.Builder->GetPgBuilder());
        }
        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    IComputationNode* const Flow_;
    TType* ItemType_;
    const ui32 StateIndex_;
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
            s.Count_ = GetCount(s.Values_.back());
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

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);

        TLLVMFieldsStructureState stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Get));
        const auto getType = FunctionType::get(valueType, {statePtrType, ctx.GetFactory()->getType(), indexType}, false);
        const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", &ctx.Func->getEntryBlock().back());
        const auto stateOnStack = new AllocaInst(statePtrType, 0U, "state_on_stack", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantPointerNull::get(statePtrType), stateOnStack, &ctx.Func->getEntryBlock().back());

        const auto name = "GetCount";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&GetCount));
        const auto getCountType = NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget() ?
            FunctionType::get(indexType, { valueType }, false):
            FunctionType::get(indexType, { ptrValueType }, false);
        const auto getCount = ctx.Codegen.GetModule().getOrInsertFunction(name, getCountType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto save = BasicBlock::Create(context, "save", ctx.Func);
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

        const auto pointerPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetPointer() }, "pointer_ptr", block);
        const auto pointer = new LoadInst(ptrValueType, pointerPtr, "pointer", block);

        std::vector<Value*> pointers(Types_.size());
        for (size_t idx = 0U; idx < pointers.size(); ++idx) {
            pointers[idx] = GetElementPtrInst::CreateInBounds(valueType, pointer, { ConstantInt::get(Type::getInt32Ty(context), idx) }, (TString("ptr_") += ToString(idx)).c_str(), block);
            SafeUnRefUnboxed(pointers[idx], ctx, block);
        }

        const auto getres = GetNodeValues(Flow_, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        const auto result = PHINode::Create(statusType, 2U, "result", over);
        result->addIncoming(getres.first, block);

        BranchInst::Create(over, good, special, block);

        block = good;

        const auto countValue = getres.second.back()(ctx, block);
        const auto height = CallInst::Create(getCount, { WrapArgumentForWindows(countValue, ctx, block) }, "height", block);
        CleanupBoxed(countValue, ctx, block);

        new StoreInst(height, countPtr, block);
        new StoreInst(ConstantInt::get(indexType, 0), indexPtr, block);

        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, ConstantInt::get(indexType, 0), height, "empty", block);

        BranchInst::Create(more, save, empty, block);

        block = save;

        for (size_t idx = 0U; idx < pointers.size(); ++idx) {
            const auto value = getres.second[idx](ctx, block);
            AddRefBoxed(value, ctx, block);
            new StoreInst(value, pointers[idx], block);
        }

        BranchInst::Create(work, block);

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

        ICodegeneratorInlineWideNode::TGettersList getters(Types_.size());
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, getType, getPtr, indexType, statePtrType, stateOnStack](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto stateArg = new LoadInst(statePtrType, stateOnStack, "state", block);
                return CallInst::Create(getType, getPtr, {stateArg, ctx.GetFactory(), ConstantInt::get(indexType, idx)}, "get", block);
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

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, size_t wideFieldsIndex, const TVector<TType*>& types)
            : TComputationValue(memInfo)
            , Values_(types.size() + 1)
        {
            Pointer_ = Values_.data();
            auto**const fields = ctx.WideFields.data() + wideFieldsIndex;
            for (size_t i = 0; i < types.size() + 1; ++i) {
                fields[i] = &Values_[i];
            }

            const auto& pgBuilder = ctx.Builder->GetPgBuilder();
            for (size_t i = 0; i < types.size(); ++i) {
                Readers_.push_back(MakeBlockReader(TTypeInfoHelper(), types[i]));
                Converters_.push_back(MakeBlockItemConverter(TTypeInfoHelper(), types[i], pgBuilder));
            }
        }

        NUdf::TUnboxedValuePod Get(const THolderFactory& holderFactory, size_t idx) const {
            TBlockItem item;
            if (const auto& datum = TArrowBlock::From(Values_[idx]).GetDatum(); datum.is_scalar()) {
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

        TLLVMFieldsStructureState(llvm::LLVMContext& context)
            : TBase(context)
            , CountType(Type::getInt64Ty(Context))
            , IndexType(Type::getInt64Ty(Context))
            , CurrentType(Type::getInt64Ty(Context))
            , PointerType(PointerType::getUnqual(Type::getInt128Ty(Context)))
        {}
    };
#endif
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ctx, WideFieldsIndex_, Types_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue())
            MakeState(ctx, state);
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

class TAsScalarWrapper : public TMutableComputationNode<TAsScalarWrapper> {
public:
    TAsScalarWrapper(TComputationMutables& mutables, IComputationNode* arg, TType* type)
        : TMutableComputationNode(mutables)
        , Arg_(arg)
        , Type_(type)
    {
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(Type_, arrowType), "Unsupported type of scalar");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Arg_->GetValue(ctx);
        arrow::Datum result = ConvertScalar(Type_, value, ctx.ArrowMemoryPool);
        return ctx.HolderFactory.CreateArrowBlock(std::move(result));
    }

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        auto value = Arg_->GetValue(ctx);
        arrow::Datum result = ConvertScalar(Type_, value, ctx.ArrowMemoryPool);
        return std::make_unique<TPrecomputedArrowNode>(result, "AsScalar");
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Arg_);
    }

private:
    IComputationNode* const Arg_;
    TType* Type_;
};

class TReplicateScalarWrapper : public TMutableComputationNode<TReplicateScalarWrapper> {
public:
    TReplicateScalarWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* count, TType* type)
        : TMutableComputationNode(mutables)
        , Value_(value)
        , Count_(count)
        , Type_(type)
    {
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(Type_, arrowType), "Unsupported type of scalar");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateArrowBlock(DoReplicate(ctx));
    }

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        return std::make_unique<TPrecomputedArrowNode>(DoReplicate(ctx), "ReplicateScalar");
    }

private:
    arrow::Datum DoReplicate(TComputationContext& ctx) const {
        auto value = TArrowBlock::From(Value_->GetValue(ctx)).GetDatum().scalar();
        const ui64 count = TArrowBlock::From(Count_->GetValue(ctx)).GetDatum().scalar_as<arrow::UInt64Scalar>().value;

        auto reader = MakeBlockReader(TTypeInfoHelper(), Type_);
        auto builder = MakeArrayBuilder(TTypeInfoHelper(), Type_, ctx.ArrowMemoryPool, count, &ctx.Builder->GetPgBuilder());

        TBlockItem item = reader->GetScalarItem(*value);
        builder->Add(item, count);
        return builder->Build(true);
    }

    void RegisterDependencies() const final {
        DependsOn(Value_);
        DependsOn(Count_);
    }

private:
    IComputationNode* const Value_;
    IComputationNode* const Count_;
    TType* Type_;
};

class TBlockExpandChunkedWrapper : public TStatefulWideFlowBlockComputationNode<TBlockExpandChunkedWrapper> {
public:
    TBlockExpandChunkedWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 width)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, width)
        , Flow_(flow)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx,
                             NUdf::TUnboxedValue*const* output) const
    {
        Y_UNUSED(state);
        return Flow_->FetchValues(ctx, output);
    }

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode* const Flow_;
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
    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
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
        const auto blockType = AS_TYPE(TBlockType, wideComponents[i]);
        items.push_back(blockType->GetItemType());
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
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

    auto value = LocateNode(ctx.NodeLocator, callable, 0);
    auto count = LocateNode(ctx.NodeLocator, callable, 1);
    return new TReplicateScalarWrapper(ctx.Mutables, value, count, valueType->GetItemType());
}

IComputationNode* WrapBlockExpandChunked(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TBlockExpandChunkedWrapper(ctx.Mutables, wideFlow, wideComponents.size());
}

}
}
