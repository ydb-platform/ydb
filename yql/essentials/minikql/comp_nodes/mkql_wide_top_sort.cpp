#include "mkql_wide_top_sort.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_llvm_base.h>                // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_spiller_adapter.h>
#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/public/udf/udf_log.h>
#include <yql/essentials/utils/cast.h>
#include <yql/essentials/utils/log/log.h>

#include <yql/essentials/utils/sort.h>

#include <deque>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TKeyInfo {
    NUdf::EDataSlot Slot;
    bool IsOptional;
    NUdf::ICompare::TPtr Compare;
    TType* PresortType = nullptr;
};

struct TRuntimeKeyInfo {
    explicit TRuntimeKeyInfo(const TKeyInfo& keyInfo)
        : Slot(keyInfo.Slot)
        , IsOptional(keyInfo.IsOptional)
        , Compare(keyInfo.Compare.Get())
    {
        if (keyInfo.PresortType) {
            LeftPacker = TGenericPresortEncoder(keyInfo.PresortType);
            RightPacker = TGenericPresortEncoder(keyInfo.PresortType);
        }
    }

    const NUdf::EDataSlot Slot;
    const bool IsOptional;
    const NUdf::ICompare* const Compare;
    mutable std::optional<TGenericPresortEncoder> LeftPacker;
    mutable std::optional<TGenericPresortEncoder> RightPacker;
};

struct TMyValueCompare {
    explicit TMyValueCompare(const std::vector<TKeyInfo>& keys)
        : Keys(keys.cbegin(), keys.cend())
    {
    }

    int operator()(const bool* directions, const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right) const {
        for (auto i = 0u; i < Keys.size(); ++i) {
            auto& key = Keys[i];
            int cmp;
            if (key.Compare) {
                cmp = key.Compare->Compare(left[i], right[i]);
                if (!directions[i]) {
                    cmp = -cmp;
                }
            } else if (key.LeftPacker) {
                auto strLeft = key.LeftPacker->Encode(left[i], false);
                auto strRight = key.RightPacker->Encode(right[i], false);
                cmp = strLeft.compare(strRight);
                if (!directions[i]) {
                    cmp = -cmp;
                }
            } else {
                cmp = CompareValues(key.Slot, directions[i], key.IsOptional, left[i], right[i]);
            }

            if (cmp) {
                return cmp;
            }
        }

        return 0;
    }

    const std::vector<TRuntimeKeyInfo> Keys;
};

using NYql::TChunkedBuffer;
using TAsyncWriteOperation = std::optional<NThreading::TFuture<ISpiller::TKey>>;
using TAsyncReadOperation = std::optional<NThreading::TFuture<std::optional<TChunkedBuffer>>>;
using TStorage = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue, EMemorySubPool::Temporary>>;
using TStorageDeque = std::deque<TStorage, TMKQLAllocator<TStorage, EMemorySubPool::Temporary>>;

class TBucket {
public:
    enum class EState {
        Empty,    // Free, can be acquired for writing
        Writing,  // Accepting rows via Write()
        Sealed,   // FinishWrite() completed, ready for reading
        Reading   // Being read via Read()
    };

    TBucket() = default;

    void Init(ISpiller::TPtr spiller, const TMultiType* tupleMultiType, size_t packSize) {
        Spiller_ = spiller;
        TupleMultiType_ = tupleMultiType;
        PackSize_ = packSize;
        State_ = EState::Empty;
    }

    // Prepare bucket for writing (must be Empty)
    void Open() {
        MKQL_ENSURE(State_ == EState::Empty, "Bucket must be Empty to Open");
        Adapter_ = std::make_unique<TWideUnboxedValuesSpillerAdapter>(Spiller_, TupleMultiType_, PackSize_);
        RowCount_ = 0;
        AsyncWriteOperation_ = std::nullopt;
        AsyncReadOperation_ = std::nullopt;
        ReadFinished_ = false;
        State_ = EState::Writing;
    }

    // Write a row to the bucket
    TAsyncWriteOperation Write(NUdf::TUnboxedValue* item, size_t size) {
        MKQL_ENSURE(State_ == EState::Writing, "Bucket must be in Writing state to Write");
        ++RowCount_;
        AsyncWriteOperation_ = Adapter_->WriteWideItem({item, size});
        return AsyncWriteOperation_;
    }

    // Finish writing — transitions to Sealed when complete
    TAsyncWriteOperation FinishWrite() {
        MKQL_ENSURE(State_ == EState::Writing, "Bucket must be in Writing state to FinishWrite");
        AsyncWriteOperation_ = Adapter_->FinishWriting();
        return AsyncWriteOperation_;
    }

    // Complete an async write operation
    void AsyncWriteCompleted() {
        MKQL_ENSURE(AsyncWriteOperation_.has_value() && AsyncWriteOperation_->HasValue(),
                     "No completed async write operation");
        Adapter_->AsyncWriteCompleted(AsyncWriteOperation_->ExtractValue());
        AsyncWriteOperation_ = std::nullopt;
    }

    // Mark bucket as sealed (call after FinishWrite completes)
    void Seal() {
        State_ = EState::Sealed;
    }

    // Read a row from the bucket (must be Sealed or Reading)
    TAsyncReadOperation Read(TStorage& buffer, const TComputationContext& ctx) {
        if (State_ == EState::Sealed) {
            State_ = EState::Reading;
        }
        MKQL_ENSURE(State_ == EState::Reading, "Bucket must be Sealed/Reading to Read");
        if (AsyncReadOperation_) {
            if (AsyncReadOperation_->HasValue()) {
                Adapter_->AsyncReadCompleted(AsyncReadOperation_->ExtractValue().value(), ctx.HolderFactory);
                AsyncReadOperation_ = std::nullopt;
            } else {
                return AsyncReadOperation_;
            }
        }
        if (Adapter_->Empty()) {
            ReadFinished_ = true;
            return std::nullopt;
        }
        AsyncReadOperation_ = Adapter_->ExtractWideItem(buffer);
        return AsyncReadOperation_;
    }

    // Reset bucket to Empty state for reuse
    void Reset() {
        Adapter_.reset();
        RowCount_ = 0;
        AsyncWriteOperation_ = std::nullopt;
        AsyncReadOperation_ = std::nullopt;
        ReadFinished_ = false;
        State_ = EState::Empty;
    }

    bool IsEmpty() const { return State_ == EState::Empty; }
    bool IsWriting() const { return State_ == EState::Writing; }
    bool IsSealed() const { return State_ == EState::Sealed; }
    bool IsReadFinished() const { return ReadFinished_; }
    size_t GetRowCount() const { return RowCount_; }
    EState GetState() const { return State_; }

    bool HasPendingWrite() const { return AsyncWriteOperation_.has_value(); }
    bool IsWriteReady() const { return AsyncWriteOperation_.has_value() && AsyncWriteOperation_->HasValue(); }

private:
    ISpiller::TPtr Spiller_;
    const TMultiType* TupleMultiType_ = nullptr;
    size_t PackSize_ = 0;
    std::unique_ptr<TWideUnboxedValuesSpillerAdapter> Adapter_;
    size_t RowCount_ = 0;
    EState State_ = EState::Empty;
    TAsyncWriteOperation AsyncWriteOperation_ = std::nullopt;
    TAsyncReadOperation AsyncReadOperation_ = std::nullopt;
    bool ReadFinished_ = false;
};

class TSpilledUnboxedValuesIterator {
private:
    TStorage Data;
    TBucket& Bucket_;
    std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc;
    ui32 Width_;
    const TComputationContext& Ctx;
    bool HasValue = false;

public:
    TSpilledUnboxedValuesIterator(
        const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>& lessFunc,
        TBucket& bucket,
        size_t dataWidth,
        const TComputationContext& ctx)
        : Bucket_(bucket)
        , LessFunc(lessFunc)
        , Width_(dataWidth)
        , Ctx(ctx)
    {
        Data.resize(Width_);
    }

    EFetchResult Read() {
        if (!HasValue) {
            if (Bucket_.Read(Data, Ctx)) {
                return EFetchResult::Yield;
            }
            if (Bucket_.IsReadFinished()) {
                return EFetchResult::Finish;
            }
            HasValue = true;
        }
        return EFetchResult::One;
    }

    bool CheckForInit() {
        if (HasValue || IsFinished()) {
            return true;
        }
        // Try to read the first value
        EFetchResult result = Read();
        return result != EFetchResult::Yield;
    }

    bool IsFinished() const {
        return Bucket_.IsReadFinished();
    }

    bool operator<(const TSpilledUnboxedValuesIterator& item) const {
        return !LessFunc(GetValue(), item.GetValue());
    }

    ui32 Width() const {
        return Width_;
    }

    TStorage Pop() {
        auto data(std::move(Data));
        Data.resize(Width_);
        HasValue = false;
        Read();
        return data;
    }

    const NUdf::TUnboxedValue* GetValue() const {
        return &Data.front();
    }
};

using TComparePtr = int (*)(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*);
using TCompareFunc = std::function<int(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>;

template <bool Sort>
class TState: public TComputationValue<TState<Sort>> {
    using TBase = TComputationValue<TState<Sort>>;

private:
    using TFields = std::vector<NUdf::TUnboxedValue*, TMKQLAllocator<NUdf::TUnboxedValue*, EMemorySubPool::Temporary>>;
    using TPointers = std::vector<NUdf::TUnboxedValuePod*, TMKQLAllocator<NUdf::TUnboxedValuePod*, EMemorySubPool::Temporary>>;

    size_t GetStorageSize() const {
        return std::max<size_t>(Count << 2ULL, 1ULL << 8ULL);
    }

    static constexpr size_t GetStorageBlockSize() {
        return 1ULL << 10ULL;
    }

    void ResetFields() {
        MaybeGrowStorage();
        auto ptr = Tongue = Free.back();
        std::for_each(Indexes.cbegin(), Indexes.cend(), [&](ui32 index) { Fields[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
    }

    void MaybeGrowStorage() {
        if (Free.empty()) {
            const size_t fieldsCount = Indexes.size();
            const size_t blockSize = std::min(GetStorageBlockSize(), GetStorageSize());
            TStorage& newStorageBlock = Storage.emplace_back(blockSize * fieldsCount);
            auto* ptr = newStorageBlock.data();
            Free.reserve(Free.size() + blockSize);
            for (size_t i = 0; i < blockSize; ++i) {
                Free.emplace_back(ptr);
                ptr += fieldsCount;
            }
        }
    }

public:
    TState(TMemoryUsageInfo* memInfo, ui64 count, const bool* directons, size_t keyWidth, const TCompareFunc& compare, const std::vector<ui32>& indexes)
        : TBase(memInfo)
        , Count(count)
        , Indexes(indexes)
        , Directions(directons, directons + keyWidth)
        , LessFunc(std::bind(std::less<int>(), std::bind(compare, Directions.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Fields(Indexes.size(), nullptr)
    {
        if (Count) {
            Full.reserve(std::min(GetStorageBlockSize(), GetStorageSize()));
            ResetFields();
        } else {
            InputStatus = EFetchResult::Finish;
        }
    }

    NUdf::TUnboxedValue* const* GetFields() const {
        return Fields.data();
    }

    bool Put() {
        if (Full.size() + 1U == GetStorageSize()) {
            Free.pop_back();

            NYql::FastNthElement(Full.begin(), Full.begin() + Count, Full.end(), LessFunc);
            std::copy(Full.cbegin() + Count, Full.cend(), std::back_inserter(Free));
            Full.resize(Count);

            std::for_each(Free.cbegin(), Free.cend(), [this](NUdf::TUnboxedValuePod* ptr) {
                std::fill_n(static_cast<NUdf::TUnboxedValue*>(ptr), Indexes.size(), NUdf::TUnboxedValuePod());
            });
            Free.emplace_back(Tongue);
            Throat = nullptr;
        }

        if (Full.size() >= Count) {
            if (!Throat) {
                Throat = *std::max_element(Full.cbegin(), Full.cend(), LessFunc);
            }

            if (!LessFunc(Tongue, Throat)) {
                return false;
            }
        }

        Full.emplace_back(Free.back());
        Free.pop_back();
        ResetFields();
        return true;
    }

    void Seal() {
        Free.clear();
        Free.shrink_to_fit();

        if (Full.size() > Count) {
            NYql::FastNthElement(Full.begin(), Full.begin() + Count, Full.end(), LessFunc);
            Full.resize(Count);
        }

        if constexpr (Sort) {
            std::sort(Full.rbegin(), Full.rend(), LessFunc);
        }
    }

    NUdf::TUnboxedValue* Extract() {
        if (Full.empty()) {
            return nullptr;
        }

        const auto ptr = Full.back();
        Full.pop_back();
        return static_cast<NUdf::TUnboxedValue*>(ptr);
    }

    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Tongue = nullptr;
    NUdf::TUnboxedValuePod* Throat = nullptr;

private:
    const ui64 Count;
    const std::vector<ui32> Indexes;
    const std::vector<bool> Directions;
    const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc;
    TStorageDeque Storage;
    TPointers Free, Full;
    TFields Fields;
};

#ifndef MKQL_DISABLE_CODEGEN
template <class TState>
class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
    llvm::IntegerType* ValueType;
    llvm::PointerType* PtrValueType;
    llvm::IntegerType* StatusType;

protected:
    using TBase::Context;

public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StatusType);   // status
        result.emplace_back(PtrValueType); // tongue
        return result;
    }

    llvm::Constant* GetStatus() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetTongue() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 1);
    }

    TLLVMFieldsStructureState(llvm::LLVMContext& context)
        : TBase(context)
        , ValueType(Type::getInt128Ty(Context))
        , PtrValueType(PointerType::getUnqual(ValueType))
        , StatusType(Type::getInt32Ty(Context))
    {
    }
};
#endif

template <bool Sort>
class TWideTopWrapper: public TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort>>
#ifndef MKQL_DISABLE_CODEGEN
    ,
                       public ICodegeneratorRootNode
#endif
{
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort>>;

public:
    TWideTopWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, TComputationNodePtrVector&& directions, std::vector<TKeyInfo>&& keys,
                    std::vector<ui32>&& indexes, std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow(flow)
        , Count(count)
        , Directions(std::move(directions))
        , Keys(std::move(keys))
        , Indexes(std::move(indexes))
        , Representations(std::move(representations))
    {
        for (const auto& x : Keys) {
            if (x.Compare || x.PresortType) {
                KeyTypes.clear();
                HasComplexType = true;
                break;
            }

            KeyTypes.emplace_back(x.Slot, x.IsOptional);
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            const auto count = Count->GetValue(ctx).Get<ui64>();
            std::vector<bool> dirs(Directions.size());
            std::transform(Directions.cbegin(), Directions.cend(), dirs.begin(), [&ctx](IComputationNode* dir) { return dir->GetValue(ctx).Get<bool>(); });
            MakeState(ctx, state, count, dirs.data());
        }

        if (const auto ptr = static_cast<TState<Sort>*>(state.AsBoxed().Get())) {
            while (EFetchResult::Finish != ptr->InputStatus) {
                switch (ptr->InputStatus = Flow->FetchValues(ctx, ptr->GetFields())) {
                    case EFetchResult::One:
                        ptr->Put();
                        continue;
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                    case EFetchResult::Finish:
                        ptr->Seal();
                        break;
                }
            }

            if (auto extract = ptr->Extract()) {
                for (const auto index : Indexes) {
                    if (const auto to = output[index]) {
                        *to = std::move(*extract++);
                    } else {
                        ++extract;
                    }
                }
                return EFetchResult::One;
            }

            return EFetchResult::Finish;
        }

        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        DIScopeAnnotator annotate(ctx.Annotator);

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(ctx.Codegen.GetContext());

        TLLVMFieldsStructureState<TState<Sort>> stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto outputType = ArrayType::get(valueType, Representations.size());
        const auto outputPtrType = PointerType::getUnqual(outputType);
        const auto outs = annotate(new AllocaInst(outputPtrType, 0U, "outs", &ctx.Func->getEntryBlock().back()));

        ICodegeneratorInlineWideNode::TGettersList getters(Representations.size());

        for (auto i = 0U; i < getters.size(); ++i) {
            getters[Indexes[i]] = [i, outs, indexType, valueType, outputPtrType, outputType](const TCodegenContext& ctx, BasicBlock*& block) {
                DIScopeAnnotator annotate(ctx.Annotator);
                const auto values = annotate(new LoadInst(outputPtrType, outs, "values", block));
                const auto pointer = annotate(GetElementPtrInst::CreateInBounds(outputType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block));
                return annotate(new LoadInst(valueType, pointer, (TString("load_") += ToString(i)).c_str(), block));
            };
        }

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        annotate(BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block));
        block = make;

        const auto count = GetNodeValue(Count, ctx, block);
        const auto trunc = GetterFor<ui64>(count, context, block);

        const auto arrayType = ArrayType::get(Type::getInt1Ty(context), Directions.size());
        const auto dirs = annotate(new AllocaInst(arrayType, 0U, "dirs", block));
        for (auto i = 0U; i < Directions.size(); ++i) {
            const auto dir = GetNodeValue(Directions[i], ctx, block);
            const auto cut = GetterFor<bool>(dir, context, block);
            const auto ptr = annotate(GetElementPtrInst::CreateInBounds(arrayType, dirs, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, "ptr", block));
            annotate(new StoreInst(cut, ptr, block));
        }

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = annotate(CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block));
        EmitFunctionCall<&TWideTopWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr, trunc, dirs}, ctx, block);
        annotate(BranchInst::Create(main, block));

        block = main;

        const auto state = annotate(new LoadInst(valueType, statePtr, "state", block));
        const auto half = annotate(CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block));
        const auto stateArg = annotate(CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block));
        annotate(BranchInst::Create(more, block));

        block = more;

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 3U, "result", over);

        const auto statusPtr = annotate(GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStatus()}, "last", block));
        const auto last = annotate(new LoadInst(statusType, statusPtr, "last", block));
        const auto finish = annotate(CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, last, ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), "finish", block));

        annotate(BranchInst::Create(full, loop, finish, block));

        {
            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            block = loop;

            const auto getres = GetNodeValues(Flow, ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            const auto choise = static_cast<SwitchInst*>(annotate(SwitchInst::Create(getres.first, good, 2U, block)));
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), over);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), rest);

            block = rest;

            annotate(new StoreInst(ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), statusPtr, block));
            EmitFunctionCall<&TState<Sort>::Seal>(Type::getVoidTy(context), {stateArg}, ctx, block);

            annotate(BranchInst::Create(full, block));

            block = good;

            const auto tonguePtr = annotate(GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetTongue()}, "tongue_ptr", block));
            const auto tongue = annotate(new LoadInst(ptrValueType, tonguePtr, "tongue", block));

            std::vector<Value*> placeholders(Representations.size());
            for (auto i = 0U; i < placeholders.size(); ++i) {
                placeholders[i] = annotate(GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(indexType, i)}, (TString("placeholder_") += ToString(i)).c_str(), block));
            }

            for (auto i = 0U; i < Keys.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                annotate(new StoreInst(item, placeholders[i], block));
            }

            const auto accepted = EmitFunctionCall<&TState<Sort>::Put>(Type::getInt1Ty(context), {stateArg}, ctx, block);

            const auto push = BasicBlock::Create(context, "push", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            annotate(BranchInst::Create(push, skip, accepted, block));

            block = push;

            for (auto i = 0U; i < Keys.size(); ++i) {
                ValueAddRef(Representations[i], placeholders[i], ctx, block);
            }

            for (auto i = Keys.size(); i < Representations.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                ValueAddRef(Representations[i], item, ctx, block);
                annotate(new StoreInst(item, placeholders[i], block));
            }

            annotate(BranchInst::Create(loop, block));

            block = skip;

            for (auto i = 0U; i < Keys.size(); ++i) {
                ValueCleanup(Representations[i], placeholders[i], ctx, block);
                annotate(new StoreInst(ConstantInt::get(valueType, 0), placeholders[i], block));
            }

            annotate(BranchInst::Create(loop, block));
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto out = EmitFunctionCall<&TState<Sort>::Extract>(outputPtrType, {stateArg}, ctx, block);
            const auto has = annotate(CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(outputPtrType), "has", block));

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

            annotate(BranchInst::Create(good, over, has, block));

            block = good;

            annotate(new StoreInst(out, outs, block));

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            annotate(BranchInst::Create(over, block));
        }

        block = over;
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state, ui64 count, const bool* directions) const {
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TState<Sort>>(count, directions, Directions.size(), TMyValueCompare(Keys), Indexes);
#else
        state = ctx.HolderFactory.Create<TState<Sort>>(count, directions, Directions.size(), ctx.ExecuteLLVM && Compare ? TCompareFunc(Compare) : TCompareFunc(TMyValueCompare(Keys)), Indexes);
#endif
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            TWideTopWrapper::DependsOn(flow, Count);
            std::for_each(Directions.cbegin(), Directions.cend(), std::bind(&TWideTopWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
    const TComputationNodePtrVector Directions;
    const std::vector<TKeyInfo> Keys;
    const std::vector<ui32> Indexes;
    const std::vector<EValueRepresentation> Representations;
    TKeyTypes KeyTypes;
    bool HasComplexType = false;

#ifndef MKQL_DISABLE_CODEGEN
    TComparePtr Compare = nullptr;

    Function* CompareFunc = nullptr;

    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::Compare_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (CompareFunc) {
            Compare = reinterpret_cast<TComparePtr>(codegen.GetPointerToFunction(CompareFunc));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (!HasComplexType) {
            codegen.ExportSymbol(CompareFunc = GenerateCompareFunction(codegen, MakeName(), KeyTypes));
        }
    }
#endif
};

class TSpillingSupportState: public TComputationValue<TSpillingSupportState> {
    using TBase = TComputationValue<TSpillingSupportState>;

private:
    using TStorage = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue, EMemorySubPool::Temporary>>;
    using TFields = std::vector<NUdf::TUnboxedValue*, TMKQLAllocator<NUdf::TUnboxedValue*, EMemorySubPool::Temporary>>;
    using TPointers = std::vector<NUdf::TUnboxedValuePod*, TMKQLAllocator<NUdf::TUnboxedValuePod*, EMemorySubPool::Temporary>>;

    enum class EOperatingMode {
        InMemory,
        Spilling,
        MergeSpilled,
        ProcessSpilled
    };

    void ResetFields() {
        auto pos = Storage.size();
        Storage.insert(Storage.end(), Indexes.size(), {});
        auto ptr = Pointer = Storage.data() + pos;
        std::for_each(Indexes.cbegin(), Indexes.cend(), [&](ui32 index) { Fields[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
    }

public:
    TSpillingSupportState(TMemoryUsageInfo* memInfo, const bool* directons, size_t keyWidth, const TCompareFunc& compare,
                          const std::vector<ui32>& indexes, TMultiType* tupleMultiType, const TComputationContext& ctx,
                          bool allowSpilling, NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent)
        : TBase(memInfo)
        , Indexes(indexes)
        , Directions(directons, directons + keyWidth)
        , LessFunc(std::bind(std::less<int>(), std::bind(compare, Directions.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Fields(Indexes.size(), nullptr)
        , TupleMultiType(tupleMultiType)
        , Ctx(ctx)
        , AllowSpilling(allowSpilling)
        , Logger(std::move(logger))
        , LogComponent(logComponent)
    {
        if (AllowSpilling && Ctx.SpillerFactory) {
            Spiller = Ctx.SpillerFactory->CreateSpiller();
            const size_t PACK_SIZE = 5_MB;
            for (size_t i = 0; i < MaxBuckets; ++i) {
                Buckets[i].Init(Spiller, TupleMultiType, PACK_SIZE);
            }
        }
        UDF_LOG(Logger, LogComponent, NUdf::ELogLevel::Info, TStringBuilder() << (const void*)this << "# Sort state initialized, allowSpilling=" << AllowSpilling);
        ResetFields();
    }

    bool IsReadyToContinue() {
        switch (GetMode()) {
            case EOperatingMode::InMemory:
                return true;
            case EOperatingMode::Spilling: {
                if (!SpillState()) {
                    return false;
                }
                ResetFields();
                EOperatingMode nextMode;
                if (SealedBucketCount() >= MaxBuckets - 2) {
                    // Too many spilled states — merge two smallest before continuing
                    nextMode = EOperatingMode::MergeSpilled;
                } else if (IsReadFromChannelFinished()) {
                    nextMode = EOperatingMode::ProcessSpilled;
                } else {
                    nextMode = EOperatingMode::InMemory;
                }
                SwitchMode(nextMode);
                return IsReadyToContinue();
            }
            case EOperatingMode::MergeSpilled: {
                if (!MergeStep()) {
                    return false;
                }
                EOperatingMode nextMode;
                if (SealedBucketCount() >= MaxBuckets - 2) {
                    // Still too many — merge again
                    nextMode = EOperatingMode::MergeSpilled;
                } else if (IsReadFromChannelFinished()) {
                    nextMode = EOperatingMode::ProcessSpilled;
                } else {
                    nextMode = EOperatingMode::InMemory;
                }
                SwitchMode(nextMode);
                return IsReadyToContinue();
            }
            case EOperatingMode::ProcessSpilled: {
                if (SpilledUnboxedValuesIterators.empty()) {
                    return true;
                }
                // Initiate reads from ALL iterators simultaneously before checking results
                bool allReady = true;
                for (size_t i = 0; i < SpilledUnboxedValuesIterators.size(); ++i) {
                    if (!SpilledUnboxedValuesIterators[i].CheckForInit()) {
                        allReady = false;
                    }
                }
                return allReady;
            }
        }
    }

    bool IsFinished() const {
        if (!IsReadFromChannelFinished()) {
            return false;
        }
        // If we're still in spilling or merging mode, we're not finished
        if (Mode == EOperatingMode::Spilling || Mode == EOperatingMode::MergeSpilled) {
            return false;
        }
        // In ProcessSpilled mode, we're finished when all iterators are exhausted
        if (Mode == EOperatingMode::ProcessSpilled) {
            return SpilledUnboxedValuesIterators.empty();
        }
        // InMemory mode: finished when no spilled states exist
        return SealedBucketCount() == 0;
    }

    EOperatingMode GetModeDbg() const { return Mode; }
    size_t GetSpilledStatesCountDbg() const { return SealedBucketCount(); }
    size_t GetIteratorsCountDbg() const { return SpilledUnboxedValuesIterators.size(); }

    NUdf::TUnboxedValue* const* GetFields() const {
        return Fields.data();
    }

    void Put() {
        if (AllowSpilling && Ctx.SpillerFactory && !HasMemoryForProcessing()) {
            const size_t rowsInMemory = Storage.size() / Indexes.size();
            if (rowsInMemory >= MinSpillBatchRows) {
                SwitchMode(EOperatingMode::Spilling);
                return;
            }
        }
        try {
            Storage.insert(Storage.end(), Indexes.size(), {});
        } catch (TMemoryLimitExceededException) {
            if (AllowSpilling && Ctx.SpillerFactory) {
                const size_t rowsInMemory = Storage.size() / Indexes.size();
                UDF_LOG(Logger, LogComponent, NUdf::ELogLevel::Info, TStringBuilder()
                     << (const void*)this << "# TMemoryLimitExceededException caught in Put()"
                     << " | rowsInMemory=" << rowsInMemory
                     << " memUsed=" << TlsAllocState->GetUsed()
                     << " memLimit=" << TlsAllocState->GetLimit());
                if (rowsInMemory >= MinSpillBatchRows) {
                    SwitchMode(EOperatingMode::Spilling);
                    return;
                }
            }
            throw;
        }
        // Opportunistically pre-grow Full pointer array.
        // If allocation fails, just continue — SealInMemory() will handle
        // the case where Full.capacity() < rowCount via chunked sorting.
        const size_t rowsInMemory = Storage.size() / Indexes.size();
        if (Full.capacity() < rowsInMemory) {
            try {
                Full.reserve(rowsInMemory);
            } catch (TMemoryLimitExceededException) {
                // Can't grow Full — that's OK, SealInMemory will use chunked sort
            }
        }
        auto ptr = Pointer = Storage.data() + Storage.size() - Indexes.size();
        std::for_each(Indexes.cbegin(), Indexes.cend(), [&](ui32 index) { Fields[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
    }

    bool Seal() {
        if (SealedBucketCount() == 0 && CurrentSpillBucket == NoIndex) {
            SealInMemory();
            // If chunked sort was triggered (Full couldn't hold all rows),
            // we need to spill the chunks — switch to spilling mode
            if (HasMoreChunksToSeal()) {
                SwitchMode(EOperatingMode::Spilling);
            }
        } else if (CurrentSpillBucket != NoIndex || SealedBucketCount() > 0) {
            SwitchMode(EOperatingMode::Spilling);
        }
        return IsReadyToContinue();
    }

    NUdf::TUnboxedValue* Extract() {
        if (!IsReadyToContinue()) {
            return nullptr;
        }

        if (SpilledUnboxedValuesIterators.empty()) {
            return ExtractInMemory();
        }

        auto end = std::remove_if(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end(),
            [](const TSpilledUnboxedValuesIterator& it) { return it.IsFinished(); });
        SpilledUnboxedValuesIterators.erase(end, SpilledUnboxedValuesIterators.end());
        if (SpilledUnboxedValuesIterators.empty()) {
            return nullptr;
        }

        auto minIt = std::min_element(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end(),
            [](const TSpilledUnboxedValuesIterator& a, const TSpilledUnboxedValuesIterator& b) { return b < a; });
        Storage = minIt->Pop();
        return Storage.data();
    }

private:
    EOperatingMode GetMode() const {
        return Mode;
    }

    bool HasMemoryForProcessing() const {
        if (TlsAllocState->IsMemoryYellowZoneEnabled()) {
            return false;
        }
        if (TlsAllocState->GetMaximumLimitValueReached()) {
            return false;
        }
        return true;
    }

    bool IsReadFromChannelFinished() const {
        return InputStatus == EFetchResult::Finish;
    }

    static const char* ModeName(EOperatingMode m) {
        switch (m) {
            case EOperatingMode::InMemory:       return "InMemory";
            case EOperatingMode::Spilling:       return "Spilling";
            case EOperatingMode::MergeSpilled:   return "MergeSpilled";
            case EOperatingMode::ProcessSpilled: return "ProcessSpilled";
        }
        return "Unknown";
    }

    void SwitchMode(EOperatingMode mode) {
        {
            const size_t rowsInMemory = Indexes.size() > 0 ? Storage.size() / Indexes.size() : 0;
            UDF_LOG(Logger, LogComponent, NUdf::ELogLevel::Info, TStringBuilder()
                << (const void*)this << "# SwitchMode "
                << ModeName(Mode) << " -> " << ModeName(mode)
                << " | memUsed=" << TlsAllocState->GetUsed()
                << " memLimit=" << TlsAllocState->GetLimit()
                << " yellowZone=" << (TlsAllocState->IsMemoryYellowZoneEnabled() ? "yes" : "no")
                << " maxLimitReached=" << (TlsAllocState->GetMaximumLimitValueReached() ? "yes" : "no")
                << " rowsInMemory=" << rowsInMemory
                << " lastSpilledRows=" << LastSpilledRows
                << " sealedBuckets=" << SealedBucketCount());
        }
        switch (mode) {
            case EOperatingMode::InMemory:
                break;
            case EOperatingMode::Spilling: {
                CurrentSpillBucket = AcquireFreeBucket();
                Buckets[CurrentSpillBucket].Open();
                break;
            }
            case EOperatingMode::MergeSpilled: {
                StartMerge();
                break;
            }
            case EOperatingMode::ProcessSpilled: {
                SpilledUnboxedValuesIterators.clear();
                for (size_t i = 0; i < MaxBuckets; ++i) {
                    if (Buckets[i].IsSealed()) {
                        SpilledUnboxedValuesIterators.emplace_back(LessFunc, Buckets[i], Indexes.size(), Ctx);
                    }
                }
                break;
            }
        }
        Mode = mode;
    }

    bool SpillState() {
        MKQL_ENSURE(CurrentSpillBucket != NoIndex, "No active spill bucket");
        auto& bucket = Buckets[CurrentSpillBucket];
        if (bucket.HasPendingWrite()) {
            if (!bucket.IsWriteReady()) {
                return false;
            }
            bucket.AsyncWriteCompleted();
            // If FinishWrite was already initiated and just completed, go straight to cleanup
            if (IsFinishWriteInProgress) {
                IsFinishWriteInProgress = false;
                bucket.Seal();
                CurrentSpillBucket = NoIndex;
                TStorage().swap(Storage);
                Full.clear();
                Full.shrink_to_fit();
                SealStarted = false;
                return true;
            }
        } else {
            SealInMemory();
            LastSpilledRows = Full.size();
            if (Full.empty()) {
                // Nothing to spill — release the bucket
                bucket.Reset();
                CurrentSpillBucket = NoIndex;
                SealStarted = false;
                return true;
            }
        }

        for (;;) {
            while (auto extract = ExtractInMemory()) {
                auto writeOp = bucket.Write(extract, Indexes.size());
                if (writeOp) {
                    return false;
                }
            }

            // If there are more chunks to sort and spill, seal the next chunk
            if (!HasMoreChunksToSeal()) {
                break;
            }
            SealInMemory();
            LastSpilledRows += Full.size();
            if (Full.empty()) {
                break;
            }
        }

        auto writeFinishOp = bucket.FinishWrite();
        if (writeFinishOp) {
            IsFinishWriteInProgress = true;
            return false;
        }
        bucket.Seal();
        CurrentSpillBucket = NoIndex;
        TStorage().swap(Storage);
        Full.clear();
        Full.shrink_to_fit();
        SealStarted = false;
        return true;
    }

    size_t AcquireFreeBucket() const {
        for (size_t i = 0; i < MaxBuckets; ++i) {
            if (Buckets[i].IsEmpty()) {
                return i;
            }
        }
        MKQL_ENSURE(false, "No free buckets available");
        return NoIndex;
    }

    size_t SealedBucketCount() const {
        size_t count = 0;
        for (size_t i = 0; i < MaxBuckets; ++i) {
            if (Buckets[i].IsSealed()) {
                ++count;
            }
        }
        return count;
    }

    // Find the two sealed buckets with smallest RowCount
    std::pair<size_t, size_t> FindTwoSmallestBuckets() const {
        size_t min1 = NoIndex, min2 = NoIndex;
        for (size_t i = 0; i < MaxBuckets; ++i) {
            if (!Buckets[i].IsSealed()) continue;
            if (min1 == NoIndex) {
                min1 = i;
            } else if (min2 == NoIndex) {
                min2 = i;
                if (Buckets[min2].GetRowCount() < Buckets[min1].GetRowCount()) {
                    std::swap(min1, min2);
                }
            } else if (Buckets[i].GetRowCount() < Buckets[min2].GetRowCount()) {
                min2 = i;
                if (Buckets[min2].GetRowCount() < Buckets[min1].GetRowCount()) {
                    std::swap(min1, min2);
                }
            }
        }
        MKQL_ENSURE(min1 != NoIndex && min2 != NoIndex, "Need at least 2 sealed buckets to merge");
        return {min1, min2};
    }

    void StartMerge() {
        auto [src1, src2] = FindTwoSmallestBuckets();
        MergeSourceBuckets[0] = src1;
        MergeSourceBuckets[1] = src2;
        MergeSourceCount = 2;
        MergeRowCount = 0;

        // Acquire a free bucket for the merge target
        // We need to temporarily mark sources as non-empty so AcquireFreeBucket skips them
        MergeTargetBucket = AcquireFreeBucket();
        Buckets[MergeTargetBucket].Open();

        MergeIterators.clear();
        MergeIterators.reserve(MergeSourceCount);
        for (size_t i = 0; i < MergeSourceCount; ++i) {
            MergeIterators.emplace_back(LessFunc, Buckets[MergeSourceBuckets[i]], Indexes.size(), Ctx);
        }
        MergeHeapBuilt = false;
        MergeFinishWriteInProgress = false;
    }

    bool MergeStep() {
        auto& target = Buckets[MergeTargetBucket];
        if (target.HasPendingWrite()) {
            if (!target.IsWriteReady()) {
                return false;
            }
            target.AsyncWriteCompleted();
            if (MergeFinishWriteInProgress) {
                MergeFinishWriteInProgress = false;
                FinishMerge();
                return true;
            }
        }

        // Initiate reads from ALL merge iterators simultaneously before checking results
        {
            bool allReady = true;
            for (auto& it : MergeIterators) {
                if (!it.CheckForInit()) {
                    allReady = false;
                }
            }
            if (!allReady) {
                return false;
            }
        }

        if (!MergeHeapBuilt) {
            auto end = std::remove_if(MergeIterators.begin(), MergeIterators.end(),
                [](const TSpilledUnboxedValuesIterator& it) { return it.IsFinished(); });
            MergeIterators.erase(end, MergeIterators.end());
            std::make_heap(MergeIterators.begin(), MergeIterators.end());
            MergeHeapBuilt = true;
        }

        while (!MergeIterators.empty()) {
            std::pop_heap(MergeIterators.begin(), MergeIterators.end());
            auto& currentIt = MergeIterators.back();
            auto row = currentIt.Pop();
            bool finished = currentIt.IsFinished();
            if (finished) {
                MergeIterators.pop_back();
            } else if (currentIt.CheckForInit()) {
                std::push_heap(MergeIterators.begin(), MergeIterators.end());
            } else {
                ++MergeRowCount;
                auto writeOp = target.Write(row.data(), Indexes.size());
                if (writeOp) {
                    MergeHeapBuilt = false;
                    return false;
                }
                MergeHeapBuilt = false;
                return false;
            }

            ++MergeRowCount;
            auto writeOp = target.Write(row.data(), Indexes.size());
            if (writeOp) {
                return false;
            }
        }

        auto writeFinishOp = target.FinishWrite();
        if (writeFinishOp) {
            MergeFinishWriteInProgress = true;
            return false;
        }
        FinishMerge();
        return true;
    }

    void FinishMerge() {
        // Reset source buckets — they are now free for reuse
        for (size_t i = 0; i < MergeSourceCount; ++i) {
            Buckets[MergeSourceBuckets[i]].Reset();
        }
        // Seal the target bucket
        Buckets[MergeTargetBucket].Seal();
        MergeIterators.clear();
        MergeSourceCount = 0;
        MergeTargetBucket = NoIndex;
    }

    NUdf::TUnboxedValue* ExtractInMemory() {
        if (Full.empty()) {
            return nullptr;
        }

        const auto ptr = Full.back();
        Full.pop_back();
        return static_cast<NUdf::TUnboxedValue*>(ptr);
    }

    void SealInMemory() {
        // Remove placeholder for new data
        if (!SealStarted) {
            Storage.resize(Storage.size() - Indexes.size());
            SealOffset = 0;
            SealStarted = true;
        }

        const size_t totalRows = Storage.size() / Indexes.size();
        const size_t alreadyProcessed = SealOffset / Indexes.size();
        const size_t remainingRows = totalRows - alreadyProcessed;

        if (remainingRows == 0) {
            SealStarted = false;
            return;
        }

        // Try to reserve Full for all remaining rows
        try {
            Full.reserve(remainingRows);
        } catch (TMemoryLimitExceededException) {
            // Can't fit all rows — use whatever capacity Full already has
            UDF_LOG(Logger, LogComponent, NUdf::ELogLevel::Info, TStringBuilder()
                 << (const void*)this << "# Full.reserve failed, using chunked sort"
                 << " | remainingRows=" << remainingRows
                 << " fullCapacity=" << Full.capacity());
            if (Full.capacity() == 0) {
                // Can't sort at all — fatal
                throw;
            }
        }

        // Fill Full with as many rows as capacity allows
        const size_t chunkSize = std::min(remainingRows, Full.capacity());
        Full.clear();
        auto it = Storage.begin() + SealOffset;
        for (size_t i = 0; i < chunkSize; ++i, it += Indexes.size()) {
            Full.emplace_back(&*it);
        }
        SealOffset += chunkSize * Indexes.size();

        std::sort(Full.rbegin(), Full.rend(), LessFunc);
    }

    bool HasMoreChunksToSeal() const {
        return SealStarted && SealOffset < Storage.size();
    }

public:
    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Pointer = nullptr;

private:
    const std::vector<ui32> Indexes;
    const std::vector<bool> Directions;
    const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc;
    TStorage Storage;
    TPointers Free, Full;
    TFields Fields;
    TMultiType* TupleMultiType;
    const TComputationContext& Ctx;
    const bool AllowSpilling;
    const NUdf::TLoggerPtr Logger;
    const NUdf::TLogComponentId LogComponent;
    static constexpr size_t MaxBuckets = 10;
    static constexpr size_t NoIndex = std::numeric_limits<size_t>::max();
    static constexpr size_t MinSpillBatchRows = 2;
    std::array<TBucket, MaxBuckets> Buckets;
    size_t CurrentSpillBucket = NoIndex;
    EOperatingMode Mode = EOperatingMode::InMemory;
    std::vector<TSpilledUnboxedValuesIterator> SpilledUnboxedValuesIterators;
    ISpiller::TPtr Spiller = nullptr;
    bool IsFinishWriteInProgress = false;
    std::vector<TSpilledUnboxedValuesIterator> MergeIterators;
    std::array<size_t, 2> MergeSourceBuckets = {NoIndex, NoIndex};
    size_t MergeTargetBucket = NoIndex;
    size_t MergeSourceCount = 0;
    size_t MergeRowCount = 0;
    bool MergeHeapBuilt = false;
    bool MergeFinishWriteInProgress = false;
    size_t LastSpilledRows = 0;
    bool SealStarted = false;
    size_t SealOffset = 0;
};

class TWideSortWrapper: public TStatefulWideFlowCodegeneratorNode<TWideSortWrapper>
#ifndef MKQL_DISABLE_CODEGEN
    ,
                        public ICodegeneratorRootNode
#endif
{
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideSortWrapper>;

public:
    TWideSortWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationNodePtrVector&& directions, std::vector<TKeyInfo>&& keys,
                     std::vector<ui32>&& indexes, std::vector<EValueRepresentation>&& representations, TMultiType* tupleMultiType, bool allowSpilling)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow(flow)
        , Directions(std::move(directions))
        , Keys(std::move(keys))
        , Indexes(std::move(indexes))
        , Representations(std::move(representations))
        , TupleMultiType(tupleMultiType)
        , AllowSpilling(allowSpilling)
    {
        for (const auto& x : Keys) {
            if (x.Compare || x.PresortType) {
                KeyTypes.clear();
                HasComplexType = true;
                break;
            }

            KeyTypes.emplace_back(x.Slot, x.IsOptional);
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            std::vector<bool> dirs(Directions.size());
            std::transform(Directions.cbegin(), Directions.cend(), dirs.begin(), [&ctx](IComputationNode* dir) { return dir->GetValue(ctx).Get<bool>(); });
            MakeState(ctx, state, dirs.data());
        }

        if (const auto ptr = static_cast<TSpillingSupportState*>(state.AsBoxed().Get())) {
            while (EFetchResult::Finish != ptr->InputStatus) {
                if (!ptr->IsReadyToContinue()) {
                    return EFetchResult::Yield;
                }
                switch (ptr->InputStatus = Flow->FetchValues(ctx, ptr->GetFields())) {
                    case EFetchResult::One:
                        ptr->Put();
                        continue;
                    case EFetchResult::Finish:
                        if (ptr->Seal()) {
                            break;
                        }
                        [[fallthrough]];
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                }
            }

            if (auto extract = ptr->Extract()) {
                for (const auto index : Indexes) {
                    if (const auto to = output[index]) {
                        *to = std::move(*extract++);
                    } else {
                        ++extract;
                    }
                }
                return EFetchResult::One;
            }

            auto finished = ptr->IsFinished();
            return finished ? EFetchResult::Finish : EFetchResult::Yield;
        }

        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        DIScopeAnnotator annotate(ctx.Annotator);

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(ctx.Codegen.GetContext());

        TLLVMFieldsStructureState<TSpillingSupportState> stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto outputType = ArrayType::get(valueType, Representations.size());
        const auto outputPtrType = PointerType::getUnqual(outputType);
        const auto outs = annotate(new AllocaInst(outputPtrType, 0U, "outs", &ctx.Func->getEntryBlock().back()));

        ICodegeneratorInlineWideNode::TGettersList getters(Representations.size());

        for (auto i = 0U; i < getters.size(); ++i) {
            getters[Indexes[i]] = [i, outs, indexType, valueType, outputPtrType, outputType](const TCodegenContext& ctx, BasicBlock*& block) {
                DIScopeAnnotator annotate(ctx.Annotator);
                const auto values = annotate(new LoadInst(outputPtrType, outs, "values", block));
                const auto pointer = annotate(GetElementPtrInst::CreateInBounds(outputType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block));
                return annotate(new LoadInst(valueType, pointer, (TString("load_") += ToString(i)).c_str(), block));
            };
        }

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        annotate(BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block));
        block = make;

        const auto arrayType = ArrayType::get(Type::getInt1Ty(context), Directions.size());
        const auto dirs = annotate(new AllocaInst(arrayType, 0U, "dirs", block));
        for (auto i = 0U; i < Directions.size(); ++i) {
            const auto dir = GetNodeValue(Directions[i], ctx, block);
            const auto cut = GetterFor<bool>(dir, context, block);
            const auto ptr = annotate(GetElementPtrInst::CreateInBounds(arrayType, dirs, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, "ptr", block));
            annotate(new StoreInst(cut, ptr, block));
        }

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = annotate(CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block));
        EmitFunctionCall<&TWideSortWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr, dirs}, ctx, block);
        annotate(BranchInst::Create(main, block));

        block = main;

        const auto state = annotate(new LoadInst(valueType, statePtr, "state", block));
        const auto half = annotate(CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block));
        const auto stateArg = annotate(CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block));
        annotate(BranchInst::Create(more, block));

        block = more;

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 5U, "result", over);

        const auto statusPtr = annotate(GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStatus()}, "last", block));
        const auto last = annotate(new LoadInst(statusType, statusPtr, "last", block));
        const auto finish = annotate(CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, last, ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), "finish", block));

        annotate(BranchInst::Create(full, loop, finish, block));

        {
            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);

            block = loop;

            const auto process = EmitFunctionCall<&TSpillingSupportState::IsReadyToContinue>(Type::getInt1Ty(context), {stateArg}, ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            annotate(BranchInst::Create(pull, over, process, block));

            block = pull;

            const auto getres = GetNodeValues(Flow, ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            const auto choise = static_cast<SwitchInst*>(annotate(SwitchInst::Create(getres.first, good, 2U, block)));
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), over);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), rest);

            block = rest;

            annotate(new StoreInst(ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), statusPtr, block));
            const auto stop = EmitFunctionCall<&TSpillingSupportState::Seal>(Type::getInt1Ty(context), {stateArg}, ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            annotate(BranchInst::Create(full, over, stop, block));

            block = good;

            const auto tonguePtr = annotate(GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetTongue()}, "tongue_ptr", block));
            const auto tongue = annotate(new LoadInst(ptrValueType, tonguePtr, "tongue", block));

            std::vector<Value*> placeholders(Representations.size());
            for (auto i = 0U; i < placeholders.size(); ++i) {
                placeholders[i] = annotate(GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(indexType, i)}, (TString("placeholder_") += ToString(i)).c_str(), block));
            }

            for (auto i = 0U; i < Representations.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                ValueAddRef(Representations[i], item, ctx, block);
                annotate(new StoreInst(item, placeholders[i], block));
            }

            EmitFunctionCall<&TSpillingSupportState::Put>(Type::getVoidTy(context), {stateArg}, ctx, block);

            annotate(BranchInst::Create(loop, block));
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto last = BasicBlock::Create(context, "last", ctx.Func);

            const auto out = EmitFunctionCall<&TSpillingSupportState::Extract>(outputPtrType, {stateArg}, ctx, block);
            const auto has = annotate(CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(outputPtrType), "has", block));

            annotate(BranchInst::Create(good, last, has, block));

            block = good;

            annotate(new StoreInst(out, outs, block));

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            annotate(BranchInst::Create(over, block));

            block = last;

            const auto finished = EmitFunctionCall<&TSpillingSupportState::IsFinished>(Type::getInt1Ty(context), {stateArg}, ctx, block);
            const auto output = SelectInst::Create(finished,
                                                   ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)),
                                                   ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)),
                                                   "output", block);

            result->addIncoming(output, block);
            annotate(BranchInst::Create(over, block));
        }

        block = over;
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state, const bool* directions) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
        NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("Sort");
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TSpillingSupportState>(directions, Directions.size(), TMyValueCompare(Keys), Indexes, TupleMultiType, ctx, AllowSpilling, std::move(logger), logComponent);
#else
        state = ctx.HolderFactory.Create<TSpillingSupportState>(directions, Directions.size(), ctx.ExecuteLLVM && Compare ? TCompareFunc(Compare) : TCompareFunc(TMyValueCompare(Keys)), Indexes, TupleMultiType, ctx, AllowSpilling, std::move(logger), logComponent);
#endif
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Directions.cbegin(), Directions.cend(), std::bind(&TWideSortWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow;
    const TComputationNodePtrVector Directions;
    const std::vector<TKeyInfo> Keys;
    const std::vector<ui32> Indexes;
    const std::vector<EValueRepresentation> Representations;
    TKeyTypes KeyTypes;
    TMultiType* const TupleMultiType;
    const bool AllowSpilling;
    bool HasComplexType = false;

#ifndef MKQL_DISABLE_CODEGEN
    TComparePtr Compare = nullptr;

    Function* CompareFunc = nullptr;

    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::Compare_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (CompareFunc) {
            Compare = reinterpret_cast<TComparePtr>(codegen.GetPointerToFunction(CompareFunc));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (!HasComplexType) {
            codegen.ExportSymbol(CompareFunc = GenerateCompareFunction(codegen, MakeName(), KeyTypes));
        }
    }
#endif
};

} // namespace

template <bool Sort, bool HasCount>
IComputationNode* WrapWideTopT(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    constexpr ui32 offset = HasCount ? 0 : 1;
    const ui32 inputsWithCount = callable.GetInputsCount() + offset;
    MKQL_ENSURE(inputsWithCount > 2U && !(inputsWithCount % 2U), "Expected more arguments.");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    IComputationNode* count = nullptr;
    if constexpr (HasCount) {
        const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
        MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");
        count = LocateNode(ctx.NodeLocator, callable, 1);
    }

    const auto keyWidth = (inputsWithCount >> 1U) - 1U;
    const auto inputWideComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    std::vector<ui32> indexes(inputWideComponents.size());

    std::unordered_set<ui32> keyIndexes;
    std::vector<TKeyInfo> keys(keyWidth);
    std::vector<TType*> tupleTypes;
    tupleTypes.reserve(inputWideComponents.size());

    for (auto i = 0U; i < keyWidth; ++i) {
        const auto keyIndex = AS_VALUE(TDataLiteral, callable.GetInput(((i + 1U) << 1U) - offset))->AsValue().Get<ui32>();
        indexes[i] = keyIndex;
        keyIndexes.emplace(keyIndex);
        tupleTypes.emplace_back(inputWideComponents[keyIndex]);

        bool isTuple;
        bool encoded;
        bool useIHash;
        TKeyTypes oneKeyTypes;
        GetDictionaryKeyTypes(inputWideComponents[keyIndex], oneKeyTypes, isTuple, encoded, useIHash, false);
        if (useIHash) {
            keys[i].Compare = MakeCompareImpl(inputWideComponents[keyIndex]);
        } else if (encoded) {
            keys[i].PresortType = inputWideComponents[keyIndex];
        } else {
            Y_ENSURE(oneKeyTypes.size() == 1);
            keys[i].Slot = oneKeyTypes.front().first;
            keys[i].IsOptional = oneKeyTypes.front().second;
        }
    }

    size_t payloadPos = keyWidth;
    for (auto i = 0U; i < indexes.size(); ++i) {
        if (keyIndexes.contains(i)) {
            continue;
        }

        indexes[payloadPos++] = i;
        tupleTypes.emplace_back(inputWideComponents[i]);
    }

    std::vector<EValueRepresentation> representations(inputWideComponents.size());
    for (auto i = 0U; i < representations.size(); ++i) {
        representations[i] = GetValueRepresentation(inputWideComponents[indexes[i]]);
    }

    auto tupleMultiType = TMultiType::Create(tupleTypes.size(), tupleTypes.data(), ctx.Env);
    TComputationNodePtrVector directions(keyWidth);
    auto index = 1U - offset;
    std::generate(directions.begin(), directions.end(), [&]() { return LocateNode(ctx.NodeLocator, callable, ++ ++index); });

    const bool allowSpilling = HasSpillingFlag(callable);

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        if constexpr (HasCount) {
            return new TWideTopWrapper<Sort>(ctx.Mutables, wide, count, std::move(directions), std::move(keys),
                                             std::move(indexes), std::move(representations));
        } else {
            return new TWideSortWrapper(ctx.Mutables, wide, std::move(directions), std::move(keys),
                                        std::move(indexes), std::move(representations), tupleMultiType, allowSpilling);
        }
    }

    THROW yexception() << "Expected wide flow.";
}

IComputationNode* WrapWideTop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideTopT<false, true>(callable, ctx);
}

IComputationNode* WrapWideTopSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideTopT<true, true>(callable, ctx);
}

IComputationNode* WrapWideSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideTopT<true, false>(callable, ctx);
}

} // namespace NMiniKQL
} // namespace NKikimr
