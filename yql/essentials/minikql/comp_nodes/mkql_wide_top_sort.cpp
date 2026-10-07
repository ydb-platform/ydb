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

namespace NKikimr::NMiniKQL {

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
        for (auto i = 0U; i < Keys.size(); ++i) {
            auto& key = Keys[i];
            int cmp;
            if (key.Compare) {
                cmp = key.Compare->Compare(left[i], right[i]);
                if (!directions[i]) {
                    cmp = -cmp;
                }
            } else if (key.LeftPacker) {
                auto strLeft = key.LeftPacker->Encode(left[i], /*desc=*/false);
                auto strRight = key.RightPacker->Encode(right[i], /*desc=*/false);
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

struct TSpilledData {
    using TPtr = std::shared_ptr<TSpilledData>;

    TSpilledData() = default;

    void Open(ISpiller::TPtr spiller, const TMultiType* tupleMultiType, size_t packSize, NYql::EDatumValidationMode datumValidationMode) {
        MKQL_ENSURE(IsEmpty(), "SpilledData_ must be empty to Open");
        Spiller_ = std::make_unique<TWideUnboxedValuesSpillerAdapter>(spiller, tupleMultiType, packSize, datumValidationMode);
        RowCount_ = 0;
        AsyncWriteOperation_ = std::nullopt;
        AsyncReadOperation_ = std::nullopt;
        IsFinished_ = false;
        Sealed_ = false;
    }

    TAsyncWriteOperation Write(NUdf::TUnboxedValue* item, size_t size) {
        MKQL_ENSURE(Spiller_, "SpilledData_ must be opened to Write");
        MKQL_ENSURE(!Sealed_, "SpilledData_ must not be sealed to Write");
        ++RowCount_;
        AsyncWriteOperation_ = Spiller_->WriteWideItem({item, size});
        return AsyncWriteOperation_;
    }

    TAsyncWriteOperation FinishWrite() {
        MKQL_ENSURE(Spiller_, "SpilledData_ must be opened to FinishWrite");
        MKQL_ENSURE(!Sealed_, "SpilledData_ must not be sealed to FinishWrite");
        AsyncWriteOperation_ = Spiller_->FinishWriting();
        return AsyncWriteOperation_;
    }

    void CompleteAsyncWrite() {
        MKQL_ENSURE(AsyncWriteOperation_.has_value() && AsyncWriteOperation_->HasValue(),
                    "No completed async write operation");
        Spiller_->AsyncWriteCompleted(AsyncWriteOperation_->ExtractValue());
        AsyncWriteOperation_ = std::nullopt;
    }

    void Seal() {
        Sealed_ = true;
    }

    TAsyncReadOperation Read(TStorage& buffer, const TComputationContext& ctx) {
        if (AsyncReadOperation_) {
            if (AsyncReadOperation_->HasValue()) {
                Spiller_->AsyncReadCompleted(AsyncReadOperation_->ExtractValue().value(), ctx.HolderFactory);
                AsyncReadOperation_ = std::nullopt;
            } else {
                return AsyncReadOperation_;
            }
        }
        if (Spiller_->Empty()) {
            IsFinished_ = true;
            return std::nullopt;
        }
        AsyncReadOperation_ = Spiller_->ExtractWideItem(buffer);
        return AsyncReadOperation_;
    }

    void Reset() {
        Spiller_.reset();
        RowCount_ = 0;
        AsyncWriteOperation_ = std::nullopt;
        AsyncReadOperation_ = std::nullopt;
        IsFinished_ = false;
        Sealed_ = false;
    }

    bool IsEmpty() const {
        return !Spiller_;
    }
    bool IsSealed() const {
        return Sealed_;
    }
    bool IsReadFinished() const {
        return IsFinished_;
    }
    size_t GetRowCount() const {
        return RowCount_;
    }

    bool HasPendingWrite() const {
        return AsyncWriteOperation_.has_value();
    }
    bool IsWriteReady() const {
        return AsyncWriteOperation_.has_value() && AsyncWriteOperation_->HasValue();
    }

private:
    std::unique_ptr<TWideUnboxedValuesSpillerAdapter> Spiller_;
    size_t RowCount_ = 0;
    bool Sealed_ = false;
    TAsyncWriteOperation AsyncWriteOperation_ = std::nullopt;
    TAsyncReadOperation AsyncReadOperation_ = std::nullopt;
    bool IsFinished_ = false;
};

class TSpilledUnboxedValuesIterator {
private:
    TStorage Data_;
    TSpilledData::TPtr SpilledData_;
    std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc_;
    ui32 Width_;
    const TComputationContext* Ctx_;
    bool HasValue_ = false;

public:
    TSpilledUnboxedValuesIterator(
        const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>& lessFunc,
        TSpilledData::TPtr spilledData,
        size_t dataWidth,
        const TComputationContext* ctx)
        : SpilledData_(std::move(spilledData))
        , LessFunc_(lessFunc)
        , Width_(dataWidth)
        , Ctx_(ctx)
    {
        Data_.resize(Width_);
    }

    EFetchResult Read() {
        if (!HasValue_) {
            MKQL_ENSURE(SpilledData_, "Spilled iterator data is null");
            MKQL_ENSURE(Ctx_, "Spilled iterator context is null");
            if (SpilledData_->Read(Data_, *Ctx_)) {
                return EFetchResult::Yield;
            }
            if (SpilledData_->IsReadFinished()) {
                return EFetchResult::Finish;
            }
            HasValue_ = true;
        }
        return EFetchResult::One;
    }

    bool CheckForInit() {
        if (HasValue_ || IsFinished()) {
            return true;
        }
        EFetchResult result = Read();
        return result != EFetchResult::Yield;
    }

    bool IsFinished() const {
        return SpilledData_->IsReadFinished();
    }

    bool operator<(const TSpilledUnboxedValuesIterator& item) const {
        return !LessFunc_(GetValue(), item.GetValue());
    }

    ui32 Width() const {
        return Width_;
    }

    TStorage Pop() {
        auto data(std::move(Data_));
        Data_.resize(Width_);
        HasValue_ = false;
        Read();
        return data;
    }

    const NUdf::TUnboxedValue* GetValue() const {
        return &Data_.front();
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
        return std::max<size_t>(Count_ << 2ULL, 1ULL << 8ULL);
    }

    static constexpr size_t GetStorageBlockSize() {
        return 1ULL << 10ULL;
    }

    void ResetFields() {
        MaybeGrowStorage();
        auto ptr = Tongue = Free_.back();
        std::for_each(Indexes_.cbegin(), Indexes_.cend(), [&](ui32 index) { Fields_[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
    }

    void MaybeGrowStorage() {
        if (Free_.empty()) {
            const size_t fieldsCount = Indexes_.size();
            const size_t blockSize = std::min(GetStorageBlockSize(), GetStorageSize());
            TStorage& newStorageBlock = Storage_.emplace_back(blockSize * fieldsCount);
            auto* ptr = newStorageBlock.data();
            Free_.reserve(Free_.size() + blockSize);
            for (size_t i = 0; i < blockSize; ++i) {
                Free_.emplace_back(ptr);
                ptr += fieldsCount;
            }
        }
    }

public:
    TState(TMemoryUsageInfo* memInfo, ui64 count, const bool* directons, size_t keyWidth, const TCompareFunc& compare, const std::vector<ui32>& indexes)
        : TBase(memInfo)
        , Count_(count)
        , Indexes_(indexes)
        , Directions_(directons, directons + keyWidth)
        , LessFunc_(std::bind(std::less<int>(), std::bind(compare, Directions_.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Fields_(Indexes_.size(), nullptr)
    {
        if (Count_) {
            Full_.reserve(std::min(GetStorageBlockSize(), GetStorageSize()));
            ResetFields();
        } else {
            InputStatus = EFetchResult::Finish;
        }
    }

    NUdf::TUnboxedValue* const* GetFields() const {
        return Fields_.data();
    }

    bool Put() {
        if (Full_.size() + 1U == GetStorageSize()) {
            Free_.pop_back();

            NYql::FastNthElement(Full_.begin(), Full_.begin() + Count_, Full_.end(), LessFunc_);
            std::copy(Full_.cbegin() + Count_, Full_.cend(), std::back_inserter(Free_));
            Full_.resize(Count_);

            std::for_each(Free_.cbegin(), Free_.cend(), [this](NUdf::TUnboxedValuePod* ptr) {
                std::fill_n(static_cast<NUdf::TUnboxedValue*>(ptr), Indexes_.size(), NUdf::TUnboxedValuePod());
            });
            Free_.emplace_back(Tongue);
            Throat = nullptr;
        }

        if (Full_.size() >= Count_) {
            if (!Throat) {
                Throat = *std::max_element(Full_.cbegin(), Full_.cend(), LessFunc_);
            }

            if (!LessFunc_(Tongue, Throat)) {
                return false;
            }
        }

        Full_.emplace_back(Free_.back());
        Free_.pop_back();
        ResetFields();
        return true;
    }

    void Seal() {
        Free_.clear();
        Free_.shrink_to_fit();

        if (Full_.size() > Count_) {
            NYql::FastNthElement(Full_.begin(), Full_.begin() + Count_, Full_.end(), LessFunc_);
            Full_.resize(Count_);
        }

        if constexpr (Sort) {
            std::sort(Full_.rbegin(), Full_.rend(), LessFunc_);
        }
    }

    NUdf::TUnboxedValue* Extract() {
        if (Full_.empty()) {
            return nullptr;
        }

        const auto ptr = Full_.back();
        Full_.pop_back();
        return static_cast<NUdf::TUnboxedValue*>(ptr);
    }

    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Tongue = nullptr;
    NUdf::TUnboxedValuePod* Throat = nullptr;

private:
    const ui64 Count_;
    const std::vector<ui32> Indexes_;
    const std::vector<bool> Directions_;
    const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc_;
    TStorageDeque Storage_;
    TPointers Free_, Full_;
    TFields Fields_;
};

#ifndef MKQL_DISABLE_CODEGEN
template <class TState>
class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
    llvm::IntegerType* ValueType_;
    llvm::PointerType* PtrValueType_;
    llvm::IntegerType* StatusType_;

protected:
    using TBase::GetContext;

public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StatusType_);   // status
        result.emplace_back(PtrValueType_); // tongue
        return result;
    }

    llvm::Constant* GetStatus() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetTongue() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 1);
    }

    explicit TLLVMFieldsStructureState(llvm::LLVMContext& context)
        : TBase(context)
        , ValueType_(Type::getInt128Ty(context))
        , PtrValueType_(PointerType::getUnqual(ValueType_))
        , StatusType_(Type::getInt32Ty(context))
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
        , Flow_(flow)
        , Count_(count)
        , Directions_(std::move(directions))
        , Keys_(std::move(keys))
        , Indexes_(std::move(indexes))
        , Representations_(std::move(representations))
    {
        for (const auto& x : Keys_) {
            if (x.Compare || x.PresortType) {
                KeyTypes_.clear();
                HasComplexType_ = true;
                break;
            }

            KeyTypes_.emplace_back(x.Slot, x.IsOptional);
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            const auto count = Count_->GetValue(ctx).Get<ui64>();
            std::vector<bool> dirs(Directions_.size());
            std::transform(Directions_.cbegin(), Directions_.cend(), dirs.begin(), [&ctx](IComputationNode* dir) { return dir->GetValue(ctx).Get<bool>(); });
            MakeState(ctx, state, count, dirs.data());
        }

        if (const auto ptr = static_cast<TState<Sort>*>(state.AsBoxed().Get())) {
            while (EFetchResult::Finish != ptr->InputStatus) {
                switch (ptr->InputStatus = Flow_->FetchValues(ctx, ptr->GetFields())) {
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
                for (const auto index : Indexes_) {
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
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        DIScopeAnnotator annotate(ctx.Annotator);

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(ctx.Codegen.GetContext());

        TLLVMFieldsStructureState<TState<Sort>> stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto outputType = ArrayType::get(valueType, Representations_.size());
        const auto outputPtrType = PointerType::getUnqual(outputType);
        const auto outs = annotate(new AllocaInst(outputPtrType, 0U, "outs", &ctx.Func->getEntryBlock().back()));

        ICodegeneratorInlineWideNode::TGettersList getters(Representations_.size());

        for (auto i = 0U; i < getters.size(); ++i) {
            getters[Indexes_[i]] = [i, outs, indexType, valueType, outputPtrType, outputType](const TCodegenContext& ctx, BasicBlock*& block) {
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

        const auto count = GetNodeValue(Count_, ctx, block);
        const auto trunc = GetterFor<ui64>(count, context, block);

        const auto arrayType = ArrayType::get(Type::getInt1Ty(context), Directions_.size());
        const auto dirs = annotate(new AllocaInst(arrayType, 0U, "dirs", block));
        for (auto i = 0U; i < Directions_.size(); ++i) {
            const auto dir = GetNodeValue(Directions_[i], ctx, block);
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

            const auto getres = GetNodeValues(Flow_, ctx, block);

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

            std::vector<Value*> placeholders(Representations_.size());
            for (auto i = 0U; i < placeholders.size(); ++i) {
                placeholders[i] = annotate(GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(indexType, i)}, (TString("placeholder_") += ToString(i)).c_str(), block));
            }

            for (auto i = 0U; i < Keys_.size(); ++i) {
                const auto item = getres.second[Indexes_[i]](ctx, block);
                annotate(new StoreInst(item, placeholders[i], block));
            }

            const auto accepted = EmitFunctionCall<&TState<Sort>::Put>(Type::getInt1Ty(context), {stateArg}, ctx, block);

            const auto push = BasicBlock::Create(context, "push", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            annotate(BranchInst::Create(push, skip, accepted, block));

            block = push;

            for (auto i = 0U; i < Keys_.size(); ++i) {
                ValueAddRef(Representations_[i], placeholders[i], ctx, block);
            }

            for (auto i = Keys_.size(); i < Representations_.size(); ++i) {
                const auto item = getres.second[Indexes_[i]](ctx, block);
                ValueAddRef(Representations_[i], item, ctx, block);
                annotate(new StoreInst(item, placeholders[i], block));
            }

            annotate(BranchInst::Create(loop, block));

            block = skip;

            for (auto i = 0U; i < Keys_.size(); ++i) {
                ValueCleanup(Representations_[i], placeholders[i], ctx, block);
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
        state = ctx.HolderFactory.Create<TState<Sort>>(count, directions, Directions_.size(), TMyValueCompare(Keys_), Indexes_);
#else
        state = ctx.HolderFactory.Create<TState<Sort>>(count, directions, Directions_.size(), ctx.ExecuteLLVM && Compare_ ? TCompareFunc(Compare_) : TCompareFunc(TMyValueCompare(Keys_)), Indexes_);
#endif
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            TWideTopWrapper::DependsOn(flow, Count_);
            std::for_each(Directions_.cbegin(), Directions_.cend(), std::bind(&TWideTopWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow_;
    IComputationNode* const Count_;
    const TComputationNodePtrVector Directions_;
    const std::vector<TKeyInfo> Keys_;
    const std::vector<ui32> Indexes_;
    const std::vector<EValueRepresentation> Representations_;
    TKeyTypes KeyTypes_;
    bool HasComplexType_ = false;

#ifndef MKQL_DISABLE_CODEGEN
    TComparePtr Compare_ = nullptr;

    Function* CompareFunc_ = nullptr;

    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::Compare_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (CompareFunc_) {
            Compare_ = reinterpret_cast<TComparePtr>(codegen.GetPointerToFunction(CompareFunc_));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (!HasComplexType_) {
            codegen.ExportSymbol(CompareFunc_ = GenerateCompareFunction(codegen, MakeName(), KeyTypes_));
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

    enum class ESpillReason {
        None,
        YellowZone,
        FinalFlush,
        Exception,
    };

    TString SpillReasonName(ESpillReason r) {
        switch (r) {
            case ESpillReason::None:
                return "None";
            case ESpillReason::YellowZone:
                return "YellowZone";
            case ESpillReason::FinalFlush:
                return "FinalFlush";
            case ESpillReason::Exception:
                return "Exception";
        }
        return "Unknown";
    }

    bool HasPlaceholder() const {
        return SpillReason_ != ESpillReason::Exception;
    }

    struct TMergeState {
        TSpilledData::TPtr Target;
        std::vector<TSpilledUnboxedValuesIterator> Iterators;
        bool HeapBuilt = false;
        bool FinishWriteInProgress = false;
    };

    void ResetFields() {
        const size_t newRowCount = Storage_.size() / Indexes_.size() + 1;

        try {
            if (Full_.capacity() < newRowCount) {
                Full_.reserve(newRowCount);
            }
            Storage_.insert(Storage_.end(), Indexes_.size(), {});
        } catch (const TMemoryLimitExceededException&) {
            if (CanSpill()) {
                SpillReason_ = ESpillReason::Exception;
                SwitchMode(EOperatingMode::Spilling);
                return;
            }
            throw;
        }

        const auto pos = Storage_.size() - Indexes_.size();
        auto ptr = Pointer = Storage_.data() + pos;
        std::for_each(Indexes_.cbegin(), Indexes_.cend(), [&](ui32 index) { Fields_[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });

        if (CanSpill() && !HasMemoryForProcessing() && HasEnoughRowsToSpill()) {
            SpillReason_ = ESpillReason::YellowZone;
            SwitchMode(EOperatingMode::Spilling);
        }
    }

public:
    TSpillingSupportState(TMemoryUsageInfo* memInfo, const bool* directons, size_t keyWidth, const TCompareFunc& compare,
                          const std::vector<ui32>& indexes, TMultiType* tupleMultiType, const TComputationContext& ctx,
                          bool allowSpilling, NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent)
        : TBase(memInfo)
        , Indexes_(indexes)
        , Directions_(directons, directons + keyWidth)
        , LessFunc_(std::bind(std::less<>(), std::bind(compare, Directions_.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Fields_(Indexes_.size(), nullptr)
        , TupleMultiType_(tupleMultiType)
        , Ctx_(ctx)
        , AllowSpilling_(allowSpilling)
        , Logger_(std::move(logger))
        , LogComponent_(logComponent)
    {
        if (AllowSpilling_ && Ctx_.SpillerFactory) {
            Spiller_ = Ctx_.SpillerFactory->CreateSpiller();
        }
        UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << (const void*)this << "# Sort state initialized, allowSpilling=" << AllowSpilling_);
        ResetFields();
    }

    bool IsReadyToContinue() {
        switch (Mode_) {
            case EOperatingMode::InMemory:
                return true;
            case EOperatingMode::Spilling: {
                if (!SpillState()) {
                    return false;
                }
                ResetFields();
                if (ActiveSpill_) {
                    return IsReadyToContinue();
                }
                SwitchMode(ChooseNextMode());
                return IsReadyToContinue();
            }
            case EOperatingMode::MergeSpilled: {
                if (!MergeStep()) {
                    return false;
                }
                SwitchMode(ChooseNextMode());
                return IsReadyToContinue();
            }
            case EOperatingMode::ProcessSpilled: {
                if (SpilledUnboxedValuesIterators_.empty()) {
                    return true;
                }
                bool allReady = true;
                for (auto& it : SpilledUnboxedValuesIterators_) {
                    if (!it.CheckForInit()) {
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
        if (Mode_ == EOperatingMode::Spilling || Mode_ == EOperatingMode::MergeSpilled) {
            return false;
        }
        if (Mode_ == EOperatingMode::ProcessSpilled) {
            return SpilledUnboxedValuesIterators_.empty();
        }
        return SealedStates_.empty();
    }

    NUdf::TUnboxedValue* const* GetFields() const {
        return Fields_.data();
    }

    void Put() {
        ResetFields();
    }

    bool Seal() {
        if (SealedStates_.empty() && !ActiveSpill_) {
            SealInMemory();
        } else {
            SpillReason_ = ESpillReason::FinalFlush;
            SwitchMode(EOperatingMode::Spilling);
        }
        return IsReadyToContinue();
    }

    NUdf::TUnboxedValue* Extract() {
        if (!IsReadyToContinue()) {
            return nullptr;
        }

        if (SpilledUnboxedValuesIterators_.empty()) {
            return ExtractInMemory();
        }

        auto end = std::remove_if(SpilledUnboxedValuesIterators_.begin(), SpilledUnboxedValuesIterators_.end(),
                                  [](const TSpilledUnboxedValuesIterator& it) { return it.IsFinished(); });
        SpilledUnboxedValuesIterators_.erase(end, SpilledUnboxedValuesIterators_.end());
        if (SpilledUnboxedValuesIterators_.empty()) {
            return nullptr;
        }

        auto minIt = std::min_element(SpilledUnboxedValuesIterators_.begin(), SpilledUnboxedValuesIterators_.end(),
                                      [](const TSpilledUnboxedValuesIterator& a, const TSpilledUnboxedValuesIterator& b) { return b < a; });
        Storage_ = minIt->Pop();
        return Storage_.data();
    }

private:
    bool CanSpill() const {
        return AllowSpilling_ && Ctx_.SpillerFactory;
    }

    bool HasEnoughRowsToSpill() const {
        return Storage_.size() / Indexes_.size() >= MinSpillBatchRows;
    }

    bool HasMemoryForProcessing() const {
        return !TlsAllocState->IsMemoryYellowZoneEnabled();
    }

    bool IsReadFromChannelFinished() const {
        return InputStatus == EFetchResult::Finish;
    }

    EOperatingMode ChooseNextMode() const {
        MKQL_ENSURE(Mode_ == EOperatingMode::Spilling || Mode_ == EOperatingMode::MergeSpilled,
                    "ChooseNextMode called from unexpected mode: " << ModeName(Mode_));
        MKQL_ENSURE(!ActiveSpill_, "ChooseNextMode called with active spill still in progress");
        MKQL_ENSURE(!Merge_, "ChooseNextMode called with active merge still in progress");
        if (SealedStates_.size() >= MaxSealedStates) {
            return EOperatingMode::MergeSpilled;
        } else if (IsReadFromChannelFinished()) {
            return EOperatingMode::ProcessSpilled;
        } else {
            return EOperatingMode::InMemory;
        }
    }

    static const char* ModeName(EOperatingMode m) {
        switch (m) {
            case EOperatingMode::InMemory:
                return "InMemory";
            case EOperatingMode::Spilling:
                return "Spilling";
            case EOperatingMode::MergeSpilled:
                return "MergeSpilled";
            case EOperatingMode::ProcessSpilled:
                return "ProcessSpilled";
        }
        return "Unknown";
    }

    void SwitchMode(EOperatingMode mode) {
        const size_t rowsInMemory = !Indexes_.empty() ? Storage_.size() / Indexes_.size() : 0;
        // clang-format off
        UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, TStringBuilder()
            << (const void*)this << "# SwitchMode "
            << ModeName(Mode_) << " -> " << ModeName(mode)
            << " | reason=" << SpillReasonName(SpillReason_)
            << " memUsed=" << TlsAllocState->GetUsed()
            << " memLimit=" << TlsAllocState->GetLimit()
            << " yellowZone=" << (TlsAllocState->IsMemoryYellowZoneEnabled() ? "yes" : "no")
            << " maxLimitReached=" << (TlsAllocState->GetMaximumLimitValueReached() ? "yes" : "no")
            << " rowsInMemory=" << rowsInMemory
            << " lastSpilledRows=" << LastSpilledRows_
            << " sealedStates=" << SealedStates_.size() << "\n");
        // clang-format on
        switch (mode) {
            case EOperatingMode::InMemory:
                break;
            case EOperatingMode::Spilling: {
                ActiveSpill_ = std::make_shared<TSpilledData>();
                ActiveSpill_->Open(Spiller_, TupleMultiType_, PackSize, Ctx_.RuntimeSettings.DatumValidation.Get());
                break;
            }
            case EOperatingMode::MergeSpilled: {
                StartMerge();
                break;
            }
            case EOperatingMode::ProcessSpilled: {
                SpilledUnboxedValuesIterators_.clear();
                for (auto& state : SealedStates_) {
                    SpilledUnboxedValuesIterators_.emplace_back(LessFunc_, state, Indexes_.size(), &Ctx_);
                }
                break;
            }
        }
        Mode_ = mode;
    }

    void SealActiveSpill() {
        ActiveSpill_->Seal();
        SealedStates_.push_back(std::move(ActiveSpill_));
        TStorage().swap(Storage_);
        Full_.clear();
        Full_.shrink_to_fit();
    }

    bool SpillState() {
        MKQL_ENSURE(ActiveSpill_, "No active spill");
        if (ActiveSpill_->HasPendingWrite()) {
            if (!ActiveSpill_->IsWriteReady()) {
                return false;
            }
            ActiveSpill_->CompleteAsyncWrite();
            if (IsFinishWriteInProgress_) {
                IsFinishWriteInProgress_ = false;
                SealActiveSpill();
                return true;
            }
        } else {
            SealInMemory();
            LastSpilledRows_ = Full_.size();
            if (Full_.empty()) {
                ActiveSpill_.reset();
                return true;
            }
        }

        while (auto extract = ExtractInMemory()) {
            auto writeOp = ActiveSpill_->Write(extract, Indexes_.size());
            if (writeOp) {
                return false;
            }
        }

        auto writeFinishOp = ActiveSpill_->FinishWrite();
        if (writeFinishOp) {
            IsFinishWriteInProgress_ = true;
            return false;
        }
        SealActiveSpill();
        return true;
    }

    std::pair<size_t, size_t> FindTwoSmallestSealed() const {
        MKQL_ENSURE(SealedStates_.size() >= 2, "Need at least 2 sealed states to merge");
        size_t min1 = 0;
        size_t min2 = 1;
        if (SealedStates_[min2]->GetRowCount() < SealedStates_[min1]->GetRowCount()) {
            std::swap(min1, min2);
        }
        for (size_t i = 2; i < SealedStates_.size(); ++i) {
            if (SealedStates_[i]->GetRowCount() < SealedStates_[min2]->GetRowCount()) {
                min2 = i;
                if (SealedStates_[min2]->GetRowCount() < SealedStates_[min1]->GetRowCount()) {
                    std::swap(min1, min2);
                }
            }
        }
        return {min1, min2};
    }

    void StartMerge() {
        auto [src1, src2] = FindTwoSmallestSealed();

        Merge_.emplace();
        Merge_->Target = std::make_shared<TSpilledData>();
        Merge_->Target->Open(Spiller_, TupleMultiType_, PackSize, Ctx_.RuntimeSettings.DatumValidation.Get());

        Merge_->Iterators.reserve(2);
        Merge_->Iterators.emplace_back(LessFunc_, SealedStates_[src1], Indexes_.size(), &Ctx_);
        Merge_->Iterators.emplace_back(LessFunc_, SealedStates_[src2], Indexes_.size(), &Ctx_);

        // Remove sources from SealedStates_ (remove larger index first to preserve smaller)
        size_t first = std::min(src1, src2);
        size_t second = std::max(src1, src2);
        SealedStates_.erase(SealedStates_.begin() + second);
        SealedStates_.erase(SealedStates_.begin() + first);
    }

    bool MergeStep() {
        MKQL_ENSURE(Merge_, "No active merge");
        auto& target = *Merge_->Target;

        if (target.HasPendingWrite()) {
            if (!target.IsWriteReady()) {
                return false;
            }
            target.CompleteAsyncWrite();
            if (Merge_->FinishWriteInProgress) {
                Merge_->FinishWriteInProgress = false;
                FinishMerge();
                return true;
            }
        }

        {
            bool allReady = true;
            for (auto& it : Merge_->Iterators) {
                if (!it.CheckForInit()) {
                    allReady = false;
                }
            }
            if (!allReady) {
                return false;
            }
        }

        if (!Merge_->HeapBuilt) {
            auto end = std::remove_if(Merge_->Iterators.begin(), Merge_->Iterators.end(),
                                      [](const TSpilledUnboxedValuesIterator& it) { return it.IsFinished(); });
            Merge_->Iterators.erase(end, Merge_->Iterators.end());
            std::make_heap(Merge_->Iterators.begin(), Merge_->Iterators.end());
            Merge_->HeapBuilt = true;
        }

        while (!Merge_->Iterators.empty()) {
            std::pop_heap(Merge_->Iterators.begin(), Merge_->Iterators.end());
            auto& currentIt = Merge_->Iterators.back();
            auto row = currentIt.Pop();
            bool iteratorFinished = currentIt.IsFinished();
            bool iteratorReady = !iteratorFinished && currentIt.CheckForInit();

            if (iteratorFinished) {
                Merge_->Iterators.pop_back();
            } else if (iteratorReady) {
                std::push_heap(Merge_->Iterators.begin(), Merge_->Iterators.end());
            } else {
                Merge_->HeapBuilt = false;
            }

            auto writeOp = target.Write(row.data(), Indexes_.size());
            if (writeOp) {
                return false;
            }
            if (!iteratorReady && !iteratorFinished) {
                return false;
            }
        }

        auto writeFinishOp = target.FinishWrite();
        if (writeFinishOp) {
            Merge_->FinishWriteInProgress = true;
            return false;
        }
        FinishMerge();
        return true;
    }

    void FinishMerge() {
        Merge_->Target->Seal();
        SealedStates_.push_back(std::move(Merge_->Target));
        Merge_.reset();
    }

    NUdf::TUnboxedValue* ExtractInMemory() {
        if (Full_.empty()) {
            return nullptr;
        }

        const auto ptr = Full_.back();
        Full_.pop_back();
        return static_cast<NUdf::TUnboxedValue*>(ptr);
    }

    void SealInMemory() {
        if (HasPlaceholder()) {
            MKQL_ENSURE(Storage_.size() >= Indexes_.size(), "Cannot drop placeholder: Storage_ too small");
            Storage_.resize(Storage_.size() - Indexes_.size());
        }
        Full_.clear();
        for (auto it = Storage_.begin(); it != Storage_.end(); it += Indexes_.size()) {
            Full_.emplace_back(&*it);
        }
        std::sort(Full_.rbegin(), Full_.rend(), LessFunc_);
    }

public:
    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Pointer = nullptr;

private:
    const std::vector<ui32> Indexes_;
    const std::vector<bool> Directions_;
    const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc_;
    TStorage Storage_;
    TPointers Full_;
    TFields Fields_;
    TMultiType* TupleMultiType_;
    const TComputationContext& Ctx_;
    const bool AllowSpilling_;
    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
    static constexpr size_t PackSize = 1_MB;
    static constexpr size_t MaxSealedStates = 64;
    static constexpr size_t MinSpillBatchRows = 1024;
    std::vector<TSpilledData::TPtr> SealedStates_;
    TSpilledData::TPtr ActiveSpill_;
    EOperatingMode Mode_ = EOperatingMode::InMemory;
    std::vector<TSpilledUnboxedValuesIterator> SpilledUnboxedValuesIterators_;
    ISpiller::TPtr Spiller_ = nullptr;
    bool IsFinishWriteInProgress_ = false;
    ESpillReason SpillReason_ = ESpillReason::None;
    std::optional<TMergeState> Merge_;
    size_t LastSpilledRows_ = 0;
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
        , Flow_(flow)
        , Directions_(std::move(directions))
        , Keys_(std::move(keys))
        , Indexes_(std::move(indexes))
        , Representations_(std::move(representations))
        , TupleMultiType_(tupleMultiType)
        , AllowSpilling_(allowSpilling)
    {
        for (const auto& x : Keys_) {
            if (x.Compare || x.PresortType) {
                KeyTypes_.clear();
                HasComplexType_ = true;
                break;
            }

            KeyTypes_.emplace_back(x.Slot, x.IsOptional);
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            std::vector<bool> dirs(Directions_.size());
            std::transform(Directions_.cbegin(), Directions_.cend(), dirs.begin(), [&ctx](IComputationNode* dir) { return dir->GetValue(ctx).Get<bool>(); });
            MakeState(ctx, state, dirs.data());
        }

        if (const auto ptr = static_cast<TSpillingSupportState*>(state.AsBoxed().Get())) {
            while (EFetchResult::Finish != ptr->InputStatus) {
                if (!ptr->IsReadyToContinue()) {
                    return EFetchResult::Yield;
                }
                switch (ptr->InputStatus = Flow_->FetchValues(ctx, ptr->GetFields())) {
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
                for (const auto index : Indexes_) {
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
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        DIScopeAnnotator annotate(ctx.Annotator);

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(ctx.Codegen.GetContext());

        TLLVMFieldsStructureState<TSpillingSupportState> stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto outputType = ArrayType::get(valueType, Representations_.size());
        const auto outputPtrType = PointerType::getUnqual(outputType);
        const auto outs = annotate(new AllocaInst(outputPtrType, 0U, "outs", &ctx.Func->getEntryBlock().back()));

        ICodegeneratorInlineWideNode::TGettersList getters(Representations_.size());

        for (auto i = 0U; i < getters.size(); ++i) {
            getters[Indexes_[i]] = [i, outs, indexType, valueType, outputPtrType, outputType](const TCodegenContext& ctx, BasicBlock*& block) {
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

        const auto arrayType = ArrayType::get(Type::getInt1Ty(context), Directions_.size());
        const auto dirs = annotate(new AllocaInst(arrayType, 0U, "dirs", block));
        for (auto i = 0U; i < Directions_.size(); ++i) {
            const auto dir = GetNodeValue(Directions_[i], ctx, block);
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

            const auto getres = GetNodeValues(Flow_, ctx, block);

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

            std::vector<Value*> placeholders(Representations_.size());
            for (auto i = 0U; i < placeholders.size(); ++i) {
                placeholders[i] = annotate(GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(indexType, i)}, (TString("placeholder_") += ToString(i)).c_str(), block));
            }

            for (auto i = 0U; i < Representations_.size(); ++i) {
                const auto item = getres.second[Indexes_[i]](ctx, block);
                ValueAddRef(Representations_[i], item, ctx, block);
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
        state = ctx.HolderFactory.Create<TSpillingSupportState>(directions, Directions_.size(), TMyValueCompare(Keys_), Indexes_, TupleMultiType_, ctx, AllowSpilling_, std::move(logger), logComponent);
#else
        state = ctx.HolderFactory.Create<TSpillingSupportState>(directions, Directions_.size(), ctx.ExecuteLLVM && Compare_ ? TCompareFunc(Compare_) : TCompareFunc(TMyValueCompare(Keys_)), Indexes_, TupleMultiType_, ctx, AllowSpilling_, std::move(logger), logComponent);
#endif
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            std::for_each(Directions_.cbegin(), Directions_.cend(), std::bind(&TWideSortWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow_;
    const TComputationNodePtrVector Directions_;
    const std::vector<TKeyInfo> Keys_;
    const std::vector<ui32> Indexes_;
    const std::vector<EValueRepresentation> Representations_;
    TKeyTypes KeyTypes_;
    TMultiType* const TupleMultiType_;
    const bool AllowSpilling_;
    bool HasComplexType_ = false;

#ifndef MKQL_DISABLE_CODEGEN
    TComparePtr Compare_ = nullptr;

    Function* CompareFunc_ = nullptr;

    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::Compare_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (CompareFunc_) {
            Compare_ = reinterpret_cast<TComparePtr>(codegen.GetPointerToFunction(CompareFunc_));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (!HasComplexType_) {
            codegen.ExportSymbol(CompareFunc_ = GenerateCompareFunction(codegen, MakeName(), KeyTypes_));
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
        GetDictionaryKeyTypes(inputWideComponents[keyIndex], oneKeyTypes, isTuple, encoded, useIHash, /*expandTuple=*/false);
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

} // namespace NKikimr::NMiniKQL
