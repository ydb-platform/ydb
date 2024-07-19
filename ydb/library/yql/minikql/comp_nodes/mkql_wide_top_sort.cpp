#include "mkql_wide_top_sort.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_spiller_adapter.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/utils/cast.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/utils/sort.h>


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
    TRuntimeKeyInfo(const TKeyInfo& keyInfo)
        : Slot(keyInfo.Slot)
        , IsOptional(keyInfo.IsOptional)
        , Compare(keyInfo.Compare.Get())
    {
        if (keyInfo.PresortType) {
            LeftPacker = keyInfo.PresortType;
            RightPacker = keyInfo.PresortType;
        }
    }

    const NUdf::EDataSlot Slot;
    const bool IsOptional;
    const NUdf::ICompare* const Compare;
    mutable std::optional<TGenericPresortEncoder> LeftPacker;
    mutable std::optional<TGenericPresortEncoder> RightPacker;
};

struct TMyValueCompare {
    TMyValueCompare(const std::vector<TKeyInfo>& keys)
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

            if (cmp)  {
                return cmp;
            }
        }

        return 0;
    }

    const std::vector<TRuntimeKeyInfo> Keys;
};

using TAsyncWriteOperation = std::optional<NThreading::TFuture<ISpiller::TKey>>;
using TAsyncReadOperation = std::optional<NThreading::TFuture<std::optional<TRope>>>;
using TStorage = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue, EMemorySubPool::Temporary>>;

struct TSpilledData {
    using TPtr = TSpilledData*;

    TSpilledData(std::unique_ptr<TWideUnboxedValuesSpillerAdapter> &&spiller)
    : Spiller(std::move(spiller)) {}

    TAsyncWriteOperation Write(NUdf::TUnboxedValue* item, size_t size) {
        AsyncWriteOperation = Spiller->WriteWideItem({item, size});
        return AsyncWriteOperation;
    }

    TAsyncWriteOperation FinishWrite() {
        AsyncWriteOperation = Spiller->FinishWriting();
        return AsyncWriteOperation;
    }

    TAsyncReadOperation Read(TStorage &buffer, const TComputationContext& ctx) {
        if (AsyncReadOperation) {
            if (AsyncReadOperation->HasValue()) {
                Spiller->AsyncReadCompleted(AsyncReadOperation->ExtractValue().value(), ctx.HolderFactory);
                AsyncReadOperation = std::nullopt;
            } else {
                return AsyncReadOperation;
            }
        }
        if (Spiller->Empty()) {
            IsFinished = true;
            return std::nullopt;
        }
        AsyncReadOperation = Spiller->ExtractWideItem(buffer);
        return AsyncReadOperation;
    }

    bool Empty() const {
        return IsFinished;
    }

    std::unique_ptr<TWideUnboxedValuesSpillerAdapter> Spiller;
    TAsyncWriteOperation AsyncWriteOperation = std::nullopt;
    TAsyncReadOperation AsyncReadOperation = std::nullopt;
    bool IsFinished = false;
};

class TSpilledUnboxedValuesIterator {
private:
    TStorage Data;
    TSpilledData::TPtr SpilledData;
    std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc;
    ui32 Width_;
    const TComputationContext* Ctx;
    bool HasValue = false;
public:

    TSpilledUnboxedValuesIterator(
        const std::function<bool(const NUdf::TUnboxedValuePod*,const NUdf::TUnboxedValuePod*)>& lessFunc,
        TSpilledData::TPtr spilledData,
        size_t dataWidth,
        const TComputationContext* ctx
        )
        : SpilledData(spilledData)
        , LessFunc(lessFunc)
        , Width_(dataWidth)
        , Ctx(ctx)
    {
        Data.resize(Width_);
    }

    EFetchResult Read() {
        if (!HasValue) {
            if (SpilledData->Read(Data, *Ctx)) {
                return EFetchResult::Yield;
            }
            if (SpilledData->Empty()) {
                return EFetchResult::Finish;
            }
        }
        HasValue = true;
        return EFetchResult::One;
    }

    bool CheckForInit() {
        Read();
        return HasValue;
    }

    bool IsFinished() const {
        return SpilledData->Empty();
    }

    bool operator<(const TSpilledUnboxedValuesIterator& item) const {
        return !LessFunc(GetValue(), item.GetValue());
    }

    ui32 Width() const {
        return Width_;
    }

    TStorage Pop() {
        auto data(std::move(Data));
        HasValue = false;
        Read();
        return data;
    }

    const NUdf::TUnboxedValue* GetValue() const {
        return &Data.front();
    }
};

using TComparePtr = int(*)(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*);
using TCompareFunc = std::function<int(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>;

template <bool Sort>
class TState : public TComputationValue<TState<Sort>> {
using TBase = TComputationValue<TState<Sort>>;
private:
    using TFields = std::vector<NUdf::TUnboxedValue*, TMKQLAllocator<NUdf::TUnboxedValue*, EMemorySubPool::Temporary>>;
    using TPointers = std::vector<NUdf::TUnboxedValuePod*, TMKQLAllocator<NUdf::TUnboxedValuePod*, EMemorySubPool::Temporary>>;

    size_t GetStorageSize() const {
        return std::max<size_t>(Count << 2ULL, 1ULL << 8ULL);
    }

    void ResetFields() {
        auto ptr = Tongue = Free.back();
        std::for_each(Indexes.cbegin(), Indexes.cend(), [&](ui32 index) { Fields[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
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
        Storage.resize(GetStorageSize() * Indexes.size());
        Free.resize(GetStorageSize(), nullptr);
        if (Count) {
            Full.reserve(GetStorageSize());
            auto ptr = Storage.data();
            std::generate(Free.begin(), Free.end(), [&ptr, this]() {
                const auto p = ptr;
                ptr += Indexes.size();
                return p;
            });
            ResetFields();
        } else
            InputStatus = EFetchResult::Finish;
    }

    NUdf::TUnboxedValue*const* GetFields() const {
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
            if (!Throat)
                Throat = *std::max_element(Full.cbegin(), Full.cend(), LessFunc);

            if (!LessFunc(Tongue, Throat))
                return false;
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

        if constexpr (Sort)
            std::sort(Full.rbegin(), Full.rend(), LessFunc);
    }

    NUdf::TUnboxedValue* Extract() {
        if (Full.empty())
            return nullptr;

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
    TStorage Storage;
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
        result.emplace_back(StatusType); //status
        result.emplace_back(PtrValueType); //tongue
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
        , StatusType(Type::getInt32Ty(Context)) {

    }
};
#endif

template<bool Sort>
class TWideTopWrapper: public TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort>>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort>>;
public:
    TWideTopWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, TComputationNodePtrVector&& directions, std::vector<TKeyInfo>&& keys,
        std::vector<ui32>&& indexes, std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed), Flow(flow), Count(count), Directions(std::move(directions)), Keys(std::move(keys))
        , Indexes(std::move(indexes)), Representations(std::move(representations))
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

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            const auto count = Count->GetValue(ctx).Get<ui64>();
            std::vector<bool> dirs(Directions.size());
            std::transform(Directions.cbegin(), Directions.cend(), dirs.begin(), [&ctx](IComputationNode* dir){ return dir->GetValue(ctx).Get<bool>(); });
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
                    if (const auto to = output[index])
                        *to = std::move(*extract++);
                    else
                        ++extract;
                }
                return EFetchResult::One;
            }

            return EFetchResult::Finish;
        }

        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(ctx.Codegen.GetContext());

        TLLVMFieldsStructureState<TState<Sort>> stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto outputType = ArrayType::get(valueType, Representations.size());
        const auto outputPtrType = PointerType::getUnqual(outputType);
        const auto outs = new AllocaInst(outputPtrType, 0U, "outs", &ctx.Func->getEntryBlock().back());

        ICodegeneratorInlineWideNode::TGettersList getters(Representations.size());

        for (auto i = 0U; i < getters.size(); ++i) {
            getters[Indexes[i]] = [i, outs, indexType, valueType, outputPtrType, outputType](const TCodegenContext& ctx, BasicBlock*& block) {
                Y_UNUSED(ctx);
                const auto values = new LoadInst(outputPtrType, outs, "values", block);
                const auto pointer = GetElementPtrInst::CreateInBounds(outputType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                return new LoadInst(valueType, pointer, (TString("load_") += ToString(i)).c_str(), block);
            };
        }

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto count = GetNodeValue(Count, ctx, block);
        const auto trunc = GetterFor<ui64>(count, context, block);

        const auto arrayType = ArrayType::get(Type::getInt1Ty(context), Directions.size());
        const auto dirs = new AllocaInst(arrayType, 0U, "dirs", block);
        for (auto i = 0U; i < Directions.size(); ++i) {
            const auto dir = GetNodeValue(Directions[i], ctx, block);
            const auto cut = GetterFor<bool>(dir, context, block);
            const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, dirs, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, "ptr", block);
            new StoreInst(cut, ptr, block);
        }

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideTopWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType(), trunc->getType(), dirs->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr, trunc, dirs}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        BranchInst::Create(more, block);

        block = more;

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 3U, "result", over);

        const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStatus()}, "last", block);
        const auto last = new LoadInst(statusType, statusPtr, "last", block);
        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, last, ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), "finish", block);

        BranchInst::Create(full, loop, finish, block);

        {
            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            block = loop;

            const auto getres = GetNodeValues(Flow, ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            const auto choise = SwitchInst::Create(getres.first, good, 2U, block);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), over);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), rest);

            block = rest;

            new StoreInst(ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), statusPtr, block);
            const auto sealFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState<Sort>::Seal));
            const auto sealType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType()}, false);
            const auto sealPtr = CastInst::Create(Instruction::IntToPtr, sealFunc, PointerType::getUnqual(sealType), "seal", block);
            CallInst::Create(sealType, sealPtr, {stateArg}, "", block);

            BranchInst::Create(full, block);

            block = good;

            const auto tonguePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetTongue() }, "tongue_ptr", block);
            const auto tongue = new LoadInst(ptrValueType, tonguePtr, "tongue", block);

            std::vector<Value*> placeholders(Representations.size());
            for (auto i = 0U; i < placeholders.size(); ++i) {
                placeholders[i] = GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(indexType, i)}, (TString("placeholder_") += ToString(i)).c_str(), block);
            }

            for (auto i = 0U; i < Keys.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                new StoreInst(item, placeholders[i], block);
            }

            const auto pushFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState<Sort>::Put));
            const auto pushType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType()}, false);
            const auto pushPtr = CastInst::Create(Instruction::IntToPtr, pushFunc, PointerType::getUnqual(pushType), "function", block);
            const auto accepted = CallInst::Create(pushType, pushPtr, {stateArg}, "accepted", block);

            const auto push = BasicBlock::Create(context, "push", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            BranchInst::Create(push, skip, accepted, block);

            block = push;

            for (auto i = 0U; i < Keys.size(); ++i) {
                ValueAddRef(Representations[i], placeholders[i], ctx, block);
            }

            for (auto i = Keys.size(); i < Representations.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                ValueAddRef(Representations[i], item, ctx, block);
                new StoreInst(item, placeholders[i], block);
            }

            BranchInst::Create(loop, block);

            block = skip;

            for (auto i = 0U; i < Keys.size(); ++i) {
                ValueCleanup(Representations[i], placeholders[i], ctx, block);
                new StoreInst(ConstantInt::get(valueType, 0), placeholders[i], block);
            }

            BranchInst::Create(loop, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto extractFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState<Sort>::Extract));
            const auto extractType = FunctionType::get(outputPtrType, {stateArg->getType()}, false);
            const auto extractPtr = CastInst::Create(Instruction::IntToPtr, extractFunc, PointerType::getUnqual(extractType), "extract", block);
            const auto out = CallInst::Create(extractType, extractPtr, {stateArg}, "out", block);
            const auto has = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(outputPtrType), "has", block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

            BranchInst::Create(good, over, has, block);

            block = good;

            new StoreInst(out, outs, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            BranchInst::Create(over, block);
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

    IComputationWideFlowNode *const Flow;
    IComputationNode *const Count;
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

class TSpillingSupportState : public TComputationValue<TSpillingSupportState> {
using TBase = TComputationValue<TSpillingSupportState>;
private:
    using TStorage = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue, EMemorySubPool::Temporary>>;
    using TFields = std::vector<NUdf::TUnboxedValue*, TMKQLAllocator<NUdf::TUnboxedValue*, EMemorySubPool::Temporary>>;
    using TPointers = std::vector<NUdf::TUnboxedValuePod*, TMKQLAllocator<NUdf::TUnboxedValuePod*, EMemorySubPool::Temporary>>;

    enum class EOperatingMode {
        InMemory,
        Spilling,
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
        const std::vector<ui32>& indexes, TMultiType* tupleMultiType, const TComputationContext& ctx)
        : TBase(memInfo)
        , Indexes(indexes)
        , Directions(directons, directons + keyWidth)
        , LessFunc(std::bind(std::less<int>(), std::bind(compare, Directions.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Fields(Indexes.size(), nullptr)
        , TupleMultiType(tupleMultiType)
        , Ctx(ctx)
    {
        if (Ctx.SpillerFactory) {
            Spiller = Ctx.SpillerFactory->CreateSpiller();
        }
        ResetFields();
    }

    bool IsReadyToContinue() {
        switch (GetMode()) {
            case EOperatingMode::InMemory:
               return true;
            case EOperatingMode::Spilling:
            {
                if (!SpillState()) {
                    return false;
                }
                ResetFields();
                auto nextMode = (IsReadFromChannelFinished() ? EOperatingMode::ProcessSpilled : EOperatingMode::InMemory);

                YQL_LOG(INFO) << (nextMode == EOperatingMode::ProcessSpilled ? "Switching to ProcessSpilled" : "Switching to Memory mode");

                SwitchMode(nextMode);
                return IsReadyToContinue();
            }
            case EOperatingMode::ProcessSpilled:
            {
                if (SpilledUnboxedValuesIterators.empty()) {
                    return true;
                }
                for (auto &spilledUnboxedValuesIterator : SpilledUnboxedValuesIterators) {
                    if (!spilledUnboxedValuesIterator.CheckForInit()) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    bool IsFinished() const {
        return IsReadFromChannelFinished() && SpilledUnboxedValuesIterators.empty();
    }

    NUdf::TUnboxedValue*const* GetFields() const {
        return Fields.data();
    }

    void Put() {
        ResetFields();
        if (Ctx.SpillerFactory && !HasMemoryForProcessing()) {
            const auto used = TlsAllocState->GetUsed();
            const auto limit = TlsAllocState->GetLimit();

            YQL_LOG(INFO) << "Yellow zone reached " << (used*100/limit) << "%=" << used << "/" << limit;
            YQL_LOG(INFO) << "Switching Memory mode to Spilling";

            SwitchMode(EOperatingMode::Spilling);
        }
    }

    bool Seal() {
        if (SpilledStates.empty()) {
            SealInMemory();
        } else {
            SwitchMode(EOperatingMode::Spilling);
        }
        return IsReadyToContinue();
    }

    NUdf::TUnboxedValue* Extract() {
        if (!IsReadyToContinue()) {
            return nullptr;
        }

        if (SpilledUnboxedValuesIterators.empty()) {
            // No spilled data
            return ExtractInMemory();
        }

        if (!IsHeapBuilt) {
            std::make_heap(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end());
            IsHeapBuilt = true;
        } else {
            std::push_heap(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end());
        }

        std::pop_heap(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end());
        auto &currentIt = SpilledUnboxedValuesIterators.back();
        Storage = currentIt.Pop();
        if (currentIt.IsFinished()) {
            SpilledUnboxedValuesIterators.pop_back();
        }
        return Storage.data();
    }
private:
    EOperatingMode GetMode() const { return Mode; }

    bool HasMemoryForProcessing() const {
        // We decided to turn off spilling in Sort nodes for now
        // Because in current benchmarks we don't have huge amount of data to sort
        // return !TlsAllocState->IsMemoryYellowZoneEnabled();
        return true;
    }

    bool IsReadFromChannelFinished() const {
        return InputStatus == EFetchResult::Finish;
    }

    void SwitchMode(EOperatingMode mode) {
        switch(mode) {
            case EOperatingMode::InMemory:
                break;
            case EOperatingMode::Spilling:
            {
                const size_t PACK_SIZE = 5_MB;
                SpilledStates.emplace_back(std::make_unique<TWideUnboxedValuesSpillerAdapter>(Spiller, TupleMultiType, PACK_SIZE));
                break;
            }
            case EOperatingMode::ProcessSpilled:
            {
                SpilledUnboxedValuesIterators.reserve(SpilledStates.size());
                for (auto &state: SpilledStates) {
                    SpilledUnboxedValuesIterators.emplace_back(LessFunc, &state, Indexes.size(), &Ctx);
                }
                break;
            }
        }
        Mode = mode;
    }

    bool SpillState() {
        MKQL_ENSURE(!SpilledStates.empty(), "At least one Spiller must be created to spill data in Sort operation.");
        auto &lastSpilledState = SpilledStates.back();
        if (lastSpilledState.AsyncWriteOperation.has_value()) {
            if (!lastSpilledState.AsyncWriteOperation->HasValue()) {
                return false;
            }
            lastSpilledState.Spiller->AsyncWriteCompleted(lastSpilledState.AsyncWriteOperation->ExtractValue());
            lastSpilledState.AsyncWriteOperation = std::nullopt;
        } else {
            SealInMemory();
            if (Full.empty()) {
                // Nothing to spill
                SpilledStates.pop_back();
                return true;
            }
        }

        while (auto extract = ExtractInMemory()) {
            auto writeOp = lastSpilledState.Write(extract, Indexes.size());
            if (writeOp) {
                return false;
            }
        }

        auto writeFinishOp = lastSpilledState.FinishWrite();
        if (writeFinishOp){
            return false;
        }
        Storage.resize(0);
        return true;
    }

    NUdf::TUnboxedValue* ExtractInMemory() {
        if (Full.empty())
            return nullptr;

        const auto ptr = Full.back();
        Full.pop_back();
        return static_cast<NUdf::TUnboxedValue*>(ptr);
    }

    void SealInMemory() {
        // Remove placeholder for new data
        Storage.resize(Storage.size() - Indexes.size());

        Full.reserve(Storage.size() / Indexes.size());
        for (auto it = Storage.begin(); it != Storage.end(); it += Indexes.size()) {
            Full.emplace_back(&*it);
        }

        std::sort(Full.rbegin(), Full.rend(), LessFunc);
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
    std::vector<TSpilledData> SpilledStates;
    EOperatingMode Mode = EOperatingMode::InMemory;
    std::vector<TSpilledUnboxedValuesIterator> SpilledUnboxedValuesIterators;
    ISpiller::TPtr Spiller = nullptr;
    bool IsHeapBuilt = false;
};

class TWideSortWrapper: public TStatefulWideFlowCodegeneratorNode<TWideSortWrapper>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideSortWrapper>;
public:
    TWideSortWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationNodePtrVector&& directions, std::vector<TKeyInfo>&& keys,
        std::vector<ui32>&& indexes, std::vector<EValueRepresentation>&& representations, TMultiType* tupleMultiType)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed), Flow(flow), Directions(std::move(directions)), Keys(std::move(keys))
        , Indexes(std::move(indexes)), Representations(std::move(representations)), TupleMultiType(tupleMultiType)
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

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            std::vector<bool> dirs(Directions.size());
            std::transform(Directions.cbegin(), Directions.cend(), dirs.begin(), [&ctx](IComputationNode* dir){ return dir->GetValue(ctx).Get<bool>(); });
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
                        if (ptr->Seal())
                            break;
                        [[fallthrough]];
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                }
            }

            if (auto extract = ptr->Extract()) {
                for (const auto index : Indexes) {
                    if (const auto to = output[index])
                        *to = std::move(*extract++);
                    else
                        ++extract;
                }
                return EFetchResult::One;
            }

            return ptr->IsFinished() ? EFetchResult::Finish : EFetchResult::Yield;
        }

        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(ctx.Codegen.GetContext());

        TLLVMFieldsStructureState<TSpillingSupportState> stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto outputType = ArrayType::get(valueType, Representations.size());
        const auto outputPtrType = PointerType::getUnqual(outputType);
        const auto outs = new AllocaInst(outputPtrType, 0U, "outs", &ctx.Func->getEntryBlock().back());

        ICodegeneratorInlineWideNode::TGettersList getters(Representations.size());

        for (auto i = 0U; i < getters.size(); ++i) {
            getters[Indexes[i]] = [i, outs, indexType, valueType, outputPtrType, outputType](const TCodegenContext& ctx, BasicBlock*& block) {
                Y_UNUSED(ctx);
                const auto values = new LoadInst(outputPtrType, outs, "values", block);
                const auto pointer = GetElementPtrInst::CreateInBounds(outputType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                return new LoadInst(valueType, pointer, (TString("load_") += ToString(i)).c_str(), block);
            };
        }

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto arrayType = ArrayType::get(Type::getInt1Ty(context), Directions.size());
        const auto dirs = new AllocaInst(arrayType, 0U, "dirs", block);
        for (auto i = 0U; i < Directions.size(); ++i) {
            const auto dir = GetNodeValue(Directions[i], ctx, block);
            const auto cut = GetterFor<bool>(dir, context, block);
            const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, dirs, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, "ptr", block);
            new StoreInst(cut, ptr, block);
        }

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideSortWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType(), dirs->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr, dirs}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto boolFuncType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType()}, false);
        BranchInst::Create(more, block);

        block = more;

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 5U, "result", over);

        const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStatus()}, "last", block);
        const auto last = new LoadInst(statusType, statusPtr, "last", block);
        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, last, ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), "finish", block);

        BranchInst::Create(full, loop, finish, block);

        {
            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);

            block = loop;

            const auto readyFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::IsReadyToContinue));
            const auto readyPtr = CastInst::Create(Instruction::IntToPtr, readyFunc, PointerType::getUnqual(boolFuncType), "ready", block);
            const auto process = CallInst::Create(boolFuncType, readyPtr, {stateArg}, "process", block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            BranchInst::Create(pull, over, process, block);

            block = pull;

            const auto getres = GetNodeValues(Flow, ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            const auto choise = SwitchInst::Create(getres.first, good, 2U, block);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), over);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), rest);

            block = rest;

            new StoreInst(ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), statusPtr, block);
            const auto sealFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::Seal));
            const auto sealPtr = CastInst::Create(Instruction::IntToPtr, sealFunc, PointerType::getUnqual(boolFuncType), "seal", block);
            const auto stop = CallInst::Create(boolFuncType, sealPtr, {stateArg}, "stop", block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            BranchInst::Create(full, over, stop, block);

            block = good;

            const auto tonguePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetTongue() }, "tongue_ptr", block);
            const auto tongue = new LoadInst(ptrValueType, tonguePtr, "tongue", block);

            std::vector<Value*> placeholders(Representations.size());
            for (auto i = 0U; i < placeholders.size(); ++i) {
                placeholders[i] = GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(indexType, i)}, (TString("placeholder_") += ToString(i)).c_str(), block);
            }

            for (auto i = 0U; i < Representations.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                ValueAddRef(Representations[i], item, ctx, block);
                new StoreInst(item, placeholders[i], block);
            }

            const auto pushFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::Put));
            const auto pushType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType()}, false);
            const auto pushPtr = CastInst::Create(Instruction::IntToPtr, pushFunc, PointerType::getUnqual(pushType), "function", block);
            CallInst::Create(pushType, pushPtr, {stateArg}, "", block);

            BranchInst::Create(loop, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto last = BasicBlock::Create(context, "last", ctx.Func);

            const auto extractFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::Extract));
            const auto extractType = FunctionType::get(outputPtrType, {stateArg->getType()}, false);
            const auto extractPtr = CastInst::Create(Instruction::IntToPtr, extractFunc, PointerType::getUnqual(extractType), "extract", block);
            const auto out = CallInst::Create(extractType, extractPtr, {stateArg}, "out", block);
            const auto has = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(outputPtrType), "has", block);

            BranchInst::Create(good, last, has, block);

            block = good;

            new StoreInst(out, outs, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            BranchInst::Create(over, block);

            block = last;

            const auto finishedFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::IsFinished));
            const auto finishedPtr = CastInst::Create(Instruction::IntToPtr, finishedFunc, PointerType::getUnqual(boolFuncType), "finished_ptr", block);
            const auto finished = CallInst::Create(boolFuncType, finishedPtr, {stateArg}, "finished", block);
            const auto output = SelectInst::Create(finished,
                ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)),
                ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)),
                "output", block);

            result->addIncoming(output, block);
            BranchInst::Create(over, block);
        }

        block = over;
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state, const bool* directions) const {
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TSpillingSupportState>(directions, Directions.size(), TMyValueCompare(Keys), Indexes, TupleMultiType, ctx);
#else
        state = ctx.HolderFactory.Create<TSpillingSupportState>(directions, Directions.size(), ctx.ExecuteLLVM && Compare ? TCompareFunc(Compare) : TCompareFunc(TMyValueCompare(Keys)), Indexes, TupleMultiType, ctx);
#endif
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Directions.cbegin(), Directions.cend(), std::bind(&TWideSortWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode *const Flow;
    const TComputationNodePtrVector Directions;
    const std::vector<TKeyInfo> Keys;
    const std::vector<ui32> Indexes;
    const std::vector<EValueRepresentation> Representations;
    TKeyTypes KeyTypes;
    TMultiType *const TupleMultiType;
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

}

template<bool Sort, bool HasCount>
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
        GetDictionaryKeyTypes(inputWideComponents[keyIndex], oneKeyTypes, isTuple,encoded, useIHash, false);
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
    for (auto i = 0U; i < representations.size(); ++i)
        representations[i] = GetValueRepresentation(inputWideComponents[indexes[i]]);

    auto tupleMultiType = TMultiType::Create(tupleTypes.size(),tupleTypes.data(), ctx.Env);
    TComputationNodePtrVector directions(keyWidth);
    auto index = 1U - offset;
    std::generate(directions.begin(), directions.end(), [&](){ return LocateNode(ctx.NodeLocator, callable, ++++index); });

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        if constexpr (HasCount)
            return new TWideTopWrapper<Sort>(ctx.Mutables, wide, count, std::move(directions), std::move(keys),
                std::move(indexes), std::move(representations));
        else
            return new TWideSortWrapper(ctx.Mutables, wide, std::move(directions), std::move(keys),
                std::move(indexes), std::move(representations), tupleMultiType);
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

}
}
