#include "mkql_wide_top_sort.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_spiller_adapter.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/utils/cast.h>

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

    TAsyncReadOperation Read(TStorage &buffer, TComputationContext& ctx) {
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
    TComputationContext* Ctx;
    bool HasValue = false;
public:

    TSpilledUnboxedValuesIterator(
        const std::function<bool(const NUdf::TUnboxedValuePod*,const NUdf::TUnboxedValuePod*)>& lessFunc,
        TSpilledData::TPtr spilledData,
        size_t dataWidth,
        TComputationContext* ctx
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

    void Pop() {
        HasValue = false;
        Read();
    }

    NKikimr::NUdf::TUnboxedValue* GetValue() {
        return &*Data.begin();
    }
    const NKikimr::NUdf::TUnboxedValue* GetValue() const {
        return &*Data.begin();
    }
};

using TComparePtr = int(*)(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*);
using TCompareFunc = std::function<int(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>;

template <bool Sort, bool HasCount>
class TState : public TComputationValue<TState<Sort, HasCount>> {
using TBase = TComputationValue<TState<Sort, HasCount>>;
private:
    using TFields = std::vector<NUdf::TUnboxedValue*, TMKQLAllocator<NUdf::TUnboxedValue*, EMemorySubPool::Temporary>>;
    using TPointers = std::vector<NUdf::TUnboxedValuePod*, TMKQLAllocator<NUdf::TUnboxedValuePod*, EMemorySubPool::Temporary>>;

    size_t GetStorageSize() const {
        return std::max<size_t>(Count << 2ULL, 1ULL << 8ULL);
    }

    void ResetFields() {
        NUdf::TUnboxedValuePod* ptr;
        if constexpr (HasCount) {
            ptr = Tongue = Free.back();
        } else {
            auto pos = Storage.size();
            Storage.insert(Storage.end(), Indexes.size(), {});
            ptr = Tongue = Storage.data() + pos;
        }

        std::for_each(Indexes.cbegin(), Indexes.cend(), [&](ui32 index) { Fields[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
    }
public:
    TState(TMemoryUsageInfo* memInfo, ui64 count, const bool* directons, size_t keyWidth, const TCompareFunc& compare, const std::vector<ui32>& indexes, IComputationWideFlowNode *const flow)
        : TBase(memInfo)
        , Flow(flow)
        , Count(count)
        , Indexes(indexes)
        , Directions(directons, directons + keyWidth)
        , LessFunc(std::bind(std::less<int>(), std::bind(compare, Directions.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Fields(Indexes.size(), nullptr)
    {
        if constexpr (!HasCount) {
            ResetFields();
            return;
        }

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

    virtual EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        while (EFetchResult::Finish != InputStatus) {
            switch (InputStatus = Flow->FetchValues(ctx, GetFields())) {
                case EFetchResult::One:
                    Put();
                    continue;
                case EFetchResult::Yield:
                    return EFetchResult::Yield;
                case EFetchResult::Finish:
                    Seal();
                    break;
            }
        }

        if (auto extract = Extract()) {
            for (const auto index : Indexes)
                if (const auto to = output[index])
                    *to = std::move(*extract++);
                else
                    ++extract;
            return EFetchResult::One;
        }

        return EFetchResult::Finish;
    }

    NUdf::TUnboxedValue*const* GetFields() const {
        return Fields.data();
    }

    bool Put() {
        if constexpr (!HasCount) {
            ResetFields();
            return true;
        }

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
        if constexpr (!HasCount) {
            static_assert (Sort);
            Storage.resize(Storage.size() - Indexes.size());
            Full.reserve(Storage.size() / Indexes.size());
            for (auto it = Storage.begin(); it != Storage.end(); it += Indexes.size()) {
                Full.emplace_back(&*it);
            }

            std::sort(Full.rbegin(), Full.rend(), LessFunc);
            return;
        }

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
    IComputationWideFlowNode *const Flow;
    const ui64 Count;
    const std::vector<ui32> Indexes;
    const std::vector<bool> Directions;
    const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc;
    TStorage Storage;
    TPointers Free, Full;
    TFields Fields;
};

template <bool Sort, bool HasCount>
class TSpillingSupportState : public TComputationValue<TSpillingSupportState<Sort, HasCount>> {
using TBase = TComputationValue<TSpillingSupportState<Sort, HasCount>>;
private:
    using TStorage = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue, EMemorySubPool::Temporary>>;
    using TFields = std::vector<NUdf::TUnboxedValue*, TMKQLAllocator<NUdf::TUnboxedValue*, EMemorySubPool::Temporary>>;
    using TPointers = std::vector<NUdf::TUnboxedValuePod*, TMKQLAllocator<NUdf::TUnboxedValuePod*, EMemorySubPool::Temporary>>;

    enum class EOperatingMode {
        InMemory,
        Spilling,
        ProcessSpilled
    };

    size_t GetStorageSize() const {
        return std::max<size_t>(Count << 2ULL, 1ULL << 8ULL);
    }

    void ResetFields() {
        NUdf::TUnboxedValuePod* ptr;
        if constexpr (!HasCount) {
            auto pos = Storage.size();
            Storage.insert(Storage.end(), Indexes.size(), {});
            ptr = Storage.data() + pos;
        }

        std::for_each(Indexes.cbegin(), Indexes.cend(), [&](ui32 index) { Fields[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
    }

public:
    TSpillingSupportState(TMemoryUsageInfo* memInfo, ui64 count, const bool* directons, size_t keyWidth, const TCompareFunc& compare,
        const std::vector<ui32>& indexes, IComputationWideFlowNode *const flow, TMultiType* tupleMultiType)
        : TBase(memInfo)
        , Flow(flow)
        , Count(count)
        , Indexes(indexes)
        , Directions(directons, directons + keyWidth)
        , LessFunc(std::bind(std::less<int>(), std::bind(compare, Directions.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Fields(Indexes.size(), nullptr)
        , TupleMultiType(tupleMultiType)
    {
        if constexpr (!HasCount) {
            ResetFields();
            return;
        }
        throw yexception() << "Spilling doesn't support TopSort.";
    }

    virtual EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        while (true) {
            switch(GetMode()) {
                case EOperatingMode::InMemory: {
                    auto r = DoCalculateInMemory(ctx, output);
                    if (GetMode() == TSpillingSupportState::EOperatingMode::InMemory) {
                        return r;
                    }
                    break;
                }
                case EOperatingMode::Spilling: {
                    DoCalculateWithSpilling(ctx);
                    if (GetMode() == EOperatingMode::Spilling) {
                        return EFetchResult::Yield;
                    }
                    break;
                }
                case EOperatingMode::ProcessSpilled: {
                    return ProcessSpilledData(output);
                }

            }
        }
        Y_UNREACHABLE();
    }

private:

    EFetchResult DoCalculateInMemory(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        while (EFetchResult::Finish != InputStatus) {
            switch (InputStatus = Flow->FetchValues(ctx, GetFields())) {
                case EFetchResult::One:
                    if (Put()) {
                        if (ctx.SpillerFactory && !HasMemoryForProcessing()) {
                            SwitchMode(EOperatingMode::Spilling, ctx);
                            return EFetchResult::Yield;
                        }
                    }
                    continue;
                case EFetchResult::Yield:
                    return EFetchResult::Yield;
                case EFetchResult::Finish:
                {
                    if (!SpilledStates.empty()) {
                        SwitchMode(EOperatingMode::Spilling, ctx);
                        return EFetchResult::Yield;
                    }
                    Seal();
                    break;
                }
            }
        }

        if (auto extract = Extract()) {
            for (const auto index : Indexes)
                if (const auto to = output[index])
                    *to = std::move(*extract++);
                else
                    ++extract;
            return EFetchResult::One;
        }

        return EFetchResult::Finish;
    }

    EFetchResult DoCalculateWithSpilling(TComputationContext& ctx) {
        if (!SpillState()) {
            return EFetchResult::Yield;
        }
        ResetFields();
        auto nextMode = (IsReadFromChannelFinished() ? EOperatingMode::ProcessSpilled : EOperatingMode::InMemory);
        SwitchMode(nextMode, ctx);
        return EFetchResult::Yield;
    }

    EFetchResult ProcessSpilledData(NUdf::TUnboxedValue*const* output) {
        if (SpilledUnboxedValuesIterators.empty()) {
            return EFetchResult::Finish;
        }

        for (auto &spilledUnboxedValuesIterator : SpilledUnboxedValuesIterators) {
            if (!spilledUnboxedValuesIterator.CheckForInit()) {
                return EFetchResult::Yield;
            }
        }
        if (!IsHeapBuilt) {
            std::make_heap(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end());
            IsHeapBuilt = true;
        } else {
            std::push_heap(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end());
        }

        std::pop_heap(SpilledUnboxedValuesIterators.begin(), SpilledUnboxedValuesIterators.end());
        auto &currentIt = SpilledUnboxedValuesIterators.back();
        NKikimr::NUdf::TUnboxedValue* res = currentIt.GetValue();
        for (const auto index : Indexes)
        {
            if (const auto to = output[index])
                *to = std::move(*res++);
            else
                ++res;
        }
        currentIt.Pop();
        if (currentIt.IsFinished()) {
            SpilledUnboxedValuesIterators.pop_back();
        }
        return EFetchResult::One;
    }

    NUdf::TUnboxedValue*const* GetFields() const {
        return Fields.data();
    }

    bool Put() {
        if constexpr (!HasCount) {
            ResetFields();
            return true;
        }

        throw yexception() << "Spilling doesn't support TopSort.";
    }

    void Seal() {
        if constexpr (!HasCount) {
            static_assert (Sort);
            // Remove placeholder for new data
            Storage.resize(Storage.size() - Indexes.size());

            Full.reserve(Storage.size() / Indexes.size());
            for (auto it = Storage.begin(); it != Storage.end(); it += Indexes.size()) {
                Full.emplace_back(&*it);
            }

            std::sort(Full.rbegin(), Full.rend(), LessFunc);
            return;
        }

        throw yexception() << "Spilling doesn't support TopSort.";
    }

    NUdf::TUnboxedValue* Extract() {
        if (Full.empty())
            return nullptr;

        const auto ptr = Full.back();
        Full.pop_back();
        return static_cast<NUdf::TUnboxedValue*>(ptr);
    }

    EOperatingMode GetMode() const { return Mode; }

    bool HasMemoryForProcessing() const {
        return !TlsAllocState->IsMemoryYellowZoneEnabled();
    }

    bool IsReadFromChannelFinished() const {
        return InputStatus == EFetchResult::Finish;
    }

    void SwitchMode(EOperatingMode mode, TComputationContext& ctx) {
        switch(mode) {
            case EOperatingMode::InMemory:
                break;
            case EOperatingMode::Spilling:
            {
                auto spiller = ctx.SpillerFactory->CreateSpiller();
                const size_t PACK_SIZE = 5_MB;
                SpilledStates.emplace_back(std::make_unique<TWideUnboxedValuesSpillerAdapter>(spiller, TupleMultiType, PACK_SIZE));
                break;
            }
            case EOperatingMode::ProcessSpilled:
            {
                SpilledUnboxedValuesIterators.reserve(SpilledStates.size());
                for (auto &state: SpilledStates) {
                    SpilledUnboxedValuesIterators.emplace_back(LessFunc, &state, Indexes.size(), &ctx);
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
            Seal();
            if (Full.empty()) {
                // Nothing to spill
                SpilledStates.pop_back();
                return true;
            }
        }

        while (auto extract = Extract()) {
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

    EFetchResult InputStatus = EFetchResult::One;
    IComputationWideFlowNode *const Flow;
    const ui64 Count;
    const std::vector<ui32> Indexes;
    const std::vector<bool> Directions;
    const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc;
    TStorage Storage;
    TPointers Free, Full;
    TFields Fields;
    TMultiType* TupleMultiType;
    std::vector<TSpilledData> SpilledStates;
    EOperatingMode Mode = EOperatingMode::InMemory;
    std::vector<TSpilledUnboxedValuesIterator> SpilledUnboxedValuesIterators;
    bool IsHeapBuilt = false;
};

#ifndef MKQL_DISABLE_CODEGEN
template <bool Sort, bool HasCount>
class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState<Sort, HasCount>>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState<Sort, HasCount>>>;
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
        result.emplace_back(PtrValueType); //throat
        result.emplace_back(Type::getInt64Ty(Context)); //count
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

template<bool Sort, bool HasCount>
class TWideTopWrapper: public TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort, HasCount>>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort, HasCount>>;
public:
    TWideTopWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, TComputationNodePtrVector&& directions, std::vector<TKeyInfo>&& keys,
        std::vector<ui32>&& indexes, std::vector<EValueRepresentation>&& representations, TMultiType* tupleMultiType)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed), Flow(flow), Count(count), Directions(std::move(directions)), Keys(std::move(keys))
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
            ui64 count;
            if constexpr (HasCount) {
                count = Count->GetValue(ctx).Get<ui64>();
            } else {
                count = 0;
            }

            std::vector<bool> dirs(Directions.size());
            std::transform(Directions.cbegin(), Directions.cend(), dirs.begin(), [&ctx](IComputationNode* dir){ return dir->GetValue(ctx).Get<bool>(); });
            if (!ctx.ExecuteLLVM) {
                MakeSpillingSupportState(ctx, state, count, dirs.data());
            } else {
                MakeState(ctx, state, count, dirs.data());
            }
        }

        // To avoid dynamic_cast implementation in LLVM implementation
        // This is temporary solution. Final result will have just one state here.
        if (!ctx.ExecuteLLVM) {
            if (const auto ptr = static_cast<TSpillingSupportState<Sort, HasCount>*>(state.AsBoxed().Get())) {
                return ptr->DoCalculate(ctx, output);
            }
        } else {
            if (const auto ptr = static_cast<TState<Sort, HasCount>*>(state.AsBoxed().Get())) {
                return ptr->DoCalculate(ctx, output);
            }
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

        TLLVMFieldsStructureState<Sort, HasCount> stateFields(context);
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

        llvm::Value* trunc;
        if constexpr (HasCount) {
            const auto count = GetNodeValue(Count, ctx, block);
            trunc = GetterFor<ui64>(count, context, block);
        } else {
            trunc = ConstantInt::get(Type::getInt64Ty(context), 0U);
        }

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
            const auto sealFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState<Sort, HasCount>::Seal));
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

            if constexpr (!HasCount) {
                for (auto i = 0U; i < Representations.size(); ++i) {
                    const auto item = getres.second[Indexes[i]](ctx, block);
                    ValueAddRef(Representations[i], item, ctx, block);
                    new StoreInst(item, placeholders[i], block);
                }
            } else {
                for (auto i = 0U; i < Keys.size(); ++i) {
                    const auto item = getres.second[Indexes[i]](ctx, block);
                    new StoreInst(item, placeholders[i], block);
                }
            }


            const auto pushFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState<Sort, HasCount>::Put));
            const auto pushType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType()}, false);
            const auto pushPtr = CastInst::Create(Instruction::IntToPtr, pushFunc, PointerType::getUnqual(pushType), "function", block);
            const auto accepted = CallInst::Create(pushType, pushPtr, {stateArg}, "accepted", block);
            if constexpr (HasCount) {
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
            }

            BranchInst::Create(loop, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto extractFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState<Sort, HasCount>::Extract));
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
        state = ctx.HolderFactory.Create<TState<Sort, HasCount>>(count, directions, Directions.size(), TMyValueCompare(Keys), Indexes, Flow);
#else
        state = ctx.HolderFactory.Create<TState<Sort, HasCount>>(count, directions, Directions.size(), ctx.ExecuteLLVM && Compare ? TCompareFunc(Compare) : TCompareFunc(TMyValueCompare(Keys)), Indexes, Flow);
#endif
    }

    void MakeSpillingSupportState(TComputationContext& ctx, NUdf::TUnboxedValue& state, ui64 count, const bool* directions) const {
        if (Sort && !HasCount && !ctx.ExecuteLLVM) {
            state = ctx.HolderFactory.Create<TSpillingSupportState<Sort, HasCount>>(count, directions, Directions.size(), TMyValueCompare(Keys), Indexes, Flow, TupleMultiType);
            return;
        }
        state = ctx.HolderFactory.Create<TState<Sort, HasCount>>(count, directions, Directions.size(), TMyValueCompare(Keys), Indexes, Flow);
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            if constexpr (HasCount) {
                TWideTopWrapper::DependsOn(flow, Count);
            }

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
    TMultiType* TupleMultiType;
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
        return new TWideTopWrapper<Sort, HasCount>(ctx.Mutables, wide, count, std::move(directions), std::move(keys),
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
