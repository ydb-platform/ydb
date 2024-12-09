#include "mkql_counters.h"
#include "mkql_rh_hash.h"
#include "mkql_wide_combine.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_spiller_adapter.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/utils/cast.h>
#include <yql/essentials/utils/log/log.h>

#include <util/string/cast.h>


#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;
using NYql::TChunkedBuffer;

extern TStatKey Combine_FlushesCount;
extern TStatKey Combine_MaxRowsCount;

namespace {

struct TMyValueEqual {
    TMyValueEqual(const TKeyTypes& types)
        : Types(types)
    {}

    bool operator()(const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right) const {
        for (ui32 i = 0U; i < Types.size(); ++i)
            if (CompareValues(Types[i].first, true, Types[i].second, left[i], right[i]))
                return false;
        return true;
    }

    const TKeyTypes& Types;
};

struct TMyValueHasher {
    TMyValueHasher(const TKeyTypes& types)
        : Types(types)
    {}

    NUdf::THashType operator()(const NUdf::TUnboxedValuePod* values) const {
        if (Types.size() == 1U)
            if (const auto v = *values)
                return NUdf::GetValueHash(Types.front().first, v);
            else
                return HashOfNull;

        NUdf::THashType hash = 0ULL;
        for (const auto& type : Types) {
            if (const auto v = *values++)
                hash = CombineHashes(hash, NUdf::GetValueHash(type.first, v));
            else
                hash = CombineHashes(hash, HashOfNull);
        }
        return hash;
    }

    const TKeyTypes& Types;
};

using TEqualsPtr = bool(*)(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*);
using THashPtr = NUdf::THashType(*)(const NUdf::TUnboxedValuePod*);

using TEqualsFunc = std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>;
using THashFunc = std::function<NUdf::THashType(const NUdf::TUnboxedValuePod*)>;

using TDependsOn = std::function<void(IComputationNode*)>;
using TOwn = std::function<void(IComputationExternalNode*)>;

struct TCombinerNodes {
    TComputationExternalNodePtrVector ItemNodes, KeyNodes, StateNodes, FinishNodes;
    TComputationNodePtrVector KeyResultNodes, InitResultNodes, UpdateResultNodes, FinishResultNodes;

    TPasstroughtMap
        KeysOnItems,
        InitOnKeys,
        InitOnItems,
        UpdateOnKeys,
        UpdateOnItems,
        UpdateOnState,
        StateOnUpdate,
        ItemsOnResult,
        ResultOnItems;

    std::vector<bool> PasstroughtItems;

    void BuildMaps() {
        KeysOnItems = GetPasstroughtMap(KeyResultNodes, ItemNodes);
        InitOnKeys = GetPasstroughtMap(InitResultNodes, KeyNodes);
        InitOnItems = GetPasstroughtMap(InitResultNodes, ItemNodes);
        UpdateOnKeys = GetPasstroughtMap(UpdateResultNodes, KeyNodes);
        UpdateOnItems = GetPasstroughtMap(UpdateResultNodes, ItemNodes);
        UpdateOnState = GetPasstroughtMap(UpdateResultNodes, StateNodes);
        StateOnUpdate = GetPasstroughtMap(StateNodes, UpdateResultNodes);
        ItemsOnResult = GetPasstroughtMap(FinishNodes, FinishResultNodes);
        ResultOnItems = GetPasstroughtMap(FinishResultNodes, FinishNodes);

        PasstroughtItems.resize(ItemNodes.size());
        auto anyResults = KeyResultNodes;
        anyResults.insert(anyResults.cend(), InitResultNodes.cbegin(), InitResultNodes.cend());
        anyResults.insert(anyResults.cend(), UpdateResultNodes.cbegin(), UpdateResultNodes.cend());
        const auto itemsOnResults = GetPasstroughtMap(ItemNodes, anyResults);
        std::transform(itemsOnResults.cbegin(), itemsOnResults.cend(), PasstroughtItems.begin(), [](const TPasstroughtMap::value_type& v) { return v.has_value(); });
    }

    bool IsInputItemNodeUsed(size_t i) const {
        return (ItemNodes[i]->GetDependencesCount() > 0U || PasstroughtItems[i]);
    }

    NUdf::TUnboxedValue* GetUsedInputItemNodePtrOrNull(TComputationContext& ctx, size_t i) const {
        return IsInputItemNodeUsed(i) ?
               &ItemNodes[i]->RefValue(ctx) :
               nullptr;
    }

    void ExtractKey(TComputationContext& ctx, NUdf::TUnboxedValue** values, NUdf::TUnboxedValue* keys) const {
        std::for_each(ItemNodes.cbegin(), ItemNodes.cend(), [&](IComputationExternalNode* item) {
            if (const auto pointer = *values++)
                item->SetValue(ctx, std::move(*pointer));
        });
        for (ui32 i = 0U; i < KeyNodes.size(); ++i) {
            auto& key = KeyNodes[i]->RefValue(ctx);
            *keys++ = key = KeyResultNodes[i]->GetValue(ctx);
        }
    }

    void ConsumeRawData(TComputationContext& /*ctx*/, NUdf::TUnboxedValue* keys, NUdf::TUnboxedValue** from, NUdf::TUnboxedValue* to) const {
        std::fill_n(keys, KeyResultNodes.size(), NUdf::TUnboxedValuePod());
        for (ui32 i = 0U; i < ItemNodes.size(); ++i) {
            if (from[i] && IsInputItemNodeUsed(i)) {
                to[i] = std::move(*(from[i]));
            }
        }
    }

    void ExtractRawData(TComputationContext& ctx, NUdf::TUnboxedValue* from, NUdf::TUnboxedValue* keys) const {
        for (ui32 i = 0U; i != ItemNodes.size(); ++i) {
            if (IsInputItemNodeUsed(i)) {
                ItemNodes[i]->SetValue(ctx, std::move(from[i]));
            }
        }
        for (ui32 i = 0U; i < KeyNodes.size(); ++i) {
            auto& key = KeyNodes[i]->RefValue(ctx);
            *keys++ = key = KeyResultNodes[i]->GetValue(ctx);
        }
    }

    void ProcessItem(TComputationContext& ctx, NUdf::TUnboxedValue* keys, NUdf::TUnboxedValue* state) const {
        if (keys) {
            std::fill_n(keys, KeyResultNodes.size(), NUdf::TUnboxedValuePod());
            auto source = state;
            std::for_each(StateNodes.cbegin(), StateNodes.cend(), [&](IComputationExternalNode* item){ item->SetValue(ctx, std::move(*source++)); });
            std::transform(UpdateResultNodes.cbegin(), UpdateResultNodes.cend(), state, [&](IComputationNode* node) { return node->GetValue(ctx); });
        } else {
            std::transform(InitResultNodes.cbegin(), InitResultNodes.cend(), state, [&](IComputationNode* node) { return node->GetValue(ctx); });
        }
    }

    void FinishItem(TComputationContext& ctx, NUdf::TUnboxedValue* state, NUdf::TUnboxedValue*const* output) const {
        std::for_each(FinishNodes.cbegin(), FinishNodes.cend(), [&](IComputationExternalNode* item) { item->SetValue(ctx, std::move(*state++)); });
        for (const auto node : FinishResultNodes)
            if (const auto out = *output++)
                *out = node->GetValue(ctx);
    }

    void RegisterDependencies(const TDependsOn& dependsOn, const TOwn& own) const {
        std::for_each(ItemNodes.cbegin(), ItemNodes.cend(), own);
        std::for_each(KeyNodes.cbegin(), KeyNodes.cend(), own);
        std::for_each(StateNodes.cbegin(), StateNodes.cend(), own);
        std::for_each(FinishNodes.cbegin(), FinishNodes.cend(), own);

        std::for_each(KeyResultNodes.cbegin(), KeyResultNodes.cend(), dependsOn);
        std::for_each(InitResultNodes.cbegin(), InitResultNodes.cend(), dependsOn);
        std::for_each(UpdateResultNodes.cbegin(), UpdateResultNodes.cend(), dependsOn);
        std::for_each(FinishResultNodes.cbegin(), FinishResultNodes.cend(), dependsOn);
    }
};

class TState : public TComputationValue<TState> {
    typedef TComputationValue<TState> TBase;
private:
    using TStates = TRobinHoodHashSet<NUdf::TUnboxedValuePod*, TEqualsFunc, THashFunc, TMKQLAllocator<char, EMemorySubPool::Temporary>>;
    using TRow = std::vector<NUdf::TUnboxedValuePod, TMKQLAllocator<NUdf::TUnboxedValuePod>>;
    using TStorage = std::deque<TRow, TMKQLAllocator<TRow>>;

    class TStorageIterator {
    private:
        TStorage& Storage;
        const ui32 RowSize = 0;
        const ui64 Count = 0;
        ui64 Ready = 0;
        TStorage::iterator ItStorage;
        TRow::iterator ItRow;
    public:
        TStorageIterator(TStorage& storage, const ui32 rowSize, const ui64 count)
            : Storage(storage)
            , RowSize(rowSize)
            , Count(count)
        {
            ItStorage = Storage.begin();
            if (ItStorage != Storage.end()) {
                ItRow = ItStorage->begin();
            }
        }

        bool IsValid() {
            return Ready < Count;
        }

        bool Next() {
            if (++Ready >= Count) {
                return false;
            }
            ItRow += RowSize;
            if (ItRow == ItStorage->end()) {
                ++ItStorage;
                ItRow = ItStorage->begin();
            }

            return true;
        }

        NUdf::TUnboxedValuePod* GetValuePtr() const {
            return &*ItRow;
        }
    };

    static constexpr ui32 CountRowsOnPage = 128;

    ui32 RowSize() const {
        return KeyWidth + StateWidth;
    }
public:
    TState(TMemoryUsageInfo* memInfo, ui32 keyWidth, ui32 stateWidth, const THashFunc& hash, const TEqualsFunc& equal, bool allowOutOfMemory = false)
        : TBase(memInfo), KeyWidth(keyWidth), StateWidth(stateWidth), AllowOutOfMemory(allowOutOfMemory), States(hash, equal, CountRowsOnPage) {
        CurrentPage = &Storage.emplace_back(RowSize() * CountRowsOnPage, NUdf::TUnboxedValuePod());
        CurrentPosition = 0;
        Tongue = CurrentPage->data();
    }

    ~TState() {
    //Workaround for YQL-16663, consider to rework this class in a safe manner
        while (auto row = Extract()) {
            for (size_t i = 0; i != RowSize(); ++i) {
                row[i].UnRef();
            }
        }

        ExtractIt.reset();
        Storage.clear();
        States.Clear();

        CleanupCurrentContext();
    }

    bool TasteIt() {
        Y_ABORT_UNLESS(!ExtractIt);
        bool isNew = false;
        auto itInsert = States.Insert(Tongue, isNew);
        if (isNew) {
            CurrentPosition += RowSize();
            if (CurrentPosition == CurrentPage->size()) {
                CurrentPage = &Storage.emplace_back(RowSize() * CountRowsOnPage, NUdf::TUnboxedValuePod());
                CurrentPosition = 0;
            }
            Tongue = CurrentPage->data() + CurrentPosition;
        }
        Throat = States.GetKey(itInsert) + KeyWidth;
        if (isNew) {
            GrowStates();
        }
        return isNew;
    }

    void GrowStates() {
        try {
            States.CheckGrow();
        } catch (TMemoryLimitExceededException) {
            YQL_LOG(INFO) << "State failed to grow";
            if (IsOutOfMemory || !AllowOutOfMemory) {
                throw;
            } else {
                IsOutOfMemory = true;
            }
        }
    }

    bool CheckIsOutOfMemory() const {
        return IsOutOfMemory;
    }

    template<bool SkipYields>
    bool ReadMore() {
        if constexpr (SkipYields) {
            if (EFetchResult::Yield == InputStatus)
                return true;
        }

        if (!States.Empty())
            return false;

        {
            TStorage localStorage;
            std::swap(localStorage, Storage);
        }
        CurrentPage = &Storage.emplace_back(RowSize() * CountRowsOnPage, NUdf::TUnboxedValuePod());
        CurrentPosition = 0;
        Tongue = CurrentPage->data();
        StoredDataSize = 0;

        CleanupCurrentContext();
        return true;
    }

    void PushStat(IStatsRegistry* stats) const {
        if (!States.Empty()) {
            MKQL_SET_MAX_STAT(stats, Combine_MaxRowsCount, static_cast<i64>(States.GetSize()));
            MKQL_INC_STAT(stats, Combine_FlushesCount);
        }
    }

    NUdf::TUnboxedValuePod* Extract() {
        if (!ExtractIt) {
            ExtractIt.emplace(Storage, RowSize(), States.GetSize());
        } else {
            ExtractIt->Next();
        }
        if (!ExtractIt->IsValid()) {
            ExtractIt.reset();
            States.Clear();
            return nullptr;
        }
        NUdf::TUnboxedValuePod* result = ExtractIt->GetValuePtr();
        CounterOutputRows_.Inc();
        return result;
    }

    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Tongue = nullptr;
    NUdf::TUnboxedValuePod* Throat = nullptr;
    i64 StoredDataSize = 0;
    NYql::NUdf::TCounter CounterOutputRows_;

private:
    std::optional<TStorageIterator> ExtractIt;
    const ui32 KeyWidth, StateWidth;
    const bool AllowOutOfMemory;
    bool IsOutOfMemory = false;
    ui64 CurrentPosition = 0;
    TRow* CurrentPage = nullptr;
    TStorage Storage;
    TStates States;
};

class TSpillingSupportState : public TComputationValue<TSpillingSupportState> {
    typedef TComputationValue<TSpillingSupportState> TBase;
    typedef std::optional<NThreading::TFuture<ISpiller::TKey>> TAsyncWriteOperation;
    typedef std::optional<NThreading::TFuture<std::optional<TChunkedBuffer>>> TAsyncReadOperation;

    struct TSpilledBucket {
        std::unique_ptr<TWideUnboxedValuesSpillerAdapter> SpilledState; //state collected before switching to spilling mode
        std::unique_ptr<TWideUnboxedValuesSpillerAdapter> SpilledData; //data collected in spilling mode
        std::unique_ptr<TState> InMemoryProcessingState;
        TAsyncWriteOperation AsyncWriteOperation;

        enum class EBucketState {
            InMemory,
            SpillingState,
            SpillingData
        };

        EBucketState BucketState = EBucketState::InMemory;
        ui64 LineCount = 0;
    };

    enum class EOperatingMode {
        InMemory,
        SplittingState,
        Spilling,
        ProcessSpilled
    };

public:
    enum class ETasteResult: i8 {
        Init = -1,
        Update,
        ConsumeRawData
    };

    enum class EUpdateResult: i8 {
        Yield = -1,
        ExtractRawData,
        ReadInput,
        Extract,
        Finish
    };

    TSpillingSupportState(
        TMemoryUsageInfo* memInfo,
        const TMultiType* usedInputItemType, const TMultiType* keyAndStateType, ui32 keyWidth, size_t itemNodesSize,
        const THashFunc& hash, const TEqualsFunc& equal, bool allowSpilling, TComputationContext& ctx
    )
        : TBase(memInfo)
        , InMemoryProcessingState(memInfo, keyWidth, keyAndStateType->GetElementsCount() - keyWidth, hash, equal, allowSpilling && ctx.SpillerFactory)
        , UsedInputItemType(usedInputItemType)
        , KeyAndStateType(keyAndStateType)
        , KeyWidth(keyWidth)
        , ItemNodesSize(itemNodesSize)
        , Hasher(hash)
        , Mode(EOperatingMode::InMemory)
        , ViewForKeyAndState(keyAndStateType->GetElementsCount())
        , MemInfo(memInfo)
        , Equal(equal)
        , AllowSpilling(allowSpilling)
        , Ctx(ctx)
    {
        BufferForUsedInputItems.reserve(usedInputItemType->GetElementsCount());
        Tongue = InMemoryProcessingState.Tongue;
        Throat = InMemoryProcessingState.Throat;
        if (ctx.CountersProvider) {
            // id will be assigned externally in future versions
            TString id = TString(Operator_Aggregation) + "0";
            CounterOutputRows_ = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, false);
        }
    }

    EUpdateResult Update() {
        if (IsEverythingExtracted)  {
            return EUpdateResult::Finish;
        }

        switch (GetMode()) {
            case EOperatingMode::InMemory: {
                Tongue = InMemoryProcessingState.Tongue;
                if (CheckMemoryAndSwitchToSpilling()) {
                    return Update();
                }
                if (InputStatus == EFetchResult::Finish) return EUpdateResult::Extract;

                return EUpdateResult::ReadInput;
            }
            case EOperatingMode::SplittingState: {
                if (SplitStateIntoBucketsAndWait()) return EUpdateResult::Yield;
                return Update();
            }
            case EOperatingMode::Spilling: {
                UpdateSpillingBuckets();


                if (!HasMemoryForProcessing() && InputStatus != EFetchResult::Finish && TryToReduceMemoryAndWait()) {
                    return EUpdateResult::Yield;
                }

                if (BufferForUsedInputItems.size()) {
                    auto& bucket = SpilledBuckets[BufferForUsedInputItemsBucketId];
                    if (bucket.AsyncWriteOperation.has_value()) return EUpdateResult::Yield;

                    bucket.AsyncWriteOperation = bucket.SpilledData->WriteWideItem(BufferForUsedInputItems);
                    BufferForUsedInputItems.resize(0); //for freeing allocated key value asap
                }

                if (InputStatus == EFetchResult::Finish) return FlushSpillingBuffersAndWait();

                return EUpdateResult::ReadInput;
            }
            case EOperatingMode::ProcessSpilled:
                return ProcessSpilledData();
        }
    }

    ETasteResult TasteIt() {
        if (GetMode() == EOperatingMode::InMemory) {
            bool isNew = InMemoryProcessingState.TasteIt();
            if (InMemoryProcessingState.CheckIsOutOfMemory()) {
                StateWantsToSpill = true;
            }
            Throat = InMemoryProcessingState.Throat;
            return isNew ? ETasteResult::Init : ETasteResult::Update;
        }
        if (GetMode() == EOperatingMode::ProcessSpilled) {
            // while restoration we process buckets one by one starting from the first in a queue
            bool isNew = SpilledBuckets.front().InMemoryProcessingState->TasteIt();
            Throat = SpilledBuckets.front().InMemoryProcessingState->Throat;
            BufferForUsedInputItems.resize(0);
            return isNew ? ETasteResult::Init : ETasteResult::Update;
        }

        auto bucketId = ChooseBucket(ViewForKeyAndState.data());
        auto& bucket = SpilledBuckets[bucketId];

        if (bucket.BucketState == TSpilledBucket::EBucketState::InMemory) {
            std::copy_n(ViewForKeyAndState.data(), KeyWidth, static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Tongue));

            bool isNew = bucket.InMemoryProcessingState->TasteIt();
            Throat = bucket.InMemoryProcessingState->Throat;
            bucket.LineCount += isNew;

            return isNew ? ETasteResult::Init : ETasteResult::Update;
        }
        bucket.LineCount++;

        // Prepare space for raw data
        MKQL_ENSURE(BufferForUsedInputItems.size() == 0, "Internal logic error");
        BufferForUsedInputItems.resize(ItemNodesSize);
        BufferForUsedInputItemsBucketId = bucketId;

        Throat = BufferForUsedInputItems.data();

        return ETasteResult::ConsumeRawData;
    }

    NUdf::TUnboxedValuePod* Extract() {
        NUdf::TUnboxedValue* value = nullptr;
        if (GetMode() == EOperatingMode::InMemory) {
            value = static_cast<NUdf::TUnboxedValue*>(InMemoryProcessingState.Extract());
            if (value) {
                CounterOutputRows_.Inc();
            } else {
                IsEverythingExtracted = true;
            }
            return value;
        }

        MKQL_ENSURE(SpilledBuckets.front().BucketState == TSpilledBucket::EBucketState::InMemory, "Internal logic error");
        MKQL_ENSURE(SpilledBuckets.size() > 0, "Internal logic error");

        value = static_cast<NUdf::TUnboxedValue*>(SpilledBuckets.front().InMemoryProcessingState->Extract());
        if (value) {
            CounterOutputRows_.Inc();
        } else {
            SpilledBuckets.front().InMemoryProcessingState->ReadMore<false>();
            SpilledBuckets.pop_front();
            if (SpilledBuckets.empty()) IsEverythingExtracted = true;
        }

        return value;
    }

private:
    ui64 ChooseBucket(const NUdf::TUnboxedValuePod *const key) {
        auto provided_hash = Hasher(key);
        XXH64_hash_t bucket = XXH64(&provided_hash, sizeof(provided_hash), 0) % SpilledBucketCount;
        return bucket;
    }

    EUpdateResult FlushSpillingBuffersAndWait() {
        UpdateSpillingBuckets();

        ui64 finishedCount = 0;
        for (auto& bucket : SpilledBuckets) {
            MKQL_ENSURE(bucket.BucketState != TSpilledBucket::EBucketState::SpillingState, "Internal logic error");
            if (!bucket.AsyncWriteOperation.has_value()) {
                auto writeOperation = bucket.SpilledData->FinishWriting();
                if (!writeOperation) {
                    ++finishedCount;
                } else {
                    bucket.AsyncWriteOperation = writeOperation;
                }
            }
        }

        if (finishedCount != SpilledBuckets.size()) return EUpdateResult::Yield;

        SwitchMode(EOperatingMode::ProcessSpilled);

        return ProcessSpilledData();
    }

    ui32 GetLargestInMemoryBucketNumber() const {
        ui64 maxSize = 0;
        ui32 largestInMemoryBucketNum = (ui32)-1;
        for (ui64 i = 0; i < SpilledBucketCount; ++i) {
            if (SpilledBuckets[i].BucketState == TSpilledBucket::EBucketState::InMemory) {
                if (SpilledBuckets[i].LineCount >= maxSize) {
                    largestInMemoryBucketNum = i;
                    maxSize = SpilledBuckets[i].LineCount;
                }
            }
        }
        return largestInMemoryBucketNum;
    }

    bool IsSpillingWhileStateSplitAllowed() const {
        // TODO: Write better condition here. For example: InMemorybuckets > 64
        return true;
    }

    bool SplitStateIntoBucketsAndWait() {
        if (SplitStateSpillingBucket != -1) {
            auto& bucket = SpilledBuckets[SplitStateSpillingBucket];
            MKQL_ENSURE(bucket.AsyncWriteOperation.has_value(), "Internal logic error");
            if (!bucket.AsyncWriteOperation->HasValue()) return true;
            bucket.SpilledState->AsyncWriteCompleted(bucket.AsyncWriteOperation->ExtractValue());
            bucket.AsyncWriteOperation = std::nullopt;

            while (const auto keyAndState = static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Extract())) {
                bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType->GetElementsCount()});
                for (size_t i = 0; i < KeyAndStateType->GetElementsCount(); ++i) {
                    //releasing values stored in unsafe TUnboxedValue buffer
                    keyAndState[i].UnRef();
                }
                if (bucket.AsyncWriteOperation) return true;
            }

            SplitStateSpillingBucket = -1;
        }
        while (const auto keyAndState = static_cast<NUdf::TUnboxedValue *>(InMemoryProcessingState.Extract())) {
            auto bucketId = ChooseBucket(keyAndState);  // This uses only key for hashing
            auto& bucket = SpilledBuckets[bucketId];

            bucket.LineCount++;

            if (bucket.BucketState != TSpilledBucket::EBucketState::InMemory) {
                if (bucket.BucketState != TSpilledBucket::EBucketState::SpillingState) {
                    bucket.BucketState = TSpilledBucket::EBucketState::SpillingState;
                    SpillingBucketsCount++;
                }

                bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType->GetElementsCount()});
                for (size_t i = 0; i < KeyAndStateType->GetElementsCount(); ++i) {
                    //releasing values stored in unsafe TUnboxedValue buffer
                    keyAndState[i].UnRef();
                }
                if (bucket.AsyncWriteOperation) {
                    SplitStateSpillingBucket = bucketId;
                    return true;
                }
                continue;
            }

            auto& processingState = *bucket.InMemoryProcessingState;

            for (size_t i = 0; i < KeyWidth; ++i) {
                //jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(processingState.Tongue[i]) = std::move(keyAndState[i]);
            }
            processingState.TasteIt();
            for (size_t i = KeyWidth; i < KeyAndStateType->GetElementsCount(); ++i) {
                //jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(processingState.Throat[i - KeyWidth]) = std::move(keyAndState[i]);
            }

            if (InMemoryBucketsCount && !HasMemoryForProcessing() && IsSpillingWhileStateSplitAllowed()) {
                ui32 bucketNumToSpill = GetLargestInMemoryBucketNumber();

                SplitStateSpillingBucket = bucketNumToSpill;

                auto& bucket = SpilledBuckets[bucketNumToSpill];
                bucket.BucketState = TSpilledBucket::EBucketState::SpillingState;
                SpillingBucketsCount++;
                InMemoryBucketsCount--;

                while (const auto keyAndState = static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Extract())) {
                    bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType->GetElementsCount()});
                    for (size_t i = 0; i < KeyAndStateType->GetElementsCount(); ++i) {
                        //releasing values stored in unsafe TUnboxedValue buffer
                        keyAndState[i].UnRef();
                    }
                    if (bucket.AsyncWriteOperation) return true;
                }

                bucket.AsyncWriteOperation = bucket.SpilledState->FinishWriting();
                if (bucket.AsyncWriteOperation) return true;
            }
        }

        for (ui64 i = 0; i < SpilledBucketCount; ++i) {
            auto& bucket = SpilledBuckets[i];
            if (bucket.BucketState == TSpilledBucket::EBucketState::SpillingState) {
                if (bucket.AsyncWriteOperation.has_value()) {
                    if (!bucket.AsyncWriteOperation->HasValue()) return true;
                    bucket.SpilledState->AsyncWriteCompleted(bucket.AsyncWriteOperation->ExtractValue());
                    bucket.AsyncWriteOperation = std::nullopt;
                }

                bucket.AsyncWriteOperation = bucket.SpilledState->FinishWriting();
                if (bucket.AsyncWriteOperation) return true;
                bucket.InMemoryProcessingState->ReadMore<false>();

                bucket.BucketState = TSpilledBucket::EBucketState::SpillingData;
                SpillingBucketsCount--;
            }
        }

        InMemoryProcessingState.ReadMore<false>();
        IsInMemoryProcessingStateSplitted = true;
        SwitchMode(EOperatingMode::Spilling);
        return false;
    }

    bool CheckMemoryAndSwitchToSpilling() {
        if (!(AllowSpilling && Ctx.SpillerFactory)) {
            return false;
        }
        if (StateWantsToSpill || IsSwitchToSpillingModeCondition()) {
            StateWantsToSpill = false;
            LogMemoryUsage();

            SwitchMode(EOperatingMode::SplittingState);
            return true;
        }

        return false;
    }

    void LogMemoryUsage() const {
        const auto used = TlsAllocState->GetUsed();
        const auto limit = TlsAllocState->GetLimit();
        TStringBuilder logmsg;
        logmsg << "Memory usage: ";
        if (limit) {
            logmsg << (used*100/limit) << "%=";
        }
        logmsg << (used/1_MB) << "MB/" << (limit/1_MB) << "MB";

        YQL_LOG(INFO) << logmsg;
    }

    void SpillMoreStateFromBucket(TSpilledBucket& bucket) {
        MKQL_ENSURE(!bucket.AsyncWriteOperation.has_value(), "Internal logic error");

        if (bucket.BucketState == TSpilledBucket::EBucketState::InMemory) {
            bucket.BucketState = TSpilledBucket::EBucketState::SpillingState;
            SpillingBucketsCount++;
            InMemoryBucketsCount--;
        }

        while (const auto keyAndState = static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Extract())) {
            bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType->GetElementsCount()});
            for (size_t i = 0; i < KeyAndStateType->GetElementsCount(); ++i) {
                //releasing values stored in unsafe TUnboxedValue buffer
                keyAndState[i].UnRef();
            }
            if (bucket.AsyncWriteOperation) return;
        }

        bucket.AsyncWriteOperation = bucket.SpilledState->FinishWriting();
        if (bucket.AsyncWriteOperation) return;

        bucket.InMemoryProcessingState->ReadMore<false>();

        bucket.BucketState = TSpilledBucket::EBucketState::SpillingData;
        SpillingBucketsCount--;
    }

    void UpdateSpillingBuckets() {
        for (ui64 i = 0; i < SpilledBucketCount; ++i) {
            auto& bucket = SpilledBuckets[i];
            if (bucket.AsyncWriteOperation.has_value() && bucket.AsyncWriteOperation->HasValue()) {
                if (bucket.BucketState == TSpilledBucket::EBucketState::SpillingState) {
                    bucket.SpilledState->AsyncWriteCompleted(bucket.AsyncWriteOperation->ExtractValue());
                    bucket.AsyncWriteOperation = std::nullopt;

                    SpillMoreStateFromBucket(bucket);

                } else {
                    bucket.SpilledData->AsyncWriteCompleted(bucket.AsyncWriteOperation->ExtractValue());
                    bucket.AsyncWriteOperation = std::nullopt;
                }
            }
        }
    }

    bool TryToReduceMemoryAndWait() {
        if (SpillingBucketsCount > 0) {
            return true;
        }
        while (InMemoryBucketsCount > 0) {
            ui32 maxLineBucketInd = GetLargestInMemoryBucketNumber();
            MKQL_ENSURE(maxLineBucketInd != (ui32)-1, "Internal logic error");

            auto& bucketToSpill = SpilledBuckets[maxLineBucketInd];
            SpillMoreStateFromBucket(bucketToSpill);
            if (bucketToSpill.BucketState == TSpilledBucket::EBucketState::SpillingState) {
                return true;
            }
        }
        return false;
    }

    EUpdateResult ProcessSpilledData() {
        if (AsyncReadOperation) {
            if (!AsyncReadOperation->HasValue()) return EUpdateResult::Yield;
            if (RecoverState) {
                SpilledBuckets[0].SpilledState->AsyncReadCompleted(AsyncReadOperation->ExtractValue().value(), Ctx.HolderFactory);
            } else {
                SpilledBuckets[0].SpilledData->AsyncReadCompleted(AsyncReadOperation->ExtractValue().value(), Ctx.HolderFactory);
            }
            AsyncReadOperation = std::nullopt;
        }

        auto& bucket = SpilledBuckets.front();
        if (bucket.BucketState == TSpilledBucket::EBucketState::InMemory) return EUpdateResult::Extract;

        //recover spilled state
        while(!bucket.SpilledState->Empty()) {
            RecoverState = true;
            TTemporaryUnboxedValueVector bufferForKeyAndState(KeyAndStateType->GetElementsCount());
            AsyncReadOperation = bucket.SpilledState->ExtractWideItem(bufferForKeyAndState);
            if (AsyncReadOperation) {
                return EUpdateResult::Yield;
            }
            for (size_t i = 0; i< KeyWidth; ++i) {
                //jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(bucket.InMemoryProcessingState->Tongue[i]) = std::move(bufferForKeyAndState[i]);
            }
            auto isNew = bucket.InMemoryProcessingState->TasteIt();
            MKQL_ENSURE(isNew, "Internal logic error");
            for (size_t i = KeyWidth; i < KeyAndStateType->GetElementsCount(); ++i) {
                //jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(bucket.InMemoryProcessingState->Throat[i - KeyWidth]) = std::move(bufferForKeyAndState[i]);
            }
        }
        //process spilled data
        if (!bucket.SpilledData->Empty()) {
            RecoverState = false;
            BufferForUsedInputItems.resize(UsedInputItemType->GetElementsCount());
            AsyncReadOperation = bucket.SpilledData->ExtractWideItem(BufferForUsedInputItems);
            if (AsyncReadOperation) {
                return EUpdateResult::Yield;
            }

            Throat = BufferForUsedInputItems.data();
            Tongue = bucket.InMemoryProcessingState->Tongue;

            return EUpdateResult::ExtractRawData;
        }
        bucket.BucketState = TSpilledBucket::EBucketState::InMemory;
        return EUpdateResult::Extract;
    }

    EOperatingMode GetMode() const {
        return Mode;
    }

    void SwitchMode(EOperatingMode mode) {
        switch(mode) {
            case EOperatingMode::InMemory: {
                YQL_LOG(INFO) << "switching Memory mode to InMemory";
                MKQL_ENSURE(false, "Internal logic error");
                break;
            }
            case EOperatingMode::SplittingState: {
                YQL_LOG(INFO) << "switching Memory mode to SplittingState";
                MKQL_ENSURE(EOperatingMode::InMemory == Mode, "Internal logic error");
                SpilledBuckets.resize(SpilledBucketCount);
                auto spiller = Ctx.SpillerFactory->CreateSpiller();
                for (auto &b: SpilledBuckets) {
                    b.SpilledState = std::make_unique<TWideUnboxedValuesSpillerAdapter>(spiller, KeyAndStateType, 5_MB);
                    b.SpilledData = std::make_unique<TWideUnboxedValuesSpillerAdapter>(spiller, UsedInputItemType, 5_MB);
                    b.InMemoryProcessingState = std::make_unique<TState>(MemInfo, KeyWidth, KeyAndStateType->GetElementsCount() - KeyWidth, Hasher, Equal);
                }
                break;
            }
            case EOperatingMode::Spilling: {
                YQL_LOG(INFO) << "switching Memory mode to Spilling";
                MKQL_ENSURE(EOperatingMode::SplittingState == Mode || EOperatingMode::InMemory == Mode, "Internal logic error");

                Tongue = ViewForKeyAndState.data();
                break;
            }
            case EOperatingMode::ProcessSpilled: {
                YQL_LOG(INFO) << "switching Memory mode to ProcessSpilled";
                MKQL_ENSURE(EOperatingMode::Spilling == Mode, "Internal logic error");
                MKQL_ENSURE(SpilledBuckets.size() == SpilledBucketCount, "Internal logic error");

                std::sort(SpilledBuckets.begin(), SpilledBuckets.end(), [](const TSpilledBucket& lhs, const TSpilledBucket& rhs) {
                    bool lhs_in_memory = lhs.BucketState == TSpilledBucket::EBucketState::InMemory;
                    bool rhs_in_memory = rhs.BucketState == TSpilledBucket::EBucketState::InMemory;
                    return lhs_in_memory > rhs_in_memory;
                });
                break;
            }

        }
        Mode = mode;
    }

    bool HasMemoryForProcessing() const {
        return !TlsAllocState->IsMemoryYellowZoneEnabled();
    }

    bool IsSwitchToSpillingModeCondition() const {
        return !HasMemoryForProcessing() || TlsAllocState->GetMaximumLimitValueReached();
    }

public:
    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Tongue = nullptr;
    NUdf::TUnboxedValuePod* Throat = nullptr;

private:
    bool StateWantsToSpill = false;
    bool IsEverythingExtracted = false;

    TState InMemoryProcessingState;
    bool IsInMemoryProcessingStateSplitted = false;
    const TMultiType* const UsedInputItemType;
    const TMultiType* const KeyAndStateType;
    const size_t KeyWidth;
    const size_t ItemNodesSize;
    THashFunc const Hasher;
    EOperatingMode Mode;
    bool RecoverState; //sub mode for ProcessSpilledData

    TAsyncReadOperation AsyncReadOperation = std::nullopt;
    static constexpr size_t SpilledBucketCount = 128;
    std::deque<TSpilledBucket> SpilledBuckets;
    ui32 SpillingBucketsCount = 0;
    ui32 InMemoryBucketsCount = SpilledBucketCount;
    ui64 BufferForUsedInputItemsBucketId;
    TUnboxedValueVector BufferForUsedInputItems;
    std::vector<NUdf::TUnboxedValuePod, TMKQLAllocator<NUdf::TUnboxedValuePod>> ViewForKeyAndState;
    i64 SplitStateSpillingBucket = -1;

    TMemoryUsageInfo* MemInfo = nullptr;
    TEqualsFunc const Equal;
    const bool AllowSpilling;

    TComputationContext& Ctx;
    NYql::NUdf::TCounter CounterOutputRows_;
};

#ifndef MKQL_DISABLE_CODEGEN
class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
    llvm::IntegerType* ValueType;
    llvm::PointerType* PtrValueType;
    llvm::IntegerType* StatusType;
    llvm::IntegerType* StoredType;
protected:
    using TBase::Context;
public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StatusType); //status
        result.emplace_back(PtrValueType); //tongue
        result.emplace_back(PtrValueType); //throat
        result.emplace_back(StoredType); //StoredDataSize
        result.emplace_back(Type::getInt32Ty(Context)); //size
        result.emplace_back(Type::getInt32Ty(Context)); //size
        return result;
    }

    llvm::Constant* GetStatus() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetTongue() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 1);
    }

    llvm::Constant* GetThroat() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 2);
    }

    llvm::Constant* GetStored() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 3);
    }

    TLLVMFieldsStructureState(llvm::LLVMContext& context)
        : TBase(context)
        , ValueType(Type::getInt128Ty(Context))
        , PtrValueType(PointerType::getUnqual(ValueType))
        , StatusType(Type::getInt32Ty(Context))
        , StoredType(Type::getInt64Ty(Context)) {

    }
};
#endif

template <bool TrackRss, bool SkipYields>
class TWideCombinerWrapper: public TStatefulWideFlowCodegeneratorNode<TWideCombinerWrapper<TrackRss, SkipYields>>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideCombinerWrapper<TrackRss, SkipYields>>;
public:
    TWideCombinerWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TCombinerNodes&& nodes, TKeyTypes&& keyTypes, ui64 memLimit)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow(flow)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , MemLimit(memLimit)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Nodes.ItemNodes.size()))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        while (const auto ptr = static_cast<TState*>(state.AsBoxed().Get())) {
            if (ptr->ReadMore<SkipYields>()) {
                switch (ptr->InputStatus) {
                    case EFetchResult::One:
                        break;
                    case EFetchResult::Yield:
                        ptr->InputStatus = EFetchResult::One;
                        if constexpr (SkipYields)
                            break;
                        else
                            return EFetchResult::Yield;
                    case EFetchResult::Finish:
                        return EFetchResult::Finish;
                }

                const auto initUsage = MemLimit ? ctx.HolderFactory.GetMemoryUsed() : 0ULL;

                auto **fields = ctx.WideFields.data() + WideFieldsIndex;

                do {
                    for (auto i = 0U; i < Nodes.ItemNodes.size(); ++i)
                        if (Nodes.ItemNodes[i]->GetDependencesCount() > 0U || Nodes.PasstroughtItems[i])
                            fields[i] = &Nodes.ItemNodes[i]->RefValue(ctx);

                    ptr->InputStatus = Flow->FetchValues(ctx, fields);
                    if constexpr (SkipYields) {
                        if (EFetchResult::Yield == ptr->InputStatus) {
                            if (MemLimit) {
                                const auto currentUsage = ctx.HolderFactory.GetMemoryUsed();
                                ptr->StoredDataSize += currentUsage > initUsage ? currentUsage - initUsage : 0;
                            }
                            return EFetchResult::Yield;
                        } else if (EFetchResult::Finish == ptr->InputStatus) {
                            break;
                        }
                    } else {
                        if (EFetchResult::One != ptr->InputStatus) {
                            break;
                        }
                    }

                    Nodes.ExtractKey(ctx, fields, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue));
                    Nodes.ProcessItem(ctx, ptr->TasteIt() ? nullptr : static_cast<NUdf::TUnboxedValue*>(ptr->Tongue), static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                } while (!ctx.template CheckAdjustedMemLimit<TrackRss>(MemLimit, initUsage - ptr->StoredDataSize));

                ptr->PushStat(ctx.Stats);
            }

            if (const auto values = static_cast<NUdf::TUnboxedValue*>(ptr->Extract())) {
                Nodes.FinishItem(ctx, values, output);
                return EFetchResult::One;
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
        const auto storedType = Type::getInt64Ty(context);

        TLLVMFieldsStructureState stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideCombinerWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        BranchInst::Create(more, block);

        block = more;

        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 3U, "result", over);

        const auto readMoreFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::ReadMore<SkipYields>));
        const auto readMoreFuncType = FunctionType::get(Type::getInt1Ty(context), { statePtrType }, false);
        const auto readMoreFuncPtr = CastInst::Create(Instruction::IntToPtr, readMoreFunc, PointerType::getUnqual(readMoreFuncType), "read_more_func", block);
        const auto readMore = CallInst::Create(readMoreFuncType, readMoreFuncPtr, { stateArg }, "read_more", block);

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);

        BranchInst::Create(next, full, readMore, block);

        {
            block = next;

            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetStatus() }, "last", block);

            const auto last = new LoadInst(statusType, statusPtr, "last", block);

            result->addIncoming(last, block);

            const auto choise = SwitchInst::Create(last, pull, 2U, block);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), rest);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), over);

            block = rest;
            new StoreInst(ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::One)), statusPtr, block);

            if constexpr (SkipYields) {
                new StoreInst(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), statusPtr, block);

                BranchInst::Create(pull, block);
            } else {
                result->addIncoming(last, block);

                BranchInst::Create(over, block);
            }

            block = pull;

            const auto used = GetMemoryUsed(MemLimit, ctx, block);

            BranchInst::Create(loop, block);

            block = loop;

            const auto getres = GetNodeValues(Flow, ctx, block);

            if constexpr (SkipYields) {
                const auto save = BasicBlock::Create(context, "save", ctx.Func);

                const auto way = SwitchInst::Create(getres.first, good, 2U, block);
                way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), save);
                way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), done);

                block = save;

                if (MemLimit) {
                    const auto storedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetStored() }, "stored_ptr", block);
                    const auto lastStored = new LoadInst(storedType, storedPtr, "last_stored", block);
                    const auto currentUsage = GetMemoryUsed(MemLimit, ctx, block);


                    const auto skipSavingUsed = BasicBlock::Create(context, "skip_saving_used", ctx.Func);
                    const auto saveUsed = BasicBlock::Create(context, "save_used", ctx.Func);
                    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, currentUsage, used, "check", block);
                    BranchInst::Create(saveUsed, skipSavingUsed, check, block);

                    block = saveUsed;

                    const auto usedMemory = BinaryOperator::CreateSub(GetMemoryUsed(MemLimit, ctx, block), used, "used_memory", block);
                    const auto inc = BinaryOperator::CreateAdd(lastStored, usedMemory, "inc", block);
                    new StoreInst(inc, storedPtr, block);

                    BranchInst::Create(skipSavingUsed, block);

                    block = skipSavingUsed;
                }

                new StoreInst(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), statusPtr, block);
                result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);
                BranchInst::Create(over, block);
            } else {
                const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);
                BranchInst::Create(done, good, special, block);
            }

            block = good;

            std::vector<Value*> items(Nodes.ItemNodes.size(), nullptr);
            for (ui32 i = 0U; i < items.size(); ++i) {
                if (Nodes.ItemNodes[i]->GetDependencesCount() > 0U)
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.ItemNodes[i])->CreateSetValue(ctx, block, items[i] = getres.second[i](ctx, block));
                else if (Nodes.PasstroughtItems[i])
                    items[i] = getres.second[i](ctx, block);
            }

            const auto tonguePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetTongue() }, "tongue_ptr", block);
            const auto tongue = new LoadInst(ptrValueType, tonguePtr, "tongue", block);

            std::vector<Value*> keyPointers(Nodes.KeyResultNodes.size(), nullptr), keys(Nodes.KeyResultNodes.size(), nullptr);
            for (ui32 i = 0U; i < Nodes.KeyResultNodes.size(); ++i) {
                auto& key = keys[i];
                const auto keyPtr = keyPointers[i] = GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("key_") += ToString(i)).c_str(), block);
                if (const auto map = Nodes.KeysOnItems[i]) {
                    auto& it = items[*map];
                    if (!it)
                        it = getres.second[*map](ctx, block);
                    key = it;
                } else {
                    key = GetNodeValue(Nodes.KeyResultNodes[i], ctx, block);
                }

                if (Nodes.KeyNodes[i]->GetDependencesCount() > 0U)
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.KeyNodes[i])->CreateSetValue(ctx, block, key);

                new StoreInst(key, keyPtr, block);
            }

            const auto atFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::TasteIt));
            const auto atType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType()}, false);
            const auto atPtr = CastInst::Create(Instruction::IntToPtr, atFunc, PointerType::getUnqual(atType), "function", block);
            const auto newKey = CallInst::Create(atType, atPtr, {stateArg}, "new_key", block);

            const auto init = BasicBlock::Create(context, "init", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto test = BasicBlock::Create(context, "test", ctx.Func);

            const auto throatPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetThroat() }, "throat_ptr", block);
            const auto throat = new LoadInst(ptrValueType, throatPtr, "throat", block);

            std::vector<Value*> pointers;
            pointers.reserve(Nodes.StateNodes.size());
            for (ui32 i = 0U; i < Nodes.StateNodes.size(); ++i) {
                pointers.emplace_back(GetElementPtrInst::CreateInBounds(valueType, throat, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("state_") += ToString(i)).c_str(), block));
            }

            BranchInst::Create(init, next, newKey, block);

            block = init;

            for (ui32 i = 0U; i < Nodes.KeyResultNodes.size(); ++i) {
                ValueAddRef(Nodes.KeyResultNodes[i]->GetRepresentation(), keyPointers[i], ctx, block);
            }

            for (ui32 i = 0U; i < Nodes.InitResultNodes.size(); ++i) {
                if (const auto map = Nodes.InitOnItems[i]) {
                    auto& it = items[*map];
                    if (!it)
                        it = getres.second[*map](ctx, block);
                    new StoreInst(it, pointers[i], block);
                    ValueAddRef(Nodes.InitResultNodes[i]->GetRepresentation(), it, ctx, block);
                }  else if (const auto map = Nodes.InitOnKeys[i]) {
                    const auto key = keys[*map];
                    new StoreInst(key, pointers[i], block);
                    ValueAddRef(Nodes.InitResultNodes[i]->GetRepresentation(), key, ctx, block);
                } else {
                    GetNodeValue(pointers[i], Nodes.InitResultNodes[i], ctx, block);
                }
            }

            BranchInst::Create(test, block);

            block = next;

            for (ui32 i = 0U; i < Nodes.KeyResultNodes.size(); ++i) {
                if (Nodes.KeysOnItems[i] || Nodes.KeyResultNodes[i]->IsTemporaryValue())
                    ValueCleanup(Nodes.KeyResultNodes[i]->GetRepresentation(), keyPointers[i], ctx, block);
            }

            std::vector<Value*> stored(Nodes.StateNodes.size(), nullptr);
            for (ui32 i = 0U; i < stored.size(); ++i) {
                const bool hasDependency = Nodes.StateNodes[i]->GetDependencesCount() > 0U;
                if (const auto map = Nodes.StateOnUpdate[i]) {
                    if (hasDependency || i != *map) {
                        stored[i] = new LoadInst(valueType, pointers[i], (TString("state_") += ToString(i)).c_str(), block);
                        if (hasDependency)
                            EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.StateNodes[i])->CreateSetValue(ctx, block, stored[i]);
                    }
                } else if (hasDependency) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.StateNodes[i])->CreateSetValue(ctx, block, pointers[i]);
                } else {
                    ValueUnRef(Nodes.StateNodes[i]->GetRepresentation(), pointers[i], ctx, block);
                }
            }

            for (ui32 i = 0U; i < Nodes.UpdateResultNodes.size(); ++i) {
                if (const auto map = Nodes.UpdateOnState[i]) {
                    if (const auto j = *map; i != j) {
                        auto& it = stored[j];
                        if (!it)
                            it = new LoadInst(valueType, pointers[j], (TString("state_") += ToString(j)).c_str(), block);
                        new StoreInst(it, pointers[i], block);
                        if (i != *Nodes.StateOnUpdate[j])
                            ValueAddRef(Nodes.UpdateResultNodes[i]->GetRepresentation(), it, ctx, block);
                    }
                } else if (const auto map = Nodes.UpdateOnItems[i]) {
                    auto& it = items[*map];
                    if (!it)
                        it = getres.second[*map](ctx, block);
                    new StoreInst(it, pointers[i], block);
                    ValueAddRef(Nodes.UpdateResultNodes[i]->GetRepresentation(), it, ctx, block);
                }  else if (const auto map = Nodes.UpdateOnKeys[i]) {
                    const auto key = keys[*map];
                    new StoreInst(key, pointers[i], block);
                    ValueAddRef(Nodes.UpdateResultNodes[i]->GetRepresentation(), key, ctx, block);
                } else {
                    GetNodeValue(pointers[i], Nodes.UpdateResultNodes[i], ctx, block);
                }
            }

            BranchInst::Create(test, block);

            block = test;

            auto totalUsed = used;
            if (MemLimit) {
                const auto storedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetStored() }, "stored_ptr", block);
                const auto lastStored = new LoadInst(storedType, storedPtr, "last_stored", block);
                totalUsed = BinaryOperator::CreateSub(used, lastStored, "decr", block);
            }

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit, totalUsed, ctx, block);
            BranchInst::Create(done, loop, check, block);

            block = done;

            new StoreInst(getres.first, statusPtr, block);

            const auto stat = ctx.GetStat();
            const auto statFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::PushStat));
            const auto statType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), stat->getType()}, false);
            const auto statPtr = CastInst::Create(Instruction::IntToPtr, statFunc, PointerType::getUnqual(statType), "stat", block);
            CallInst::Create(statType, statPtr, {stateArg, stat}, "", block);

            BranchInst::Create(full, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto extractFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Extract));
            const auto extractType = FunctionType::get(ptrValueType, {stateArg->getType()}, false);
            const auto extractPtr = CastInst::Create(Instruction::IntToPtr, extractFunc, PointerType::getUnqual(extractType), "extract", block);
            const auto out = CallInst::Create(extractType, extractPtr, {stateArg}, "out", block);
            const auto has = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(ptrValueType), "has", block);

            BranchInst::Create(good, more, has, block);

            block = good;

            for (ui32 i = 0U; i < Nodes.FinishNodes.size(); ++i) {
                const auto ptr = GetElementPtrInst::CreateInBounds(valueType, out, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("out_key_") += ToString(i)).c_str(), block);
                if (Nodes.FinishNodes[i]->GetDependencesCount() > 0 || Nodes.ItemsOnResult[i])
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.FinishNodes[i])->CreateSetValue(ctx, block, ptr);
                else
                    ValueUnRef(Nodes.FinishNodes[i]->GetRepresentation(), ptr, ctx, block);
            }

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            BranchInst::Create(over, block);
        }

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(Nodes.FinishResultNodes.size());
        std::transform(Nodes.FinishResultNodes.cbegin(), Nodes.FinishResultNodes.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block){ return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TState>(Nodes.KeyNodes.size(), Nodes.StateNodes.size(), TMyValueHasher(KeyTypes), TMyValueEqual(KeyTypes));
#else
        state = ctx.HolderFactory.Create<TState>(Nodes.KeyNodes.size(), Nodes.StateNodes.size(),
            ctx.ExecuteLLVM && Hash ? THashFunc(std::ptr_fun(Hash)) : THashFunc(TMyValueHasher(KeyTypes)),
            ctx.ExecuteLLVM && Equals ? TEqualsFunc(std::ptr_fun(Equals)) : TEqualsFunc(TMyValueEqual(KeyTypes))
        );
#endif
        if (ctx.CountersProvider) {
            const auto ptr = static_cast<TState*>(state.AsBoxed().Get());
            // id will be assigned externally in future versions
            TString id = TString(Operator_Aggregation) + "0";
            ptr->CounterOutputRows_ = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, false);
        }
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            Nodes.RegisterDependencies(
                [this, flow](IComputationNode* node){ this->DependsOn(flow, node); },
                [this, flow](IComputationExternalNode* node){ this->Own(flow, node); }
            );
        }
    }

    IComputationWideFlowNode *const Flow;
    const TCombinerNodes Nodes;
    const TKeyTypes KeyTypes;
    const ui64 MemLimit;

    const ui32 WideFieldsIndex;

#ifndef MKQL_DISABLE_CODEGEN
    TEqualsPtr Equals = nullptr;
    THashPtr Hash = nullptr;

    Function* EqualsFunc = nullptr;
    Function* HashFunc = nullptr;

    template <bool EqualsOrHash>
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::" << (EqualsOrHash ? "Equals" : "Hash") << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (EqualsFunc) {
            Equals = reinterpret_cast<TEqualsPtr>(codegen.GetPointerToFunction(EqualsFunc));
        }
        if (HashFunc) {
            Hash = reinterpret_cast<THashPtr>(codegen.GetPointerToFunction(HashFunc));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(HashFunc = GenerateHashFunction(codegen, MakeName<false>(), KeyTypes));
        codegen.ExportSymbol(EqualsFunc = GenerateEqualsFunction(codegen, MakeName<true>(), KeyTypes));
    }
#endif
};

class TWideLastCombinerWrapper: public TStatefulWideFlowCodegeneratorNode<TWideLastCombinerWrapper>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideLastCombinerWrapper>;
public:
    TWideLastCombinerWrapper(
        TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TCombinerNodes&& nodes,
        const TMultiType* usedInputItemType,
        TKeyTypes&& keyTypes,
        const TMultiType* keyAndStateType,
        bool allowSpilling)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow(flow)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , UsedInputItemType(usedInputItemType)
        , KeyAndStateType(keyAndStateType)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Nodes.ItemNodes.size()))
        , AllowSpilling(allowSpilling)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        if (const auto ptr = static_cast<TSpillingSupportState*>(state.AsBoxed().Get())) {
            auto **fields = ctx.WideFields.data() + WideFieldsIndex;

            while (true) {
                switch(ptr->Update()) {
                    case TSpillingSupportState::EUpdateResult::ReadInput: {
                        for (auto i = 0U; i < Nodes.ItemNodes.size(); ++i)
                            fields[i] = Nodes.GetUsedInputItemNodePtrOrNull(ctx, i);
                        switch (ptr->InputStatus = Flow->FetchValues(ctx, fields)) {
                            case EFetchResult::One:
                                break;
                            case EFetchResult::Finish:
                                continue;
                            case EFetchResult::Yield:
                                return EFetchResult::Yield;
                        }
                        Nodes.ExtractKey(ctx, fields, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue));
                        break;
                    }
                    case TSpillingSupportState::EUpdateResult::Yield:
                        return EFetchResult::Yield;
                    case TSpillingSupportState::EUpdateResult::ExtractRawData:
                        Nodes.ExtractRawData(ctx, static_cast<NUdf::TUnboxedValue*>(ptr->Throat), static_cast<NUdf::TUnboxedValue*>(ptr->Tongue));
                        break;
                    case TSpillingSupportState::EUpdateResult::Extract:
                        if (const auto values = static_cast<NUdf::TUnboxedValue*>(ptr->Extract())) {
                            Nodes.FinishItem(ctx, values, output);
                            return EFetchResult::One;
                        }
                        continue;
                    case TSpillingSupportState::EUpdateResult::Finish:
                        return EFetchResult::Finish;
                }

                switch(ptr->TasteIt()) {
                    case TSpillingSupportState::ETasteResult::Init:
                        Nodes.ProcessItem(ctx, nullptr, static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                        break;
                    case TSpillingSupportState::ETasteResult::Update:
                        Nodes.ProcessItem(ctx, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue), static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                        break;
                    case TSpillingSupportState::ETasteResult::ConsumeRawData:
                        Nodes.ConsumeRawData(ctx, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue), fields, static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                        break;
                }

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
        const auto wayType = Type::getInt8Ty(context);

        TLLVMFieldsStructureState stateFields(context);

        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideLastCombinerWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        BranchInst::Create(more, block);

        const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
        const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto load = BasicBlock::Create(context, "load", ctx.Func);
        const auto fill = BasicBlock::Create(context, "fill", ctx.Func);
        const auto data = BasicBlock::Create(context, "data", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto stub = BasicBlock::Create(context, "stub", ctx.Func);

        new UnreachableInst(context, stub);

        const auto result = PHINode::Create(statusType, 4U, "result", over);

        std::vector<PHINode*> phis(Nodes.ItemNodes.size(), nullptr);
        auto j = 0U;
        std::generate(phis.begin(), phis.end(), [&]() {
            return Nodes.IsInputItemNodeUsed(j++) ?
                PHINode::Create(valueType, 2U, (TString("item_") += ToString(j)).c_str(), test) : nullptr;
        });

        block = more;

        const auto updateFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::Update));
        const auto updateType = FunctionType::get(wayType, {stateArg->getType()}, false);
        const auto updateFuncPtr = CastInst::Create(Instruction::IntToPtr, updateFunc, PointerType::getUnqual(updateType), "update_func", block);
        const auto update = CallInst::Create(updateType, updateFuncPtr, { stateArg }, "update", block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

        const auto updateWay = SwitchInst::Create(update, stub, 5U, block);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::Yield)), over);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::Extract)), fill);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::Finish)), done);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::ReadInput)), pull);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::ExtractRawData)), load);

        block = load;

        const auto extractorPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetThroat() }, "extractor_ptr", block);
        const auto extractor = new LoadInst(ptrValueType, extractorPtr, "extractor", block);

        std::vector<Value*> items(phis.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            const auto ptr = GetElementPtrInst::CreateInBounds(valueType, extractor, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("load_ptr_") += ToString(i)).c_str(), block);
            if (phis[i])
                items[i] = new LoadInst(valueType, ptr, (TString("load_") += ToString(i)).c_str(), block);
            if (i < Nodes.ItemNodes.size() && Nodes.ItemNodes[i]->GetDependencesCount() > 0U)
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.ItemNodes[i])->CreateSetValue(ctx, block, items[i]);
        }

        for (ui32 i = 0U; i < phis.size(); ++i) {
            if (const auto phi = phis[i]) {
                phi->addIncoming(items[i], block);
            }
        }

        BranchInst::Create(test, block);

        block = pull;

        const auto getres = GetNodeValues(Flow, ctx, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

        const auto choise = SwitchInst::Create(getres.first, good, 2U, block);
        choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), over);
        choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), rest);

        block = rest;
        const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetStatus() }, "last", block);
        new StoreInst(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), statusPtr, block);
        BranchInst::Create(more, block);

        block = good;

        for (ui32 i = 0U; i < items.size(); ++i) {
            if (phis[i])
                items[i] = getres.second[i](ctx, block);
            if (Nodes.ItemNodes[i]->GetDependencesCount() > 0U)
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.ItemNodes[i])->CreateSetValue(ctx, block, items[i]);
        }

        for (ui32 i = 0U; i < phis.size(); ++i) {
            if (const auto phi = phis[i]) {
                phi->addIncoming(items[i], block);
            }
        }

        BranchInst::Create(test, block);

        block = test;

        const auto tonguePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetTongue() }, "tongue_ptr", block);
        const auto tongue = new LoadInst(ptrValueType, tonguePtr, "tongue", block);

        std::vector<Value*> keyPointers(Nodes.KeyResultNodes.size(), nullptr), keys(Nodes.KeyResultNodes.size(), nullptr);
        for (ui32 i = 0U; i < Nodes.KeyResultNodes.size(); ++i) {
            auto& key = keys[i];
            const auto keyPtr = keyPointers[i] = GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("key_") += ToString(i)).c_str(), block);
            if (const auto map = Nodes.KeysOnItems[i]) {
                key = phis[*map];
            } else {
                key = GetNodeValue(Nodes.KeyResultNodes[i], ctx, block);
            }

            if (Nodes.KeyNodes[i]->GetDependencesCount() > 0U)
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.KeyNodes[i])->CreateSetValue(ctx, block, key);

            new StoreInst(key, keyPtr, block);
        }

        const auto atFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::TasteIt));
        const auto atType = FunctionType::get(wayType, {stateArg->getType()}, false);
        const auto atPtr = CastInst::Create(Instruction::IntToPtr, atFunc, PointerType::getUnqual(atType), "function", block);
        const auto taste= CallInst::Create(atType, atPtr, {stateArg}, "taste", block);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto save = BasicBlock::Create(context, "save", ctx.Func);

        const auto throatPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetThroat() }, "throat_ptr", block);
        const auto throat = new LoadInst(ptrValueType, throatPtr, "throat", block);

        std::vector<Value*> pointers;
        const auto width = std::max(Nodes.StateNodes.size(), phis.size());
        pointers.reserve(width);
        for (ui32 i = 0U; i < width; ++i) {
            pointers.emplace_back(GetElementPtrInst::CreateInBounds(valueType, throat, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("state_") += ToString(i)).c_str(), block));
        }

        const auto way = SwitchInst::Create(taste, stub, 3U, block);
        way->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::ETasteResult::Init)), init);
        way->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::ETasteResult::Update)), next);
        way->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::ETasteResult::ConsumeRawData)), save);

        block = init;

        for (ui32 i = 0U; i < Nodes.KeyResultNodes.size(); ++i) {
            ValueAddRef(Nodes.KeyResultNodes[i]->GetRepresentation(), keyPointers[i], ctx, block);
        }

        for (ui32 i = 0U; i < Nodes.InitResultNodes.size(); ++i) {
            if (const auto map = Nodes.InitOnItems[i]) {
                const auto item = phis[*map];
                new StoreInst(item, pointers[i], block);
                ValueAddRef(Nodes.InitResultNodes[i]->GetRepresentation(), item, ctx, block);
            }  else if (const auto map = Nodes.InitOnKeys[i]) {
                const auto key = keys[*map];
                new StoreInst(key, pointers[i], block);
                ValueAddRef(Nodes.InitResultNodes[i]->GetRepresentation(), key, ctx, block);
            } else {
                GetNodeValue(pointers[i], Nodes.InitResultNodes[i], ctx, block);
            }
        }

        BranchInst::Create(more, block);

        block = next;

        std::vector<Value*> stored(Nodes.StateNodes.size(), nullptr);
        for (ui32 i = 0U; i < stored.size(); ++i) {
            const bool hasDependency = Nodes.StateNodes[i]->GetDependencesCount() > 0U;
            if (const auto map = Nodes.StateOnUpdate[i]) {
                if (hasDependency || i != *map) {
                    stored[i] = new LoadInst(valueType, pointers[i], (TString("state_") += ToString(i)).c_str(), block);
                    if (hasDependency)
                        EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.StateNodes[i])->CreateSetValue(ctx, block, stored[i]);
                }
            } else if (hasDependency) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.StateNodes[i])->CreateSetValue(ctx, block, pointers[i]);
            } else {
                ValueUnRef(Nodes.StateNodes[i]->GetRepresentation(), pointers[i], ctx, block);
            }
        }

        for (ui32 i = 0U; i < Nodes.UpdateResultNodes.size(); ++i) {
            if (const auto map = Nodes.UpdateOnState[i]) {
                if (const auto j = *map; i != j) {
                    const auto it = stored[j];
                    new StoreInst(it, pointers[i], block);
                    if (i != *Nodes.StateOnUpdate[j])
                        ValueAddRef(Nodes.UpdateResultNodes[i]->GetRepresentation(), it, ctx, block);
                }
            } else if (const auto map = Nodes.UpdateOnItems[i]) {
                const auto item = phis[*map];
                new StoreInst(item, pointers[i], block);
                ValueAddRef(Nodes.UpdateResultNodes[i]->GetRepresentation(), item, ctx, block);
            }  else if (const auto map = Nodes.UpdateOnKeys[i]) {
                const auto key = keys[*map];
                new StoreInst(key, pointers[i], block);
                ValueAddRef(Nodes.UpdateResultNodes[i]->GetRepresentation(), key, ctx, block);
            } else {
                GetNodeValue(pointers[i], Nodes.UpdateResultNodes[i], ctx, block);
            }
        }

        BranchInst::Create(more, block);

        block = save;

        for (ui32 i = 0U; i < phis.size(); ++i) {
            if (const auto item = phis[i]) {
                new StoreInst(item, pointers[i], block);
                ValueAddRef(Nodes.ItemNodes[i]->GetRepresentation(), item, ctx, block);
            }
        }

        BranchInst::Create(more, block);

        block = fill;

        const auto extractFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSpillingSupportState::Extract));
        const auto extractType = FunctionType::get(ptrValueType, {stateArg->getType()}, false);
        const auto extractPtr = CastInst::Create(Instruction::IntToPtr, extractFunc, PointerType::getUnqual(extractType), "extract", block);
        const auto out = CallInst::Create(extractType, extractPtr, {stateArg}, "out", block);
        const auto has = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(ptrValueType), "has", block);

        BranchInst::Create(data, more, has, block);

        block = data;

        for (ui32 i = 0U; i < Nodes.FinishNodes.size(); ++i) {
            const auto ptr = GetElementPtrInst::CreateInBounds(valueType, out, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("out_key_") += ToString(i)).c_str(), block);
            if (Nodes.FinishNodes[i]->GetDependencesCount() > 0 || Nodes.ItemsOnResult[i])
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes.FinishNodes[i])->CreateSetValue(ctx, block, ptr);
            else
                ValueUnRef(Nodes.FinishNodes[i]->GetRepresentation(), ptr, ctx, block);
        }

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = done;

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);
        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(Nodes.FinishResultNodes.size());
        std::transform(Nodes.FinishResultNodes.cbegin(), Nodes.FinishResultNodes.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block){ return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TSpillingSupportState>(UsedInputItemType, KeyAndStateType,
            Nodes.KeyNodes.size(),
            Nodes.ItemNodes.size(),
#ifdef MKQL_DISABLE_CODEGEN
            TMyValueHasher(KeyTypes),
            TMyValueEqual(KeyTypes),
#else
           ctx.ExecuteLLVM && Hash ? THashFunc(std::ptr_fun(Hash)) : THashFunc(TMyValueHasher(KeyTypes)),
           ctx.ExecuteLLVM && Equals ? TEqualsFunc(std::ptr_fun(Equals)) : TEqualsFunc(TMyValueEqual(KeyTypes)),
#endif
            AllowSpilling,
            ctx
        );
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            Nodes.RegisterDependencies(
                [this, flow](IComputationNode* node){ this->DependsOn(flow, node); },
                [this, flow](IComputationExternalNode* node){ this->Own(flow, node); }
            );
        }
    }

    IComputationWideFlowNode *const Flow;
    const TCombinerNodes Nodes;
    const TKeyTypes KeyTypes;

    const TMultiType* const UsedInputItemType;
    const TMultiType* const KeyAndStateType;

    const ui32 WideFieldsIndex;

    const bool AllowSpilling;
#ifndef MKQL_DISABLE_CODEGEN
    TEqualsPtr Equals = nullptr;
    THashPtr Hash = nullptr;

    Function* EqualsFunc = nullptr;
    Function* HashFunc = nullptr;

    template <bool EqualsOrHash>
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::" << (EqualsOrHash ? "Equals" : "Hash") << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (EqualsFunc) {
            Equals = reinterpret_cast<TEqualsPtr>(codegen.GetPointerToFunction(EqualsFunc));
        }
        if (HashFunc) {
            Hash = reinterpret_cast<THashPtr>(codegen.GetPointerToFunction(HashFunc));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(HashFunc = GenerateHashFunction(codegen, MakeName<false>(), KeyTypes));
        codegen.ExportSymbol(EqualsFunc = GenerateEqualsFunction(codegen, MakeName<true>(), KeyTypes));
    }
#endif
};

}

template<bool Last>
IComputationNode* WrapWideCombinerT(TCallable& callable, const TComputationNodeFactoryContext& ctx, bool allowSpilling) {
    MKQL_ENSURE(callable.GetInputsCount() >= (Last ? 3U : 4U), "Expected more arguments.");

    const auto inputType = AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType());
    const auto inputWidth = GetWideComponentsCount(inputType);
    const auto outputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);

    auto index = Last ? 0U : 1U;

    const auto keysSize = AS_VALUE(TDataLiteral, callable.GetInput(++index))->AsValue().Get<ui32>();
    const auto stateSize = AS_VALUE(TDataLiteral, callable.GetInput(++index))->AsValue().Get<ui32>();

    ++index += inputWidth;

    std::vector<TType*> keyAndStateItemTypes;
    keyAndStateItemTypes.reserve(keysSize + stateSize);

    TKeyTypes keyTypes;
    keyTypes.reserve(keysSize);
    for (ui32 i = index; i < index + keysSize; ++i) {
        TType *type = callable.GetInput(i).GetStaticType();
        keyAndStateItemTypes.push_back(type);
        bool optional;
        keyTypes.emplace_back(*UnpackOptionalData(callable.GetInput(i).GetStaticType(), optional)->GetDataSlot(), optional);
    }

    TCombinerNodes nodes;
    nodes.KeyResultNodes.reserve(keysSize);
    std::generate_n(std::back_inserter(nodes.KeyResultNodes), keysSize, [&](){ return LocateNode(ctx.NodeLocator, callable, index++); } );

    index += keysSize;
    nodes.InitResultNodes.reserve(stateSize);
    for (size_t i = 0; i != stateSize; ++i) {
        TType *type = callable.GetInput(index).GetStaticType();
        keyAndStateItemTypes.push_back(type);
        nodes.InitResultNodes.push_back(LocateNode(ctx.NodeLocator, callable, index++));
    }

    index += stateSize;
    nodes.UpdateResultNodes.reserve(stateSize);
    std::generate_n(std::back_inserter(nodes.UpdateResultNodes), stateSize, [&](){ return LocateNode(ctx.NodeLocator, callable, index++); } );

    index += keysSize + stateSize;
    nodes.FinishResultNodes.reserve(outputWidth);
    std::generate_n(std::back_inserter(nodes.FinishResultNodes), outputWidth, [&](){ return LocateNode(ctx.NodeLocator, callable, index++); } );

    index = Last ? 3U : 4U;

    nodes.ItemNodes.reserve(inputWidth);
    std::generate_n(std::back_inserter(nodes.ItemNodes), inputWidth, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, index++); } );

    index += keysSize;
    nodes.KeyNodes.reserve(keysSize);
    std::generate_n(std::back_inserter(nodes.KeyNodes), keysSize, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, index++); } );

    index += stateSize;
    nodes.StateNodes.reserve(stateSize);
    std::generate_n(std::back_inserter(nodes.StateNodes), stateSize, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, index++); } );

    index += stateSize;
    nodes.FinishNodes.reserve(keysSize + stateSize);
    std::generate_n(std::back_inserter(nodes.FinishNodes), keysSize + stateSize, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, index++); } );

    nodes.BuildMaps();
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        if constexpr (Last) {
            const auto inputItemTypes = GetWideComponents(inputType);
            return new TWideLastCombinerWrapper(ctx.Mutables, wide, std::move(nodes),
                TMultiType::Create(inputItemTypes.size(), inputItemTypes.data(), ctx.Env),
                std::move(keyTypes),
                TMultiType::Create(keyAndStateItemTypes.size(),keyAndStateItemTypes.data(), ctx.Env),
                allowSpilling
            );
        } else {
            if constexpr (RuntimeVersion < 46U) {
                const auto memLimit = AS_VALUE(TDataLiteral, callable.GetInput(1U))->AsValue().Get<ui64>();
                if (EGraphPerProcess::Single == ctx.GraphPerProcess)
                    return new TWideCombinerWrapper<true, false>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), memLimit);
                else
                    return new TWideCombinerWrapper<false, false>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), memLimit);
            } else {
                if (const auto memLimit = AS_VALUE(TDataLiteral, callable.GetInput(1U))->AsValue().Get<i64>(); memLimit >= 0)
                    if (EGraphPerProcess::Single == ctx.GraphPerProcess)
                        return new TWideCombinerWrapper<true, false>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(memLimit));
                    else
                        return new TWideCombinerWrapper<false, false>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(memLimit));
                else
                    if (EGraphPerProcess::Single == ctx.GraphPerProcess)
                        return new TWideCombinerWrapper<true, true>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(-memLimit));
                    else
                        return new TWideCombinerWrapper<false, true>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(-memLimit));
            }
        }
    }

    THROW yexception() << "Expected wide flow.";
}

IComputationNode* WrapWideCombiner(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideCombinerT<false>(callable, ctx, false);
}

IComputationNode* WrapWideLastCombiner(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    YQL_LOG(INFO) << "Found non-serializable type, spilling is disabled";
    return WrapWideCombinerT<true>(callable, ctx, false);
}

IComputationNode* WrapWideLastCombinerWithSpilling(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideCombinerT<true>(callable, ctx, true);
}

}
}
