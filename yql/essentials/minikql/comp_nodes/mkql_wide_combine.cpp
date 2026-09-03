#include "mkql_counters.h"
#include "mkql_rh_hash.h"
#include "mkql_wide_combine.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_llvm_base.h>                // Y_IGNORE
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

#include <utility>

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using NYql::EnsureDynamicCast;
#endif
using NYql::TChunkedBuffer;

extern TStatKey Combine_FlushesCount;
extern TStatKey Combine_MaxRowsCount;

namespace {

bool HasMemoryForProcessing() {
    return !TlsAllocState->IsMemoryYellowZoneEnabled();
}

struct TMyValueEqual {
    explicit TMyValueEqual(const TKeyTypes& types)
        : Types(types)
    {
    }

    bool operator()(const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right) const {
        for (ui32 i = 0U; i < Types.size(); ++i) {
            if (CompareValues(Types[i].first, /*asc=*/true, Types[i].second, left[i], right[i])) {
                return false;
            }
        }
        return true;
    }

    const TKeyTypes& Types;
};

struct TMyValueHasher {
    explicit TMyValueHasher(const TKeyTypes& types)
        : Types(types)
    {
    }

    NUdf::THashType operator()(const NUdf::TUnboxedValuePod* values) const {
        if (Types.size() == 1U) {
            if (const auto v = *values) {
                return NUdf::GetValueHash(Types.front().first, v);
            } else {
                return HashOfNull;
            }
        }

        NUdf::THashType hash = 0ULL;
        for (const auto& type : Types) {
            if (const auto v = *values++) {
                hash = CombineHashes(hash, NUdf::GetValueHash(type.first, v));
            } else {
                hash = CombineHashes(hash, HashOfNull);
            }
        }
        return hash;
    }

    const TKeyTypes& Types;
};

using TEqualsPtr = bool (*)(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*);
using THashPtr = NUdf::THashType (*)(const NUdf::TUnboxedValuePod*);

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
        return (ItemNodes[i]->GetDependentsCount() > 0U || PasstroughtItems[i]);
    }

    NUdf::TUnboxedValue* GetUsedInputItemNodePtrOrNull(TComputationContext& ctx, size_t i) const {
        return IsInputItemNodeUsed(i) ? &ItemNodes[i]->RefValue(ctx) : nullptr;
    }

    void ExtractKey(TComputationContext& ctx, NUdf::TUnboxedValue** values, NUdf::TUnboxedValue* keys) const {
        std::for_each(ItemNodes.cbegin(), ItemNodes.cend(), [&](IComputationExternalNode* item) {
            if (const auto pointer = *values++) {
                item->SetValue(ctx, std::move(*pointer));
            }
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
            std::for_each(StateNodes.cbegin(), StateNodes.cend(), [&](IComputationExternalNode* item) { item->SetValue(ctx, std::move(*source++)); });
            std::transform(UpdateResultNodes.cbegin(), UpdateResultNodes.cend(), state, [&](IComputationNode* node) { return node->GetValue(ctx); });
        } else {
            std::transform(InitResultNodes.cbegin(), InitResultNodes.cend(), state, [&](IComputationNode* node) { return node->GetValue(ctx); });
        }
    }

    void FinishItem(TComputationContext& ctx, NUdf::TUnboxedValue* state, NUdf::TUnboxedValue* const* output) const {
        std::for_each(FinishNodes.cbegin(), FinishNodes.cend(), [&](IComputationExternalNode* item) { item->SetValue(ctx, std::move(*state++)); });
        for (const auto node : FinishResultNodes) {
            if (const auto out = *output++) {
                *out = node->GetValue(ctx);
            }
        }
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

class TState: public TComputationValue<TState> {
    using TBase = TComputationValue<TState>;

private:
    using TStates = TRobinHoodHashSet<NUdf::TUnboxedValuePod*, TEqualsFunc, THashFunc, TMKQLAllocator<char, EMemorySubPool::Temporary>>;
    using TRow = std::vector<NUdf::TUnboxedValuePod, TMKQLAllocator<NUdf::TUnboxedValuePod>>;
    using TStorage = std::deque<TRow, TMKQLAllocator<TRow>>;

    class TStorageIterator {
    private:
        TStorage& Storage_;
        const ui32 RowSize_ = 0;
        const ui64 Count_ = 0;
        ui64 Ready_ = 0;
        TStorage::iterator ItStorage_;
        TRow::iterator ItRow_;

    public:
        TStorageIterator(TStorage& storage, const ui32 rowSize, const ui64 count)
            : Storage_(storage)
            , RowSize_(rowSize)
            , Count_(count)
        {
            ItStorage_ = Storage_.begin();
            if (ItStorage_ != Storage_.end()) {
                ItRow_ = ItStorage_->begin();
            }
        }

        bool IsValid() {
            return Ready_ < Count_;
        }

        bool Next() {
            if (++Ready_ >= Count_) {
                return false;
            }
            ItRow_ += RowSize_;
            if (ItRow_ == ItStorage_->end()) {
                ++ItStorage_;
                ItRow_ = ItStorage_->begin();
            }

            return true;
        }

        NUdf::TUnboxedValuePod* GetValuePtr() const {
            return &*ItRow_;
        }
    };

    static constexpr ui32 CountRowsOnPage = 128;

    ui32 RowSize() const {
        return KeyWidth_ + StateWidth_;
    }

public:
    TState(
        TMemoryUsageInfo* memInfo, ui32 keyWidth, ui32 stateWidth, THashFunc hash, TEqualsFunc equal,
        NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent, bool allowOutOfMemory = true)
        : TBase(memInfo)
        , KeyWidth_(keyWidth)
        , StateWidth_(stateWidth)
        , AllowOutOfMemory_(allowOutOfMemory)
        , Hash_(std::move(hash))
        , Equal_(std::move(equal))
        , Logger_(std::move(logger))
        , LogComponent_(logComponent)
    {
        CurrentPage_ = &Storage_.emplace_back(RowSize() * CountRowsOnPage, NUdf::TUnboxedValuePod());
        CurrentPosition_ = 0;
        Tongue = CurrentPage_->data();
        States_ = std::make_unique<TStates>(Hash_, Equal_, CountRowsOnPage);
    }

    ~TState() override {
        // Workaround for YQL-16663, consider to rework this class in a safe manner
        while (auto row = Extract()) {
            for (size_t i = 0; i != RowSize(); ++i) {
                row[i].UnRef();
            }
        }

        ExtractIt_.reset();
        Storage_.clear();
        States_->Clear();

        CleanupCurrentContext();
    }

    bool TasteIt() {
        Y_ABORT_UNLESS(!ExtractIt_);
        bool isNew = false;
        auto itInsert = States_->Insert(Tongue, isNew);
        if (isNew) {
            CurrentPosition_ += RowSize();
            if (CurrentPosition_ == CurrentPage_->size()) {
                CurrentPage_ = &Storage_.emplace_back(RowSize() * CountRowsOnPage, NUdf::TUnboxedValuePod());
                CurrentPosition_ = 0;
            }
            Tongue = CurrentPage_->data() + CurrentPosition_;
        }
        Throat = States_->GetKeyValue(itInsert) + KeyWidth_;
        if (isNew) {
            GrowStates();
        }
        IsOutOfMemory = IsOutOfMemory || (!HasMemoryForProcessing() && States_->GetSize() > 1000);
        return isNew;
    }

    void GrowStates() {
        try {
            States_->CheckGrow();
        } catch (const TMemoryLimitExceededException&) {
            UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, TStringBuilder() << "State failed to grow");
            if (IsOutOfMemory || !AllowOutOfMemory_) {
                throw;
            } else {
                IsOutOfMemory = true;
            }
        }
    }

    template <bool SkipYields>
    bool ReadMore() {
        if constexpr (SkipYields) {
            if (EFetchResult::Yield == InputStatus) {
                return true;
            }
        }

        if (!States_->Empty()) {
            return false;
        }

        {
            TStorage localStorage;
            std::swap(localStorage, Storage_);
        }

        if (IsOutOfMemory) {
            States_ = std::make_unique<TStates>(Hash_, Equal_, CountRowsOnPage);
        }

        CurrentPage_ = &Storage_.emplace_back(RowSize() * CountRowsOnPage, NUdf::TUnboxedValuePod());
        CurrentPosition_ = 0;
        Tongue = CurrentPage_->data();
        StoredDataSize = 0;
        IsOutOfMemory = false;

        CleanupCurrentContext();
        return true;
    }

    void PushStat(IStatsRegistry* stats) const {
        if (!States_->Empty()) {
            MKQL_SET_MAX_STAT(stats, Combine_MaxRowsCount, static_cast<i64>(States_->GetSize()));
            MKQL_INC_STAT(stats, Combine_FlushesCount);
        }
    }

    NUdf::TUnboxedValuePod* Extract() {
        if (!ExtractIt_) {
            ExtractIt_.emplace(Storage_, RowSize(), States_->GetSize());
        } else {
            ExtractIt_->Next();
        }
        if (!ExtractIt_->IsValid()) {
            ExtractIt_.reset();
            States_->Clear();
            return nullptr;
        }
        NUdf::TUnboxedValuePod* result = ExtractIt_->GetValuePtr();
        CounterOutputRows.Inc();
        return result;
    }

    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Tongue = nullptr;
    NUdf::TUnboxedValuePod* Throat = nullptr;
    i64 StoredDataSize = 0;
    bool IsOutOfMemory = false;
    NYql::NUdf::TCounter CounterOutputRows;

private:
    std::optional<TStorageIterator> ExtractIt_;
    const ui32 KeyWidth_, StateWidth_;
    const bool AllowOutOfMemory_;
    ui64 CurrentPosition_ = 0;
    TRow* CurrentPage_ = nullptr;
    TStorage Storage_;
    std::unique_ptr<TStates> States_;
    const THashFunc Hash_;
    const TEqualsFunc Equal_;
    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
};

class TSpillingSupportState: public TComputationValue<TSpillingSupportState> {
    using TBase = TComputationValue<TSpillingSupportState>;
    using TAsyncWriteOperation = std::optional<NThreading::TFuture<ISpiller::TKey>>;
    using TAsyncReadOperation = std::optional<NThreading::TFuture<std::optional<TChunkedBuffer>>>;

    struct TSpilledBucket {
        std::unique_ptr<TWideUnboxedValuesSpillerAdapter> SpilledState; // state collected before switching to spilling mode
        std::unique_ptr<TWideUnboxedValuesSpillerAdapter> SpilledData;  // data collected in spilling mode
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
        const THashFunc& hash, const TEqualsFunc& equal, bool allowSpilling, TComputationContext& ctx,
        NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent)
        : TBase(memInfo)
        , InMemoryProcessingState_(memInfo, keyWidth, keyAndStateType->GetElementsCount() - keyWidth, hash, equal, logger, logComponent, allowSpilling && ctx.SpillerFactory)
        , UsedInputItemType_(usedInputItemType)
        , KeyAndStateType_(keyAndStateType)
        , KeyWidth_(keyWidth)
        , ItemNodesSize_(itemNodesSize)
        , Hasher_(hash)
        , Mode_(EOperatingMode::InMemory)
        , ViewForKeyAndState_(keyAndStateType->GetElementsCount())
        , MemInfo_(memInfo)
        , Equal_(equal)
        , AllowSpilling_(allowSpilling)
        , Ctx_(ctx)
        , Logger_(std::move(logger))
        , LogComponent_(logComponent)
    {
        BufferForUsedInputItems_.reserve(usedInputItemType->GetElementsCount());
        Tongue = InMemoryProcessingState_.Tongue;
        Throat = InMemoryProcessingState_.Throat;
        if (ctx.CountersProvider) {
            // id will be assigned externally in future versions
            TString id = TString(Operator_Aggregation) + "0";
            CounterOutputRows_ = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, /*deriv=*/false);
        }
    }

    EUpdateResult Update() {
        if (IsEverythingExtracted_) {
            return EUpdateResult::Finish;
        }

        switch (GetMode()) {
            case EOperatingMode::InMemory: {
                Tongue = InMemoryProcessingState_.Tongue;
                if (CheckMemoryAndSwitchToSpilling()) {
                    return Update();
                }
                if (InputStatus == EFetchResult::Finish) {
                    return EUpdateResult::Extract;
                }

                return EUpdateResult::ReadInput;
            }
            case EOperatingMode::SplittingState: {
                if (SplitStateIntoBucketsAndWait()) {
                    return EUpdateResult::Yield;
                }
                return Update();
            }
            case EOperatingMode::Spilling: {
                UpdateSpillingBuckets();

                if (!HasMemoryForProcessing() && InputStatus != EFetchResult::Finish && TryToReduceMemoryAndWait()) {
                    return EUpdateResult::Yield;
                }

                if (!BufferForUsedInputItems_.empty()) {
                    auto& bucket = SpilledBuckets_[BufferForUsedInputItemsBucketId_];
                    if (bucket.AsyncWriteOperation.has_value()) {
                        return EUpdateResult::Yield;
                    }

                    bucket.AsyncWriteOperation = bucket.SpilledData->WriteWideItem(BufferForUsedInputItems_);
                    BufferForUsedInputItems_.resize(0); // for freeing allocated key value asap
                }

                if (InputStatus == EFetchResult::Finish) {
                    return FlushSpillingBuffersAndWait();
                }

                return EUpdateResult::ReadInput;
            }
            case EOperatingMode::ProcessSpilled:
                return ProcessSpilledData();
        }
    }

    ETasteResult TasteIt() {
        if (GetMode() == EOperatingMode::InMemory) {
            bool isNew = InMemoryProcessingState_.TasteIt();
            if (InMemoryProcessingState_.IsOutOfMemory) {
                StateWantsToSpill_ = true;
            }
            Throat = InMemoryProcessingState_.Throat;
            return isNew ? ETasteResult::Init : ETasteResult::Update;
        }
        if (GetMode() == EOperatingMode::ProcessSpilled) {
            // while restoration we process buckets one by one starting from the first in a queue
            bool isNew = SpilledBuckets_.front().InMemoryProcessingState->TasteIt();
            Throat = SpilledBuckets_.front().InMemoryProcessingState->Throat;
            return isNew ? ETasteResult::Init : ETasteResult::Update;
        }

        auto bucketId = ChooseBucket(ViewForKeyAndState_.data());
        auto& bucket = SpilledBuckets_[bucketId];

        if (bucket.BucketState == TSpilledBucket::EBucketState::InMemory) {
            std::copy_n(ViewForKeyAndState_.data(), KeyWidth_, static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Tongue));

            bool isNew = bucket.InMemoryProcessingState->TasteIt();
            Throat = bucket.InMemoryProcessingState->Throat;
            bucket.LineCount += isNew;

            return isNew ? ETasteResult::Init : ETasteResult::Update;
        }
        bucket.LineCount++;

        // Prepare space for raw data
        MKQL_ENSURE(BufferForUsedInputItems_.empty(), "Internal logic error");
        BufferForUsedInputItems_.resize(ItemNodesSize_);
        BufferForUsedInputItemsBucketId_ = bucketId;

        Throat = BufferForUsedInputItems_.data();

        return ETasteResult::ConsumeRawData;
    }

    NUdf::TUnboxedValuePod* Extract() {
        NUdf::TUnboxedValue* value = nullptr;
        if (GetMode() == EOperatingMode::InMemory) {
            value = static_cast<NUdf::TUnboxedValue*>(InMemoryProcessingState_.Extract());
            if (value) {
                CounterOutputRows_.Inc();
            } else {
                IsEverythingExtracted_ = true;
            }
            return value;
        }

        MKQL_ENSURE(SpilledBuckets_.front().BucketState == TSpilledBucket::EBucketState::InMemory, "Internal logic error");
        MKQL_ENSURE(!SpilledBuckets_.empty(), "Internal logic error");

        value = static_cast<NUdf::TUnboxedValue*>(SpilledBuckets_.front().InMemoryProcessingState->Extract());
        if (value) {
            CounterOutputRows_.Inc();
        } else {
            SpilledBuckets_.front().InMemoryProcessingState->ReadMore<false>();
            SpilledBuckets_.pop_front();
            if (SpilledBuckets_.empty()) {
                IsEverythingExtracted_ = true;
            }
        }

        return value;
    }

private:
    ui64 ChooseBucket(const NUdf::TUnboxedValuePod* const key) {
        auto provided_hash = Hasher_(key);
        XXH64_hash_t bucket = XXH64(&provided_hash, sizeof(provided_hash), 0) % SpilledBucketCount;
        return bucket;
    }

    EUpdateResult FlushSpillingBuffersAndWait() {
        UpdateSpillingBuckets();

        ui64 finishedCount = 0;
        for (auto& bucket : SpilledBuckets_) {
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

        if (finishedCount != SpilledBuckets_.size()) {
            return EUpdateResult::Yield;
        }

        SwitchMode(EOperatingMode::ProcessSpilled);

        return ProcessSpilledData();
    }

    ui32 GetLargestInMemoryBucketNumber() const {
        ui64 maxSize = 0;
        ui32 largestInMemoryBucketNum = (ui32)-1;
        for (ui64 i = 0; i < SpilledBucketCount; ++i) {
            if (SpilledBuckets_[i].BucketState == TSpilledBucket::EBucketState::InMemory) {
                if (SpilledBuckets_[i].LineCount >= maxSize) {
                    largestInMemoryBucketNum = i;
                    maxSize = SpilledBuckets_[i].LineCount;
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
        if (SplitStateSpillingBucket_ != -1) {
            auto& bucket = SpilledBuckets_[SplitStateSpillingBucket_];
            MKQL_ENSURE(bucket.AsyncWriteOperation.has_value(), "Internal logic error");
            if (!bucket.AsyncWriteOperation->HasValue()) {
                return true;
            }
            bucket.SpilledState->AsyncWriteCompleted(bucket.AsyncWriteOperation->ExtractValue());
            bucket.AsyncWriteOperation = std::nullopt;

            while (const auto keyAndState = static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Extract())) {
                bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType_->GetElementsCount()});
                for (size_t i = 0; i < KeyAndStateType_->GetElementsCount(); ++i) {
                    // releasing values stored in unsafe TUnboxedValue buffer
                    keyAndState[i].UnRef();
                }
                if (bucket.AsyncWriteOperation) {
                    return true;
                }
            }

            SplitStateSpillingBucket_ = -1;
        }
        while (const auto keyAndState = static_cast<NUdf::TUnboxedValue*>(InMemoryProcessingState_.Extract())) {
            auto bucketId = ChooseBucket(keyAndState); // This uses only key for hashing
            auto& bucket = SpilledBuckets_[bucketId];

            bucket.LineCount++;

            if (bucket.BucketState != TSpilledBucket::EBucketState::InMemory) {
                if (bucket.BucketState != TSpilledBucket::EBucketState::SpillingState) {
                    bucket.BucketState = TSpilledBucket::EBucketState::SpillingState;
                    SpillingBucketsCount_++;
                }

                bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType_->GetElementsCount()});
                for (size_t i = 0; i < KeyAndStateType_->GetElementsCount(); ++i) {
                    // releasing values stored in unsafe TUnboxedValue buffer
                    keyAndState[i].UnRef();
                }
                if (bucket.AsyncWriteOperation) {
                    SplitStateSpillingBucket_ = bucketId;
                    return true;
                }
                continue;
            }

            auto& processingState = *bucket.InMemoryProcessingState;

            for (size_t i = 0; i < KeyWidth_; ++i) {
                // jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(processingState.Tongue[i]) = std::move(keyAndState[i]);
            }
            processingState.TasteIt();
            for (size_t i = KeyWidth_; i < KeyAndStateType_->GetElementsCount(); ++i) {
                // jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(processingState.Throat[i - KeyWidth_]) = std::move(keyAndState[i]);
            }

            if (InMemoryBucketsCount_ && !HasMemoryForProcessing() && IsSpillingWhileStateSplitAllowed()) {
                ui32 bucketNumToSpill = GetLargestInMemoryBucketNumber();

                SplitStateSpillingBucket_ = bucketNumToSpill;

                auto& bucket = SpilledBuckets_[bucketNumToSpill];
                bucket.BucketState = TSpilledBucket::EBucketState::SpillingState;
                SpillingBucketsCount_++;
                InMemoryBucketsCount_--;

                while (const auto keyAndState = static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Extract())) {
                    bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType_->GetElementsCount()});
                    for (size_t i = 0; i < KeyAndStateType_->GetElementsCount(); ++i) {
                        // releasing values stored in unsafe TUnboxedValue buffer
                        keyAndState[i].UnRef();
                    }
                    if (bucket.AsyncWriteOperation) {
                        return true;
                    }
                }

                bucket.AsyncWriteOperation = bucket.SpilledState->FinishWriting();
                if (bucket.AsyncWriteOperation) {
                    return true;
                }
            }
        }

        for (ui64 i = 0; i < SpilledBucketCount; ++i) {
            auto& bucket = SpilledBuckets_[i];
            if (bucket.BucketState == TSpilledBucket::EBucketState::SpillingState) {
                if (bucket.AsyncWriteOperation.has_value()) {
                    if (!bucket.AsyncWriteOperation->HasValue()) {
                        return true;
                    }
                    bucket.SpilledState->AsyncWriteCompleted(bucket.AsyncWriteOperation->ExtractValue());
                    bucket.AsyncWriteOperation = std::nullopt;
                }

                bucket.AsyncWriteOperation = bucket.SpilledState->FinishWriting();
                if (bucket.AsyncWriteOperation) {
                    return true;
                }
                bucket.InMemoryProcessingState->ReadMore<false>();

                bucket.BucketState = TSpilledBucket::EBucketState::SpillingData;
                SpillingBucketsCount_--;
            }
        }

        InMemoryProcessingState_.ReadMore<false>();
        IsInMemoryProcessingStateSplitted_ = true;
        SwitchMode(EOperatingMode::Spilling);
        return false;
    }

    bool CheckMemoryAndSwitchToSpilling() {
        if (!(AllowSpilling_ && Ctx_.SpillerFactory)) {
            return false;
        }
        if (StateWantsToSpill_ || IsSwitchToSpillingModeCondition()) {
            StateWantsToSpill_ = false;
            LogMemoryUsage();

            SwitchMode(EOperatingMode::SplittingState);
            return true;
        }

        return false;
    }

    void LogMemoryUsage() const {
        const auto memoryUsageLogLevel = NUdf::ELogLevel::Info;
        if (!Logger_->IsActive(LogComponent_, memoryUsageLogLevel)) {
            return;
        }
        const auto used = TlsAllocState->GetUsed();
        const auto limit = TlsAllocState->GetLimit();
        TStringBuilder logmsg;
        logmsg << "Memory usage: ";
        if (limit) {
            logmsg << (used * 100 / limit) << "%=";
        }
        logmsg << (used / 1_MB) << "MB/" << (limit / 1_MB) << "MB";

        UDF_LOG(Logger_, LogComponent_, memoryUsageLogLevel, logmsg);
    }

    void SpillMoreStateFromBucket(TSpilledBucket& bucket) {
        MKQL_ENSURE(!bucket.AsyncWriteOperation.has_value(), "Internal logic error");

        if (bucket.BucketState == TSpilledBucket::EBucketState::InMemory) {
            bucket.BucketState = TSpilledBucket::EBucketState::SpillingState;
            SpillingBucketsCount_++;
            InMemoryBucketsCount_--;
        }

        while (const auto keyAndState = static_cast<NUdf::TUnboxedValue*>(bucket.InMemoryProcessingState->Extract())) {
            bucket.AsyncWriteOperation = bucket.SpilledState->WriteWideItem({keyAndState, KeyAndStateType_->GetElementsCount()});
            for (size_t i = 0; i < KeyAndStateType_->GetElementsCount(); ++i) {
                // releasing values stored in unsafe TUnboxedValue buffer
                keyAndState[i].UnRef();
            }
            if (bucket.AsyncWriteOperation) {
                return;
            }
        }

        bucket.AsyncWriteOperation = bucket.SpilledState->FinishWriting();
        if (bucket.AsyncWriteOperation) {
            return;
        }

        bucket.InMemoryProcessingState->ReadMore<false>();

        bucket.BucketState = TSpilledBucket::EBucketState::SpillingData;
        SpillingBucketsCount_--;
    }

    void UpdateSpillingBuckets() {
        for (ui64 i = 0; i < SpilledBucketCount; ++i) {
            auto& bucket = SpilledBuckets_[i];
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
        if (SpillingBucketsCount_ > 0) {
            return true;
        }
        while (InMemoryBucketsCount_ > 0) {
            ui32 maxLineBucketInd = GetLargestInMemoryBucketNumber();
            MKQL_ENSURE(maxLineBucketInd != (ui32)-1, "Internal logic error");

            auto& bucketToSpill = SpilledBuckets_[maxLineBucketInd];
            SpillMoreStateFromBucket(bucketToSpill);
            if (bucketToSpill.BucketState == TSpilledBucket::EBucketState::SpillingState) {
                return true;
            }
        }
        return false;
    }

    EUpdateResult ProcessSpilledData() {
        if (AsyncReadOperation_) {
            if (!AsyncReadOperation_->HasValue()) {
                return EUpdateResult::Yield;
            }
            if (RecoverState_) {
                SpilledBuckets_[0].SpilledState->AsyncReadCompleted(AsyncReadOperation_->ExtractValue().value(), Ctx_.HolderFactory);
            } else {
                SpilledBuckets_[0].SpilledData->AsyncReadCompleted(AsyncReadOperation_->ExtractValue().value(), Ctx_.HolderFactory);
            }
            AsyncReadOperation_ = std::nullopt;
        }

        auto& bucket = SpilledBuckets_.front();
        if (bucket.BucketState == TSpilledBucket::EBucketState::InMemory) {
            return EUpdateResult::Extract;
        }

        // recover spilled state
        while (!bucket.SpilledState->Empty()) {
            RecoverState_ = true;
            TTemporaryUnboxedValueVector bufferForKeyAndState(KeyAndStateType_->GetElementsCount());
            AsyncReadOperation_ = bucket.SpilledState->ExtractWideItem(bufferForKeyAndState);
            if (AsyncReadOperation_) {
                return EUpdateResult::Yield;
            }
            for (size_t i = 0; i < KeyWidth_; ++i) {
                // jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(bucket.InMemoryProcessingState->Tongue[i]) = std::move(bufferForKeyAndState[i]);
            }
            auto isNew = bucket.InMemoryProcessingState->TasteIt();
            MKQL_ENSURE(isNew, "Internal logic error");
            for (size_t i = KeyWidth_; i < KeyAndStateType_->GetElementsCount(); ++i) {
                // jumping into unsafe world, refusing ownership
                static_cast<NUdf::TUnboxedValue&>(bucket.InMemoryProcessingState->Throat[i - KeyWidth_]) = std::move(bufferForKeyAndState[i]);
            }
        }
        // process spilled data
        if (!bucket.SpilledData->Empty()) {
            RecoverState_ = false;
            std::fill(BufferForUsedInputItems_.begin(), BufferForUsedInputItems_.end(), NUdf::TUnboxedValuePod());
            AsyncReadOperation_ = bucket.SpilledData->ExtractWideItem(BufferForUsedInputItems_);
            if (AsyncReadOperation_) {
                return EUpdateResult::Yield;
            }

            Throat = BufferForUsedInputItems_.data();
            Tongue = bucket.InMemoryProcessingState->Tongue;

            return EUpdateResult::ExtractRawData;
        }
        bucket.BucketState = TSpilledBucket::EBucketState::InMemory;
        return EUpdateResult::Extract;
    }

    EOperatingMode GetMode() const {
        return Mode_;
    }

    void SwitchMode(EOperatingMode mode) {
        switch (mode) {
            case EOperatingMode::InMemory: {
                UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, "switching Memory mode to InMemory");
                MKQL_ENSURE(false, "Internal logic error");
                break;
            }
            case EOperatingMode::SplittingState: {
                UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, "switching Memory mode to SplittingState");
                MKQL_ENSURE(EOperatingMode::InMemory == Mode_, "Internal logic error");
                SpilledBuckets_.resize(SpilledBucketCount);
                auto spiller = Ctx_.SpillerFactory->CreateSpiller();
                for (auto& b : SpilledBuckets_) {
                    b.SpilledState = std::make_unique<TWideUnboxedValuesSpillerAdapter>(spiller, KeyAndStateType_, 5_MB, Ctx_.RuntimeSettings.DatumValidation.Get());
                    b.SpilledData = std::make_unique<TWideUnboxedValuesSpillerAdapter>(spiller, UsedInputItemType_, 5_MB, Ctx_.RuntimeSettings.DatumValidation.Get());
                    b.InMemoryProcessingState = std::make_unique<TState>(MemInfo_, KeyWidth_,
                                                                         KeyAndStateType_->GetElementsCount() - KeyWidth_, Hasher_, Equal_, Logger_, LogComponent_, false);
                }
                break;
            }
            case EOperatingMode::Spilling: {
                UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, "switching Memory mode to Spilling");
                MKQL_ENSURE(EOperatingMode::SplittingState == Mode_ || EOperatingMode::InMemory == Mode_, "Internal logic error");

                Tongue = ViewForKeyAndState_.data();
                break;
            }
            case EOperatingMode::ProcessSpilled: {
                UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Info, "switching Memory mode to ProcessSpilled");
                MKQL_ENSURE(EOperatingMode::Spilling == Mode_, "Internal logic error");
                MKQL_ENSURE(SpilledBuckets_.size() == SpilledBucketCount, "Internal logic error");
                MKQL_ENSURE(BufferForUsedInputItems_.empty(), "Internal logic error");

                BufferForUsedInputItems_.resize(UsedInputItemType_->GetElementsCount());

                std::sort(SpilledBuckets_.begin(), SpilledBuckets_.end(), [](const TSpilledBucket& lhs, const TSpilledBucket& rhs) {
                    bool lhs_in_memory = lhs.BucketState == TSpilledBucket::EBucketState::InMemory;
                    bool rhs_in_memory = rhs.BucketState == TSpilledBucket::EBucketState::InMemory;
                    return lhs_in_memory > rhs_in_memory;
                });
                break;
            }
        }
        Mode_ = mode;
    }

    bool IsSwitchToSpillingModeCondition() const {
        return !HasMemoryForProcessing() || TlsAllocState->GetMaximumLimitValueReached();
    }

public:
    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Tongue = nullptr;
    NUdf::TUnboxedValuePod* Throat = nullptr;

private:
    bool StateWantsToSpill_ = false;
    bool IsEverythingExtracted_ = false;

    TState InMemoryProcessingState_;
    bool IsInMemoryProcessingStateSplitted_ = false;
    const TMultiType* const UsedInputItemType_;
    const TMultiType* const KeyAndStateType_;
    const size_t KeyWidth_;
    const size_t ItemNodesSize_;
    THashFunc const Hasher_;
    EOperatingMode Mode_;
    bool RecoverState_; // sub mode for ProcessSpilledData

    TAsyncReadOperation AsyncReadOperation_ = std::nullopt;
    static constexpr size_t SpilledBucketCount = 128;
    std::deque<TSpilledBucket> SpilledBuckets_;
    ui32 SpillingBucketsCount_ = 0;
    ui32 InMemoryBucketsCount_ = SpilledBucketCount;
    ui64 BufferForUsedInputItemsBucketId_;
    TUnboxedValueVector BufferForUsedInputItems_;
    std::vector<NUdf::TUnboxedValuePod, TMKQLAllocator<NUdf::TUnboxedValuePod>> ViewForKeyAndState_;
    i64 SplitStateSpillingBucket_ = -1;

    TMemoryUsageInfo* MemInfo_ = nullptr;
    TEqualsFunc const Equal_;
    const bool AllowSpilling_;

    TComputationContext& Ctx_;
    NYql::NUdf::TCounter CounterOutputRows_;

    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
};

#ifndef MKQL_DISABLE_CODEGEN
class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
    llvm::IntegerType* ValueType_;
    llvm::PointerType* PtrValueType_;
    llvm::IntegerType* StatusType_;
    llvm::IntegerType* StoredType_;
    llvm::IntegerType* BoolType_;

protected:
    using TBase::GetContext;

public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StatusType_);                    // status
        result.emplace_back(PtrValueType_);                  // tongue
        result.emplace_back(PtrValueType_);                  // throat
        result.emplace_back(StoredType_);                    // StoredDataSize
        result.emplace_back(BoolType_);                      // IsOutOfMemory
        result.emplace_back(Type::getInt32Ty(GetContext())); // size
        result.emplace_back(Type::getInt32Ty(GetContext())); // size
        return result;
    }

    llvm::Constant* GetStatus() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetTongue() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 1);
    }

    llvm::Constant* GetThroat() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 2);
    }

    llvm::Constant* GetStored() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 3);
    }

    llvm::Constant* GetIsOutOfMemory() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 4);
    }

    explicit TLLVMFieldsStructureState(llvm::LLVMContext& context)
        : TBase(context)
        , ValueType_(Type::getInt128Ty(context))
        , PtrValueType_(PointerType::getUnqual(ValueType_))
        , StatusType_(Type::getInt32Ty(context))
        , StoredType_(Type::getInt64Ty(context))
        , BoolType_(Type::getInt1Ty(context))
    {
    }
};
#endif

template <bool TrackRss, bool SkipYields>
class TWideCombinerWrapper: public TStatefulWideFlowCodegeneratorNode<TWideCombinerWrapper<TrackRss, SkipYields>>
#ifndef MKQL_DISABLE_CODEGEN
    ,
                            public ICodegeneratorRootNode
#endif
{
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideCombinerWrapper<TrackRss, SkipYields>>;

public:
    TWideCombinerWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TCombinerNodes&& nodes, TKeyTypes&& keyTypes, ui64 memLimit)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Nodes_(std::move(nodes))
        , KeyTypes_(std::move(keyTypes))
        , MemLimit_(memLimit)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Nodes_.ItemNodes.size()))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
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
                        if constexpr (SkipYields) {
                            break;
                        } else {
                            return EFetchResult::Yield;
                        }
                    case EFetchResult::Finish:
                        return EFetchResult::Finish;
                }

                const auto initUsage = MemLimit_ ? ctx.HolderFactory.GetMemoryUsed() : 0ULL;

                auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

                do {
                    for (auto i = 0U; i < Nodes_.ItemNodes.size(); ++i) {
                        if (Nodes_.ItemNodes[i]->GetDependentsCount() > 0U || Nodes_.PasstroughtItems[i]) {
                            fields[i] = &Nodes_.ItemNodes[i]->RefValue(ctx);
                        }
                    }

                    ptr->InputStatus = Flow_->FetchValues(ctx, fields);
                    if constexpr (SkipYields) {
                        if (EFetchResult::Yield == ptr->InputStatus) {
                            if (MemLimit_) {
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

                    Nodes_.ExtractKey(ctx, fields, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue));
                    Nodes_.ProcessItem(ctx, ptr->TasteIt() ? nullptr : static_cast<NUdf::TUnboxedValue*>(ptr->Tongue), static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                } while (!ctx.template CheckAdjustedMemLimit<TrackRss>(MemLimit_, initUsage - ptr->StoredDataSize) && !ptr->IsOutOfMemory);

                ptr->PushStat(ctx.Stats);
            }

            if (const auto values = static_cast<NUdf::TUnboxedValue*>(ptr->Extract())) {
                Nodes_.FinishItem(ctx, values, output);
                return EFetchResult::One;
            }
        }
        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TWideCombinerWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        BranchInst::Create(more, block);

        block = more;

        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 3U, "result", over);

        const auto readMore = EmitFunctionCall<&TState::ReadMore<SkipYields>>(Type::getInt1Ty(context), {stateArg}, ctx, block);

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

            const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStatus()}, "last", block);

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

            const auto used = GetMemoryUsed(MemLimit_, ctx, block);

            BranchInst::Create(loop, block);

            block = loop;

            const auto getres = GetNodeValues(Flow_, ctx, block);

            if constexpr (SkipYields) {
                const auto save = BasicBlock::Create(context, "save", ctx.Func);

                const auto way = SwitchInst::Create(getres.first, good, 2U, block);
                way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), save);
                way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), done);

                block = save;

                if (MemLimit_) {
                    const auto storedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStored()}, "stored_ptr", block);
                    const auto lastStored = new LoadInst(storedType, storedPtr, "last_stored", block);
                    const auto currentUsage = GetMemoryUsed(MemLimit_, ctx, block);

                    const auto skipSavingUsed = BasicBlock::Create(context, "skip_saving_used", ctx.Func);
                    const auto saveUsed = BasicBlock::Create(context, "save_used", ctx.Func);
                    const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, currentUsage, used, "check", block);
                    BranchInst::Create(saveUsed, skipSavingUsed, check, block);

                    block = saveUsed;

                    const auto usedMemory = BinaryOperator::CreateSub(GetMemoryUsed(MemLimit_, ctx, block), used, "used_memory", block);
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

            std::vector<Value*> items(Nodes_.ItemNodes.size(), nullptr);
            for (ui32 i = 0U; i < items.size(); ++i) {
                if (Nodes_.ItemNodes[i]->GetDependentsCount() > 0U) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.ItemNodes[i])->CreateSetValue(ctx, block, items[i] = getres.second[i](ctx, block));
                } else if (Nodes_.PasstroughtItems[i]) {
                    items[i] = getres.second[i](ctx, block);
                }
            }

            const auto tonguePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetTongue()}, "tongue_ptr", block);
            const auto tongue = new LoadInst(ptrValueType, tonguePtr, "tongue", block);

            std::vector<Value*> keyPointers(Nodes_.KeyResultNodes.size(), nullptr);
            std::vector<Value*> keys(Nodes_.KeyResultNodes.size(), nullptr);
            for (ui32 i = 0U; i < Nodes_.KeyResultNodes.size(); ++i) {
                auto& key = keys[i];
                const auto keyPtr = keyPointers[i] = GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("key_") += ToString(i)).c_str(), block);
                if (const auto map = Nodes_.KeysOnItems[i]) {
                    auto& it = items[*map];
                    if (!it) {
                        it = getres.second[*map](ctx, block);
                    }
                    key = it;
                } else {
                    key = GetNodeValue(Nodes_.KeyResultNodes[i], ctx, block);
                }

                if (Nodes_.KeyNodes[i]->GetDependentsCount() > 0U) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.KeyNodes[i])->CreateSetValue(ctx, block, key);
                }

                new StoreInst(key, keyPtr, block);
            }

            const auto newKey = EmitFunctionCall<&TState::TasteIt>(Type::getInt1Ty(context), {stateArg}, ctx, block);

            const auto init = BasicBlock::Create(context, "init", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto test = BasicBlock::Create(context, "test", ctx.Func);

            const auto throatPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetThroat()}, "throat_ptr", block);
            const auto throat = new LoadInst(ptrValueType, throatPtr, "throat", block);

            std::vector<Value*> pointers;
            pointers.reserve(Nodes_.StateNodes.size());
            for (ui32 i = 0U; i < Nodes_.StateNodes.size(); ++i) {
                pointers.emplace_back(GetElementPtrInst::CreateInBounds(valueType, throat, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("state_") += ToString(i)).c_str(), block));
            }

            BranchInst::Create(init, next, newKey, block);

            block = init;

            for (ui32 i = 0U; i < Nodes_.KeyResultNodes.size(); ++i) {
                ValueAddRef(Nodes_.KeyResultNodes[i]->GetRepresentation(), keyPointers[i], ctx, block);
            }

            for (ui32 i = 0U; i < Nodes_.InitResultNodes.size(); ++i) {
                if (const auto map = Nodes_.InitOnItems[i]) {
                    auto& it = items[*map];
                    if (!it) {
                        it = getres.second[*map](ctx, block);
                    }
                    new StoreInst(it, pointers[i], block);
                    ValueAddRef(Nodes_.InitResultNodes[i]->GetRepresentation(), it, ctx, block);
                } else if (const auto map = Nodes_.InitOnKeys[i]) {
                    const auto key = keys[*map];
                    new StoreInst(key, pointers[i], block);
                    ValueAddRef(Nodes_.InitResultNodes[i]->GetRepresentation(), key, ctx, block);
                } else {
                    GetNodeValue(pointers[i], Nodes_.InitResultNodes[i], ctx, block);
                }
            }

            BranchInst::Create(test, block);

            block = next;

            for (ui32 i = 0U; i < Nodes_.KeyResultNodes.size(); ++i) {
                if (Nodes_.KeysOnItems[i] || Nodes_.KeyResultNodes[i]->IsTemporaryValue()) {
                    ValueCleanup(Nodes_.KeyResultNodes[i]->GetRepresentation(), keyPointers[i], ctx, block);
                }
            }

            std::vector<Value*> stored(Nodes_.StateNodes.size(), nullptr);
            for (ui32 i = 0U; i < stored.size(); ++i) {
                const bool hasDependency = Nodes_.StateNodes[i]->GetDependentsCount() > 0U;
                if (const auto map = Nodes_.StateOnUpdate[i]) {
                    if (hasDependency || i != *map) {
                        stored[i] = new LoadInst(valueType, pointers[i], (TString("state_") += ToString(i)).c_str(), block);
                        if (hasDependency) {
                            EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.StateNodes[i])->CreateSetValue(ctx, block, stored[i]);
                        }
                    }
                } else if (hasDependency) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.StateNodes[i])->CreateSetValue(ctx, block, pointers[i]);
                } else {
                    ValueUnRef(Nodes_.StateNodes[i]->GetRepresentation(), pointers[i], ctx, block);
                }
            }

            for (ui32 i = 0U; i < Nodes_.UpdateResultNodes.size(); ++i) {
                if (const auto map = Nodes_.UpdateOnState[i]) {
                    if (const auto j = *map; i != j) {
                        auto& it = stored[j];
                        if (!it) {
                            it = new LoadInst(valueType, pointers[j], (TString("state_") += ToString(j)).c_str(), block);
                        }
                        new StoreInst(it, pointers[i], block);
                        if (i != *Nodes_.StateOnUpdate[j]) {
                            ValueAddRef(Nodes_.UpdateResultNodes[i]->GetRepresentation(), it, ctx, block);
                        }
                    }
                } else if (const auto map = Nodes_.UpdateOnItems[i]) {
                    auto& it = items[*map];
                    if (!it) {
                        it = getres.second[*map](ctx, block);
                    }
                    new StoreInst(it, pointers[i], block);
                    ValueAddRef(Nodes_.UpdateResultNodes[i]->GetRepresentation(), it, ctx, block);
                } else if (const auto map = Nodes_.UpdateOnKeys[i]) {
                    const auto key = keys[*map];
                    new StoreInst(key, pointers[i], block);
                    ValueAddRef(Nodes_.UpdateResultNodes[i]->GetRepresentation(), key, ctx, block);
                } else {
                    GetNodeValue(pointers[i], Nodes_.UpdateResultNodes[i], ctx, block);
                }
            }

            BranchInst::Create(test, block);

            block = test;

            auto totalUsed = used;
            if (MemLimit_) {
                const auto storedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStored()}, "stored_ptr", block);
                const auto lastStored = new LoadInst(storedType, storedPtr, "last_stored", block);
                totalUsed = BinaryOperator::CreateSub(used, lastStored, "decr", block);
            }

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit_, totalUsed, ctx, block);

            const auto isOutOfMemoryPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetIsOutOfMemory()}, "is_out_of_memory_ptr", block);
            const auto isOutOfMemory = new LoadInst(Type::getInt1Ty(context), isOutOfMemoryPtr, "is_out_of_memory", block);
            const auto checkIsOutOfMemory = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, isOutOfMemory, ConstantInt::getTrue(context), "check_is_out_of_memory", block);

            const auto any = BinaryOperator::CreateOr(check, checkIsOutOfMemory, "any", block);
            BranchInst::Create(done, loop, any, block);

            block = done;

            new StoreInst(getres.first, statusPtr, block);

            EmitFunctionCall<&TState::PushStat>(Type::getVoidTy(context), {stateArg, ctx.GetStat()}, ctx, block);

            BranchInst::Create(full, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto out = EmitFunctionCall<&TState::Extract>(ptrValueType, {stateArg}, ctx, block);
            const auto has = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(ptrValueType), "has", block);

            BranchInst::Create(good, more, has, block);

            block = good;

            for (ui32 i = 0U; i < Nodes_.FinishNodes.size(); ++i) {
                const auto ptr = GetElementPtrInst::CreateInBounds(valueType, out, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("out_key_") += ToString(i)).c_str(), block);
                if (Nodes_.FinishNodes[i]->GetDependentsCount() > 0 || Nodes_.ItemsOnResult[i]) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.FinishNodes[i])->CreateSetValue(ctx, block, ptr);
                } else {
                    ValueUnRef(Nodes_.FinishNodes[i]->GetRepresentation(), ptr, ctx, block);
                }
            }

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            BranchInst::Create(over, block);
        }

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(Nodes_.FinishResultNodes.size());
        std::transform(Nodes_.FinishResultNodes.cbegin(), Nodes_.FinishResultNodes.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
        NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("WideCombine");
        UDF_LOG(logger, logComponent, NUdf::ELogLevel::Debug, TStringBuilder() << "State initialized");

#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TState>(Nodes_.KeyNodes.size(), Nodes_.StateNodes.size(),
                                                 TMyValueHasher(KeyTypes_), TMyValueEqual(KeyTypes_), logger, logComponent);
#else
        state = ctx.HolderFactory.Create<TState>(Nodes_.KeyNodes.size(), Nodes_.StateNodes.size(),
                                                 ctx.ExecuteLLVM && Hash_ ? THashFunc(std::ptr_fun(Hash_)) : THashFunc(TMyValueHasher(KeyTypes_)),
                                                 ctx.ExecuteLLVM && Equals_ ? TEqualsFunc(std::ptr_fun(Equals_)) : TEqualsFunc(TMyValueEqual(KeyTypes_)),
                                                 logger, logComponent);
#endif
        if (ctx.CountersProvider) {
            const auto ptr = static_cast<TState*>(state.AsBoxed().Get());
            // id will be assigned externally in future versions
            TString id = TString(Operator_Aggregation) + "0";
            ptr->CounterOutputRows = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, /*deriv=*/false);
        }
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            Nodes_.RegisterDependencies(
                [this, flow](IComputationNode* node) { this->DependsOn(flow, node); },
                [this, flow](IComputationExternalNode* node) { this->Own(flow, node); });
        }
    }

    IComputationWideFlowNode* const Flow_;
    const TCombinerNodes Nodes_;
    const TKeyTypes KeyTypes_;
    const ui64 MemLimit_;

    const ui32 WideFieldsIndex_;

#ifndef MKQL_DISABLE_CODEGEN
    TEqualsPtr Equals_ = nullptr;
    THashPtr Hash_ = nullptr;

    Function* EqualsFunc_ = nullptr;
    Function* HashFunc_ = nullptr;

    template <bool EqualsOrHash>
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::" << (EqualsOrHash ? "Equals" : "Hash") << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (EqualsFunc_) {
            Equals_ = reinterpret_cast<TEqualsPtr>(codegen.GetPointerToFunction(EqualsFunc_));
        }
        if (HashFunc_) {
            Hash_ = reinterpret_cast<THashPtr>(codegen.GetPointerToFunction(HashFunc_));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(HashFunc_ = GenerateHashFunction(codegen, MakeName<false>(), KeyTypes_));
        codegen.ExportSymbol(EqualsFunc_ = GenerateEqualsFunction(codegen, MakeName<true>(), KeyTypes_));
    }
#endif
};

class TWideLastCombinerWrapper: public TStatefulWideFlowCodegeneratorNode<TWideLastCombinerWrapper>
#ifndef MKQL_DISABLE_CODEGEN
    ,
                                public ICodegeneratorRootNode
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
        , Flow_(flow)
        , Nodes_(std::move(nodes))
        , KeyTypes_(std::move(keyTypes))
        , UsedInputItemType_(usedInputItemType)
        , KeyAndStateType_(keyAndStateType)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Nodes_.ItemNodes.size()))
        , AllowSpilling_(allowSpilling)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        if (const auto ptr = static_cast<TSpillingSupportState*>(state.AsBoxed().Get())) {
            auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

            while (true) {
                switch (ptr->Update()) {
                    case TSpillingSupportState::EUpdateResult::ReadInput: {
                        for (auto i = 0U; i < Nodes_.ItemNodes.size(); ++i) {
                            fields[i] = Nodes_.GetUsedInputItemNodePtrOrNull(ctx, i);
                        }
                        switch (ptr->InputStatus = Flow_->FetchValues(ctx, fields)) {
                            case EFetchResult::One:
                                break;
                            case EFetchResult::Finish:
                                continue;
                            case EFetchResult::Yield:
                                return EFetchResult::Yield;
                        }
                        Nodes_.ExtractKey(ctx, fields, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue));
                        break;
                    }
                    case TSpillingSupportState::EUpdateResult::Yield:
                        return EFetchResult::Yield;
                    case TSpillingSupportState::EUpdateResult::ExtractRawData:
                        Nodes_.ExtractRawData(ctx, static_cast<NUdf::TUnboxedValue*>(ptr->Throat), static_cast<NUdf::TUnboxedValue*>(ptr->Tongue));
                        break;
                    case TSpillingSupportState::EUpdateResult::Extract:
                        if (const auto values = static_cast<NUdf::TUnboxedValue*>(ptr->Extract())) {
                            Nodes_.FinishItem(ctx, values, output);
                            return EFetchResult::One;
                        }
                        continue;
                    case TSpillingSupportState::EUpdateResult::Finish:
                        return EFetchResult::Finish;
                }

                switch (ptr->TasteIt()) {
                    case TSpillingSupportState::ETasteResult::Init:
                        Nodes_.ProcessItem(ctx, /*keys=*/nullptr, static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                        break;
                    case TSpillingSupportState::ETasteResult::Update:
                        Nodes_.ProcessItem(ctx, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue), static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                        break;
                    case TSpillingSupportState::ETasteResult::ConsumeRawData:
                        Nodes_.ConsumeRawData(ctx, static_cast<NUdf::TUnboxedValue*>(ptr->Tongue), fields, static_cast<NUdf::TUnboxedValue*>(ptr->Throat));
                        break;
                }
            }
        }
        MKQL_ENSURE(false, "Unreachable");
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
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

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TWideLastCombinerWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
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

        std::vector<PHINode*> phis(Nodes_.ItemNodes.size(), nullptr);
        auto j = 0U;
        std::generate(phis.begin(), phis.end(), [&]() {
            return Nodes_.IsInputItemNodeUsed(j++) ? PHINode::Create(valueType, 2U, (TString("item_") += ToString(j)).c_str(), test) : nullptr;
        });

        block = more;

        const auto update = EmitFunctionCall<&TSpillingSupportState::Update>(wayType, {stateArg}, ctx, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

        const auto updateWay = SwitchInst::Create(update, stub, 5U, block);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::Yield)), over);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::Extract)), fill);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::Finish)), done);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::ReadInput)), pull);
        updateWay->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::EUpdateResult::ExtractRawData)), load);

        block = load;

        const auto extractorPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetThroat()}, "extractor_ptr", block);
        const auto extractor = new LoadInst(ptrValueType, extractorPtr, "extractor", block);

        std::vector<Value*> items(phis.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            const auto ptr = GetElementPtrInst::CreateInBounds(valueType, extractor, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("load_ptr_") += ToString(i)).c_str(), block);
            if (phis[i]) {
                items[i] = new LoadInst(valueType, ptr, (TString("load_") += ToString(i)).c_str(), block);
            }
            if (i < Nodes_.ItemNodes.size() && Nodes_.ItemNodes[i]->GetDependentsCount() > 0U) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.ItemNodes[i])->CreateSetValue(ctx, block, items[i]);
            }
        }

        for (ui32 i = 0U; i < phis.size(); ++i) {
            if (const auto phi = phis[i]) {
                phi->addIncoming(items[i], block);
            }
        }

        BranchInst::Create(test, block);

        block = pull;

        const auto getres = GetNodeValues(Flow_, ctx, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

        const auto choise = SwitchInst::Create(getres.first, good, 2U, block);
        choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), over);
        choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), rest);

        block = rest;
        const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetStatus()}, "last", block);
        new StoreInst(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), statusPtr, block);
        BranchInst::Create(more, block);

        block = good;

        for (ui32 i = 0U; i < items.size(); ++i) {
            if (phis[i]) {
                items[i] = getres.second[i](ctx, block);
            }
            if (Nodes_.ItemNodes[i]->GetDependentsCount() > 0U) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.ItemNodes[i])->CreateSetValue(ctx, block, items[i]);
            }
        }

        for (ui32 i = 0U; i < phis.size(); ++i) {
            if (const auto phi = phis[i]) {
                phi->addIncoming(items[i], block);
            }
        }

        BranchInst::Create(test, block);

        block = test;

        const auto tonguePtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetTongue()}, "tongue_ptr", block);
        const auto tongue = new LoadInst(ptrValueType, tonguePtr, "tongue", block);

        std::vector<Value*> keyPointers(Nodes_.KeyResultNodes.size(), nullptr);
        std::vector<Value*> keys(Nodes_.KeyResultNodes.size(), nullptr);
        for (ui32 i = 0U; i < Nodes_.KeyResultNodes.size(); ++i) {
            auto& key = keys[i];
            const auto keyPtr = keyPointers[i] = GetElementPtrInst::CreateInBounds(valueType, tongue, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("key_") += ToString(i)).c_str(), block);
            if (const auto map = Nodes_.KeysOnItems[i]) {
                key = phis[*map];
            } else {
                key = GetNodeValue(Nodes_.KeyResultNodes[i], ctx, block);
            }

            if (Nodes_.KeyNodes[i]->GetDependentsCount() > 0U) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.KeyNodes[i])->CreateSetValue(ctx, block, key);
            }

            new StoreInst(key, keyPtr, block);
        }

        const auto taste = EmitFunctionCall<&TSpillingSupportState::TasteIt>(wayType, {stateArg}, ctx, block);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto save = BasicBlock::Create(context, "save", ctx.Func);

        const auto throatPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {stateFields.This(), stateFields.GetThroat()}, "throat_ptr", block);
        const auto throat = new LoadInst(ptrValueType, throatPtr, "throat", block);

        std::vector<Value*> pointers;
        const auto width = std::max(Nodes_.StateNodes.size(), phis.size());
        pointers.reserve(width);
        for (ui32 i = 0U; i < width; ++i) {
            pointers.emplace_back(GetElementPtrInst::CreateInBounds(valueType, throat, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("state_") += ToString(i)).c_str(), block));
        }

        const auto way = SwitchInst::Create(taste, stub, 3U, block);
        way->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::ETasteResult::Init)), init);
        way->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::ETasteResult::Update)), next);
        way->addCase(ConstantInt::get(wayType, static_cast<i8>(TSpillingSupportState::ETasteResult::ConsumeRawData)), save);

        block = init;

        for (ui32 i = 0U; i < Nodes_.KeyResultNodes.size(); ++i) {
            ValueAddRef(Nodes_.KeyResultNodes[i]->GetRepresentation(), keyPointers[i], ctx, block);
        }

        for (ui32 i = 0U; i < Nodes_.InitResultNodes.size(); ++i) {
            if (const auto map = Nodes_.InitOnItems[i]) {
                const auto item = phis[*map];
                new StoreInst(item, pointers[i], block);
                ValueAddRef(Nodes_.InitResultNodes[i]->GetRepresentation(), item, ctx, block);
            } else if (const auto map = Nodes_.InitOnKeys[i]) {
                const auto key = keys[*map];
                new StoreInst(key, pointers[i], block);
                ValueAddRef(Nodes_.InitResultNodes[i]->GetRepresentation(), key, ctx, block);
            } else {
                GetNodeValue(pointers[i], Nodes_.InitResultNodes[i], ctx, block);
            }
        }

        BranchInst::Create(more, block);

        block = next;

        std::vector<Value*> stored(Nodes_.StateNodes.size(), nullptr);
        for (ui32 i = 0U; i < stored.size(); ++i) {
            const bool hasDependency = Nodes_.StateNodes[i]->GetDependentsCount() > 0U;
            if (const auto map = Nodes_.StateOnUpdate[i]) {
                if (hasDependency || i != *map) {
                    stored[i] = new LoadInst(valueType, pointers[i], (TString("state_") += ToString(i)).c_str(), block);
                    if (hasDependency) {
                        EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.StateNodes[i])->CreateSetValue(ctx, block, stored[i]);
                    }
                }
            } else if (hasDependency) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.StateNodes[i])->CreateSetValue(ctx, block, pointers[i]);
            } else {
                ValueUnRef(Nodes_.StateNodes[i]->GetRepresentation(), pointers[i], ctx, block);
            }
        }

        for (ui32 i = 0U; i < Nodes_.UpdateResultNodes.size(); ++i) {
            if (const auto map = Nodes_.UpdateOnState[i]) {
                if (const auto j = *map; i != j) {
                    const auto it = stored[j];
                    new StoreInst(it, pointers[i], block);
                    if (i != *Nodes_.StateOnUpdate[j]) {
                        ValueAddRef(Nodes_.UpdateResultNodes[i]->GetRepresentation(), it, ctx, block);
                    }
                }
            } else if (const auto map = Nodes_.UpdateOnItems[i]) {
                const auto item = phis[*map];
                new StoreInst(item, pointers[i], block);
                ValueAddRef(Nodes_.UpdateResultNodes[i]->GetRepresentation(), item, ctx, block);
            } else if (const auto map = Nodes_.UpdateOnKeys[i]) {
                const auto key = keys[*map];
                new StoreInst(key, pointers[i], block);
                ValueAddRef(Nodes_.UpdateResultNodes[i]->GetRepresentation(), key, ctx, block);
            } else {
                GetNodeValue(pointers[i], Nodes_.UpdateResultNodes[i], ctx, block);
            }
        }

        BranchInst::Create(more, block);

        block = save;

        for (ui32 i = 0U; i < phis.size(); ++i) {
            if (const auto item = phis[i]) {
                new StoreInst(item, pointers[i], block);
                ValueAddRef(Nodes_.ItemNodes[i]->GetRepresentation(), item, ctx, block);
            }
        }

        BranchInst::Create(more, block);

        block = fill;

        const auto out = EmitFunctionCall<&TSpillingSupportState::Extract>(ptrValueType, {stateArg}, ctx, block);
        const auto has = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(ptrValueType), "has", block);

        BranchInst::Create(data, more, has, block);

        block = data;

        for (ui32 i = 0U; i < Nodes_.FinishNodes.size(); ++i) {
            const auto ptr = GetElementPtrInst::CreateInBounds(valueType, out, {ConstantInt::get(Type::getInt32Ty(context), i)}, (TString("out_key_") += ToString(i)).c_str(), block);
            if (Nodes_.FinishNodes[i]->GetDependentsCount() > 0 || Nodes_.ItemsOnResult[i]) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Nodes_.FinishNodes[i])->CreateSetValue(ctx, block, ptr);
            } else {
                ValueUnRef(Nodes_.FinishNodes[i]->GetRepresentation(), ptr, ctx, block);
            }
        }

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(over, block);

        block = done;

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);
        BranchInst::Create(over, block);

        block = over;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(Nodes_.FinishResultNodes.size());
        std::transform(Nodes_.FinishResultNodes.cbegin(), Nodes_.FinishResultNodes.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
        NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("WideLastCombine");
        UDF_LOG(logger, logComponent, NUdf::ELogLevel::Debug, TStringBuilder() << "State initialized");

        state = ctx.HolderFactory.Create<TSpillingSupportState>(UsedInputItemType_, KeyAndStateType_,
                                                                Nodes_.KeyNodes.size(),
                                                                Nodes_.ItemNodes.size(),
#ifdef MKQL_DISABLE_CODEGEN
                                                                TMyValueHasher(KeyTypes_),
                                                                TMyValueEqual(KeyTypes_),
#else
                                                                ctx.ExecuteLLVM && Hash_ ? THashFunc(std::ptr_fun(Hash_)) : THashFunc(TMyValueHasher(KeyTypes_)),
                                                                ctx.ExecuteLLVM && Equals_ ? TEqualsFunc(std::ptr_fun(Equals_)) : TEqualsFunc(TMyValueEqual(KeyTypes_)),
#endif
                                                                AllowSpilling_,
                                                                ctx,
                                                                logger,
                                                                logComponent);
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            Nodes_.RegisterDependencies(
                [this, flow](IComputationNode* node) { this->DependsOn(flow, node); },
                [this, flow](IComputationExternalNode* node) { this->Own(flow, node); });
        }
    }

    IComputationWideFlowNode* const Flow_;
    const TCombinerNodes Nodes_;
    const TKeyTypes KeyTypes_;

    const TMultiType* const UsedInputItemType_;
    const TMultiType* const KeyAndStateType_;

    const ui32 WideFieldsIndex_;

    const bool AllowSpilling_;
#ifndef MKQL_DISABLE_CODEGEN
    TEqualsPtr Equals_ = nullptr;
    THashPtr Hash_ = nullptr;

    Function* EqualsFunc_ = nullptr;
    Function* HashFunc_ = nullptr;

    template <bool EqualsOrHash>
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::" << (EqualsOrHash ? "Equals" : "Hash") << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (EqualsFunc_) {
            Equals_ = reinterpret_cast<TEqualsPtr>(codegen.GetPointerToFunction(EqualsFunc_));
        }
        if (HashFunc_) {
            Hash_ = reinterpret_cast<THashPtr>(codegen.GetPointerToFunction(HashFunc_));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(HashFunc_ = GenerateHashFunction(codegen, MakeName<false>(), KeyTypes_));
        codegen.ExportSymbol(EqualsFunc_ = GenerateEqualsFunction(codegen, MakeName<true>(), KeyTypes_));
    }
#endif
};

} // namespace

template <bool Last>
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
        TType* type = callable.GetInput(i).GetStaticType();
        keyAndStateItemTypes.push_back(type);
        bool optional;
        keyTypes.emplace_back(*UnpackOptionalData(callable.GetInput(i).GetStaticType(), optional)->GetDataSlot(), optional);
    }

    TCombinerNodes nodes;
    nodes.KeyResultNodes.reserve(keysSize);
    std::generate_n(std::back_inserter(nodes.KeyResultNodes), keysSize, [&]() { return LocateNode(ctx.NodeLocator, callable, index++); });

    index += keysSize;
    nodes.InitResultNodes.reserve(stateSize);
    for (size_t i = 0; i != stateSize; ++i) {
        TType* type = callable.GetInput(index).GetStaticType();
        keyAndStateItemTypes.push_back(type);
        nodes.InitResultNodes.push_back(LocateNode(ctx.NodeLocator, callable, index++));
    }

    index += stateSize;
    nodes.UpdateResultNodes.reserve(stateSize);
    std::generate_n(std::back_inserter(nodes.UpdateResultNodes), stateSize, [&]() { return LocateNode(ctx.NodeLocator, callable, index++); });

    index += keysSize + stateSize;
    nodes.FinishResultNodes.reserve(outputWidth);
    std::generate_n(std::back_inserter(nodes.FinishResultNodes), outputWidth, [&]() { return LocateNode(ctx.NodeLocator, callable, index++); });

    index = Last ? 3U : 4U;

    nodes.ItemNodes.reserve(inputWidth);
    std::generate_n(std::back_inserter(nodes.ItemNodes), inputWidth, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, index++); });

    index += keysSize;
    nodes.KeyNodes.reserve(keysSize);
    std::generate_n(std::back_inserter(nodes.KeyNodes), keysSize, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, index++); });

    index += stateSize;
    nodes.StateNodes.reserve(stateSize);
    std::generate_n(std::back_inserter(nodes.StateNodes), stateSize, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, index++); });

    index += stateSize;
    nodes.FinishNodes.reserve(keysSize + stateSize);
    std::generate_n(std::back_inserter(nodes.FinishNodes), keysSize + stateSize, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, index++); });

    nodes.BuildMaps();
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        if constexpr (Last) {
            const auto inputItemTypes = GetWideComponents(inputType);
            return new TWideLastCombinerWrapper(ctx.Mutables, wide, std::move(nodes),
                                                TMultiType::Create(inputItemTypes.size(), inputItemTypes.data(), ctx.Env),
                                                std::move(keyTypes),
                                                TMultiType::Create(keyAndStateItemTypes.size(), keyAndStateItemTypes.data(), ctx.Env),
                                                allowSpilling);
        } else {
            if (const auto memLimit = AS_VALUE(TDataLiteral, callable.GetInput(1U))->AsValue().Get<i64>(); memLimit >= 0) {
                if (EGraphPerProcess::Single == ctx.GraphPerProcess) {
                    return new TWideCombinerWrapper<true, false>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(memLimit));
                } else {
                    return new TWideCombinerWrapper<false, false>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(memLimit));
                }
            } else if (EGraphPerProcess::Single == ctx.GraphPerProcess) {
                return new TWideCombinerWrapper<true, true>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(-memLimit));
            } else {
                return new TWideCombinerWrapper<false, true>(ctx.Mutables, wide, std::move(nodes), std::move(keyTypes), ui64(-memLimit));
            }
        }
    }

    THROW yexception() << "Expected wide flow.";
}

IComputationNode* WrapWideCombiner(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideCombinerT<false>(callable, ctx, /*allowSpilling=*/false);
}

IComputationNode* WrapWideLastCombiner(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideCombinerT<true>(callable, ctx, /*allowSpilling=*/false);
}

IComputationNode* WrapWideLastCombinerWithSpilling(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideCombinerT<true>(callable, ctx, /*allowSpilling=*/true);
}

} // namespace NKikimr::NMiniKQL
