#include "dq_hash_combine.h"
#include "dq_hash_operator_common.h"
#include "dq_hash_operator_serdes.h"
#include "type_utils.h"

#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>
#include <yql/essentials/minikql/comp_nodes/mkql_counters.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_spiller_adapter.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>

#include <util/system/backtrace.h>

#include <util/system/mutex.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr {
namespace NMiniKQL {

using NUdf::TUnboxedValue;
using NUdf::TUnboxedValuePod;

namespace {

bool HasMemoryForProcessing() {
    return !TlsAllocState->IsMemoryYellowZoneEnabled();
}

bool SpillingTime() {
    return !HasMemoryForProcessing() || TlsAllocState->GetMaximumLimitValueReached();
}

[[maybe_unused]] void DebugPrintUV(TUnboxedValuePod& uv) {
    Cerr << "----- UV at " << (size_t)(&uv) << Endl;
    Cerr << "Refcount: " << uv.RefCount() << Endl;
    Cerr << "IsString: " << uv.IsString() << "; IsEmbedded: " << uv.IsEmbedded() << Endl;
    if (uv.IsString()) {
        Cerr << "Raw string ptr: " << (size_t)uv.AsRawStringValue()->Data() << Endl;
    }
    uv.Dump(Cerr);
    Cerr << Endl;
}

size_t CalcMaxBlockLenForOutput(std::vector<TType*> wideComponents) {
    size_t maxBlockItemSize = 0;
    for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
        maxBlockItemSize = std::max(maxBlockItemSize, CalcMaxBlockItemSize(wideComponents[i]));
    }

    return CalcBlockLen(maxBlockItemSize);
}

using TEqualsPtr = bool(*)(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*);
using THashPtr = NUdf::THashType(*)(const NUdf::TUnboxedValuePod*);

using TEqualsFunc = std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>;
using THashFunc = std::function<NUdf::THashType(const NUdf::TUnboxedValuePod*)>;

struct TSegmentedArena
{
    // TODO: Account for MKQL-specific headers
    // TODO: Must be variable, Max(PageSize, AllocSize)
    // TODO: Build a proper class
    static constexpr const size_t PageSize = 64_KB - 64;

    struct TPageEntry {
        TPageEntry* Prev;
        void* Page;
        ui32 Tag;
        ui32 Used;
    };

    TPagedArena Storage;

    using TPageList = std::deque<TPageEntry>;
    TPageList Pages;
    std::vector<TPageEntry*> PagesByTag;

    size_t AllocSize = 0;
    size_t UsedMem = 0;
    ui32 PageCapacity = 0;
    TPageList::iterator LastUsedPage;
    bool NeedNewPages = true;

    struct TIterator {
        bool Valid = false;
        TPageList::iterator Page;
        TPageList::iterator PageEnd;
        ui32 Index;
        ui32 AllocSize;

        void* Next() {
            if (!Valid) {
                [[unlikely]] return nullptr;
            }
            while (Page != PageEnd) {
                if (Index >= Page->Used) {
                    ++Page;
                    Index = 0;
                    continue;
                }
                return static_cast<char*>(Page->Page) + (AllocSize * Index++);
            }
            Valid = false;
            return nullptr;
        }
    };

    TSegmentedArena()
        : Storage(TlsAllocState)
        , LastUsedPage(Pages.end())
    {
    }

    void* Alloc(const ui32 tag) {
        YQL_ENSURE(AllocSize > 0, "Allocation size must be specified via Format(...)");

        auto& prevPtr = PagesByTag.at(tag);

        TPageEntry* pagePtr = nullptr;
        if (prevPtr == nullptr || prevPtr->Used >= PageCapacity) {
            if (LastUsedPage == Pages.end()) {
                auto rawPtr = Storage.Alloc(PageSize, EMemorySubPool::Temporary);
                pagePtr = &Pages.emplace_back(TPageEntry{
                    .Prev = prevPtr,
                    .Page = rawPtr,
                    .Tag = tag,
                    .Used = 0,
                });
                LastUsedPage = Pages.end();
            } else {
                pagePtr = &*LastUsedPage;
                ++LastUsedPage;
                pagePtr->Prev = prevPtr;
                pagePtr->Tag = tag;
                pagePtr->Used = 0;
            }
            NeedNewPages = (LastUsedPage == Pages.end() && PageCapacity == 1);
            PagesByTag[tag] = pagePtr;
        } else {
            pagePtr = prevPtr;
            NeedNewPages = (LastUsedPage == Pages.end() && (PageCapacity - 1) == pagePtr->Used);
        }

        UsedMem += AllocSize;
        return static_cast<void*>(static_cast<char*>(pagePtr->Page) + (AllocSize * pagePtr->Used++));
    }

    void CancelAlloc(const ui32 tag) {
        auto& prevPtr = PagesByTag.at(tag);
        YQL_ENSURE(prevPtr != nullptr && prevPtr->Used, "CancelAlloc doesn't match Alloc");
        --prevPtr->Used;
        UsedMem -= AllocSize;
    }

    TIterator Iterator() {
        return TIterator {
            .Valid = (Pages.begin() != LastUsedPage),
            .Page = Pages.begin(),
            .PageEnd = LastUsedPage,
            .Index = 0u,
            .AllocSize = static_cast<ui32>(AllocSize)
        };
    }

    void Format(const ui32 numTags, const size_t allocSize) {
        MKQL_ENSURE(allocSize <= PageSize, "PageSize must be adjusted to be >= allocSize");
        AllocSize = allocSize;
        PageCapacity = PageSize / AllocSize;
        PagesByTag.clear();
        PagesByTag.resize(numTags, nullptr);
        LastUsedPage = Pages.begin();
        NeedNewPages = Pages.empty();
        UsedMem = 0;
    }

    void Clear() {
        AllocSize = 0;
        Pages.clear();
        LastUsedPage = Pages.end();
        PagesByTag.clear();
        Storage.Clear();
    }

    size_t GetUsedMem()
    {
        return UsedMem;
    }
};

std::optional<size_t> EstimateUvPackSize(const TArrayRef<const TUnboxedValuePod> items, const TArrayRef<TType* const> types) {
    constexpr const size_t uvSize = sizeof(TUnboxedValuePod);

    size_t sizeSum = 0;

    auto currType = types.begin();
    for (const auto& item : items) {
        if (!item.HasValue() || item.IsEmbedded() || item.IsInvalid()) {
            sizeSum += uvSize;
        } else if (item.IsString()) {
            sizeSum += uvSize + item.AsStringRef().Size();
        } else if (!item.IsBoxed()) {
            return {};
        } else {
            auto ty = *currType;
            while (ty->IsOptional()) {
                ty = AS_TYPE(TOptionalType, ty)->GetItemType();
            }
            if (ty->IsTuple()) {
                auto tupleType = AS_TYPE(TTupleType, ty);
                auto elements = tupleType->GetElements();
                auto tupleSize = EstimateUvPackSize(TArrayRef(item.GetElements(), elements.size()), elements);
                if (!tupleSize.has_value()) {
                    return {};
                }
                // Tuple contents are generally boxed into a TDirectArrayHolderInplace instance
                sizeSum += uvSize + sizeof(TDirectArrayHolderInplace) + tupleSize.value();
            } else {
                return {};
            }
        }
        ++currType;
    }

    return sizeSum;
}

// Calculate static memory size bounds from TType*s
class TMemoryEstimationHelper
{
private:
    static std::optional<size_t> GetUVSizeBound(TType* type) {
        if (type->IsData()) {
            using NYql::NUdf::EDataSlot;

            bool optional = false;
            auto dataSlot = UnpackOptionalData(type, optional)->GetDataSlot();

            if (dataSlot.Empty()) {
                return {};
            }

            switch (dataSlot.GetRef()) {
            case EDataSlot::DyNumber:
            case EDataSlot::Json:
            case EDataSlot::JsonDocument:
            case EDataSlot::Yson:
            case EDataSlot::Utf8:
            case EDataSlot::String:
                return {};
            default:
                return sizeof(TUnboxedValuePod);
            }
        } else if (type->IsTuple()) {
            size_t result = 0;
            const auto tupleElements = AS_TYPE(TTupleType, type)->GetElements();
            for (auto* element : tupleElements) {
                auto sz = GetUVSizeBound(element);
                if (!sz.has_value()) {
                    return {};
                }
                result += sz.value();
            }
            return result + sizeof(TUnboxedValuePod);
        } else {
            return {};
        }
    }

    static std::optional<size_t> GetMultiUVSizeBound(std::vector<TType*>& types) {
        size_t result = 0;
        for (auto type : types) {
            auto stateItemSize = GetUVSizeBound(type);
            if (!stateItemSize.has_value()) {
                return {};
            } else {
                result += stateItemSize.value();
            }
        }
        return result;
    }

public:
    std::optional<size_t> StateSizeBound;
    std::optional<size_t> KeySizeBound;
    const size_t KeyWidth;
    const std::vector<TType*> KeyItemTypes;

    TMemoryEstimationHelper(std::vector<TType*> keyItemTypes, std::vector<TType*> stateItemTypes)
        : KeyWidth(keyItemTypes.size())
        , KeyItemTypes(keyItemTypes)
    {
        KeySizeBound = GetMultiUVSizeBound(keyItemTypes);
        StateSizeBound = GetMultiUVSizeBound(stateItemTypes);
    }

    std::optional<size_t> EstimateKeySize(const TUnboxedValuePod* keyPack) const
    {
        return EstimateUvPackSize(
            TArrayRef<const TUnboxedValuePod>(keyPack, KeyWidth),
            TArrayRef<TType* const>(KeyItemTypes.begin(), KeyItemTypes.end()));
    }
};

} // anonymous namespace

class IAggregation
{
public:
    virtual size_t GetStateSize() const = 0; // in bytes
    virtual std::optional<size_t> GetStateMemoryUsage(void* rawState) const = 0; // best estimate, in bytes, or empty if impossible to estimate
    virtual void InitState(void* rawState, TUnboxedValue* const* row) = 0;
    virtual void UpdateState(void* rawState, TUnboxedValue* const* row) = 0;
    virtual void ExtractState(void* rawState, TUnboxedValue* const* output) = 0;
    virtual void ForgetState(void* rawState) = 0;

    virtual ~IAggregation() {
    }
};

/*
Wrapper for a WideCombiner-style lambda-based aggregation.
State is an array of TUnboxedValue[Nodes.StateNodes.size()]
*/
class TGenericAggregation: public IAggregation
{
private:
    TComputationContext& Ctx;
    const NDqHashOperatorCommon::TCombinerNodes& Nodes;
    size_t StateWidth;
    size_t StateSize;
    const std::vector<TType*>& StateItemTypes;

public:
    TGenericAggregation(
        TComputationContext& ctx,
        const NDqHashOperatorCommon::TCombinerNodes& nodes,
        const std::vector<TType*>& stateItemTypes
    )
        : Ctx(ctx)
        , Nodes(nodes)
        , StateWidth(Nodes.StateNodes.size())
        , StateSize(StateWidth * sizeof(TUnboxedValue))
        , StateItemTypes(stateItemTypes)
    {
    }

    size_t GetStateSize() const override {
        return StateSize;
    }

    std::optional<size_t> GetStateMemoryUsage(void* rawState) const override final {
        return EstimateUvPackSize(
            TArrayRef<const TUnboxedValuePod>(static_cast<const TUnboxedValuePod*>(rawState), StateWidth),
            TArrayRef<TType* const>(StateItemTypes)
        );
    }

    // Assumes the input row and extracted keys have already been copied into the input nodes, so row isn't even used here
    void UpdateState(void* rawState, TUnboxedValue* const* /*row*/) override final {
        TUnboxedValue* state = static_cast<TUnboxedValue*>(rawState);
        TUnboxedValue* stateIter = state;

        std::for_each(Nodes.StateNodes.cbegin(), Nodes.StateNodes.cend(),
            [&](IComputationExternalNode* item){ item->SetValue(Ctx, std::move(*stateIter++)); });

        stateIter = state;
        std::transform(Nodes.UpdateResultNodes.cbegin(), Nodes.UpdateResultNodes.cend(), stateIter,
            [&](IComputationNode* node) { return node->GetValue(Ctx); });
    }

    // Assumes the input row has already been copied into the input nodes, so row isn't even used here
    void InitState(void* rawState, TUnboxedValue* const* /*row*/) override final {
        TUnboxedValuePod* state = static_cast<TUnboxedValuePod*>(rawState);
        for (size_t i = 0; i < StateWidth; ++i) {
            state[i] = TUnboxedValuePod();
        }
        std::transform(
            Nodes.InitResultNodes.cbegin(),
            Nodes.InitResultNodes.cend(),
            static_cast<TUnboxedValue*>(state),
            [&](IComputationNode* node) { return node->GetValue(Ctx);});
    }

    // Assumes the key part of the Finish lambda input has been initialized
    void ExtractState(void* rawState, TUnboxedValue* const* output) override {
        TUnboxedValue* state = static_cast<TUnboxedValue*>(rawState);
        TUnboxedValue* stateIter = state;

        std::for_each(Nodes.FinishStateNodes.cbegin(), Nodes.FinishStateNodes.cend(),
            [&](IComputationExternalNode* item){ item->SetValue(Ctx, std::move(*stateIter++)); });

        TUnboxedValue* const* outputIter = output;

        for (const auto& node : Nodes.FinishResultNodes) {
            *(*outputIter++) = node->GetValue(Ctx);
        }

        ForgetState(rawState);
    }

    void ForgetState(void* rawState) override {
        TUnboxedValue* state = static_cast<TUnboxedValue*>(rawState);
        for (size_t i = 0; i < StateWidth; ++i) {
            *state++ = TUnboxedValue(); // TODO: or maybe just Unref?
        }
    }
};

enum class EFillState
{
    Yield,
    ContinueFilling,
    Drain,
    SourceEmpty,
    SourceSkipped
};

namespace {

EFillState FetchFromStream(TUnboxedValue& inputStream, TUnboxedValueVector& inputBuffer)
{
    const auto result = inputStream.WideFetch(inputBuffer.data(), inputBuffer.size());
    EFillState sourceState;
    switch (result) {
    case NYql::NUdf::EFetchStatus::Ok:
        sourceState = EFillState::ContinueFilling;
        break;
    case NYql::NUdf::EFetchStatus::Finish:
        sourceState = EFillState::SourceEmpty;
        break;
    case NYql::NUdf::EFetchStatus::Yield:
        sourceState = EFillState::Yield;
        break;
    default:
        ythrow yexception() << "Unknown stream fetch result: " << (int)result;
    }
    return sourceState;
}

}

constexpr const size_t DefaultMemoryLimit = 128ull << 20; // if the runtime limit is zero
constexpr const float ExtraMapCapacity = 2.0; // hashmap size is target row count increased by this factor then adjusted up to a power of 2
constexpr const float MaxCompressionRatio = 32.0;
constexpr const size_t MemorySampleRowCount = 16384ULL; // sample size for row weight estimation, in rows
constexpr const size_t LowerFixedRowCount = 1024ULL; // minimum viable hash table size, rows
constexpr const size_t UpperFixedRowCount = 128 * 1024ULL; // maximum hash table size, rows (fixed constant for now)
constexpr const size_t BucketBits = 7;
constexpr const size_t NumBuckets = 1ULL << BucketBits;
constexpr const size_t SpillingIoBuffer = 5_MB;
constexpr const size_t StorageArenaMinSize = 64_MB;

class TBaseAggregationState: public TComputationValue<TBaseAggregationState>
{
protected:
    using TMap = TRobinHoodHashSet<NUdf::TUnboxedValuePod*, TEqualsFunc, THashFunc, TMKQLAllocator<char, EMemorySubPool::Temporary>>;

    static size_t GetStaticMaxRowCount(size_t entryPayloadSizeBytes, size_t memoryLimit) {
        size_t memoryPerRow = entryPayloadSizeBytes + static_cast<size_t>(TMap::GetCellSize() * ExtraMapCapacity);
        if (memoryPerRow >= memoryLimit) {
            return 1;
        }
        size_t dynamicResult = memoryLimit / memoryPerRow;
        if (dynamicResult > UpperFixedRowCount) {
            dynamicResult = UpperFixedRowCount;
        }
        return dynamicResult;
    }

    static size_t GetMapCapacity(size_t rowCount) {
        // TODO: Switch to CalculateRHHashTableCapacity; it tends to overshoot for smaller tables but not too much
        auto preciseCapacity = static_cast<size_t>(rowCount * ExtraMapCapacity);
        auto pow2Capacity = FastClp2(preciseCapacity);
        return pow2Capacity;
    }

    void LoadItem(TUnboxedValue* const* input)
    {
        for (auto i = 0U; i < Nodes.ItemNodes.size(); ++i) {
            // TODO: precalc unused nodes; this is too expensive to do for every row
            // if (Nodes.ItemNodes[i]->GetDependencesCount() > 0U || Nodes.PasstroughtItems[i]) {
            Nodes.ItemNodes[i]->RefValue(Ctx) = *input[i];
            // }
        }
    }

    void ExtractPassthroughKey(TUnboxedValuePod* keyBuffer, TUnboxedValue* const* input)
    {
        auto keys = keyBuffer;
        for (ui32 i = 0U; i < Nodes.KeyNodes.size(); ++i) {
            auto& keyField = Nodes.KeyNodes[i]->RefValue(Ctx);
            *keys = keyField = *input[PassthroughKeysSourceItems[i]];
            keys++;
        }
    }

    void ExtractKey(TUnboxedValuePod* keyBuffer)
    {
        auto keys = keyBuffer;
        for (ui32 i = 0U; i < Nodes.KeyNodes.size(); ++i) {
            auto& keyField = Nodes.KeyNodes[i]->RefValue(Ctx);
            keyField = Nodes.KeyResultNodes[i]->GetValue(Ctx);
            *keys = keyField;
            keys->Ref();
            keys++;
        }
    }

    struct TPerBucketSpillage {
        std::unique_ptr<TWideUnboxedValuesSpillerAdapter> SpilledState;
        std::unique_ptr<TWideUnboxedValuesSpillerAdapter> SpilledInput;
    };

    struct TTaskSpillage {
        std::vector<TPerBucketSpillage> Spillage;
        ui32 StateWidth = 0;
        ui32 NumBuckets = 0;
        ui32 CurrentBucket = 0;
        bool CurrentBucketRead = false;
    };

    std::deque<TTaskSpillage> SpillingStack;

    ISpiller::TPtr Spiller;

    void InitiateSpilling()
    {
        if (!Spiller) {
            Spiller = Ctx.SpillerFactory->CreateSpiller();
        }

        MKQL_ENSURE(SpillingStack.empty(), "Spilling buckets should not have been initialized yet");
        SpillingStack.emplace_back();
        TTaskSpillage& currentSpilling = SpillingStack.back();
        currentSpilling.Spillage.resize(NumBuckets);

        const ui32 keysAndStatesWidth = KeysAndStatesType->GetElementsCount();
        currentSpilling.StateWidth = keysAndStatesWidth;

        for (size_t i = 0; i < NumBuckets; ++i) {
            currentSpilling.Spillage[i].SpilledState = std::make_unique<TWideUnboxedValuesSpillerAdapter>(Spiller, KeysAndStatesType, SpillingIoBuffer);
            currentSpilling.Spillage[i].SpilledInput = std::make_unique<TWideUnboxedValuesSpillerAdapter>(Spiller, InputUnpackedItemsType, SpillingIoBuffer);
        }

        [[maybe_unused]] size_t totalWritten = 0;
        [[maybe_unused]] size_t totalFlushed = 0;

        for (size_t i = 0; i < NumBuckets; ++i) {
            TWideUnboxedValuesSpillerAdapter& spiller = *currentSpilling.Spillage[i].SpilledState;
            TSegmentedArena::TPageEntry* page = Store->PagesByTag[i];
            while (page != nullptr) {
                TUnboxedValuePod* item = static_cast<TUnboxedValuePod*>(page->Page);
                for (ui32 writtenFromPage = 0; writtenFromPage < page->Used; ++writtenFromPage, item += keysAndStatesWidth) {
                    TArrayRef<TUnboxedValuePod> pageItems(item, keysAndStatesWidth);
                    ++totalWritten;
                    auto pageFuture = spiller.WriteWideItem(pageItems);
                    for (auto& uv : pageItems) {
                        uv.UnRef();
                    }
                    if (!pageFuture.has_value()) {
                        continue;
                    }
                    spiller.AsyncWriteCompleted(pageFuture->ExtractValue());
                    ++totalFlushed;
                }
                page = page->Prev;
            }
            auto finishFuture = spiller.FinishWriting();
            if (finishFuture.has_value()) {
                ++totalFlushed;
                spiller.AsyncWriteCompleted(finishFuture->ExtractValue());
            }
        }

        Map->Clear();
        Store->Format(NumBuckets, sizeof(TUnboxedValuePod) * InputUnpackedWidth);
    }

    void FlushSpillingInput()
    {
        if (SpillingStack.empty()) {
            return;
        }

        MKQL_ENSURE(!!Spiller, "Spiller must have been created");

        [[maybe_unused]] size_t totalWritten = 0;
        [[maybe_unused]] size_t totalFlushed = 0;

        TTaskSpillage& currentSpill = SpillingStack.back();
        for (size_t i = 0; i < NumBuckets; ++i) {
            TWideUnboxedValuesSpillerAdapter& spiller = *currentSpill.Spillage[i].SpilledInput;
            TSegmentedArena::TPageEntry* page = Store->PagesByTag[i];
            while (page != nullptr) {
                TUnboxedValuePod* item = static_cast<TUnboxedValuePod*>(page->Page);
                for (ui32 writtenFromPage = 0; writtenFromPage < page->Used; ++writtenFromPage, item += InputUnpackedWidth) {
                    TArrayRef<TUnboxedValuePod> pageItems(item, InputUnpackedWidth);
                    ++totalWritten;
                    auto pageFuture = spiller.WriteWideItem(pageItems);
                    for (auto& uv : pageItems) {
                        uv.UnRef();
                    }
                    if (!pageFuture.has_value()) {
                        continue;
                    }
                    ++totalFlushed;
                    spiller.AsyncWriteCompleted(pageFuture->ExtractValue());
                }
                page = page->Prev;
            }
            auto finishFuture = spiller.FinishWriting();
            if (finishFuture.has_value()) {
                ++totalFlushed;
                spiller.AsyncWriteCompleted(finishFuture->ExtractValue());
            }
        }

        Store->Format(NumBuckets, sizeof(TUnboxedValuePod) * InputUnpackedWidth);
    }

    bool HasPendindSpillingBuckets()
    {
        if (SpillingStack.empty()) {
            return false;
        }
        return SpillingStack.back().CurrentBucket < NumBuckets;
    }

    void ReadBackNextSpillingBucket()
    {
        // Run aggregation on a single bucket

        // TODO: maybe reallocate?
        if (Map->GetSize() > 0) {
            Map->Clear();
        }

        TTaskSpillage& currentSpill = SpillingStack.back();

        const ui32 bucket = currentSpill.CurrentBucket;
        MKQL_ENSURE(bucket < NumBuckets, "Trying to read past the last spilling bucket");
        Store->Format(1, sizeof(TUnboxedValuePod) * KeysAndStatesWidth);

        CachedHash = 0;

        {
            // Read the state
            TWideUnboxedValuesSpillerAdapter& stateSpiller = *currentSpill.Spillage[bucket].SpilledState;
            [[maybe_unused]] size_t readStateItems = 0;

            while (!stateSpiller.Empty()) {
                TUnboxedValuePod* keyAndStateBuf = static_cast<TUnboxedValuePod*>(Store->Alloc(0));
                for (size_t i = 0; i < KeysAndStatesWidth; ++i) {
                    keyAndStateBuf[i] = TUnboxedValuePod();
                }
                TArrayRef<TUnboxedValue> keyAndStateArr(static_cast<TUnboxedValue*>(keyAndStateBuf), KeysAndStatesWidth);

                auto readFuture = stateSpiller.ExtractWideItem(keyAndStateArr);
                if (readFuture.has_value()) [[unlikely]] {
                    auto stuff = readFuture->ExtractValueSync();
                    MKQL_ENSURE(stuff.has_value(), "A spilled blob is missing while reading back the aggregation state");
                    stateSpiller.AsyncReadCompleted(std::move(stuff.value()), Ctx.HolderFactory);
                    Store->CancelAlloc(0);
                    continue;
                }
                ++readStateItems;
                // TODO: Checkpoint: ensure RefCounts are == 1
                bool isNew = false;
                Map->Insert(keyAndStateBuf, isNew);

                MKQL_ENSURE(isNew, "Every key in the spilled state must be unique");
            }
        }

        {
            // Read the saved input
            TWideUnboxedValuesSpillerAdapter& inputSpiller = *currentSpill.Spillage[bucket].SpilledInput;
            std::vector<TUnboxedValue> input;
            input.resize(InputUnpackedWidth);
            TArrayRef<TUnboxedValue> inputArr(input);
            std::vector<TUnboxedValue*> inputPtrs;
            for (auto& uv : input) {
                inputPtrs.push_back(&uv);
            }

            [[maybe_unused]] size_t readInputItems = 0;
            while (!inputSpiller.Empty()) {
                auto readFuture = inputSpiller.ExtractWideItem(inputArr);
                if (readFuture.has_value()) {
                    auto stuff = readFuture->ExtractValueSync();
                    MKQL_ENSURE(stuff.has_value(), "A spilled blob is missing while reading back spilled input rows");
                    inputSpiller.AsyncReadCompleted(std::move(stuff.value()), Ctx.HolderFactory);
                    continue;
                }
                ++readInputItems;

                if (HasGenericAggregation) {
                    LoadItem(inputPtrs.data());
                    ExtractKey(TempKeyBuffer.data());
                } else {
                    MKQL_ENSURE(false, "Not implemented yet");
                }

                // TODO: Checkpoint: ensure RefCounts are == 1

                // TODO: extract into a method (this is mostly a copy from ProcessFetchedRow)
                TUnboxedValuePod* keyBuffer = nullptr;
                bool isNew = false;
                char* mapIt = Map->Insert(TempKeyBuffer.data(), isNew);
                char* statePtr = nullptr;

                if (isNew) {
                    // Copy the value to the specified arena page
                    keyBuffer = static_cast<TUnboxedValuePod*>(Store->Alloc(0));
                    std::copy(TempKeyBuffer.begin(), TempKeyBuffer.end(), keyBuffer);
                    *(static_cast<TUnboxedValuePod**>(Map->GetKeyPtr(mapIt))) = keyBuffer;
                } else {
                    keyBuffer = Map->GetKeyValue(mapIt);
                }
                statePtr = reinterpret_cast<char *>(keyBuffer) + StatesOffset;

                for (auto& agg : Aggs) {
                    if (isNew) {
                        agg->InitState(statePtr, inputPtrs.data());
                    } else {
                        agg->UpdateState(statePtr, inputPtrs.data());
                    }
                    statePtr += agg->GetStateSize();
                }

                if (!isNew) {
                    auto keys = TempKeyBuffer.data();
                    for (ui32 i = 0U; i < TempKeyBuffer.size(); ++i) {
                        keys->UnRef();
                        keys++;
                    }
                }

                if (isNew) {
                    CheckAutoGrowMap(true);
                    if (Map->GetSize() > MaxRowCount) {
                        throw TMemoryLimitExceededException();
                    }
                }
            }
        }

        ++currentSpill.CurrentBucket;
    }

    void CheckAutoGrowMap(const bool hasMemoryForProcessing)
    {
        if (MapAutoGrowEnabled && !MapAutoGrowLimitReached && Map->GetSize() >= MaxRowCount) {
            if (hasMemoryForProcessing) {
                try {
                    Map->CheckGrow();
                    MaxRowCount = Map->GetCapacity() / 2;
                    return;
                }
                catch(const TMemoryLimitExceededException& e) {
                }
            }

            MapAutoGrowLimitReached = true;
            // Slow, but still better than spilling or crashing
            MaxRowCount = Map->GetCapacity() / 1.3;
        }
    }

    EFillState ProcessFetchedRow(TUnboxedValue* const* input) {
        TUnboxedValuePod* const tempKey = TempKeyBuffer.data();
        const ui32 tempKeySize = TempKeyBuffer.size();

        if (HasGenericAggregation) {
            LoadItem(input);
            if (PassthroughKeys) {
                ExtractPassthroughKey(tempKey, input);
            } else {
                ExtractKey(tempKey);
            }
        } else {
            MKQL_ENSURE(false, "Not implemented yet");
        }

        ui64 bucketId = 0;
        if (EnableSpilling) {
            CachedHash = 0;
            ui64 hash = Hasher(tempKey);
            bucketId = (hash * 11400714819323198485llu) & ((1ull << BucketBits) - 1);
            CachedHash = hash;
        }

        if (!SpillingStack.empty()) {
            auto rowBuffer = static_cast<TUnboxedValuePod*>(Store->Alloc(bucketId));
            for (size_t i = 0; i < InputUnpackedWidth; ++i) {
                rowBuffer[i] = *input[i];
                rowBuffer[i].Ref();
            }
            if (!PassthroughKeys) {
                auto k = tempKey;
                for (ui32 i = 0U; i < tempKeySize; ++i) {
                    k->UnRef();
                }
                k++;
            }
            // TODO: GetUsedItems actually. Maybe with an estimation of the actual memory size adjusted for non-embedded values.
            if (!HasMemoryForProcessing() && Store->GetUsedMem() > StorageArenaMinSize) {
                FlushSpillingInput();
            }
            return EFillState::ContinueFilling;
        }

        TUnboxedValuePod* keyBuffer = nullptr;
        bool isNew = false;
        auto mapIt = Map->Insert(tempKey, isNew);
        char* statePtr = nullptr;
        if (isNew) {
            // Copy the value to the specified arena page
            keyBuffer = static_cast<TUnboxedValuePod*>(Store->Alloc(bucketId));
            memcpy(keyBuffer, tempKey, tempKeySize * sizeof(TUnboxedValuePod));
            // std::copy(TempKeyBuffer.begin(), TempKeyBuffer.end(), keyBuffer);
            *static_cast<TUnboxedValuePod**>(Map->GetKeyPtr(mapIt)) = keyBuffer;
        } else {
            keyBuffer = Map->GetKeyValue(mapIt);
        }
        statePtr = reinterpret_cast<char *>(keyBuffer) + StatesOffset;

        CachedHash = 0;

        // TODO: loop over Aggs, but for now we always have one and only GenericAggregation
        if (isNew) {
            GenericAggregation->InitState(statePtr, input);
        } else {
            GenericAggregation->UpdateState(statePtr, input);
        }

        if (!PassthroughKeys && !isNew) {
            auto keys = tempKey;
            for (ui32 i = 0U; i < tempKeySize; ++i) {
                keys->UnRef();
                keys++;
            }
        } else if (PassthroughKeys && isNew) {
            auto keys = tempKey;
            for (ui32 i = 0U; i < tempKeySize; ++i) {
                keys->Ref();
                keys++;
            }
        }

        auto canFitMoreKeys = [&]() -> bool {
            if (isNew) {
                const bool hasMemoryForProcessing = HasMemoryForProcessing();
                CheckAutoGrowMap(hasMemoryForProcessing);
                if (Map->GetSize() >= MaxRowCount) {
                    return false;
                }
                if (IsAggregation && !EnableSpilling) {
                    // There is still space in the hashmap but we have no choice but to raise an OOM if/when the memory runs out.
                    // So we don't check for yellow zone in this case.
                    return true;
                }
                if (!hasMemoryForProcessing && Map->GetSize() >= LowerFixedRowCount) {
                    return false;
                }
            }
            return true;
        };

        if (!canFitMoreKeys()) {
            if (!IsAggregation) {
                OpenDrain();
                return EFillState::Drain;
            } else if (EnableSpilling) {
                InitiateSpilling();
                return EFillState::ContinueFilling;
            } else {
                throw TMemoryLimitExceededException();
            }
        }

        if (IsAggregation && EnableSpilling && SpillingTime()) {
            // The SpillingTime() limit is presumably lower than the yellow zone
            // so it can trigger separately, earlier than !HasMemoryForProcessing()
            InitiateSpilling();
        }

        return EFillState::ContinueFilling;
    }

public:
    using TBase = TComputationValue<TBaseAggregationState>;

    std::vector<ui32> PassthroughKeysSourceItems;
    bool PassthroughKeys = false;

    TBaseAggregationState(
        TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TMemoryEstimationHelper& memoryHelper, size_t memoryLimit, size_t inputUnpackedWidth,
        const NDqHashOperatorCommon::TCombinerNodes& nodes, ui32 wideFieldsIndex, const TKeyTypes& keyTypes,
        const std::vector<TType*>& keyItemTypes,
        const std::vector<TType*>& stateItemTypes,
        const bool forLLVM,
        const bool isAggregator,
        const bool enableSpilling
    )
        : TBase(memInfo)
        , Ctx(ctx)
        , MemoryHelper(memoryHelper)
        , MemoryLimit(memoryLimit)
        , ForLLVM(forLLVM)
        , IsAggregation(isAggregator)
        , EnableSpilling(enableSpilling && ctx.SpillerFactory)
        , InputUnpackedWidth(inputUnpackedWidth)
        , Nodes(nodes)
        , WideFieldsIndex(wideFieldsIndex)
        , KeyTypes(keyTypes)
        , Hasher(IsAggregation ? THashFunc(TWideUnboxedHasherWithExternalValue(KeyTypes, CachedHash)) : THashFunc(TWideUnboxedHasher(KeyTypes)))
        , Equals(TWideUnboxedEqual(KeyTypes))
        , Draining(false)
        , SourceEmpty(false)
    {
        TempKeyBuffer.resize(KeyTypes.size());

        if (!IsAggregation) {
            IsEstimating = !(MemoryHelper.KeySizeBound && MemoryHelper.StateSizeBound);
            if (IsEstimating) {
                MaxRowCount = MemorySampleRowCount;
            } else {
                MaxRowCount = GetStaticMaxRowCount(memoryHelper.KeySizeBound.value() + memoryHelper.StateSizeBound.value(), MemoryLimit);
            }
        } else {
            MaxRowCount = 64ULL * 1024;
            MapAutoGrowEnabled = true;
        }

        MaxRowCount = TryAllocMapForRowCount(MaxRowCount);

        std::vector<TType*> keyAndStateTypesVec = keyItemTypes;

        if (HasGenericAggregation) {
            auto genericAgg = std::make_unique<TGenericAggregation>(Ctx, Nodes, stateItemTypes);
            GenericAggregation = genericAgg.get();
            Aggs.emplace_back(genericAgg.release());
            keyAndStateTypesVec.insert(keyAndStateTypesVec.end(), stateItemTypes.begin(), stateItemTypes.end());
        }

        KeysAndStatesType = TMultiType::Create(keyAndStateTypesVec.size(), keyAndStateTypesVec.data(), ctx.TypeEnv);
        KeysAndStatesWidth = keyAndStateTypesVec.size();

        MKQL_ENSURE(Aggs.size(), "No aggregations defined");
        size_t allAggsSize = 0;
        for (const auto& agg : Aggs) {
            allAggsSize += agg->GetStateSize();
        }
        StatesOffset = sizeof(TUnboxedValuePod) * KeyTypes.size();
        KeyAndStatesByteSize = StatesOffset + allAggsSize;
        Store = std::make_unique<TStore>();

        PrepareForNewBatch();

        if (IsAggregation) {
            std::vector<ui32> keySourceItems;
            for (const auto& node : Nodes.KeyResultNodes) {

                ui32 itemIndex = 0;
                for (const auto& keyNode : Nodes.ItemNodes) {
                    if (keyNode == node) {
                        keySourceItems.push_back(itemIndex);
                        break;
                    }
                    ++itemIndex;
                }
            }
            if (keySourceItems.size() == Nodes.KeyResultNodes.size()) {
                PassthroughKeys = true;
                PassthroughKeysSourceItems = keySourceItems;
            }
        }
    }

    bool IsDraining() {
        // Update isDrainingMethodAddr in the LLVM IR if virtualized
        return Draining;
    }

    bool IsSourceEmpty() {
        // Update isSourceEmptyMethodAddr in the LLVM IR if virtualized
        return SourceEmpty;
    }

    virtual ~TBaseAggregationState() {
        if (ForLLVM) {
            // LLVM code doesn't ref inputs so we need to just forget the contents of the input buffer without unref-ing
            for (TUnboxedValue& val : InputBuffer) {
                static_cast<TUnboxedValuePod&>(val) = TUnboxedValuePod{};
            }
        }
        ReleaseAggregationsFromArena();
        CleanupCurrentContext();
    }

    virtual bool TryDrain(TUnboxedValue* const* output) = 0;
    virtual TUnboxedValue* const* GetInputBuffer() = 0;
    virtual TUnboxedValueVector& GetDenseInputBuffer() = 0;
    virtual EFillState ProcessInput(EFillState sourceState) = 0;

protected:
    size_t TryAllocMapForRowCount(size_t rowCount)
    {
        // Avoid reallocating the map
        // TODO: although Clear()-ing might be actually more expensive than reallocation
        if (Map) {
            const size_t oldCapacity = Map->GetCapacity();
            size_t newCapacity = GetMapCapacity(rowCount);
            if (newCapacity <= oldCapacity) {
                Map->Clear();
                return rowCount;
            }
            Map.Reset(nullptr);
        }

        auto tryAlloc = [this](size_t rows) -> bool {
            size_t newCapacity = GetMapCapacity(rows);
            try {
                Map.Reset(new TMap(Hasher, Equals, newCapacity));
                if (!HasMemoryForProcessing()) {
                    Map.Reset(nullptr);
                    return false;
                }
                return true;
            }
            catch (TMemoryLimitExceededException) {
            }
            return false;
        };

        while (rowCount > LowerFixedRowCount) {
            if (tryAlloc(rowCount)) {
                return rowCount;
            }
            rowCount = rowCount / 2;
        }

        // This can emit uncaught TMemoryLimitExceededException if we can't afford even a tiny map
        size_t smallCapacity = GetMapCapacity(LowerFixedRowCount);
        Map.Reset(new TMap(Hasher, Equals, smallCapacity));
        return LowerFixedRowCount;
    }

    void UpdateRowLimitFromSample()
    {
        // If we have achieved a "good" compression ratio (defined by a constant) then we probably don't need to resize the map further
        if (!Map->GetSize() || static_cast<double>(MaxRowCount) / Map->GetSize() >= MaxCompressionRatio) {
            return;
        }

        // TODO: also check if the input stream is incompressible; we need to derive a statistical criterion for that

        size_t totalMem = 0;
        bool unbounded = false;

        for (auto mapIter = Map->Begin(); mapIter != Map->End() && !unbounded; Map->Advance(mapIter)) {
            if (!Map->IsValid(mapIter)) {
                continue;
            }
            auto* entry = Map->GetKeyValue(mapIter);
            auto entryMem = MemoryHelper.EstimateKeySize(entry);
            if (!entryMem.has_value()) {
                unbounded = true;
                break;
            }
            totalMem += entryMem.value();
            char* statePtr = static_cast<char *>(static_cast<void *>(entry)) + StatesOffset;
            for (const auto& agg : Aggs) {
                auto stateSize = agg->GetStateMemoryUsage(statePtr);
                if (!stateSize.has_value()) {
                    unbounded = true;
                    break;
                }
                totalMem += stateSize.value();
                statePtr += agg->GetStateSize();
            }
        }

        if (unbounded || totalMem == 0) {
            // Use a small fixed-size map if we could not estimate memory usage for some of the key/state columns
            MaxRowCount = LowerFixedRowCount;
        } else {
            MaxRowCount = GetStaticMaxRowCount(totalMem / Map->GetSize(), MemoryLimit);
        }
    }

    void PrepareForNewBatch()
    {
        if (Map->GetSize() != 0) {
            if (IsEstimating && !SourceEmpty) {
                IsEstimating = false;
                MaxRowCount = TryAllocMapForRowCount(MaxRowCount);
            } else {
                Map->Clear();
            }
        }
        Store->Clear();
        Store->Format(EnableSpilling ? NumBuckets : 1, sizeof(TUnboxedValuePod) * KeysAndStatesWidth);
    }

    void OpenDrain() {
        if (!SourceEmpty && IsEstimating && Map->GetSize() > 0) {
            UpdateRowLimitFromSample();
        }
        FlushSpillingInput();
        Draining = true;
        if (!IsAggregation || SpillingStack.empty()) {
            DrainArenaIterator = Store->Iterator();
        }
    }


    bool CheckRefillFromPendingBuckets()
    {
        if (DrainArenaIterator.Valid) {
            return true;
        } else if (SpillingStack.empty()) {
            return false;
        }

        while (HasPendindSpillingBuckets()) {
            ReadBackNextSpillingBucket();
            DrainArenaIterator = Store->Iterator();
            return true;
        }

        return false;
    }

    void ReleaseAggregationsFromArena()
    {
        if (Map && Map->GetSize() > 0) {
            // Either not yet spilling or already draining
            const ui32 keyWidth = KeyTypes.size();
            if (!Draining) {
                DrainArenaIterator = Store->Iterator();
            }
            while (void* tuple = DrainArenaIterator.Next()) {
                char* statePtr = static_cast<char*>(tuple) + StatesOffset;
                for (auto& agg : Aggs) {
                    agg->ForgetState(statePtr);
                    statePtr += agg->GetStateSize();
                }
                TUnboxedValue* key = static_cast<TUnboxedValue*>(tuple);
                for (ui32 i = 0; i < keyWidth; ++i, ++key) {
                    key->UnRef();
                }
            }
        } else if (!SpillingStack.empty()) {
            // Release input tuples not yet flushed to disk
            DrainArenaIterator = Store->Iterator();
            while (void* tuple = DrainArenaIterator.Next()) {
                TUnboxedValue* uv = static_cast<TUnboxedValue*>(tuple);
                for (size_t i = 0; i < InputUnpackedWidth; ++i, ++uv) {
                    uv->UnRef();
                }
            }
        }

        if (Map) {
            Map = nullptr;
        }
        Store->Clear();
    }

    TComputationContext& Ctx;

    const TMemoryEstimationHelper& MemoryHelper;

    size_t MemoryLimit;
    const bool ForLLVM;
    const bool IsAggregation;
    const bool EnableSpilling;

    bool IsEstimating = false;
    size_t EstimateBatchSize = 0;
    size_t MaxRowCount = 0;
    size_t InitialMapCapacity = 0;
    bool MapAutoGrowEnabled = false;
    bool MapAutoGrowLimitReached = false;

    size_t InputUnpackedWidth;
    const NDqHashOperatorCommon::TCombinerNodes& Nodes;
    const ui32 WideFieldsIndex;
    std::vector<std::unique_ptr<IAggregation>> Aggs;
    TGenericAggregation* GenericAggregation = nullptr;
    const TKeyTypes& KeyTypes;
    ui64 CachedHash = 0;
    TMultiType* InputUnpackedItemsType;
    ui32 KeysAndStatesWidth;
    TMultiType* KeysAndStatesType;
    THashFunc const Hasher;
    TEqualsFunc const Equals;
    constexpr static const bool HasGenericAggregation = true;
    size_t KeyAndStatesByteSize = 0;

    using TStore = TSegmentedArena;
    std::unique_ptr<TStore> Store;
    TSegmentedArena::TIterator DrainArenaIterator;
    THolder<TMap> Map;
    std::vector<TUnboxedValuePod> TempKeyBuffer;
    TUnboxedValueVector InputBuffer;
    size_t StatesOffset;
    bool Draining;
    bool SourceEmpty;
};

class TWideAggregationState: public TBaseAggregationState
{
public:
    TWideAggregationState(
        TMemoryUsageInfo* memInfo,
        TComputationContext& ctx,
        const TMemoryEstimationHelper& memoryHelper,
        NYql::NUdf::TCounter& outputRowCounter,
        size_t memoryLimit,
        size_t inputWidth,
        size_t outputWidth,
        const NDqHashOperatorCommon::TCombinerNodes& nodes,
        ui32 wideFieldsIndex,
        const TKeyTypes& keyTypes,
        const std::vector<TType*>& inputItemTypes,
        const std::vector<TType*>& keyItemTypes,
        const std::vector<TType*>& stateItemTypes,
        const bool forLLVM,
        const bool isAggregator,
        const bool enableSpilling
    )
        : TBaseAggregationState(
            memInfo, ctx, memoryHelper, memoryLimit, inputWidth, nodes, wideFieldsIndex, keyTypes,
            keyItemTypes, stateItemTypes, forLLVM, isAggregator, enableSpilling
        )
        , OutputRowCounter(outputRowCounter)
        , StartMoment(TInstant::Now()) // Temporary. Helps correlate debug outputs with SVGs
        , OutputWidth(outputWidth)
    {
        InputBuffer.resize(inputWidth, TUnboxedValuePod());
        std::transform(InputBuffer.begin(), InputBuffer.end(), Ctx.WideFields.data() + WideFieldsIndex, [&](TUnboxedValue& val) {
            return &val;
        });

        OutputBuffer.resize(outputWidth, TUnboxedValuePod());
        OutputPtrs.resize(outputWidth, nullptr);
        std::transform(OutputBuffer.begin(), OutputBuffer.end(), OutputPtrs.begin(), [&](TUnboxedValue& val) {
            return &val;
        });

        InputUnpackedItemsType = TMultiType::Create(inputItemTypes.size(), inputItemTypes.data(), ctx.TypeEnv);
    }

    TUnboxedValue* const* GetInputBuffer() override {
        return Ctx.WideFields.data() + WideFieldsIndex;
    }

    TUnboxedValueVector& GetDenseInputBuffer() override {
        return InputBuffer;
    }

    TUnboxedValue* GetDenseInputBufferDirect() {
        return InputBuffer.data();
    }

    TUnboxedValue* GetDenseOutputBufferDirect() {
        auto result = OutputBuffer.data();
        return result;
    }

    EFillState ProcessInput(EFillState sourceState) override {
        return ProcessInputDirect(sourceState);
    }

    EFillState ProcessInputDirect(EFillState sourceState) {
        if (sourceState == EFillState::Yield) {
            return sourceState;
        } else if (sourceState == EFillState::SourceEmpty) {
            SourceEmpty = true;
            OpenDrain();
            return EFillState::SourceEmpty;
        }

        ++InputRows;
        return ProcessFetchedRow(Ctx.WideFields.data() + WideFieldsIndex);
    }

    bool TryDrain(NUdf::TUnboxedValue* const* outputPtrs) override {
        return TryDrainInternal(outputPtrs);
    }

    // Drain from the internal buffer
    bool TryDrainDirect() {
        return TryDrainInternal(OutputPtrs.data());
    }

    bool TryDrainInternal(NUdf::TUnboxedValue* const* outputPtrs) {
        void* tuple = DrainArenaIterator.Next();
        if (!tuple) {
            while (CheckRefillFromPendingBuckets()) {
                tuple = DrainArenaIterator.Next();
                if (tuple) {
                    break;
                }
            }
        }

        if (!tuple) {
            if (!IsAggregation) {
                PrepareForNewBatch();
            } else {
                Map.Reset();
                Store->Clear();
            }
            Draining = false;
            return false;
        }

        const auto key = static_cast<TUnboxedValuePod*>(tuple);

        if (HasGenericAggregation) {
            auto keyIter = key;
            for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                auto& keyField = Nodes.FinishKeyNodes[i]->RefValue(Ctx);
                keyField = *keyIter++;
            }
        }

        char* statePtr = static_cast<char *>(tuple) + StatesOffset;
        /*
        for (auto& agg : Aggs) {
            agg->ExtractState(statePtr, outputPtrs);
            statePtr += agg->GetStateSize();
        }
        */
        GenericAggregation->ExtractState(statePtr, outputPtrs);

        OutputRowCounter.Inc();

        if (HasGenericAggregation) {
            auto keyIter = key;
            for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                (keyIter++)->UnRef();
            }
        }

        return true;
    }

private:
    size_t InputRows = 0;
    NYql::NUdf::TCounter OutputRowCounter;
    TInstant StartMoment;
    [[maybe_unused]] size_t OutputWidth;
    TUnboxedValueVector OutputBuffer;
    TVector<TUnboxedValue*> OutputPtrs;
};

class TBlockAggregationState: public TBaseAggregationState
{
private:
    bool OpenBlock() {
        const auto batchLength = TArrowBlock::From(InputBuffer.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        if (!batchLength) {
            CurrentInputBatchSize = 0;
            CurrentInputBatchPtr = 0;
            return false;
        }

        CurrentInputBatchSize = batchLength;
        CurrentInputBatchPtr = 0;

        return true;
    }

public:
    TBlockAggregationState(
        TMemoryUsageInfo* memInfo,
        TComputationContext& ctx,
        const TMemoryEstimationHelper& memoryHelper,
        NYql::NUdf::TCounter& outputRowCounter,
        size_t memoryLimit,
        const std::vector<TType*>& inputTypes,
        const std::vector<TType*>& outputTypes,
        const NDqHashOperatorCommon::TCombinerNodes& nodes,
        ui32 wideFieldsIndex,
        const TKeyTypes& keyTypes,
        const std::vector<TType*>& keyItemTypes,
        const std::vector<TType*>& stateItemTypes,
        const size_t maxOutputBlockLen,
        const bool forLLVM,
        const bool isAggregator,
        const bool enableSpilling
    )
        : TBaseAggregationState(
            memInfo, ctx, memoryHelper, memoryLimit, inputTypes.size() - 1, nodes, wideFieldsIndex,
            keyTypes, keyItemTypes, stateItemTypes, forLLVM, isAggregator, enableSpilling
        )
        , OutputRowCounter(outputRowCounter)
        , InputTypes(inputTypes)
        , OutputTypes(outputTypes)
        , InputColumns(inputTypes.size() - 1)
        , OutputColumns(outputTypes.size() - 1)
        , MaxOutputBlockLen(maxOutputBlockLen)
    {
        InputBuffer.resize(InputColumns + 1, TUnboxedValuePod());
        std::transform(InputBuffer.begin(), InputBuffer.end(), Ctx.WideFields.data() + WideFieldsIndex, [&](TUnboxedValue& val) {
            return &val;
        });

        RowBuffer.resize(InputColumns, TUnboxedValuePod());
        std::transform(RowBuffer.begin(), RowBuffer.end(), std::back_inserter(RowBufferPointers), [&](TUnboxedValue& val) {
            return &val;
        });

        OutputBuffer.resize(OutputColumns, TUnboxedValuePod());
        std::transform(OutputBuffer.begin(), OutputBuffer.end(), std::back_inserter(OutputBufferPointers), [&](TUnboxedValue& val) {
            return &val;
        });

        DrainBuffer.resize(OutputColumns + 1, TUnboxedValuePod());
        std::transform(DrainBuffer.begin(), DrainBuffer.end(), std::back_inserter(DrainBufferPointers), [&](TUnboxedValue& val) {
            return &val;
        });

        const auto& pgBuilder = ctx.Builder->GetPgBuilder();
        TTypeInfoHelper typeInfoHelper;

        // TODO: don't really need the i/o converter for the last input/output column
        for (auto type : InputTypes) {
            InputReaders.push_back(MakeBlockReader(typeInfoHelper, type));
            InputItemConverters.push_back(MakeBlockItemConverter(typeInfoHelper, type, pgBuilder));
        }

        for (auto type : OutputTypes) {
            OutputItemConverters.push_back(MakeBlockItemConverter(typeInfoHelper, type, pgBuilder));
        }

        InputUnpackedItemsType = TMultiType::Create(InputTypes.size() - 1, InputTypes.data(), ctx.TypeEnv);
    }

    TUnboxedValue* const* GetInputBuffer() override {
        if (CurrentInputBatchPtr < CurrentInputBatchSize) {
            return nullptr;
        }
        return Ctx.WideFields.data() + WideFieldsIndex;
    }

    TUnboxedValueVector& GetDenseInputBuffer() override {
        if (CurrentInputBatchPtr < CurrentInputBatchSize) {
            return EmptyUVs;
        }
        return InputBuffer;
    }

    TUnboxedValue* GetDenseInputBufferDirect() {
        if (CurrentInputBatchPtr < CurrentInputBatchSize) {
            return nullptr;
        }
        return InputBuffer.data();
    }

    TUnboxedValue* GetDenseOutputBufferDirect() {
        return DrainBuffer.data();
    }

    EFillState ProcessInput(EFillState fetchResult) override {
        return ProcessInputDirect(fetchResult);
    }

    EFillState ProcessInputDirect(EFillState fetchResult) {
        if (fetchResult != EFillState::SourceSkipped) {
            if (fetchResult == EFillState::Yield) {
                return fetchResult;
            } else if (fetchResult == EFillState::SourceEmpty) {
                SourceEmpty = true;
                OpenDrain();
                return fetchResult;
            }

            if (!OpenBlock()) {
                return EFillState::ContinueFilling;
            }
        }

        MKQL_ENSURE(!Draining && !SourceEmpty, "Can't fill while draining or when the source is exhausted");

        for (size_t i = 0; i < InputColumns; ++i) {
            const auto& datum = TArrowBlock::From(InputBuffer[i]).GetDatum();
            NYql::NUdf::TBlockItem blockItem;
            if (datum.is_scalar()) {
                const auto& scalar = datum.scalar();
                MKQL_ENSURE(!!scalar, "Scalar value must not be empty");
                blockItem = InputReaders[i]->GetScalarItem(*scalar);
            } else {
                const auto& array = datum.array();
                MKQL_ENSURE(!!array, "Array value must not be empty");
                blockItem = InputReaders[i]->GetItem(*array, CurrentInputBatchPtr);
            }
            RowBuffer[i] = InputItemConverters[i]->MakeValue(blockItem, Ctx.HolderFactory);
            //RowBuffer[i].Ref(); -- might be needed if the RowBuffer is switched to a vector of TUnboxedValuePods
        }

        ++CurrentInputBatchPtr;

        return ProcessFetchedRow(RowBufferPointers.data());
    }

    bool TryDrain(NUdf::TUnboxedValue* const* output) override {
        return TryDrainInternal(output);
    }

    bool TryDrainDirect() {
        return TryDrainInternal(DrainBufferPointers.data());
    }

    bool TryDrainInternal(NUdf::TUnboxedValue* const* output) {
        MKQL_ENSURE(IsDraining(), "Cannot call TryDrain() unless IsDraining()");

        TTypeInfoHelper helper;

        std::vector<std::unique_ptr<NYql::NUdf::IArrayBuilder>> blockBuilders;
        for (size_t i = 0; i < OutputTypes.size(); ++i) {
            blockBuilders.push_back(MakeArrayBuilder(helper, OutputTypes[i], Ctx.ArrowMemoryPool, MaxOutputBlockLen, &Ctx.Builder->GetPgBuilder()));
        }

        size_t currentBlockSize = 0;
        void* tuple = nullptr;

        while (currentBlockSize < MaxOutputBlockLen) {
            tuple = DrainArenaIterator.Next();
            if (!tuple) {
                while (CheckRefillFromPendingBuckets()) {
                    tuple = DrainArenaIterator.Next();
                    if (tuple) {
                        break;
                    }
                }
            }

            if (!tuple) {
                break;
            }

            const auto key = static_cast<TUnboxedValuePod*>(tuple);

            if (HasGenericAggregation) {
                auto keyIter = key;
                for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                    auto& keyField = Nodes.FinishKeyNodes[i]->RefValue(Ctx);
                    keyField = *keyIter++;
                }
            }

            char* statePtr = static_cast<char *>(tuple) + StatesOffset;
            /*
            for (auto& agg : Aggs) {
                agg->ExtractState(statePtr, OutputBufferPointers.data());
                statePtr += agg->GetStateSize();
            }
            */
            GenericAggregation->ExtractState(statePtr, OutputBufferPointers.data());

            for (size_t i = 0; i < OutputColumns; ++i) {
                auto blockItem = OutputItemConverters[i]->MakeItem(OutputBuffer[i]);
                blockBuilders[i]->Add(blockItem);
                OutputBuffer[i] = TUnboxedValuePod();
            }

            if (HasGenericAggregation) {
                auto keyIter = key;
                for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                    (keyIter++)->UnRef();
                }
            }

            ++currentBlockSize;
        }

        if (currentBlockSize) {
            for (size_t i = 0; i < OutputColumns; ++i) {
                auto datum = blockBuilders[i]->Build(true);
                *output[i] = Ctx.HolderFactory.CreateArrowBlock(std::move(datum));
            }

            *output[OutputColumns] = Ctx.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(currentBlockSize)));
            OutputRowCounter.Inc();
        }

        if (!tuple) {
            Draining = false;
            if (!IsAggregation) {
                PrepareForNewBatch();
            } else {
                Map.Reset();
                Store->Clear();
            }
            return currentBlockSize > 0;
        }

        return true;
    }

private:
    NYql::NUdf::TCounter OutputRowCounter;

    TUnboxedValueVector EmptyUVs;

    std::vector<TType*> InputTypes;
    std::vector<TType*> OutputTypes;

    size_t InputColumns; // without the block height column
    size_t OutputColumns;

    const size_t MaxOutputBlockLen;

    std::vector<std::unique_ptr<IBlockReader>> InputReaders;
    std::vector<std::unique_ptr<IBlockItemConverter>> InputItemConverters;

    std::vector<std::unique_ptr<IBlockItemConverter>> OutputItemConverters;

    TUnboxedValueVector RowBuffer;
    std::vector<TUnboxedValue*> RowBufferPointers;

    TUnboxedValueVector OutputBuffer;
    std::vector<TUnboxedValue*> OutputBufferPointers;

    TUnboxedValueVector DrainBuffer;
    std::vector<TUnboxedValue*> DrainBufferPointers;

    size_t CurrentInputBatchSize = 0;
    size_t CurrentInputBatchPtr = 0;
};

class TDqHashCombine;

class TCombinerOutputStreamValue : public TComputationValue<TCombinerOutputStreamValue> {
public:
    using TBase = TComputationValue<TCombinerOutputStreamValue>;

    TCombinerOutputStreamValue(TMemoryUsageInfo* memInfo, TUnboxedValue boxedState, TUnboxedValue inputStream)
        : TBase(memInfo)
        , BoxedState(boxedState)
        , InputStream(inputStream)
        , UnboxedState(*static_cast<TBaseAggregationState*>(BoxedState.AsBoxed().Get()))
    {
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
        auto& state = UnboxedState;

        for (;;) {
            if (!state.IsDraining()) {
                if (state.IsSourceEmpty()) {
                    break;
                }

                EFillState sourceState;
                if (TUnboxedValueVector& buf = state.GetDenseInputBuffer(); buf.size()) {
                    sourceState = FetchFromStream(InputStream, buf);
                } else {
                    sourceState = EFillState::SourceSkipped;
                }

                auto fillResult = state.ProcessInput(sourceState);
                if (fillResult == EFillState::Yield) {
                    return NUdf::EFetchStatus::Yield;
                } else if (fillResult == EFillState::ContinueFilling) {
                    continue;
                } else {
                    MKQL_ENSURE(state.IsDraining(), "Expected state to be switched to draining");
                }
            }

            if (width && (width != OutputPtrs.size() || output != OutputPtrs.front())) {
                OutputPtrs.resize(width, nullptr);
                std::transform(output, output + width, OutputPtrs.begin(), [&](TUnboxedValue& val) {
                    return &val;
                });
            }

            if (state.TryDrain(OutputPtrs.data())) {
                return NUdf::EFetchStatus::Ok;
            } else if (state.IsSourceEmpty()) {
                break;
            }
        }

        return NUdf::EFetchStatus::Finish;
    }

private:
    TUnboxedValue BoxedState;
    TUnboxedValue InputStream;
    TBaseAggregationState& UnboxedState;
    std::vector<TUnboxedValue*> OutputPtrs;
};

class TDqHashCombineFlowWrapper: public TStatefulWideFlowCodegeneratorNode<TDqHashCombineFlowWrapper>
{
public:
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TDqHashCombineFlowWrapper>;

    TDqHashCombineFlowWrapper(
        TComputationMutables& mutables, IComputationWideFlowNode* source,
        const bool blockMode,
        const std::vector<TType*>& inputTypes, const std::vector<TType*>& outputTypes,
        size_t inputWidth, const std::vector<TType*>& keyItemTypes, const std::vector<TType*>& stateItemTypes,
        NDqHashOperatorCommon::TCombinerNodes&& nodes, TKeyTypes&& keyTypes, ui64 memoryLimit, size_t maxOutputBlockLen,
        const bool isAggregator, const bool enableSpilling
    )
        : TBaseComputation(mutables, source, EValueRepresentation::Boxed)
        , BlockMode(blockMode)
        , Source(source)
        , InputTypes(inputTypes)
        , OutputTypes(outputTypes)
        , KeyItemTypes(keyItemTypes)
        , StateItemTypes(stateItemTypes)
        , InputWidth(inputWidth)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , MemoryLimit(memoryLimit)
        , MaxOutputBlockLen(maxOutputBlockLen)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(InputWidth)) // Need to reserve this here, can't do it later after the Context is built
        , MemoryHelper(keyItemTypes, stateItemTypes)
        , IsAggregator(isAggregator)
        , EnableSpilling(enableSpilling)
    {
    }

    static EFillState FetchResultToFillState(EFetchResult fetchResult)
    {
        switch (fetchResult) {
            case EFetchResult::Finish:
                return EFillState::SourceEmpty;
            case EFetchResult::One:
                return EFillState::ContinueFilling;
            case EFetchResult::Yield:
                return EFillState::Yield;
            default:
                MKQL_ENSURE(false, "Unexpected fetch result value: " << static_cast<int>(fetchResult));
                __builtin_unreachable();
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& boxedState, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (boxedState.IsInvalid()) {
            MakeState(ctx, boxedState);
        }

        TBaseAggregationState& state = *static_cast<TBaseAggregationState*>(boxedState.AsBoxed().Get());

        for (;;) {
            if (!state.IsDraining()) {
                if (state.IsSourceEmpty()) {
                    break;
                }

                EFillState fillState;
                if (TUnboxedValue* const* buf = state.GetInputBuffer()) {
                    fillState = FetchResultToFillState(Source->FetchValues(ctx, buf));
                } else {
                    fillState = EFillState::SourceSkipped;
                }

                auto processResult = state.ProcessInput(fillState);

                if (processResult == EFillState::Yield) {
                    return EFetchResult::Yield;
                } else if (processResult == EFillState::ContinueFilling) {
                    continue;
                } else {
                    MKQL_ENSURE(state.IsDraining(), "Expected state to be switched to draining");
                }
            }

            if (state.TryDrain(output)) {
                return EFetchResult::One;
            } else if (state.IsSourceEmpty()) {
                break;
            }
        }

        return EFetchResult::Finish;
    }

    void RegisterDependencies() const final
    {
        if (auto flow = FlowDependsOn(Source)) {
            Nodes.RegisterDependencies(
                [this, flow](IComputationNode* node){ this->DependsOn(flow, node); },
                [this, flow](IComputationExternalNode* node){ this->Own(flow, node); }
            );
        }
    }

#if !defined(MKQL_DISABLE_CODEGEN)
    TGenerateResult DoGenGetValues(
        const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override
    {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context); // TUnboxedValue represented as int128
        const auto ptrValueType = PointerType::getUnqual(valueType); // int128* (pointer to an UV)
        const auto statusType = Type::getInt32Ty(context); // for enum values
        const auto sizeType = Type::getInt64Ty(context);

        const auto ptrType = PointerType::getUnqual(StructType::get(context)); // generic pointer

        // this compute node
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);

        [[maybe_unused]] const auto outputWidth = ConstantInt::get(sizeType, OutputTypes.size());

        // generated program (LLVM function) start
        const auto atFuncTop = &ctx.Func->getEntryBlock().back();

        // our node's GetNodeValues generated code starts here
        const auto dqHashGetValues = BasicBlock::Create(context, "dq_hash_get_values", ctx.Func);
        BranchInst::Create(dqHashGetValues, block);
        block = dqHashGetValues;

        const auto makeState = BasicBlock::Create(context, "dq_hash_make", ctx.Func);
        const auto main = BasicBlock::Create(context, "dq_hash_main", ctx.Func);

        // Check if the boxed state has been created and call MakeState if necessary
        BranchInst::Create(makeState, main, IsInvalid(statePtr, block, context), block);
        block = makeState;

        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TDqHashCombineFlowWrapper::MakeStateForLLVM>());
        const auto makeFuncType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeFuncType), "function", block);
        CallInst::Create(makeFuncType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);

        BranchInst::Create(main, block);
        block = main;

        const auto inputLoop = BasicBlock::Create(context, "dq_hash_input_loop", ctx.Func);
        const auto tryDrain = BasicBlock::Create(context, "dq_hash_try_drain_call", ctx.Func);
        const auto tryCheckEmptyInput = BasicBlock::Create(context, "dq_hash_check_empty_input", ctx.Func);
        const auto tryFetch = BasicBlock::Create(context, "dq_hash_try_fetch", ctx.Func);
        const auto returnFinish = BasicBlock::Create(context, "dq_hash_return_finish", ctx.Func);
        const auto returnYield = BasicBlock::Create(context, "dq_hash_return_yield", ctx.Func);
        const auto returnOne = BasicBlock::Create(context, "dq_hash_return_one", ctx.Func);

        // Extract the pointer to the boxed state so we can call non-virtual methods on it
        const auto boxedStatePtrType = PointerType::getUnqual(StructType::get(context));
        const auto stateUV = new LoadInst(valueType, statePtr, "dq_hash_load_state", block);
        const auto boxedStateHalf = CastInst::Create(Instruction::Trunc, stateUV, Type::getInt64Ty(context), "dq_hash_extract_state_ptr", block);
        const auto boxedStatePtr = CastInst::Create(Instruction::IntToPtr, boxedStateHalf, boxedStatePtrType, "self", block);

        // State method declarations depend on the boxed state pointer type
        const auto boolStateMethodType = FunctionType::get(Type::getInt1Ty(context), {boxedStatePtr->getType()}, false);
        const auto uvPtrStateMethodType = FunctionType::get(ptrValueType, {boxedStatePtr->getType()}, false);
        const auto statusToStatusMethodType = FunctionType::get(statusType, {boxedStatePtr->getType(), statusType}, false);

        // Non-virtual state methods
        auto isDrainingMethodAddr = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TBaseAggregationState::IsDraining>());
        auto isSourceEmptyMethodAddr = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TBaseAggregationState::IsSourceEmpty>());
        auto getInputBufferMethodAddr = ConstantInt::get(Type::getInt64Ty(context),
            BlockMode ? GetMethodPtr<&TBlockAggregationState::GetDenseInputBufferDirect>() : GetMethodPtr<&TWideAggregationState::GetDenseInputBufferDirect>());
        auto getOutputBufferMethodAddr = ConstantInt::get(Type::getInt64Ty(context),
            BlockMode ? GetMethodPtr<&TBlockAggregationState::GetDenseOutputBufferDirect>() : GetMethodPtr<&TWideAggregationState::GetDenseOutputBufferDirect>());
        auto processInputMethodAddr = ConstantInt::get(Type::getInt64Ty(context),
            BlockMode ? GetMethodPtr<&TBlockAggregationState::ProcessInputDirect>() : GetMethodPtr<&TWideAggregationState::ProcessInputDirect>());
        const auto drainMethodAddr = ConstantInt::get(Type::getInt64Ty(context),
            BlockMode ? GetMethodPtr<&TBlockAggregationState::TryDrainDirect>() : GetMethodPtr<&TWideAggregationState::TryDrainDirect>());

        const auto isDrainingMethodPtr = CastInst::Create(Instruction::IntToPtr, isDrainingMethodAddr, PointerType::getUnqual(boolStateMethodType), "dq_hash_is_draining", atFuncTop);
        const auto isSourceEmptyMethodPtr = CastInst::Create(Instruction::IntToPtr, isSourceEmptyMethodAddr, PointerType::getUnqual(boolStateMethodType), "dq_hash_is_source_empty", atFuncTop);
        const auto getInputBufferMethodPtr = CastInst::Create(Instruction::IntToPtr, getInputBufferMethodAddr, PointerType::getUnqual(uvPtrStateMethodType), "dq_hash_get_input_buffer", atFuncTop);
        const auto getOutputBufferMethodPtr = CastInst::Create(Instruction::IntToPtr, getOutputBufferMethodAddr, PointerType::getUnqual(uvPtrStateMethodType), "dq_hash_get_output_buffer", atFuncTop);
        const auto processInputMethodPtr = CastInst::Create(Instruction::IntToPtr, processInputMethodAddr, PointerType::getUnqual(statusToStatusMethodType), "dq_hash_process_input_fn", atFuncTop);
        const auto drainMethodPtr = CastInst::Create(Instruction::IntToPtr, drainMethodAddr, PointerType::getUnqual(boolStateMethodType), "dq_hash_try_drain_fn", atFuncTop);

        // Allocate and init a pointer to the output buffer on the stack (initialize to nullptr until after the row is processed)
        const auto outputBufPtr = new AllocaInst(ptrValueType, 0U, "dq_hash_output_buf_ptr", atFuncTop);
        new StoreInst(
            CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), 0), ptrValueType, "", atFuncTop),
        outputBufPtr, atFuncTop);

        // Re-implementation of C++ DoCalculate starts here
        BranchInst::Create(inputLoop, block);

        block = inputLoop;
        auto callIsDraining = CallInst::Create(boolStateMethodType, isDrainingMethodPtr, {boxedStatePtr}, "dq_hash_call_is_draining", block);
        BranchInst::Create(tryDrain, tryCheckEmptyInput, callIsDraining, block);

        block = tryCheckEmptyInput;
        auto callIsEmpty = CallInst::Create(boolStateMethodType, isSourceEmptyMethodPtr, {boxedStatePtr}, "dq_hash_call_is_empty", block);
        BranchInst::Create(returnFinish, tryFetch, callIsEmpty, block);

        block = tryFetch;
        auto inBuf = CallInst::Create(uvPtrStateMethodType, getInputBufferMethodPtr, {boxedStatePtr}, "dq_hash_call_get_input_buffer", block);
        auto isBufNull = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, inBuf, ConstantPointerNull::get(ptrValueType), "", block);

        const auto blockInputEnd = BasicBlock::Create(context, "dq_hash_input_end", ctx.Func);
        const auto blockBufNull = BasicBlock::Create(context, "", ctx.Func);
        const auto blockBufNotNull = BasicBlock::Create(context, "", ctx.Func);
        BranchInst::Create(blockBufNull, blockBufNotNull, isBufNull, block);

        const auto blockInputOk = BasicBlock::Create(context, "", ctx.Func);
        const auto blockInputYield = BasicBlock::Create(context, "", ctx.Func);
        const auto blockInputFinish = BasicBlock::Create(context, "", ctx.Func);
        // Join block to ensure a single predecessor for the 'OK' path
        const auto blockInputOkJoin = BasicBlock::Create(context, "dq_hash_input_ok_join", ctx.Func);

        block = blockBufNull;
        auto fillStateNull = ConstantInt::get(statusType, static_cast<i32>(EFillState::SourceSkipped));
        BranchInst::Create(blockInputEnd, block);

        block = blockBufNotNull;

        const auto getres = GetNodeValues(Source, ctx, block);
        const auto choice = SwitchInst::Create(getres.first, blockInputOk, 2U, block);
        choice->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), blockInputYield);
        choice->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), blockInputFinish);

        block = blockInputOk;
        for (size_t i = 0; i < InputTypes.size(); ++i) {
            auto val = getres.second[i](ctx, block);
            const auto storePtr = GetElementPtrInst::CreateInBounds(valueType, inBuf, {
                ConstantInt::get(Type::getInt32Ty(ctx.Codegen.GetContext()), i)
            }, "dq_hash_input_load", block);
            new StoreInst(val, storePtr, block);
        }
        auto fillStateOk = ConstantInt::get(statusType, static_cast<i32>(EFillState::ContinueFilling));
        // Route through join block to keep PHI predecessors stable
        BranchInst::Create(blockInputOkJoin, block);

        block = blockInputYield;
        auto fillStateYield = ConstantInt::get(statusType, static_cast<i32>(EFillState::Yield));
        BranchInst::Create(blockInputEnd, block);

        block = blockInputFinish;
        auto fillStateFinish = ConstantInt::get(statusType, static_cast<i32>(EFillState::SourceEmpty));
        BranchInst::Create(blockInputEnd, block);

        // Ensure the OK-path join block unconditionally reaches input_end
        block = blockInputOkJoin;
        BranchInst::Create(blockInputEnd, block);

        block = blockInputEnd;
        const auto fillState = PHINode::Create(statusType, 4U, "dq_hash_input_state", block);
        fillState->addIncoming(fillStateNull, blockBufNull);
        fillState->addIncoming(fillStateOk, blockInputOkJoin);
        fillState->addIncoming(fillStateYield, blockInputYield);
        fillState->addIncoming(fillStateFinish, blockInputFinish);

        auto processInputResult = CallInst::Create(statusToStatusMethodType, processInputMethodPtr, {boxedStatePtr, fillState}, "dq_hash_call_process_input", block);
        const auto handleProcessResult = SwitchInst::Create(processInputResult, tryDrain, 2U, block);
        handleProcessResult->addCase(ConstantInt::get(statusType, static_cast<i32>(EFillState::Yield)), returnYield);
        handleProcessResult->addCase(ConstantInt::get(statusType, static_cast<i32>(EFillState::ContinueFilling)), inputLoop);

        block = tryDrain;

        const auto blockCheckSourceEmpty = BasicBlock::Create(context, "", ctx.Func);

        auto tryDrainResult = CallInst::Create(boolStateMethodType, drainMethodPtr, {boxedStatePtr}, "", block);
        BranchInst::Create(returnOne, blockCheckSourceEmpty, tryDrainResult, block);

        block = blockCheckSourceEmpty;
        auto callIsEmptyOnDrainResult = CallInst::Create(boolStateMethodType, isSourceEmptyMethodPtr, {boxedStatePtr}, "dq_hash_call_is_empty_on_drain", block);
        BranchInst::Create(returnFinish, inputLoop, callIsEmptyOnDrainResult, block);

        const auto ret = BasicBlock::Create(context, "dq_hash_return", ctx.Func);

        block = returnFinish;
        auto retValueFinish = ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish));
        BranchInst::Create(ret, block);

        block = returnYield;
        auto retValueYield = ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield));
        BranchInst::Create(ret, block);

        block = returnOne;
        auto retValueOne = ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One));
        BranchInst::Create(ret, block);

        block = ret;
        const auto retValue = PHINode::Create(statusType, 2U, "dq_hash_ret_value", block);
        retValue->addIncoming(retValueFinish, returnFinish);
        retValue->addIncoming(retValueYield, returnYield);
        retValue->addIncoming(retValueOne, returnOne);

        // Get the output buffer allocated inside the state
        new StoreInst(
            CallInst::Create(uvPtrStateMethodType, getOutputBufferMethodPtr, {boxedStatePtr}, "dq_hash_call_get_output_buffer", block),
            outputBufPtr, block);

        TGenerateResult genResult;
        genResult.first = retValue;

        const size_t outputColumns = OutputTypes.size();
        for (size_t i = 0; i < outputColumns; ++i) {
                genResult.second.push_back([i, outputBufPtr, ptrValueType, valueType](const TCodegenContext& ctx, BasicBlock*& subblock) -> Value* {
                    auto outputPtrVal = new LoadInst(ptrValueType, outputBufPtr, "", subblock);
                    const auto loadPtr = GetElementPtrInst::CreateInBounds(valueType, outputPtrVal, {
                        ConstantInt::get(Type::getInt32Ty(ctx.Codegen.GetContext()), i)
                    }, "dq_hash_output_load", subblock);
                    return new LoadInst(valueType, loadPtr, "dq_hash_output", subblock);
                }
            );
        }

        return genResult;
    }
#endif // MKQL_DISABLE_CODEGEN

private:
    void MakeStateForLLVM(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        MakeState(ctx, state, true);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state, const bool forLLVM = false) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
        NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("DqHashCombine");
        UDF_LOG(logger, logComponent, NUdf::ELogLevel::Debug, TStringBuilder() << "State initialized");

        NYql::NUdf::TCounter rowCounter;

        if (ctx.CountersProvider) {
            TString id = TString(Operator_Aggregation) + "0";
            rowCounter = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, false);
        }

        if (!BlockMode) {
            state = ctx.HolderFactory.Create<TWideAggregationState>(
                ctx, MemoryHelper, rowCounter, MemoryLimit, InputWidth, OutputTypes.size(), Nodes, WideFieldsIndex,
                KeyTypes, InputTypes, KeyItemTypes, StateItemTypes, forLLVM, IsAggregator, EnableSpilling);
        } else {
            state = ctx.HolderFactory.Create<TBlockAggregationState>(
                ctx, MemoryHelper, rowCounter, MemoryLimit, InputTypes, OutputTypes, Nodes, WideFieldsIndex,
                KeyTypes, KeyItemTypes, StateItemTypes, MaxOutputBlockLen, forLLVM, IsAggregator, EnableSpilling);
        }
    }

    const bool BlockMode;
    IComputationWideFlowNode *const Source;
    std::vector<TType*> InputTypes;
    std::vector<TType*> OutputTypes;
    const std::vector<TType*> KeyItemTypes;
    const std::vector<TType*> StateItemTypes;
    size_t InputWidth;
    const NDqHashOperatorCommon::TCombinerNodes Nodes;
    const TKeyTypes KeyTypes;
    const ui64 MemoryLimit;
    const size_t MaxOutputBlockLen;
    const ui32 WideFieldsIndex;
    const TMemoryEstimationHelper MemoryHelper;
    const bool IsAggregator;
    const bool EnableSpilling;
};

class TDqHashCombineStreamWrapper: public TMutableComputationNode<TDqHashCombineStreamWrapper>
{
private:
    using TBaseComputation = TMutableComputationNode<TDqHashCombineStreamWrapper>;

public:
    TDqHashCombineStreamWrapper(
        TComputationMutables& mutables, IComputationNode* streamSource,
        const bool blockMode,
        const std::vector<TType*>& inputTypes, const std::vector<TType*>& outputTypes,
        size_t inputWidth, const std::vector<TType*>& keyItemTypes, const std::vector<TType*>& stateItemTypes,
        NDqHashOperatorCommon::TCombinerNodes&& nodes, TKeyTypes&& keyTypes, ui64 memoryLimit, size_t maxOutputBlockLen,
        const bool isAggregator, const bool enableSpilling
    )
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , BlockMode(blockMode)
        , StreamSource(streamSource)
        , InputTypes(inputTypes)
        , OutputTypes(outputTypes)
        , StateItemTypes(stateItemTypes)
        , InputWidth(inputWidth)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , KeyItemTypes(keyItemTypes)
        , MemoryLimit(memoryLimit)
        , MaxOutputBlockLen(maxOutputBlockLen)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(InputWidth)) // Need to reserve this here, can't do it later after the Context is built
        , MemoryHelper(keyItemTypes, stateItemTypes)
        , IsAggregator(isAggregator)
        , EnableSpilling(enableSpilling)
    {
    }

    // DoCalculate must return an object that encapsulates the node state;
    // we'll use the stream wrapper value to carry the state and the input stream along
    // TODO: separate UnboxedValue wrapper for the state is no longer necessary
    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        TUnboxedValue boxedState;
        MakeState(ctx, boxedState);
        TUnboxedValue inputStream = StreamSource->GetValue(ctx);
        return ctx.HolderFactory.Create<TCombinerOutputStreamValue>(boxedState, inputStream);
    }

private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
        NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("DqHashCombine");
        UDF_LOG(logger, logComponent, NUdf::ELogLevel::Debug, TStringBuilder() << "State initialized");

        NYql::NUdf::TCounter rowCounter;

        if (ctx.CountersProvider) {
            TString id = TString(Operator_Aggregation) + "0";
            rowCounter = ctx.CountersProvider->GetCounter(id, Counter_OutputRows, false);
        }

        if (!BlockMode) {
            state = ctx.HolderFactory.Create<TWideAggregationState>(
                ctx, MemoryHelper, rowCounter, MemoryLimit, InputWidth, OutputTypes.size(), Nodes, WideFieldsIndex,
                KeyTypes, InputTypes, KeyItemTypes, StateItemTypes, false, IsAggregator, EnableSpilling);
        } else {
            state = ctx.HolderFactory.Create<TBlockAggregationState>(
                ctx, MemoryHelper, rowCounter, MemoryLimit, InputTypes, OutputTypes, Nodes, WideFieldsIndex,
                KeyTypes, KeyItemTypes, StateItemTypes, MaxOutputBlockLen, false, IsAggregator, EnableSpilling);
        }
    }

    void RegisterDependencies() const final {
        DependsOn(StreamSource);
        Nodes.RegisterDependencies(
            [this](IComputationNode* node){ this->DependsOn(node); },
            [this](IComputationExternalNode* node){ this->Own(node); }
        );
    }

    const bool BlockMode;
    IComputationNode *const StreamSource;
    std::vector<TType*> InputTypes;
    std::vector<TType*> OutputTypes;
    const std::vector<TType*> StateItemTypes;
    size_t InputWidth;
    const NDqHashOperatorCommon::TCombinerNodes Nodes;
    const TKeyTypes KeyTypes;
    const std::vector<TType*> KeyItemTypes;
    const ui64 MemoryLimit;
    const size_t MaxOutputBlockLen;
    const ui32 WideFieldsIndex;
    const TMemoryEstimationHelper MemoryHelper;
    const bool IsAggregator;
    const bool EnableSpilling;
};

IComputationNode* WrapDqHashOperator(TCallable& callable, const TComputationNodeFactoryContext& ctx, const EOperatorKind kind) {
    TDqHashOperatorParams params = ParseCommonDqHashOperatorParams(callable, ctx);

    auto inputComponents = GetWideComponents(callable.GetInput(NDqHashOperatorParams::Input).GetStaticType());
    std::vector<TType*> inputTypes;
    bool inputIsBlocks = UnwrapBlockTypes(inputComponents, inputTypes);

    const auto outputComponents = GetWideComponents(callable.GetType()->GetReturnType());
    std::vector<TType*> outputTypes;
    bool outputIsBlocks = UnwrapBlockTypes(outputComponents, outputTypes);

    MKQL_ENSURE(inputIsBlocks == outputIsBlocks, "Inconsistent input/output item types: mixing of blocks and non-blocks detected");

    const auto input = LocateNode(ctx.NodeLocator, callable, NDqHashOperatorParams::Input);

    const TTupleLiteral* operatorParams = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::OperatorParams));

    ui64 memLimit = 0;
    bool enableSpilling = false;
    bool isAggregator = false;

    if (kind == EOperatorKind::Combiner) {
        memLimit = AS_VALUE(TDataLiteral, operatorParams->GetValue(NDqHashOperatorParams::CombineParamMemLimit))->AsValue().Get<ui64>();
        if (memLimit <= 0) {
            memLimit = DefaultMemoryLimit;
        }
    } else {
        isAggregator = true;
        enableSpilling = AS_VALUE(TDataLiteral, operatorParams->GetValue(NDqHashOperatorParams::CombineParamMemLimit))->AsValue().Get<bool>();
    }

    size_t maxOutputBlockLen = 0;
    if (inputIsBlocks) {
        maxOutputBlockLen = CalcMaxBlockLenForOutput(outputTypes);
    }

    if (params.IsStream) {
        return new TDqHashCombineStreamWrapper(
            ctx.Mutables,
            input,
            inputIsBlocks,
            inputTypes,
            outputTypes,
            params.InputWidth,
            params.KeyItemTypes,
            params.StateItemTypes,
            std::move(params.Nodes),
            std::move(params.KeyTypes),
            memLimit > 0 ? memLimit : DefaultMemoryLimit,
            maxOutputBlockLen,
            isAggregator,
            enableSpilling);
    } else {
        IComputationWideFlowNode* flowInput = dynamic_cast<IComputationWideFlowNode*>(input);
        MKQL_ENSURE(flowInput != nullptr, "Flow input is expected to be IComputationWideFlowNode*");
        return new TDqHashCombineFlowWrapper(
            ctx.Mutables,
            flowInput,
            inputIsBlocks,
            inputTypes,
            outputTypes,
            params.InputWidth,
            params.KeyItemTypes,
            params.StateItemTypes,
            std::move(params.Nodes),
            std::move(params.KeyTypes),
            memLimit > 0 ? memLimit : DefaultMemoryLimit,
            maxOutputBlockLen,
            isAggregator,
            enableSpilling);
    }
}

IComputationNode* WrapDqHashAggregate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapDqHashOperator(callable, ctx, EOperatorKind::Aggregator);
}

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapDqHashOperator(callable, ctx, EOperatorKind::Combiner);
}

}
}
