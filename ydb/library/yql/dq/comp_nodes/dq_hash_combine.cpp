#include "dq_hash_combine.h"
#include "dq_hash_operator_common.h"
#include "dq_hash_operator_serdes.h"
#include "type_utils.h"

#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>


namespace NKikimr {
namespace NMiniKQL {

using NUdf::TUnboxedValue;
using NUdf::TUnboxedValuePod;

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

// Key-state tuple arena which provides fixed-size allocations
template<typename T>
struct TStorageWrapper
{
    TPagedArena Storage;
    size_t AllocSize;

    TStorageWrapper(size_t width)
        : Storage(TlsAllocState)
        , AllocSize(width * sizeof(T))
    {
        MKQL_ENSURE_S(AllocSize > 0);
    }

    T* Alloc()
    {
        return static_cast<T*>(Storage.Alloc(AllocSize, EMemorySubPool::Temporary));
    }

    void Clear()
    {
        Storage.Clear();
    }
};

// Calculate static memory size bounds from TType*s and dynamic sizes from UVs
class TMemoryEstimationHelper
{
private:
    static std::optional<size_t> GetUVSizeBound(TType* type)
    {
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
    }

    static std::optional<size_t> GetMultiUVSizeBound(std::vector<TType*>& types)
    {
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
    const size_t KeyWidth;
    std::optional<size_t> StateSizeBound;
    std::optional<size_t> KeySizeBound;

    TMemoryEstimationHelper(std::vector<TType*> keyItemTypes, std::vector<TType*> stateItemTypes)
        : KeyWidth(keyItemTypes.size())
    {
        KeySizeBound = GetMultiUVSizeBound(keyItemTypes);
        StateSizeBound = GetMultiUVSizeBound(stateItemTypes);
    }

    std::optional<size_t> EstimateKeySize(const TUnboxedValuePod* items) const
    {
        constexpr const size_t uvSize = sizeof(TUnboxedValuePod);

        size_t sizeSum = 0;

        const TUnboxedValuePod* itemPtr = items;
        for (size_t i = 0; i < KeyWidth; ++i, ++itemPtr) {
            const TUnboxedValuePod& item = *itemPtr;
            if (!item.HasValue() || item.IsEmbedded() || item.IsInvalid()) {
                sizeSum += uvSize;
            } else if (item.IsString()) {
                sizeSum += uvSize + item.AsStringRef().Size();
            } else {
                return {};
            }
        }

        return sizeSum;
    }
};

[[maybe_unused]] void DebugPrintUV(TUnboxedValuePod& uv)
{
    Cerr << "----- UV at " << (size_t)(&uv) << Endl;
    Cerr << "Refcount: " << uv.RefCount() << Endl;
    Cerr << "IsString: " << uv.IsString() << "; IsEmbedded: " << uv.IsEmbedded() << Endl;
    if (uv.IsString()) {
        Cerr << "Raw string ptr: " << (size_t)uv.AsRawStringValue()->Data() << Endl;
    }
    uv.Dump(Cerr);
    Cerr << Endl;
}

}

class IAggregation
{
public:
    virtual size_t GetStateSize() const = 0; // in bytes
    virtual void InitState(void* rawState, TUnboxedValue* const* row) = 0;
    virtual void UpdateState(void* rawState, TUnboxedValue* const* row) = 0;
    virtual void ExtractState(void* rawState, TUnboxedValue* const* output) = 0;
    virtual void ForgetState(void* rawState) = 0;

    virtual ~IAggregation()
    {
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

public:
    TGenericAggregation(
        TComputationContext& ctx,
        const NDqHashOperatorCommon::TCombinerNodes& nodes
    )
        : Ctx(ctx)
        , Nodes(nodes)
        , StateWidth(Nodes.StateNodes.size())
        , StateSize(StateWidth * sizeof(TUnboxedValue))
    {
    }

    size_t GetStateSize() const override
    {
        return StateSize;
    }

    // Assumes the input row and extracted keys have already been copied into the input nodes, so row isn't even used here
    void UpdateState(void* rawState, TUnboxedValue* const* /*row*/) override
    {
        TUnboxedValue* state = static_cast<TUnboxedValue*>(rawState);
        TUnboxedValue* stateIter = state;

        std::for_each(Nodes.StateNodes.cbegin(), Nodes.StateNodes.cend(),
            [&](IComputationExternalNode* item){ item->SetValue(Ctx, std::move(*stateIter++)); });

        stateIter = state;
        std::transform(Nodes.UpdateResultNodes.cbegin(), Nodes.UpdateResultNodes.cend(), stateIter,
            [&](IComputationNode* node) { return node->GetValue(Ctx); });
    }

    // Assumes the input row has already been copied into the input nodes, so row isn't even used here
    void InitState(void* rawState, TUnboxedValue* const* /*row*/) override
    {
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
    void ExtractState(void* rawState, TUnboxedValue* const* output) override
    {
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

    void ForgetState(void* rawState) override
    {
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
};

constexpr const size_t TargetMemoryLimit = 128ull << 20; // TODO: use memLimit from ProgramBuilder
constexpr const float ExtraMapCapacity = 1.25; // hashmap size is target row count increased by this factor
constexpr const size_t DefaultMapLimit = static_cast<size_t>(1024 * ExtraMapCapacity); // some minimum value if we can't precalculate the row size

class TBaseAggregationState: public TComputationValue<TBaseAggregationState>
{
protected:
    using TMap = TRobinHoodHashSet<NUdf::TUnboxedValuePod*, TEqualsFunc, THashFunc, TMKQLAllocator<char, EMemorySubPool::Temporary>>;

    TComputationContext& Ctx;

    const TMemoryEstimationHelper& MemoryHelper;
    std::optional<size_t> MaxRowCount;
    size_t InitialMapCapacity;

    [[maybe_unused]] size_t InputWidth;
    TUnboxedValueVector InputBuffer;
    const NDqHashOperatorCommon::TCombinerNodes& Nodes;
    const ui32 WideFieldsIndex;
    std::vector<std::unique_ptr<IAggregation>> Aggs;
    const TKeyTypes& KeyTypes;
    THashFunc const Hasher;
    TEqualsFunc const Equals;
    const bool HasGenericAggregation;

    std::optional<size_t> CurrentMemoryUsage;

    using TStore = TStorageWrapper<char>;
    std::unique_ptr<TStore> Store;
    TMap Map;
    void* KeyStateBuffer;
    size_t StatesOffset;
    bool Draining;
    bool SourceEmpty;

    static std::optional<size_t> GetMaxRowCount(const TMemoryEstimationHelper& memoryHelper)
    {
        if (!memoryHelper.KeySizeBound.has_value() || !memoryHelper.StateSizeBound.has_value()) {
            return {};
        }

        size_t memoryPerRow = memoryHelper.KeySizeBound.value() + memoryHelper.StateSizeBound.value() + static_cast<size_t>(TMap::GetCellSize() * ExtraMapCapacity);
        if (memoryPerRow >= TargetMemoryLimit) {
            return 1;
        }

        return TargetMemoryLimit / memoryPerRow;
    }

    static size_t GetInitialMapCapacity(std::optional<size_t> rowCount)
    {
        if (!rowCount.has_value()) {
            return DefaultMapLimit;
        }
        return static_cast<size_t>(rowCount.value() * ExtraMapCapacity);
    }

    void ResetMemoryUsage()
    {
        if (!MemoryHelper.StateSizeBound) {
            CurrentMemoryUsage = {};
        }
        CurrentMemoryUsage = Map.GetCellSize() * Map.GetCapacity();
    }

    virtual void OpenDrain() = 0;

    EFillState ProcessFetchedRow(TUnboxedValue* const* input) {
        TUnboxedValuePod* keyBuffer = static_cast<TUnboxedValuePod*>(KeyStateBuffer);

        if (HasGenericAggregation) {
            for (auto i = 0U; i < Nodes.ItemNodes.size(); ++i) {
                if (Nodes.ItemNodes[i]->GetDependencesCount() > 0U || Nodes.PasstroughtItems[i]) {
                    Nodes.ItemNodes[i]->RefValue(Ctx) = *input[i];
                }
            }
            TUnboxedValue* const* source = input;
            std::for_each(Nodes.ItemNodes.cbegin(), Nodes.ItemNodes.cend(), [&](IComputationExternalNode* item) {
                if (const auto fieldPtr = *source++) {
                    auto& itemValue = item->RefValue(Ctx);
                    itemValue = *fieldPtr;
                }
            });
            auto keys = keyBuffer;
            for (ui32 i = 0U; i < Nodes.KeyNodes.size(); ++i) {
                auto& keyField = Nodes.KeyNodes[i]->RefValue(Ctx);
                *keys = keyField = Nodes.KeyResultNodes[i]->GetValue(Ctx);
                keys->Ref();
                keys++;
            }
        } else {
            MKQL_ENSURE(false, "Not implemented yet");
        }

        bool isNew = false;
        auto mapIt = Map.Insert(keyBuffer, isNew);
        char* statePtr = nullptr;
        if (isNew) {
            statePtr = static_cast<char *>(KeyStateBuffer) + StatesOffset;
        } else {
            TUnboxedValuePod* mapKeyPtr = Map.GetKey(mapIt);
            statePtr = reinterpret_cast<char *>(mapKeyPtr) + StatesOffset;
        }

        for (auto& agg : Aggs) {
            if (isNew) {
                agg->InitState(statePtr, input);
            } else {
                agg->UpdateState(statePtr, input);
            }
            statePtr += agg->GetStateSize();
        }

        if (!isNew) {
            auto keys = keyBuffer;
            for (ui32 i = 0U; i < Nodes.KeyNodes.size(); ++i) {
                keys->UnRef();
                keys++;
            }
        }

        if (isNew) {
            if (MaxRowCount.has_value() && Map.GetSize() >= MaxRowCount.value()) {
                OpenDrain();
                return EFillState::Drain;
            }
            else if (CurrentMemoryUsage.has_value()) {
                std::optional<size_t> keySize = MemoryHelper.KeySizeBound;
                if (!keySize.has_value()) {
                    keySize = MemoryHelper.EstimateKeySize(keyBuffer);
                }

                if (!keySize) {
                    CurrentMemoryUsage.reset();
                } else {
                    MKQL_ENSURE(MemoryHelper.StateSizeBound.has_value(), "State size must be known in memory estimation mode");
                    CurrentMemoryUsage.value() += (keySize.value() + MemoryHelper.StateSizeBound.value());
                }
            }

            if (CurrentMemoryUsage.has_value() && CurrentMemoryUsage.value() >= TargetMemoryLimit) {
                OpenDrain();
                return EFillState::Drain;
            }

            Map.CheckGrow(); // catch TMemoryLimitExceededException
            KeyStateBuffer = Store->Alloc();
        }

        if (!CurrentMemoryUsage.has_value()) {
            // TODO: this means we can't prove state or key size is bounded; fall back to some row limit & yellow zone, as per the old implementation
        }

        return EFillState::ContinueFilling;
    }

public:
    using TBase = TComputationValue<TBaseAggregationState>;

    TBaseAggregationState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TMemoryEstimationHelper& memoryHelper, size_t inputWidth, const NDqHashOperatorCommon::TCombinerNodes& nodes, ui32 wideFieldsIndex, const TKeyTypes& keyTypes)
        : TBase(memInfo)
        , Ctx(ctx)
        , MemoryHelper(memoryHelper)
        , MaxRowCount(GetMaxRowCount(memoryHelper))
        , InitialMapCapacity(GetInitialMapCapacity(MaxRowCount))
        , InputWidth(inputWidth)
        , Nodes(nodes)
        , WideFieldsIndex(wideFieldsIndex)
        , KeyTypes(keyTypes)
        , Hasher(TMyValueHasher(KeyTypes))
        , Equals(TMyValueEqual(KeyTypes))
        , HasGenericAggregation(nodes.StateNodes.size() > 0)
        , Map(Hasher, Equals, 128u)
        , KeyStateBuffer(nullptr)
        , Draining(false)
        , SourceEmpty(false)
    {
        if (HasGenericAggregation) {
            Aggs.push_back(std::make_unique<TGenericAggregation>(Ctx, Nodes));
        }

        MKQL_ENSURE(Aggs.size(), "No aggregations defined");
        size_t allAggsSize = 0;
        for (const auto& agg : Aggs) {
            allAggsSize += agg->GetStateSize();
        }
        StatesOffset = sizeof(TUnboxedValuePod) * KeyTypes.size();
        Store = std::make_unique<TStore>(StatesOffset + allAggsSize);
        ResetMemoryUsage();
        KeyStateBuffer = Store->Alloc();
    }

    virtual ~TBaseAggregationState()
    {
    }

    virtual bool TryDrain(NUdf::TUnboxedValue* const* output) = 0;
    virtual EFillState TryFill(IComputationWideFlowNode& flow) = 0;

    virtual bool IsDraining() = 0;
    virtual bool IsSourceEmpty() = 0;
};


class TWideAggregationState: public TBaseAggregationState
{
private:
    TUnboxedValueVector InputBuffer;
    const char* DrainMapIterator;

    void OpenDrain() override
    {
        Draining = true;
        DrainMapIterator = Map.Begin();
    }

public:
    TWideAggregationState(
        TMemoryUsageInfo* memInfo,
        TComputationContext& ctx,
        const TMemoryEstimationHelper& memoryHelper,
        size_t inputWidth,
        const NDqHashOperatorCommon::TCombinerNodes& nodes,
        ui32 wideFieldsIndex,
        const TKeyTypes& keyTypes
    )
        : TBaseAggregationState(memInfo, ctx, memoryHelper, inputWidth, nodes, wideFieldsIndex, keyTypes)
        , DrainMapIterator(nullptr)
    {
        InputBuffer.resize(inputWidth, TUnboxedValuePod());

        // Why are we even using Ctx.WideFields, we can't really survive Save/LoadGraphState
        std::transform(InputBuffer.begin(), InputBuffer.end(), Ctx.WideFields.data() + WideFieldsIndex, [&](TUnboxedValue& val) {
            return &val;
        });
    }

    bool IsDraining() override {
        return Draining;
    }

    bool IsSourceEmpty() override {
        return SourceEmpty;
    }

    EFillState TryFill(IComputationWideFlowNode& flow) override
    {
        auto **fields = Ctx.WideFields.data() + WideFieldsIndex;
        const auto result = flow.FetchValues(Ctx, fields);

        if (result == EFetchResult::Yield) {
            return EFillState::Yield;
        } else if (result == EFetchResult::Finish) {
            OpenDrain();
            SourceEmpty = true;
            return EFillState::SourceEmpty;
        }

        return ProcessFetchedRow(fields);
    }

    bool TryDrain(NUdf::TUnboxedValue* const* output) override
    {
        for (; DrainMapIterator != Map.End(); Map.Advance(DrainMapIterator)) {
            if (Map.IsValid(DrainMapIterator)) {
                break;
            }
        }

        if (DrainMapIterator == Map.End()) {
            Draining = false;
            DrainMapIterator = nullptr;
            Map.Clear();
            Store->Clear();
            ResetMemoryUsage();
            return false;
        }

        const auto key = Map.GetKey(DrainMapIterator);

        if (HasGenericAggregation) {
            auto keyIter = key;
            for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                auto& keyField = Nodes.FinishKeyNodes[i]->RefValue(Ctx);
                keyField = *keyIter++;
            }
        }

        char* statePtr = static_cast<char *>(static_cast<void *>(key)) + StatesOffset;
        for (auto& agg : Aggs) {
            agg->ExtractState(statePtr, output);
            statePtr += agg->GetStateSize();
        }

        if (HasGenericAggregation) {
            auto keyIter = key;
            for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                (keyIter++)->UnRef();
            }
        }

        Map.Advance(DrainMapIterator);

        return true;
    }

    ~TWideAggregationState()
    {
        if (!Draining) {
            DrainMapIterator = Map.Begin();
        }
        for (; DrainMapIterator != Map.End(); Map.Advance(DrainMapIterator)) {
            if (!Map.IsValid(DrainMapIterator)) {
                continue;
            }
            const auto key = Map.GetKey(DrainMapIterator);
            char* statePtr = static_cast<char *>(static_cast<void *>(key)) + StatesOffset;
            for (auto& agg : Aggs) {
                agg->ForgetState(statePtr);
                statePtr += agg->GetStateSize();
            }
        }
        Map.Clear();
        Store->Clear();

        // TODO: CleanupCurrentContext for the allocator?
    }
};

class TBlockAggregationState: public TBaseAggregationState
{
private:
    [[maybe_unused]] static constexpr const size_t OutputBlockSize = 8192;

    std::vector<TType*> InputTypes;
    std::vector<TType*> OutputTypes;

    size_t InputColumns; // without the block height column
    size_t OutputColumns;
    std::vector<std::unique_ptr<IBlockReader>> InputReaders;
    std::vector<std::unique_ptr<IBlockItemConverter>> InputItemConverters;

    std::vector<std::unique_ptr<IBlockItemConverter>> OutputItemConverters;

    TUnboxedValueVector InputBuffer;
    TUnboxedValueVector RowBuffer;
    std::vector<TUnboxedValue*> RowBufferPointers;

    TUnboxedValueVector OutputBuffer;
    std::vector<TUnboxedValue*> OutputBufferPointers;

    size_t CurrentInputBatchSize = 0;
    size_t CurrentInputBatchPtr = 0;

    const char* DrainMapIterator;

    void OpenDrain() override
    {
        Draining = true;
        DrainMapIterator = Map.Begin();
    }

    bool OpenBlock()
    {
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
        const std::vector<TType*>& inputTypes,
        const std::vector<TType*>& outputTypes,
        size_t inputWidth,
        const NDqHashOperatorCommon::TCombinerNodes& nodes,
        ui32 wideFieldsIndex,
        const TKeyTypes& keyTypes
    )
        : TBaseAggregationState(memInfo, ctx, memoryHelper, inputWidth, nodes, wideFieldsIndex, keyTypes)
        , InputTypes(inputTypes)
        , OutputTypes(outputTypes)
        , InputColumns(inputTypes.size() - 1)
        , OutputColumns(outputTypes.size() - 1)
        , DrainMapIterator(nullptr)
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
    }

    bool IsDraining() override {
        return Draining;
    }

    bool IsSourceEmpty() override {
        return SourceEmpty;
    }

    EFillState TryFill(IComputationWideFlowNode& flow) override
    {
        if (CurrentInputBatchPtr >= CurrentInputBatchSize) {
            std::vector<TUnboxedValue*> ptrs;
            ptrs.resize(InputBuffer.size(), nullptr);
            std::transform(InputBuffer.begin(), InputBuffer.end(), ptrs.begin(), [&](TUnboxedValue& val) {
                return &val;
            });

            auto fetchResult = flow.FetchValues(Ctx, ptrs.begin());
            if (fetchResult == EFetchResult::Yield) {
                return EFillState::Yield;
            } else if (fetchResult == EFetchResult::Finish) {
                Draining = true;
                SourceEmpty = true;
                DrainMapIterator = Map.Begin();
                return EFillState::SourceEmpty;
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

    bool TryDrain(NUdf::TUnboxedValue* const* output) override
    {
        MKQL_ENSURE(DrainMapIterator != nullptr, "Cannot call TryDrain when DrainMapIterator is null");

        TTypeInfoHelper helper;

        std::vector<std::unique_ptr<NYql::NUdf::IArrayBuilder>> blockBuilders;
        for (size_t i = 0; i < OutputTypes.size(); ++i) {
            blockBuilders.push_back(MakeArrayBuilder(helper, OutputTypes[i], Ctx.ArrowMemoryPool, OutputBlockSize, &Ctx.Builder->GetPgBuilder()));
        }

        size_t currentBlockSize = 0;
        for (; DrainMapIterator != Map.End() && currentBlockSize < OutputBlockSize; Map.Advance(DrainMapIterator)) {
            if (!Map.IsValid(DrainMapIterator)) {
                continue;
            }

            const auto key = Map.GetKey(DrainMapIterator);
            if (HasGenericAggregation) {
                auto keyIter = key;
                for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                    auto& keyField = Nodes.FinishKeyNodes[i]->RefValue(Ctx);
                    keyField = *keyIter++;
                }
            }

            char* statePtr = static_cast<char *>(static_cast<void *>(key)) + StatesOffset;
            for (auto& agg : Aggs) {
                agg->ExtractState(statePtr, OutputBufferPointers.data());
                statePtr += agg->GetStateSize();
            }

            for (size_t i = 0; i < OutputColumns; ++i) {
                auto blockItem = OutputItemConverters[i]->MakeItem(OutputBuffer[i]);
                blockBuilders[i]->Add(blockItem);
                OutputBuffer[i] = TUnboxedValuePod();
            }

            if (HasGenericAggregation) {
                auto keyIter = key;
                for (ui32 i = 0U; i < Nodes.FinishKeyNodes.size(); ++i) {
                    Nodes.FinishKeyNodes[i]->RefValue(Ctx) = TUnboxedValue();
                    (keyIter)->UnRef();
                    keyIter++;
                }
            }

            ++currentBlockSize;
        }

        while (DrainMapIterator != Map.End()) {
            if (Map.IsValid(DrainMapIterator)) {
                break;
            }
            Map.Advance(DrainMapIterator);
        }

        if (currentBlockSize) {
            for (size_t i = 0; i < OutputColumns; ++i) {
                auto datum = blockBuilders[i]->Build(true);
                *output[i] = Ctx.HolderFactory.CreateArrowBlock(std::move(datum));
            }

            *output[OutputColumns] = Ctx.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(currentBlockSize)));
        }

        if (DrainMapIterator == Map.End()) {
            Draining = false;
            DrainMapIterator = nullptr;
            Map.Clear();
            Store->Clear();
            ResetMemoryUsage();
            return currentBlockSize > 0;
        }
        return true;
    }

    ~TBlockAggregationState()
    {
        // TODO: clean up drainage
    }
};


class TDqHashCombine: public TStatefulWideFlowComputationNode<TDqHashCombine>
{
private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TDqHashCombine>;

public:
    TDqHashCombine(
        TComputationMutables& mutables, IComputationWideFlowNode* flow,
        const bool blockMode,
        const std::vector<TType*>& inputTypes, const std::vector<TType*>& outputTypes,
        size_t inputWidth, const std::vector<TType*>& keyItemTypes, const std::vector<TType*>& stateItemTypes,
        NDqHashOperatorCommon::TCombinerNodes&& nodes, TKeyTypes&& keyTypes, ui64 memLimit
    )
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , BlockMode(blockMode)
        , Flow(flow)
        , InputTypes(inputTypes)
        , OutputTypes(outputTypes)
        , InputWidth(inputWidth)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , MemLimit(memLimit)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(InputWidth)) // Need to reserve this here, can't do it later after the Context is built
        , MemoryHelper(keyItemTypes, stateItemTypes)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& boxedState, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (boxedState.IsInvalid()) {
            MakeState(ctx, boxedState);
        }

        TWideAggregationState* state = static_cast<TWideAggregationState*>(boxedState.AsBoxed().Get());

        for (;;) {
            if (!state->IsDraining()) {
                if (state->IsSourceEmpty()) {
                    break;
                }
                auto fillResult = state->TryFill(*Flow);
                if (fillResult == EFillState::Yield) {
                    return EFetchResult::Yield;
                } else if (fillResult == EFillState::ContinueFilling) {
                    continue;
                } else {
                    MKQL_ENSURE(state->IsDraining(), "Expected state to be switched to draining");
                }
            }

            if (state->TryDrain(output)) {
                return EFetchResult::One;
            } else if (state->IsSourceEmpty()) {
                break;
            }
        }

        boxedState = TUnboxedValue();
        return EFetchResult::Finish;
    }

private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
        NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("DqHashCombine");
        UDF_LOG(logger, logComponent, NUdf::ELogLevel::Debug, TStringBuilder() << "State initialized");

        if (!BlockMode) {
            state = ctx.HolderFactory.Create<TWideAggregationState>(ctx, MemoryHelper, InputWidth, Nodes, WideFieldsIndex, KeyTypes);
        } else {
            state = ctx.HolderFactory.Create<TBlockAggregationState>(ctx, MemoryHelper, InputTypes, OutputTypes, InputWidth, Nodes, WideFieldsIndex, KeyTypes);
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

    const bool BlockMode;
    IComputationWideFlowNode *const Flow;
    std::vector<TType*> InputTypes;
    std::vector<TType*> OutputTypes;
    size_t InputWidth;
    const NDqHashOperatorCommon::TCombinerNodes Nodes;
    const TKeyTypes KeyTypes;
    [[maybe_unused]] const ui64 MemLimit;
    const ui32 WideFieldsIndex;
    const TMemoryEstimationHelper MemoryHelper;
};

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TDqHashOperatorParams params = ParseCommonDqHashOperatorParams(callable, ctx);

    auto inputComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetInput(NDqHashOperatorParams::Flow).GetStaticType()));
    std::vector<TType*> inputTypes;
    bool inputIsBlocks = UnwrapBlockTypes(inputComponents, inputTypes);

    const auto outputComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    std::vector<TType*> outputTypes;
    bool outputIsBlocks = UnwrapBlockTypes(outputComponents, outputTypes);

    MKQL_ENSURE(inputIsBlocks == outputIsBlocks, "Inconsistent input/output item types: mixing of blocks and non-blocks detected");

    const auto flow = LocateNode(ctx.NodeLocator, callable, NDqHashOperatorParams::Flow);

    auto* wideFlow = dynamic_cast<IComputationWideFlowNode*>(flow);
    if (!wideFlow) {
        THROW yexception() << "Expected wide flow";
    };

    const TTupleLiteral* operatorParams = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::OperatorParams));
    const auto memLimit = AS_VALUE(TDataLiteral, operatorParams->GetValue(NDqHashOperatorParams::CombineParamMemLimit))->AsValue().Get<ui64>();

    return new TDqHashCombine(
        ctx.Mutables,
        wideFlow,
        inputIsBlocks,
        inputTypes,
        outputTypes,
        params.InputWidth,
        params.KeyItemTypes,
        params.StateItemTypes,
        std::move(params.Nodes),
        std::move(params.KeyTypes),
        ui64(memLimit));
}

}
}
