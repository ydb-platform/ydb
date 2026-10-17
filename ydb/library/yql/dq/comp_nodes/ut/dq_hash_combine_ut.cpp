#include "utils/dq_setup.h"
#include "utils/dq_factories.h"
#include "utils/preallocated_spiller.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine_layout.h>
#include <ydb/library/yql/dq/comp_nodes/dq_rh_hash.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>

#include <util/generic/size_literals.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <map>
#include <optional>

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr TStringBuf FromWideFlowWithNullCallable = "FromWideFlowWithNull";

class TFromWideFlowWithNullWrapper: public TMutableComputationNode<TFromWideFlowWithNullWrapper> {
    using TBaseComputation = TMutableComputationNode<TFromWideFlowWithNullWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, IComputationWideFlowNode* flow, ui32 inputWidth, ui32 nullIndex)
            : TBase(memInfo)
            , Ctx_(ctx)
            , Flow_(flow)
            , InputWidth_(inputWidth)
            , NullIndex_(nullIndex)
            , OutputPointers_(inputWidth, nullptr)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) final {
            MKQL_ENSURE(width + 1 == InputWidth_, "Unexpected output width");

            ui32 outputIndex = 0;
            for (ui32 i = 0; i < InputWidth_; ++i) {
                OutputPointers_[i] = i == NullIndex_ ? nullptr : output + outputIndex++;
            }

            switch (Flow_->FetchValues(Ctx_, OutputPointers_.data())) {
                case EFetchResult::Finish:
                    return NUdf::EFetchStatus::Finish;
                case EFetchResult::Yield:
                    return NUdf::EFetchStatus::Yield;
                case EFetchResult::One:
                    return NUdf::EFetchStatus::Ok;
            }
        }

        TComputationContext& Ctx_;
        IComputationWideFlowNode* const Flow_;
        const ui32 InputWidth_;
        const ui32 NullIndex_;
        std::vector<NUdf::TUnboxedValue*> OutputPointers_;
    };

    TFromWideFlowWithNullWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 inputWidth, ui32 nullIndex)
        : TBaseComputation(mutables)
        , Flow_(flow)
        , InputWidth_(inputWidth)
        , NullIndex_(nullIndex)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Flow_, InputWidth_, NullIndex_);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow_);
    }

    IComputationWideFlowNode* const Flow_;
    const ui32 InputWidth_;
    const ui32 NullIndex_;
};

IComputationNode* WrapFromWideFlowWithNull(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected two arguments");

    auto* flow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(flow, "Expected a wide flow");

    const auto* flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const ui32 inputWidth = AS_TYPE(TMultiType, flowType->GetItemType())->GetElementsCount();
    const ui32 nullIndex = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    MKQL_ENSURE(nullIndex < inputWidth, "Null output index is out of range");

    return new TFromWideFlowWithNullWrapper(ctx.Mutables, flow, inputWidth, nullIndex);
}

TComputationNodeFactory GetHashCombineNodeFactory() {
    return GetDqNodeFactory([](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == FromWideFlowWithNullCallable) {
            return WrapFromWideFlowWithNull(callable, ctx);
        }
        return nullptr;
    });
}

TRuntimeNode FromWideFlowWithNull(TProgramBuilder& pb, TRuntimeNode flow, ui32 nullIndex) {
    const auto* flowType = AS_TYPE(TFlowType, flow.GetStaticType());
    const auto* inputItemType = AS_TYPE(TMultiType, flowType->GetItemType());
    MKQL_ENSURE(nullIndex < inputItemType->GetElementsCount(), "Null output index is out of range");

    std::vector<TType*> outputItemTypes;
    outputItemTypes.reserve(inputItemType->GetElementsCount() - 1);
    for (ui32 i = 0; i < inputItemType->GetElementsCount(); ++i) {
        if (i != nullIndex) {
            outputItemTypes.push_back(inputItemType->GetElementType(i));
        }
    }

    TCallableBuilder callableBuilder(
        pb.GetTypeEnvironment(),
        FromWideFlowWithNullCallable,
        pb.NewStreamType(pb.NewMultiType(outputItemTypes)));
    callableBuilder.Add(flow);
    callableBuilder.Add(pb.NewDataLiteral<ui32>(nullIndex));
    return TRuntimeNode(callableBuilder.Build(), false);
}

template<typename Func>
void ApplyTestPoint(THolder<IComputationGraph>& graph, Func func)
{
    for (auto& node : graph->GetNodes()) {
        auto* testPoints = dynamic_cast<TDqHashCombineTestPoints*>(node.Get());
        if (!testPoints) {
            continue;
        }
        return std::invoke(func, *testPoints);
    }
    UNIT_FAIL("Couldn't find a DqHashCombine node wrapper in the graph");
}

void DisableKeyPassthrough(THolder<IComputationGraph>& graph)
{
    ApplyTestPoint(graph, [](TDqHashCombineTestPoints& tp) {
        tp.DisableKeyPassthrough(true);
    });
}

void SetTestStateCallback(THolder<IComputationGraph>& graph, const TTestStateCallback& callback)
{
    ApplyTestPoint(graph, [&callback](TDqHashCombineTestPoints& tp) {
        tp.SetTestStateCallback(callback);
    });
}

struct TOperatorEndState
{
    bool WasBypassActive = false;
};

void SetTestEndStateUpdater(THolder<IComputationGraph>& graph, TOperatorEndState& endState) {
    SetTestStateCallback(graph, [&endState](const TDqHashCombineTestState& state) {
        endState.WasBypassActive = endState.WasBypassActive || state.BypassActivated;
    });
}

template<bool Embedded>
void NativeToUnboxed(const ui64 value, NUdf::TUnboxedValuePod& result)
{
    result = NUdf::TUnboxedValuePod(value);
}

template<bool Embedded>
void NativeToUnboxed(const std::string& value, NUdf::TUnboxedValuePod& result)
{
    if constexpr (Embedded) {
        result = NUdf::TUnboxedValuePod::Embedded(value);
    } else {
        result = NUdf::TUnboxedValuePod(NUdf::TStringValue(value));
    }
}

template<typename T>
T UnboxedToNative(const NUdf::TUnboxedValue& result)
{
    return result.template Get<T>();
}

template<>
[[maybe_unused]] std::string UnboxedToNative(const NUdf::TUnboxedValue& result)
{
    const NUdf::TStringRef val = result.AsStringRef();
    return std::string(val.data(), val.size());
}

template<typename K, typename Item>
void AddRowToMap(std::unordered_map<K, std::vector<Item>>& map, const K& key, const std::vector<Item>& values)
{
    auto [refIter, isNew] = map.emplace(key, values);
    if (!isNew) {
        for (size_t i = 0; i < values.size(); ++i) {
            refIter->second.at(i) += values[i];
        }
    }
}

size_t UpdateMapFromBlocks(std::unordered_map<std::string, std::vector<ui64>>& map, const TArrayRef<const NYql::NUdf::TUnboxedValue>& values, const ui32 keyWidth)
{
    // Layout: keyWidth key block columns, then value block columns, then block height scalar
    UNIT_ASSERT(values.size() >= keyWidth + 2u);
    size_t valuesCount = values.size() - keyWidth - 1; // exclude key columns and block height column

    // Collect key column arrays
    std::vector<std::shared_ptr<arrow::BinaryArray>> keyArrays;
    keyArrays.reserve(keyWidth);
    for (ui32 k = 0; k < keyWidth; ++k) {
        auto datum = TArrowBlock::From(values[k]).GetDatum();
        UNIT_ASSERT_C(datum.kind() == arrow::Datum::ARRAY,
            "Key column " << k << " block must be an array; actual kind index is " << static_cast<int>(datum.kind()));
        auto arr = std::dynamic_pointer_cast<arrow::BinaryArray>(datum.make_array());
        UNIT_ASSERT(arr != nullptr);
        keyArrays.push_back(std::move(arr));
    }

    std::vector<std::shared_ptr<arrow::UInt64Array>> valueDatums;
    valueDatums.reserve(valuesCount);
    for (size_t i = 0; i < valuesCount; ++i) {
        valueDatums.push_back(TArrowBlock::From(values[keyWidth + i]).GetDatum().array_as<arrow::UInt64Array>());
    }

    const int64_t numRows = keyArrays.empty() ? 0 : keyArrays[0]->length();

    for (int64_t i = 0; i < numRows; ++i) {
        std::string gluedKey;
        for (ui32 k = 0; k < keyWidth; ++k) {
            gluedKey += keyArrays[k]->GetString(i);
            gluedKey += "//";
        }
        std::vector<ui64> rowValues;
        rowValues.reserve(valuesCount);
        for (size_t col = 0; col < valuesCount; ++col) {
            rowValues.push_back(valueDatums[col]->Value(i));
        }
        AddRowToMap(map, gluedKey, rowValues);
    }

    return static_cast<size_t>(numRows);
}

template<typename K, typename Item>
void AssertMapsEqual(std::unordered_map<K, std::vector<Item>>& left, std::unordered_map<K, std::vector<Item>>& right)
{
    UNIT_ASSERT_EQUAL(left.size(), right.size());

    for (const auto& leftItem : left) {
        const auto rightIt = right.find(leftItem.first);
        UNIT_ASSERT(rightIt != right.end());
        const auto& leftVec = leftItem.second;
        const auto& rightVec = rightIt->second;
        UNIT_ASSERT(leftVec.size() == rightVec.size());
        for (size_t i = 0; i < leftVec.size(); ++i) {
            UNIT_ASSERT(leftVec[i] == rightVec[i]);
        }
    }
}

// String -> (ui64, ...) wide row generator
class TWideStream : public NUdf::TBoxedValue
{
public:
    using TRefMap = std::unordered_map<std::string, std::vector<ui64>>;

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final override {
        Y_UNUSED(result);
        ythrow yexception() << "only WideFetch is supported here";
    }

    TWideStream(const TComputationContext& ctx, size_t numKeys, size_t repeats, const std::vector<TType*>& types, ui32 keyWidth, TRefMap& reference)
        : Context(ctx)
        , Types(types)
        , KeyWidth(keyWidth)
        , Reference(reference)
    {
        Samples.reserve(numKeys * repeats);
        for (size_t it = 0; it < repeats; ++it) {
            for (size_t key = 0; key < numKeys; ++key) {
                Samples.push_back(key);
            }
        }
        std::mt19937 gen(98273429);
        std::shuffle(Samples.begin(), Samples.end(), gen);
        CurrKey = Samples.begin();
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) override = 0;

protected:
    bool IsAtTheEnd() const {
        return CurrKey == Samples.end();
    }

    ui64 NextSample() {
        UNIT_ASSERT_C(CurrKey < Samples.end(), "out of samples");
        return *(CurrKey++);
    }

    static std::string FormatKey(ui64 nextKey) {
        return Sprintf("%08u.%08u.%08u.", nextKey, nextKey, nextKey);
    }

    using TSamples = std::vector<ui64>;

    const TComputationContext& Context;
    const std::vector<TType*> Types;
    ui32 KeyWidth;
    TRefMap& Reference;

    TSamples Samples;
    TSamples::iterator CurrKey;
};

class TBlockKVStream : public TWideStream {
public:
    using TCallback = std::function<void(const size_t rowNum)>;

    TBlockKVStream(const TComputationContext& ctx, size_t numKeys, size_t repeats, size_t blockSize, const std::vector<TType*>& types, ui32 keyWidth,
        TRefMap& reference, TCallback callback = {})
        : TWideStream(ctx, numKeys, repeats, types, keyWidth, reference)
        , BlockSize(blockSize)
        , Callback(callback)
    {
    }

private:
    size_t BlockSize;
    TCallback Callback;

    size_t ItemCount = 0;

public:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final override {
        const size_t expectedWidth = Types.size() + 1;
        if (width != expectedWidth) {
            ythrow yexception() << "width " << expectedWidth << " expected";
        }

        // Allow production of superlong blocks that will have to be sliced after being processed
        struct TOversizedBlockTypeInfoHelper: public TTypeInfoHelper {
            TOversizedBlockTypeInfoHelper()
                : TTypeInfoHelper()
            {
            }

            ui64 GetMaxBlockBytes() const override {
                return 100_MB;
            }
        };

        TVector<std::unique_ptr<IArrayBuilder>> builders;
        std::transform(Types.cbegin(), Types.cend(), std::back_inserter(builders),
        [&](const auto& type) {
            return MakeArrayBuilder(TOversizedBlockTypeInfoHelper(), type, Context.ArrowMemoryPool, BlockSize, &Context.Builder->GetPgBuilder());
        });

        size_t count = 0;
        for (; count < BlockSize; ++count) {
            if (IsAtTheEnd()) {
                break;
            }
            ui64 nextKey = NextSample();

            std::string gluedKey;
            for (ui32 k = 0; k < KeyWidth; ++k) {
                std::string strKey = FormatKey(nextKey + k);
                NUdf::TUnboxedValuePod keyUV;
                NativeToUnboxed<false>(strKey, keyUV);
                builders[k]->Add(keyUV);
                keyUV.DeleteUnreferenced();
                gluedKey += (strKey + "//");
            }

            std::vector<ui64> refValues;
            refValues.reserve(Types.size() - KeyWidth);
            for (ui64 i = KeyWidth; i < Types.size(); ++i) {
                NUdf::TUnboxedValuePod valueUV;
                ui64 val = (nextKey % 1000) + i;
                refValues.push_back(val);
                NativeToUnboxed<false>(val, valueUV);
                builders[i]->Add(valueUV);
                valueUV.DeleteUnreferenced();
            }

            AddRowToMap(Reference, gluedKey, refValues);
            ++ItemCount;
            if (Callback) {
                Callback(ItemCount);
            }
        }

        if (count > 0) {
            const bool finish = IsAtTheEnd();
            for (size_t i = 0; i < Types.size(); ++i) {
                result[i] = Context.HolderFactory.CreateArrowBlock(builders[i]->Build(finish), Context.RuntimeSettings.DatumValidation.Get());
            }
            result[Types.size()] = Context.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(count)), Context.RuntimeSettings.DatumValidation.Get());
            return NUdf::EFetchStatus::Ok;
        } else {
            return NUdf::EFetchStatus::Finish;
        }
    }
};

class TWideKVStream : public TWideStream {
public:
    using TCallback = std::function<void(const size_t rowCount, bool& yield)>;

    TWideKVStream(const TComputationContext& ctx, size_t numKeys, size_t repeats, const std::vector<TType*>& types, ui32 keyWidth,
        TRefMap& reference, TCallback callback = {})
        : TWideStream(ctx, numKeys, repeats, types, keyWidth, reference)
        , Callback(callback)
    {
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final override {
        const size_t expectedWidth = Types.size();
        if (width != expectedWidth) {
            ythrow yexception() << "width " << expectedWidth << " expected";
        }

        if (IsAtTheEnd()) {
            return NUdf::EFetchStatus::Finish;
        }

        if (InsertYield) {
            InsertYield = false;
            return NUdf::EFetchStatus::Yield;
        }

        ++FetchCount;
        if (Callback) {
            Callback(FetchCount, InsertYield);
        }

        ui64 nextKey = NextSample();

        std::string gluedKey;
        for (ui32 i = 0; i < KeyWidth; ++i) {
            std::string strKey = FormatKey(nextKey + i);
            NYql::NUdf::TUnboxedValuePod keyUV;
            NativeToUnboxed<false>(strKey, keyUV);
            result[i] = keyUV;
            gluedKey += (strKey + "//");
        }

        std::vector<ui64> refValues;
        refValues.reserve(Types.size() - 1);
        for (ui64 i = KeyWidth; i < Types.size(); ++i) {
            NYql::NUdf::TUnboxedValuePod valueUV;
            ui64 val = (nextKey % 1000) + i;
            refValues.push_back(val);
            NativeToUnboxed<false>(val, valueUV);
            result[i] = valueUV;
        }

        AddRowToMap(Reference, gluedKey, refValues);

        return NUdf::EFetchStatus::Ok;
    }

private:
    bool InsertYield = false;
    TCallback Callback;

    size_t FetchCount = 0;
};

template<class... ArgTypes>
TRuntimeNode GetOperatorNode(TDqProgramBuilder& pb, const bool isAggregator, const bool spilling, const size_t memLimit, TRuntimeNode source, ArgTypes... args)
{
    if (isAggregator) {
        return pb.DqHashAggregate(source, spilling, args...);
    } else {
        return pb.DqHashCombine(source, memLimit, args...);
    }
}

template<bool UseLLVM, bool Spilling = false>
THolder<IComputationGraph> BuildBlockGraph(TDqSetup<UseLLVM, Spilling>& setup, bool useFlow, bool isAggregator, const size_t memLimit, std::vector<TType*>& columnTypes, const ui32 keyWidth) {
    auto& pb = setup.GetDqProgramBuilder();

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto valueBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto keyBlockType = pb.NewBlockType(keyBaseType, TBlockType::EShape::Many);
    auto valueBlockType = pb.NewBlockType(valueBaseType, TBlockType::EShape::Many);
    auto blockSizeType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto blockSizeBlockType = pb.NewBlockType(blockSizeType, TBlockType::EShape::Scalar);

    std::vector<TType*> streamItemTypeComponents;
    for (ui32 k = 0; k < keyWidth; ++k) {
        streamItemTypeComponents.push_back(keyBlockType);
    }
    streamItemTypeComponents.push_back(valueBlockType);
    streamItemTypeComponents.push_back(blockSizeBlockType);

    const auto streamItemType = pb.NewMultiType(streamItemTypeComponents);
    const auto streamType = pb.NewStreamType(streamItemType);
    [[maybe_unused]] const auto streamResultType = pb.NewStreamType(streamItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();

    columnTypes.clear();
    for (ui32 k = 0; k < keyWidth; ++k) {
        columnTypes.push_back(keyBaseType);
    }
    columnTypes.push_back(valueBaseType);

    auto keyExtractor = [keyWidth](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        TRuntimeNode::TList keys;
        keys.reserve(keyWidth);
        for (ui32 k = 0; k < keyWidth; ++k) {
            keys.push_back(items[k]);
        }
        return keys;
    };
    auto initState = [](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
        return { items.back() };
    };
    auto updateState = [&pb](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
        return { pb.AggrAdd(state.front(), items.back()) };
    };
    auto finish = [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
        TRuntimeNode::TList result = keys;
        result.insert(result.end(), state.begin(), state.end());
        if constexpr (!UseLLVM) {
            if (useFlow) {
                result.push_back(state.front());
            }
        }
        return result;
    };

    TRuntimeNode rootNode;
    if (useFlow) {
        auto opNode = GetOperatorNode(
            pb,
            isAggregator,
            Spilling,
            memLimit,
            pb.ToFlow(TRuntimeNode(streamCallable, false), {}),
            keyExtractor,
            initState,
            updateState,
            finish
        );
        if constexpr (UseLLVM) {
            rootNode = pb.FromFlow(opNode);
        } else {
            rootNode = FromWideFlowWithNull(pb, opNode, columnTypes.size());
        }
    } else {
        rootNode = GetOperatorNode(
            pb,
            isAggregator,
            Spilling,
            memLimit,
            TRuntimeNode(streamCallable, false),
            keyExtractor,
            initState,
            updateState,
            finish
        );
    }

    return setup.BuildGraph(rootNode, {streamCallable});
}

template<bool LLVM, bool Spilling = false>
THolder<IComputationGraph> BuildWideGraph(
    TDqSetup<LLVM, Spilling>& setup, const bool useFlow, const bool isAggregator,
    const size_t memLimit, std::vector<TType*>& columnTypes, const ui32 keyWidth)
{
    auto& pb = setup.GetDqProgramBuilder();

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto valueBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    std::vector<TType*> streamItemTypeComponents;
    for (ui32 i = 0; i < keyWidth; ++i) {
        streamItemTypeComponents.push_back(keyBaseType);
    }
    streamItemTypeComponents.push_back(valueBaseType);

    const auto streamItemType = pb.NewMultiType(streamItemTypeComponents);
    const auto streamType = pb.NewStreamType(streamItemType);

    // Simple case for now: result stream has the same shape as the input stream
    const auto streamResultItemType = pb.NewMultiType(streamItemTypeComponents);
    [[maybe_unused]] const auto streamResultType = pb.NewStreamType(streamResultItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();

    columnTypes = streamItemTypeComponents;

    TRuntimeNode input = TRuntimeNode(streamCallable, false);
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }

    TRuntimeNode opNode = GetOperatorNode(
        pb,
        isAggregator,
        Spilling,
        memLimit,
        input,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
            TRuntimeNode::TList result = items;
            while (result.size() > keyWidth) {
                result.pop_back();
            }
            return result;
        },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {
                pb.AggrAdd(state.front(), items.back())
            };
        },
        [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            TRuntimeNode::TList result = keys;
            result.insert(result.end(), state.begin(), state.end());
            if constexpr (!LLVM) {
                if (useFlow) {
                    result.push_back(state.front());
                }
            }
            return result;
        }
    );

    TRuntimeNode rootNode;
    if (useFlow) {
        if constexpr (LLVM) {
            opNode = pb.FromFlow(opNode);
        } else {
            opNode = FromWideFlowWithNull(pb, opNode, columnTypes.size());
        }
    }
    rootNode = opNode;

    return setup.BuildGraph(rootNode, {streamCallable});
}

using TMixedKey = std::pair<ui32, std::optional<i64>>;
using TMixedState = std::pair<ui64, std::string>;
using TMixedReference = std::map<TMixedKey, TMixedState>;

std::shared_ptr<ISpillerFactory> CreateSpillerFactory();

class TMixedWideStream final : public NUdf::TBoxedValue {
public:
    TMixedWideStream(size_t rowCount, TMixedReference& reference, std::function<void(size_t)> callback = {})
        : RowCount(rowCount)
        , Reference(reference)
        , Callback(std::move(callback))
    {
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue&) final {
        ythrow yexception() << "only WideFetch is supported here";
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final {
        UNIT_ASSERT_VALUES_EQUAL(width, 4);
        if (Row == RowCount) {
            return NUdf::EFetchStatus::Finish;
        }

        if (Callback) {
            Callback(Row);
        }
        const ui32 key32 = Row % 7;
        const std::optional<i64> key64 = Row % 5 ? std::optional<i64>(Row % 11) : std::nullopt;
        const ui64 number = Row % 13 + 1;
        const std::string string = Sprintf("long-state-value-%08zu", Row % 17);

        result[0] = NUdf::TUnboxedValuePod(key32);
        result[1] = key64 ? NUdf::TUnboxedValuePod(*key64) : NUdf::TUnboxedValuePod{};
        result[2] = NUdf::TUnboxedValuePod(number);
        result[3] = NUdf::TUnboxedValuePod(NUdf::TStringValue(string));

        auto [it, inserted] = Reference.emplace(TMixedKey{key32, key64}, TMixedState{number, string});
        if (!inserted) {
            it->second.first += number;
            it->second.second = std::max(it->second.second, string);
        }
        ++Row;
        return NUdf::EFetchStatus::Ok;
    }

private:
    const size_t RowCount;
    TMixedReference& Reference;
    std::function<void(size_t)> Callback;
    size_t Row = 0;
};

template<bool LLVM, bool Spilling = false>
THolder<IComputationGraph> BuildMixedWideGraph(
    TDqSetup<LLVM, Spilling>& setup, bool useFlow, bool isAggregator, bool computedKeys)
{
    auto& pb = setup.GetDqProgramBuilder();
    auto key32Type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    auto key64Type = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id));
    auto numberType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    const auto streamType = pb.NewStreamType(pb.NewMultiType({key32Type, key64Type, numberType, stringType}));
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();

    TRuntimeNode input(streamCallable, false);
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto opNode = GetOperatorNode(
        pb,
        isAggregator,
        Spilling,
        128ull << 20,
        input,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
            if (computedKeys) {
                return {pb.AggrAdd(items[0], pb.template NewDataLiteral<ui32>(0)), items[1]};
            }
            return {items[0], items[1]};
        },
        [](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
            return {items[2], items[3]};
        },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {pb.AggrAdd(state[0], items[2]), pb.AggrMax(state[1], items[3])};
        },
        [](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {keys[0], keys[1], state[0], state[1]};
        });

    if (useFlow) {
        opNode = pb.FromFlow(opNode);
    }
    return setup.BuildGraph(opNode, {streamCallable});
}

template<bool LLVM, bool Spilling = false>
void RunMixedWideTest(TDqSetup<LLVM, Spilling>& setup, bool useFlow, bool isAggregator, bool computedKeys) {
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    auto graph = BuildMixedWideGraph(setup, useFlow, isAggregator, computedKeys);
    if constexpr (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    TMixedReference reference;
    std::function<void(size_t)> callback;
    if constexpr (Spilling) {
        callback = [&setup](size_t row) {
            if (row == 100) {
                setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
            }
        };
    }
    const size_t rowCount = Spilling ? 1000 : 300;
    graph->GetEntryPoint(0, true)->SetValue(
        graph->GetContext(), NUdf::TUnboxedValuePod(new TMixedWideStream(rowCount, reference, callback)));

    auto resultStream = graph->GetValue();
    std::vector<NUdf::TUnboxedValue> output(4);
    TMixedReference actual;
    for (;;) {
        const auto status = resultStream.WideFetch(output.data(), output.size());
        if (status == NUdf::EFetchStatus::Finish) {
            break;
        }
        if (status == NUdf::EFetchStatus::Yield) {
            continue;
        }
        std::optional<i64> key64;
        if (output[1]) {
            key64 = output[1].Get<i64>();
        }
        const TMixedKey key{output[0].Get<ui32>(), key64};
        const TMixedState state{
            output[2].Get<ui64>(),
            std::string(output[3].AsStringRef().Data(), output[3].AsStringRef().Size())
        };
        UNIT_ASSERT(actual.emplace(key, state).second);
    }
    UNIT_ASSERT(actual == reference);
}

template<bool LLVM>
void RunFinalizeSpillingTest(bool useFlow, bool useBlocks, bool expression, bool earlyStop = false) {
    TDqSetup<LLVM, true> setup(GetHashCombineNodeFactory());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    auto& pb = setup.GetDqProgramBuilder();
    const auto key32Type = pb.NewDataType(NUdf::TDataType<ui32>::Id);
    const auto key64Type = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id));
    const auto numberType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    const auto streamType = pb.NewStreamType(pb.NewMultiType({key32Type, key64Type, numberType, stringType}));
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();
    const TString keyString = "long-finalize-key-kept-across-spilling-buckets";
    const bool sparseOutput = useFlow && !LLVM && !useBlocks;

    TRuntimeNode input(streamCallable, false);
    if (useBlocks) {
        input = pb.WideToBlocks(input);
    }
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto root = GetOperatorNode(pb, true, true, 128_MB, input,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
            return {items[0], items[1], pb.template NewDataLiteral<NUdf::EDataSlot::String>(keyString), pb.template NewDataLiteral<NUdf::EDataSlot::String>("unused-long-finalize-key")};
        },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
            return {items[2], items[3], items[0], items[1],
                pb.NewTuple({items[0], items[3]}), pb.NewTuple({items[3]})};
        },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {pb.AggrAdd(state[0], items[2]), pb.AggrMax(state[1], items[3]),
                pb.AggrMax(state[2], items[0]), pb.AggrMax(state[3], items[1]),
                pb.NewTuple({items[0], pb.AggrMax(pb.Nth(state[4], 1), items[3])}), pb.NewTuple({items[3]})};
        },
        [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            TRuntimeNode::TList result = {state[1], keys[1],
                expression ? pb.AggrAdd(state[0], pb.template NewDataLiteral<ui64>(1)) : state[0],
                keys[0], state[1], state[2], state[3], keys[2], keys[2], state[4], state[4]};
            if (sparseOutput) {
                result.insert(result.begin(), state[1]);
            }
            return result;
        });
    if (useFlow) {
        root = sparseOutput ? FromWideFlowWithNull(pb, root, 0) : pb.FromFlow(root);
    }
    if (useBlocks) {
        root = pb.WideFromBlocks(root);
    }

    auto graph = setup.BuildGraph(root, {streamCallable});
    auto storage = std::make_shared<TPreallocatedSpillerFactory>(32_MB);
    graph->GetContext().SpillerFactory = std::make_shared<TSlowSpillerFactory>(storage);
    size_t bucketsRead = 0;
    SetTestStateCallback(graph, [&](const TDqHashCombineTestState& state) {
        UNIT_ASSERT_VALUES_EQUAL(state.FastFinalizeEnabled, !expression);
        bucketsRead = state.SpillingBucketsRead;
    });

    TMixedReference reference;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(),
        NUdf::TUnboxedValuePod(new TMixedWideStream(100000, reference, [&](size_t row) {
            if (row == 50000) {
                setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
            }
        })));

    auto stream = graph->GetValue();
    std::vector<NUdf::TUnboxedValue> output(11);
    std::vector<std::vector<NUdf::TUnboxedValue>> rows;
    size_t yields = 0;
    for (;;) {
        const auto status = stream.WideFetch(output.data(), output.size());
        if (status == NUdf::EFetchStatus::Finish) {
            break;
        }
        if (status == NUdf::EFetchStatus::Yield) {
            ++yields;
            Sleep(TDuration::MilliSeconds(1));
            continue;
        }
        rows.push_back(output);
        if (earlyStop) {
            break;
        }
    }
    UNIT_ASSERT(yields > 0);
    UNIT_ASSERT(bucketsRead > (earlyStop ? 0 : 1));
    UNIT_ASSERT_VALUES_EQUAL(storage->GetCreatedSpillers().size(), 1);
    const auto spiller = std::static_pointer_cast<TPreallocatedSpiller>(storage->GetCreatedSpillers().front());
    UNIT_ASSERT(spiller->GetPutSizes().size() > 1);

    stream = {};
    graph.Destroy();
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    TMixedReference actual;
    for (const auto& row : rows) {
        std::optional<i64> key64;
        if (row[1]) {
            key64 = row[1].Get<i64>();
        }
        const TMixedKey key{row[3].Get<ui32>(), key64};
        const auto& expected = reference.at(key);
        UNIT_ASSERT_VALUES_EQUAL(row[2].Get<ui64>(), expected.first + (expression ? 1 : 0));
        UNIT_ASSERT_VALUES_EQUAL(std::string(row[0].AsStringRef()), expected.second);
        UNIT_ASSERT_VALUES_EQUAL(std::string(row[4].AsStringRef()), expected.second);
        UNIT_ASSERT_VALUES_EQUAL(row[5].Get<ui32>(), key.first);
        UNIT_ASSERT_VALUES_EQUAL(bool(row[6]), key64.has_value());
        if (key64) {
            UNIT_ASSERT_VALUES_EQUAL(row[6].Get<i64>(), *key64);
        }
        for (ui32 i : {7, 8}) {
            UNIT_ASSERT_VALUES_EQUAL(TString(row[i].AsStringRef()), keyString);
        }
        for (ui32 i : {9, 10}) {
            UNIT_ASSERT_VALUES_EQUAL(row[i].GetElement(0).template Get<ui32>(), key.first);
            const auto string = row[i].GetElement(1);
            UNIT_ASSERT_VALUES_EQUAL(std::string(string.AsStringRef()), expected.second);
        }
        UNIT_ASSERT(actual.emplace(key, expected).second);
    }
    if (earlyStop) {
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
    } else {
        UNIT_ASSERT(actual == reference);
    }
}

std::vector<NUdf::TUnboxedValue> TemporalValues(ui32 group) {
    using NUdf::TUnboxedValuePod;
    std::vector<NUdf::TUnboxedValue> values = {
        TUnboxedValuePod(i16(i32(group) - 3)), TUnboxedValuePod(ui16(40000 + group)),
        TUnboxedValuePod(ui16(30000 + group)), TUnboxedValuePod(ui32(4000000000U + group)),
        TUnboxedValuePod(ui64(4000000000000000ULL + group)), TUnboxedValuePod(-i64(group)),
        TUnboxedValuePod(-i32(group)), TUnboxedValuePod(i64(-10000000000LL + group)),
        TUnboxedValuePod(i64(-10000000000000000LL + group)), TUnboxedValuePod(i64(-100000000000000000LL + group))};
    const size_t width = values.size();
    for (size_t i = 0; i < width; ++i) {
        const auto value = (group + i) % 3 ? values[i] : NUdf::TUnboxedValue{};
        values.push_back(value);
    }
    return values;
}

std::vector<NUdf::TUnboxedValue> TemporalKey(size_t group, size_t width) {
    auto values = TemporalValues(0);
    values.resize(width);
    if (group) {
        const size_t column = group - 1;
        values[column] = column < 10 || !values[column] ? TemporalValues(1)[column] : NUdf::TUnboxedValue{};
    }
    return values;
}

class TGeneratedWideStream final: public NUdf::TBoxedValue {
public:
    using TRow = std::vector<NUdf::TUnboxedValue>;
    TGeneratedWideStream(size_t rowCount, std::function<TRow(size_t)> generate)
        : RowCount(rowCount)
        , Generate(std::move(generate))
    {}

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) final {
        if (Row == RowCount) {
            return NUdf::EFetchStatus::Finish;
        }
        auto values = Generate(Row++);
        UNIT_ASSERT_VALUES_EQUAL(width, values.size());
        std::move(values.begin(), values.end(), output);
        return NUdf::EFetchStatus::Ok;
    }

private:
    const size_t RowCount;
    std::function<TRow(size_t)> Generate;
    size_t Row = 0;
};

class TSingleFieldComposite final: public NUdf::TBoxedValue {
public:
    explicit TSingleFieldComposite(NUdf::TUnboxedValue value)
        : Value(std::move(value))
    {}

    NUdf::TUnboxedValue GetElement(ui32 index) const final {
        UNIT_ASSERT_VALUES_EQUAL(index, 0);
        ++AccessCount;
        return Value;
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        ++AccessCount;
        return &Value;
    }

    mutable size_t AccessCount = 0;

private:
    const NUdf::TUnboxedValue Value;
};

NUdf::TUnboxedValue TemporalAfterUpdates(const NUdf::TUnboxedValue& value, NUdf::EDataSlot slot, size_t updates) {
    using namespace NUdf;
    if (!value) {
        return {};
    }
    switch (slot) {
        case EDataSlot::Int16: return TUnboxedValuePod(i16(value.Get<i16>() + i64(updates)));
        case EDataSlot::Uint16:
        case EDataSlot::Date: return TUnboxedValuePod(ui16(value.Get<ui16>() + updates));
        case EDataSlot::Datetime: return TUnboxedValuePod(ui32(value.Get<ui32>() + updates));
        case EDataSlot::Timestamp: return TUnboxedValuePod(value.Get<ui64>() + updates);
        case EDataSlot::Date32: return TUnboxedValuePod(i32(value.Get<i32>() + i64(updates)));
        default: return TUnboxedValuePod(value.Get<i64>() + i64(updates));
    }
}

template<bool LLVM>
void RunTemporalAggregationTest(bool useFlow, bool useBlocks, bool isAggregator, bool spilling, bool native16Only = false) {
    using namespace NUdf;
    TDqSetup<LLVM, true> setup(GetHashCombineNodeFactory());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    auto& pb = setup.GetDqProgramBuilder();
    std::vector<TType*> types;
    std::vector<EDataSlot> slots;
    for (auto id : {NUdf::TDataType<i16>::Id, NUdf::TDataType<ui16>::Id, NUdf::TDataType<TDate>::Id, NUdf::TDataType<TDatetime>::Id,
            NUdf::TDataType<TTimestamp>::Id, NUdf::TDataType<TInterval>::Id, NUdf::TDataType<TDate32>::Id,
            NUdf::TDataType<TDatetime64>::Id, NUdf::TDataType<TTimestamp64>::Id, NUdf::TDataType<TInterval64>::Id})
    {
        auto* type = pb.NewDataType(id);
        slots.push_back(*AS_TYPE(NMiniKQL::TDataType, type)->GetDataSlot());
        types.push_back(type);
        if (native16Only && types.size() == 3) {
            break;
        }
    }
    const size_t width = types.size();
    if (!native16Only) {
        for (size_t i = 0; i < width; ++i) {
            types.push_back(pb.NewOptionalType(types[i]));
        }
    }
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", pb.NewStreamType(pb.NewMultiType(types))).Build();
    TRuntimeNode input(source, false);
    if (useBlocks) {
        input = pb.WideToBlocks(input);
    }
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto root = GetOperatorNode(pb, isAggregator, spilling, 128_MB, input,
        [](TRuntimeNode::TList items) { return items; },
        [](TRuntimeNode::TList, TRuntimeNode::TList items) { return items; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) {
            for (size_t i = 0; i < state.size(); ++i) {
                const auto slot = slots[i % width];
                TRuntimeNode increment;
                if (slot == EDataSlot::Int16) {
                    increment = pb.template NewDataLiteral<i16>(1);
                } else if (slot == EDataSlot::Uint16) {
                    increment = pb.template NewDataLiteral<ui16>(1);
                } else {
                    const i64 micros = slot == EDataSlot::Date || slot == EDataSlot::Date32 ? 86400000000LL :
                        slot == EDataSlot::Datetime || slot == EDataSlot::Datetime64 ? 1000000 : 1;
                    const TStringRef bytes(reinterpret_cast<const char*>(&micros), sizeof(micros));
                    const bool extended = slot == EDataSlot::Date32 || slot == EDataSlot::Datetime64 ||
                        slot == EDataSlot::Timestamp64 || slot == EDataSlot::Interval64;
                    increment = extended ? pb.template NewDataLiteral<EDataSlot::Interval64>(bytes) :
                        pb.template NewDataLiteral<EDataSlot::Interval>(bytes);
                }
                auto next = pb.Add(state[i], increment);
                if (!types[i]->IsOptional() && next.GetStaticType()->IsOptional()) {
                    next = pb.Unwrap(next, pb.template NewDataLiteral<EDataSlot::String>("temporal increment"), __FILE__, __LINE__, 0);
                }
                state[i] = next;
            }
            return state;
        },
        [](TRuntimeNode::TList keys, TRuntimeNode::TList state) {
            keys.insert(keys.end(), state.begin(), state.end());
            return keys;
        });
    if (useFlow) {
        root = pb.FromFlow(root);
    }
    if (useBlocks) {
        root = pb.WideFromBlocks(root);
    }
    auto graph = setup.BuildGraph(root, {source});
    if (spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }
    size_t bucketsRead = 0;
    SetTestStateCallback(graph, [&](const TDqHashCombineTestState& state) {
        UNIT_ASSERT(state.FastFinalizeEnabled);
        bucketsRead = state.SpillingBucketsRead;
    });
    const size_t groups = types.size() + 1;
    const size_t repeats = spilling ? 1001 : 17;
    const size_t rowCount = groups * repeats;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), TUnboxedValuePod(
        new TGeneratedWideStream(rowCount, [&](size_t row) {
            if (spilling && row == rowCount / 2) {
                setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
            }
            return TemporalKey(row % groups, types.size());
        })));
    auto stream = graph->GetValue();
    std::vector<TUnboxedValue> output(types.size() * 2);
    std::vector<bool> seen(groups);
    for (;;) {
        const auto status = stream.WideFetch(output.data(), output.size());
        if (status == EFetchStatus::Finish) {
            break;
        }
        if (status == EFetchStatus::Yield) {
            Sleep(TDuration::MilliSeconds(1));
            continue;
        }
        size_t group = 0;
        for (; group < groups; ++group) {
            const auto key = TemporalKey(group, types.size());
            bool equal = true;
            for (size_t i = 0; i < key.size(); ++i) {
                if (bool(output[i]) != bool(key[i]) ||
                    (key[i] && CompareValues(slots[i % width], output[i], key[i]) != 0))
                {
                    equal = false;
                    break;
                }
            }
            if (equal) {
                break;
            }
        }
        UNIT_ASSERT(group < groups);
        UNIT_ASSERT(!seen[group]);
        seen[group] = true;
        auto expected = TemporalKey(group, types.size());
        for (size_t i = 0; i < types.size(); ++i) {
            expected.push_back(TemporalAfterUpdates(expected[i], slots[i % width], repeats - 1));
        }
        for (size_t i = 0; i < output.size(); ++i) {
            const auto& value = expected[i];
            UNIT_ASSERT_VALUES_EQUAL(bool(output[i]), bool(value));
            if (value) {
                UNIT_ASSERT_VALUES_EQUAL(CompareValues(slots[i % width], output[i], value), 0);
            }
        }
    }
    UNIT_ASSERT(std::all_of(seen.begin(), seen.end(), [](bool value) { return value; }));
    UNIT_ASSERT_VALUES_EQUAL(bucketsRead > 0, spilling);
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
}

template<bool LLVM>
void RunThrowingStateTest(bool useFlow, ui32 bits, size_t mixedWidth, bool failInit, bool spilling = false) {
    TDqSetup<LLVM, true> setup(GetHashCombineNodeFactory());
    auto& pb = setup.GetDqProgramBuilder();
    auto* optionalInput = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id));
    auto* stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
        pb.NewStreamType(pb.NewMultiType({optionalInput, stringType}))).Build();
    const auto keySource = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", stringType).Build();
    const bool mixed = mixedWidth != 0;
    const size_t width = mixed ? mixedWidth : 5;
    const size_t prefix = mixed ? 1 : 0;
    std::vector<TType*> types;
    for (size_t i = 0; i < width; ++i) {
        const ui32 itemBits = mixed ? (i % 3 == 0 ? 64 : i % 3 == 1 ? 32 : 16) : bits;
        types.push_back(pb.NewOptionalType(pb.NewDataType(itemBits == 64 ? NUdf::TDataType<i64>::Id :
            itemBits == 32 ? NUdf::TDataType<i32>::Id : NUdf::TDataType<i16>::Id)));
    }
    auto results = [&](TRuntimeNode::TList items, bool failing) {
        TRuntimeNode::TList state;
        if (mixed) {
            state.push_back(pb.NewTuple({items[1]}));
        }
        for (size_t i = 0; i < width; ++i) {
            if (failing && i == width - 1) {
                const auto value = pb.Unwrap(items[0],
                    pb.template NewDataLiteral<NUdf::EDataSlot::String>("aggregation partial state test"), __FILE__, __LINE__, 0);
                state.push_back(pb.Convert(pb.NewOptional(value), types[i]));
            } else if ((i % 2 == 0) == failing) {
                state.push_back(pb.Convert(pb.NewOptional(pb.template NewDataLiteral<i64>((failing ? 200 : 100) + i)), types[i]));
            } else {
                state.push_back(pb.NewEmptyOptional(types[i]));
            }
        }
        return state;
    };
    TRuntimeNode input(source, false);
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto root = GetOperatorNode(pb, true, spilling, 128_MB, input,
        [&](TRuntimeNode::TList) -> TRuntimeNode::TList { return {TRuntimeNode(keySource, false)}; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) { return results(items, failInit); },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList) { return results(items, true); },
        [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; });
    if (useFlow) {
        root = pb.FromFlow(root);
    }
    NUdf::TUnboxedValue initial = NUdf::TUnboxedValuePod(NUdf::TStringValue("initial heap string for throwing aggregation"));
    NUdf::TUnboxedValue replacement = NUdf::TUnboxedValuePod(NUdf::TStringValue("replacement heap string for throwing aggregation"));
    NUdf::TUnboxedValue key = NUdf::TUnboxedValuePod(NUdf::TStringValue("heap string key for throwing aggregation"));
    std::vector<std::vector<NUdf::TUnboxedValue>> snapshots;
    auto graph = setup.BuildGraph(root, {source, keySource});
    ApplyTestPoint(graph, [&](TDqHashCombineTestPoints& points) {
        points.SetStateSnapshotOnDestroy([&](std::vector<NUdf::TUnboxedValue> values) {
            snapshots.push_back(std::move(values));
        });
    });
    auto storage = spilling ? std::make_shared<TPreallocatedSpillerFactory>(1_MB) : nullptr;
    graph->GetContext().SpillerFactory = storage;
    graph->GetEntryPoint(1, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValue(key));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
        new TGeneratedWideStream(failInit ? 1 : 2, [&](size_t row) {
            if (spilling && row == 0) {
                setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
            }
            return std::vector<NUdf::TUnboxedValue>{
                row == 0 && !failInit ? NUdf::TUnboxedValuePod(i64{1}) : NUdf::TUnboxedValuePod{},
                row == 0 ? initial : replacement};
        })));
    auto stream = graph->GetValue();
    std::vector<NUdf::TUnboxedValue> output(prefix + width);
    {
        TThrowingBindTerminator terminator;
        auto fetch = [&] {
            while (stream.WideFetch(output.data(), output.size()) == NUdf::EFetchStatus::Yield) {
                Sleep(TDuration::MilliSeconds(1));
            }
        };
        UNIT_ASSERT_EXCEPTION_CONTAINS(fetch(), TTerminateException,
            "aggregation partial state test");
    }
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    stream = {};
    graph.Destroy();
    if (spilling) {
        UNIT_ASSERT(!storage->GetCreatedSpillers().empty());
        UNIT_ASSERT(snapshots.empty());
    } else {
        UNIT_ASSERT_VALUES_EQUAL(snapshots.size(), 1);
        const auto& state = snapshots.front();
        if (mixed) {
            const auto value = state[0].GetElement(0);
            UNIT_ASSERT_VALUES_EQUAL(TString(value.AsStringRef()), TString((failInit ? initial : replacement).AsStringRef()));
        }
        for (size_t i = 0; i < width; ++i) {
            const auto& value = state[prefix + i];
            const bool completed = i + 1 < width;
            const bool present = completed ? i % 2 == 0 : !failInit && i % 2 != 0;
            UNIT_ASSERT_VALUES_EQUAL(bool(value), present);
            if (value) {
                const i64 actual = mixed ? (i % 3 == 0 ? value.Get<i64>() : i % 3 == 1 ? value.Get<i32>() : value.Get<i16>()) :
                    bits == 64 ? value.Get<i64>() : bits == 32 ? value.Get<i32>() : value.Get<i16>();
                UNIT_ASSERT_VALUES_EQUAL(actual, (completed ? 200 : 100) + i);
            }
        }
    }
    snapshots.clear();
    UNIT_ASSERT_VALUES_EQUAL(key.RefCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(initial.RefCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(replacement.RefCount(), 1);
}

template<bool LLVM>
void RunThrowingKeyTest(bool useFlow, bool failFirst) {
    TDqSetup<LLVM> setup(GetHashCombineNodeFactory());
    auto& pb = setup.GetDqProgramBuilder();
    auto* stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto* optionalInput = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id));
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
        pb.NewStreamType(pb.NewMultiType({stringType, optionalInput}))).Build();
    TRuntimeNode input(source, false);
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto root = GetOperatorNode(pb, true, false, 128_MB, input,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
            return {items[0], pb.Unwrap(items[1],
                pb.template NewDataLiteral<NUdf::EDataSlot::String>("aggregation partial key test"), __FILE__, __LINE__, 0)};
        },
        [&](TRuntimeNode::TList, TRuntimeNode::TList) -> TRuntimeNode::TList {
            return {pb.template NewDataLiteral<ui64>(1)};
        },
        [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
        [](TRuntimeNode::TList keys, TRuntimeNode::TList) { return keys; });
    if (useFlow) {
        root = pb.FromFlow(root);
    }
    NUdf::TUnboxedValue key = NUdf::TUnboxedValuePod(NUdf::TStringValue("heap string for partial key extraction"));
    auto graph = setup.BuildGraph(root, {source});
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
        new TGeneratedWideStream(failFirst ? 1 : 2, [&](size_t row) {
            return std::vector<NUdf::TUnboxedValue>{key,
                row == 0 && !failFirst ? NUdf::TUnboxedValuePod(i64{1}) : NUdf::TUnboxedValuePod{}};
        })));
    auto stream = graph->GetValue();
    NUdf::TUnboxedValue output[2];
    {
        TThrowingBindTerminator terminator;
        UNIT_ASSERT_EXCEPTION_CONTAINS(stream.WideFetch(output, 2), TTerminateException,
            "aggregation partial key test");
    }
    stream = {};
    graph.Destroy();
    UNIT_ASSERT_VALUES_EQUAL(key.RefCount(), 1);
}

std::vector<NUdf::TUnboxedValue> WideNullableKey(size_t group) {
    std::vector<NUdf::TUnboxedValue> values(65);
    if (group) {
        const size_t column = (group - 1) / 2;
        const ui64 payload = (group - 1) % 2;
        values[column] = column % 3 == 0 ? NUdf::TUnboxedValuePod(ui16(payload)) :
            column % 3 == 1 ? NUdf::TUnboxedValuePod(ui32(payload)) : NUdf::TUnboxedValuePod(payload);
    }
    values.push_back(NUdf::TUnboxedValuePod(NUdf::TStringValue("constant long key after nullable columns")));
    return values;
}

std::vector<NUdf::TUnboxedValue> FloatingKey(size_t row) {
    const float floats[] = {0.0f, -0.0f, std::bit_cast<float>(ui32{0x7fc00001}), std::bit_cast<float>(ui32{0xffc01234}),
        1.0f, -1.0f, std::numeric_limits<float>::infinity(), -std::numeric_limits<float>::infinity()};
    const double doubles[] = {0.0, -0.0, std::bit_cast<double>(ui64{0x7ff8000000000001}),
        std::bit_cast<double>(ui64{0xfff8000000001234}), 1.0, -1.0,
        std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity()};
    const size_t f = row % 9;
    const size_t d = row / 9 % 9;
    return {f == 8 ? NUdf::TUnboxedValuePod{} : NUdf::TUnboxedValuePod(floats[f]),
        d == 8 ? NUdf::TUnboxedValuePod{} : NUdf::TUnboxedValuePod(doubles[d])};
}

template<typename T>
size_t FloatingKeyClass(const NUdf::TUnboxedValue& value) {
    if (!value) return 6;
    const auto number = value.Get<T>();
    if (number == 0) return 0;
    if (std::isnan(number)) return 1;
    if (number == 1) return 2;
    if (number == -1) return 3;
    UNIT_ASSERT(std::isinf(number));
    return number > 0 ? 4 : 5;
}

template<bool LLVM>
void RunKeyGroupingTest(bool useFlow, bool blocks, bool floating, bool spilling) {
    TDqSetup<LLVM, true> setup(GetHashCombineNodeFactory());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    auto& pb = setup.GetDqProgramBuilder();
    std::vector<TType*> types;
    if (floating) {
        types = {pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<float>::Id)),
            pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<double>::Id))};
    } else {
        for (size_t i = 0; i < 65; ++i) {
            types.push_back(pb.NewOptionalType(pb.NewDataType(i % 3 == 0 ? NUdf::TDataType<ui16>::Id :
                i % 3 == 1 ? NUdf::TDataType<ui32>::Id : NUdf::TDataType<ui64>::Id)));
        }
        types.push_back(pb.NewDataType(NUdf::TDataType<char*>::Id));
    }
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", pb.NewStreamType(pb.NewMultiType(types))).Build();
    TRuntimeNode input(source, false);
    if (blocks) input = pb.WideToBlocks(input);
    if (useFlow) input = pb.ToFlow(input, {});
    auto root = GetOperatorNode(pb, true, spilling, 128_MB, input,
        [](TRuntimeNode::TList items) { return items; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList) -> TRuntimeNode::TList { return {pb.template NewDataLiteral<ui64>(1)}; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {pb.AggrAdd(state[0], pb.template NewDataLiteral<ui64>(1))};
        },
        [](TRuntimeNode::TList keys, TRuntimeNode::TList state) {
            keys.push_back(state[0]);
            return keys;
        });
    if (useFlow) root = pb.FromFlow(root);
    if (blocks) root = pb.WideFromBlocks(root);
    auto graph = setup.BuildGraph(root, {source});
    if (spilling) graph->GetContext().SpillerFactory = CreateSpillerFactory();
    size_t bucketsRead = 0;
    SetTestStateCallback(graph, [&](const TDqHashCombineTestState& state) { bucketsRead = state.SpillingBucketsRead; });
    const size_t inputGroups = floating ? 81 : 131;
    const size_t repeats = spilling ? 200 : 3;
    const size_t rowCount = inputGroups * repeats;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
        new TGeneratedWideStream(rowCount, [&](size_t row) {
            if (spilling && row == rowCount / 2) setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
            return floating ? FloatingKey(row % inputGroups) : WideNullableKey(row % inputGroups);
        })));
    auto stream = graph->GetValue();
    std::vector<NUdf::TUnboxedValue> output(types.size() + 1);
    std::vector<bool> seen(floating ? 49 : inputGroups);
    for (;;) {
        const auto status = stream.WideFetch(output.data(), output.size());
        if (status == NUdf::EFetchStatus::Finish) break;
        if (status == NUdf::EFetchStatus::Yield) {
            Sleep(TDuration::MilliSeconds(1));
            continue;
        }
        size_t group = 0;
        size_t count = repeats;
        if (floating) {
            const auto f = FloatingKeyClass<float>(output[0]);
            const auto d = FloatingKeyClass<double>(output[1]);
            group = f + 7 * d;
            count *= (f < 2 ? 2 : 1) * (d < 2 ? 2 : 1);
        } else {
            for (size_t i = 0; i < 65; ++i) {
                if (output[i]) {
                    UNIT_ASSERT_VALUES_EQUAL(group, 0);
                    const ui64 value = i % 3 == 0 ? output[i].Get<ui16>() : i % 3 == 1 ? output[i].Get<ui32>() : output[i].Get<ui64>();
                    UNIT_ASSERT(value <= 1);
                    group = 1 + 2 * i + value;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(TString(output[65].AsStringRef()), "constant long key after nullable columns");
        }
        UNIT_ASSERT(!seen[group]);
        seen[group] = true;
        UNIT_ASSERT_VALUES_EQUAL(output.back().Get<ui64>(), count);
    }
    UNIT_ASSERT(std::all_of(seen.begin(), seen.end(), [](bool value) { return value; }));
    UNIT_ASSERT_VALUES_EQUAL(bucketsRead > 0, spilling);
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
}

template<bool LLVM, bool Spilling = false>
THolder<IComputationGraph> BuildZeroWidthWideGraph(TDqSetup<LLVM, Spilling>& setup, const bool useFlow, const bool isAggregator, const size_t memLimit, std::vector<TType*>& columnTypes) {
    auto& pb = setup.GetDqProgramBuilder();

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto valueBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto streamItemType = pb.NewMultiType({keyBaseType, valueBaseType});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamResultItemType = pb.NewMultiType({keyBaseType, valueBaseType});
    [[maybe_unused]] const auto streamResultType = pb.NewStreamType(streamResultItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();

    columnTypes = {keyBaseType, valueBaseType};

    auto finish = [&](TRuntimeNode::TList keys, [[maybe_unused]] TRuntimeNode::TList state) -> TRuntimeNode::TList {
        if constexpr (!LLVM) {
            if (useFlow) {
                return {keys.front()};
            }
        }
        return {};
    };

    TRuntimeNode rootNode;
    if (useFlow) {
        auto opNode = GetOperatorNode(
            pb,
            isAggregator,
            Spilling,
            memLimit,
            pb.ToFlow(TRuntimeNode(streamCallable, false), {}),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
            [&](TRuntimeNode::TList, [[maybe_unused]] TRuntimeNode::TList items) -> TRuntimeNode::TList { return { } ; },
            [&](TRuntimeNode::TList, [[maybe_unused]] TRuntimeNode::TList items, [[maybe_unused]] TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {};
            },
            finish
        );
        if constexpr (LLVM) {
            rootNode = pb.FromFlow(opNode);
        } else {
            rootNode = FromWideFlowWithNull(pb, opNode, 0);
        }
    } else {
        rootNode = GetOperatorNode(
            pb,
            isAggregator,
            Spilling,
            memLimit,
            TRuntimeNode(streamCallable, false),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
            [&](TRuntimeNode::TList, [[maybe_unused]] TRuntimeNode::TList items) -> TRuntimeNode::TList { return { } ; },
            [&](TRuntimeNode::TList, [[maybe_unused]] TRuntimeNode::TList items, [[maybe_unused]] TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {};
            },
            finish
        );
    }

    return setup.BuildGraph(rootNode, {streamCallable});
}

std::shared_ptr<ISpillerFactory> CreateSpillerFactory()
{
    return std::make_shared<NKikimr::NMiniKQL::TSlowSpillerFactory>(
        std::make_shared<NKikimr::NMiniKQL::TPreallocatedSpillerFactory>(100_MB)
    );
}

// Spiller whose writes never complete: deterministically parks the spilling
// coroutine on a pending future (models a query abort mid-spill).
class TPendingSpiller: public ISpiller {
public:
    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&&) override {
        Promises.push_back(NThreading::NewPromise<TKey>());
        return Promises.back().GetFuture();
    }
    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey) override {
        return NThreading::MakeFuture<std::optional<NYql::TChunkedBuffer>>(std::nullopt);
    }
    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey) override {
        return NThreading::MakeFuture<std::optional<NYql::TChunkedBuffer>>(std::nullopt);
    }
    NThreading::TFuture<void> Delete(TKey) override {
        return NThreading::MakeFuture();
    }
    void ReportAlloc(ui64) override {
    }
    void ReportFree(ui64) override {
    }

private:
    std::vector<NThreading::TPromise<TKey>> Promises;
};

class TPendingSpillerFactory: public ISpillerFactory {
public:
    void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>&) override {
    }
    void SetMemoryReportingCallbacks(ISpiller::TMemoryReportCallback, ISpiller::TMemoryReportCallback) override {
    }
    ISpiller::TPtr CreateSpiller() override {
        return std::make_shared<TPendingSpiller>();
    }
};

template<typename TMap>
size_t CollectStreamOutputs(const NUdf::TUnboxedValue& wideStream, const ui32 resultWidth, const ui32 keyWidth, TMap& resultMap, const bool useBlocks, const bool sleepOnYield)
{
    std::vector<NUdf::TUnboxedValue> fetchedValues;
    fetchedValues.resize(resultWidth);
    Y_ENSURE(wideStream.IsBoxed());
    size_t lineCount = 0;

    NUdf::EFetchStatus fetchStatus;
    while ((fetchStatus = wideStream.WideFetch(fetchedValues.data(), resultWidth)) != NUdf::EFetchStatus::Finish) {
        if (fetchStatus == NUdf::EFetchStatus::Yield) {
            if (sleepOnYield) {
                ::Sleep(TDuration::MilliSeconds(1));
            }
            continue;
        }

        if (resultWidth == 0) {
            ++lineCount;
        } else if (useBlocks) {
            lineCount += UpdateMapFromBlocks(resultMap, fetchedValues, keyWidth);
        } else {
            std::vector<ui64> valuesVec;
            valuesVec.reserve(resultWidth);
            TArrayRef aggregates(fetchedValues.data() + keyWidth, resultWidth - keyWidth);
            for (const NYql::NUdf::TUnboxedValue& value : aggregates) {
                valuesVec.push_back(UnboxedToNative<ui64>(value));
            }
            std::string gluedKey;
            for (ui32 i = 0; i < keyWidth; ++i) {
                gluedKey += UnboxedToNative<std::string>(fetchedValues[i]);
                gluedKey += "//";
            }
            AddRowToMap(resultMap, gluedKey, valuesVec);
            ++lineCount;
        }
    }

    return lineCount;
}

template<bool UseLLVM, typename StreamCreator>
TOperatorEndState RunDqCombineBlockTest(const bool useFlow, StreamCreator streamCreator, const ui32 keyWidth = 2, const bool disableKeyPassthrough = false)
{
    TDqSetup<UseLLVM> setup(GetHashCombineNodeFactory());

    std::vector<TType*> columnTypes;

    auto graph = BuildBlockGraph(setup, useFlow, false, 128ull << 20, columnTypes, keyWidth);

    TOperatorEndState endState;
    SetTestEndStateUpdater(graph, endState);

    if (disableKeyPassthrough) {
        DisableKeyPassthrough(graph);
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, keyWidth, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    CollectStreamOutputs(resultStream, columnTypes.size() + 1, keyWidth, graphResult, true, true);

    AssertMapsEqual(refResult, graphResult);

    return endState;
}

template<bool UseLLVM, typename StreamCreator>
TOperatorEndState RunDqCombineWideTest(const bool useFlow, StreamCreator streamCreator, ui32 keyWidth = 2, const bool disableKeyPassthrough = false)
{
    TDqSetup<UseLLVM> setup(GetHashCombineNodeFactory());

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, false, 128ull << 20, columnTypes, keyWidth);

    TOperatorEndState endState;
    SetTestEndStateUpdater(graph, endState);

    if (disableKeyPassthrough) {
        DisableKeyPassthrough(graph);
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, keyWidth, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    CollectStreamOutputs(resultStream, columnTypes.size(), keyWidth, graphResult, false, true);

    AssertMapsEqual(refResult, graphResult);

    return endState;
}

template<bool UseLLVM, bool Spilling, typename StreamCreator, typename StreamChecker>
void RunDqAggregateEarlyStopTest(TDqSetup<UseLLVM, Spilling>& setup, const bool useFlow,
    StreamCreator streamCreator, StreamChecker streamChecker,
    std::shared_ptr<ISpillerFactory> spillerFactory = {})
{
    const ui32 keyWidth = 2;

    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, true, 0, columnTypes, keyWidth);

    if (Spilling) {
        graph->GetContext().SpillerFactory = spillerFactory ? spillerFactory : CreateSpillerFactory();
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, keyWidth, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));

    size_t width = columnTypes.size();
    std::vector<NUdf::TUnboxedValue> resultValues;
    resultValues.resize(width);

    auto resultStream = graph->GetValue();
    Y_ENSURE(resultStream.IsBoxed());

    NUdf::EFetchStatus fetchStatus;
    while ((fetchStatus = resultStream.WideFetch(resultValues.data(), width)) != NUdf::EFetchStatus::Finish) {
        if (!streamChecker(fetchStatus)) {
            break;
        }
    }
}

template<bool UseLLVM, bool Spilling, typename StreamCreator>
void RunDqAggregateBlockTest(TDqSetup<UseLLVM, Spilling>& setup, const bool useFlow, StreamCreator streamCreator, const ui32 keyWidth = 2)
{
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildBlockGraph(setup, useFlow, true, 0, columnTypes, keyWidth);
    TOperatorEndState endState;
    SetTestEndStateUpdater(graph, endState);

    if (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, keyWidth, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    size_t numResultRows = CollectStreamOutputs(resultStream, columnTypes.size() + 1, keyWidth, graphResult, true, true);

    UNIT_ASSERT(numResultRows == refResult.size());
    AssertMapsEqual(refResult, graphResult);

    UNIT_ASSERT_C(!endState.WasBypassActive, "Bypass should NOT have been activated");
}

template<bool LLVM, bool Spilling, typename StreamCreator>
void RunDqAggregateWideTest(TDqSetup<LLVM, Spilling>& setup, const bool useFlow, StreamCreator streamCreator, const ui32 keyWidth = 2, const bool disableKeyPassthrough = false)
{
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, true, 0, columnTypes, keyWidth);
    TOperatorEndState endState;
    SetTestEndStateUpdater(graph, endState);

    if (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    if (disableKeyPassthrough) {
        DisableKeyPassthrough(graph);
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, keyWidth, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    size_t numResultItems = CollectStreamOutputs(resultStream, columnTypes.size(), keyWidth, graphResult, false, true);

    UNIT_ASSERT(numResultItems == refResult.size());
    AssertMapsEqual(refResult, graphResult);

    UNIT_ASSERT_C(!endState.WasBypassActive, "Bypass should NOT have been activated");
}

template<bool UseLLVM, bool Spilling, typename StreamCreator>
void RunDqAggregateZeroWidthTest(TDqSetup<UseLLVM, Spilling>& setup, const bool useFlow, StreamCreator streamCreator)
{
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildZeroWidthWideGraph(setup, useFlow, true, 0, columnTypes);

    if (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, 1, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    size_t numResultItems = CollectStreamOutputs(resultStream, 0, 1, graphResult, false, true);

    UNIT_ASSERT(numResultItems == refResult.size());
}


} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqHashCombineTest) {
    Y_UNIT_TEST_QUAD(TestSampledRowLimit, UseLLVM, UseFlow) {
        for (const bool structure : {false, true}) {
            TDqSetup<UseLLVM> setup(GetDqNodeFactory());
            auto& pb = setup.GetDqProgramBuilder();
            const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", pb.NewStreamType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}))).Build();
            TRuntimeNode input(source, false);
            if (UseFlow) input = pb.ToFlow(input, {});
            auto root = pb.DqHashCombine(input, 1_MB,
                [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
                [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                    const auto number = pb.template NewDataLiteral<ui64>(7);
                    const auto composite = structure ? pb.NewStruct({{"a", items[1]}, {"b", number}}) : pb.NewTuple({items[1], number});
                    return {composite, pb.NewOptional(number), pb.template NewDataLiteral<ui16>(11)};
                },
                [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
                [](TRuntimeNode::TList keys, TRuntimeNode::TList state) {
                    keys.insert(keys.end(), state.begin(), state.end());
                    return keys;
                });
            if (UseFlow) root = pb.FromFlow(root);
            const TString text("heap-backed string in the sampled aggregation state");
            NUdf::TUnboxedValue string = NUdf::TUnboxedValuePod(NUdf::TStringValue(text));
            NUdf::TUnboxedValue longerString = NUdf::TUnboxedValuePod(NUdf::TStringValue(text + "!"));
            auto graph = setup.BuildGraph(root, {source});
            size_t inputRows = 0;
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
                new TGeneratedWideStream(100000, [&](size_t row) {
                    inputRows = row + 1;
                    return std::vector<NUdf::TUnboxedValue>{NUdf::TUnboxedValuePod(ui32(row / 2)), row < 2 ? longerString : string};
                })));
            auto stream = graph->GetValue();
            NUdf::TUnboxedValue output[4];
            const size_t sampleGroups = 16384;
            for (size_t i = 0; i < sampleGroups; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output, 4), NUdf::EFetchStatus::Ok);
                UNIT_ASSERT_VALUES_EQUAL(inputRows, 2 * sampleGroups - 1);
            }
            const auto* outputType = AS_TYPE(TMultiType, AS_TYPE(TStreamType, root.GetStaticType())->GetItemType());
            const std::vector<TType*> keyTypes = {outputType->GetElementType(0)};
            const std::vector<TType*> stateTypes = {
                outputType->GetElementType(1), outputType->GetElementType(2), outputType->GetElementType(3),
            };
            const size_t recordBytes = TDqHashCombineLayout(keyTypes, stateTypes).GetRecordSize();
            // One sampled group has an extra string byte, so the average rounds up by one byte
            const size_t externalBytes = sizeof(TDirectArrayHolderInplace) + 2 * sizeof(NUdf::TUnboxedValuePod) +
                sizeof(*string.AsRawStringValue()) + text.size() + 1;
            using TMap = TDqRobinHoodHashSet<char*, TDqHashCombinePackedEqual, std::allocator<char>>;
            const size_t nextGroups = 1_MB / (recordBytes + externalBytes + 2 * TMap::GetCellSize());
            UNIT_ASSERT(nextGroups > 1024 && nextGroups < sampleGroups);
            UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output, 4), NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(inputRows, 2 * sampleGroups - 1 + 2 * nextGroups - 2);
        }
    }

    Y_UNIT_TEST_QUAD(TestNonDirectCompositeKeepsSampleRowLimit, UseLLVM, UseFlow) {
        for (const bool structure : {false, true}) {
            TDqSetup<UseLLVM> setup(GetDqNodeFactory());
            auto& pb = setup.GetDqProgramBuilder();
            auto* fieldType = pb.NewDataType(NUdf::TDataType<char*>::Id);
            TType* compositeType = structure ? pb.NewStructType({{"value", fieldType}}) :
                pb.NewTupleType({fieldType});
            const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", pb.NewStreamType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id), compositeType}))).Build();
            TRuntimeNode input(source, false);
            if (UseFlow) input = pb.ToFlow(input, {});
            auto root = pb.DqHashCombine(input, 16_MB,
                [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
                [](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1]}; },
                [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
                [](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys[0], state[0]}; });
            if (UseFlow) root = pb.FromFlow(root);

            auto* holder = new TSingleFieldComposite(NUdf::TUnboxedValuePod(
                NUdf::TStringValue("heap-backed indirect aggregation state")));
            NUdf::TUnboxedValue value = NUdf::TUnboxedValuePod(holder);
            auto graph = setup.BuildGraph(root, {source});
            size_t inputRows = 0;
            const size_t rows = 100000;
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
                new TGeneratedWideStream(rows, [&](size_t row) {
                    inputRows = row + 1;
                    return std::vector<NUdf::TUnboxedValue>{NUdf::TUnboxedValuePod(ui32(row / 2)), value};
                })));
            auto stream = graph->GetValue();
            NUdf::TUnboxedValue output[2];
            std::vector<bool> seen(rows / 2);
            const auto checkOutput = [&] {
                const auto key = output[0].Get<ui32>();
                UNIT_ASSERT(key < seen.size());
                seen[key] = true;
                UNIT_ASSERT(output[1].AsRawBoxed() == holder);
            };
            const size_t sampleGroups = 16384;
            for (size_t batch = 0; batch < 2; ++batch) {
                for (size_t i = 0; i < sampleGroups; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output, 2), NUdf::EFetchStatus::Ok);
                    UNIT_ASSERT_VALUES_EQUAL(inputRows, 2 * sampleGroups - 1 + batch * (2 * sampleGroups - 2));
                    checkOutput();
                }
            }
            for (;;) {
                const auto status = stream.WideFetch(output, 2);
                if (status == NUdf::EFetchStatus::Finish) break;
                UNIT_ASSERT_VALUES_EQUAL(status, NUdf::EFetchStatus::Ok);
                checkOutput();
            }
            UNIT_ASSERT_VALUES_EQUAL(inputRows, rows);
            UNIT_ASSERT_VALUES_EQUAL(std::count(seen.begin(), seen.end(), true), seen.size());
            UNIT_ASSERT_VALUES_EQUAL(holder->AccessCount, 0);
        }
    }

    Y_UNIT_TEST_QUAD(TestSpillingInputMemoryEstimation, UseLLVM, UseFlow) {
        for (const bool indirect : {false, true}) {
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            auto& pb = setup.GetDqProgramBuilder();
            auto* stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
            auto* compositeType = pb.NewTupleType({stringType});
            const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
                pb.NewStreamType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<ui32>::Id), compositeType}))).Build();
            TRuntimeNode input(source, false);
            if (UseFlow) input = pb.ToFlow(input, {});
            auto root = pb.DqHashAggregate(input, true,
                [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
                [](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1]}; },
                [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
                [](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys[0], state[0]}; });
            if (UseFlow) root = pb.FromFlow(root);

            auto graph = setup.BuildGraph(root, {source});
            graph->GetContext().SpillerFactory = std::make_shared<TPreallocatedSpillerFactory>(4_MB);
            TDqHashCombineTestState lastState;
            SetTestStateCallback(graph, [&](const TDqHashCombineTestState& state) { lastState = state; });
            const TString text("heap-backed field in sampled spilling input");
            NUdf::TUnboxedValue string = NUdf::TUnboxedValuePod(NUdf::TStringValue(text));
            NUdf::TUnboxedValue* items = nullptr;
            NUdf::TUnboxedValue direct = graph->GetContext().Builder->NewArray(1, items);
            items[0] = string;
            NUdf::TUnboxedValue custom = NUdf::TUnboxedValuePod(new TSingleFieldComposite(string));
            const size_t rows = 4096;
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
                new TGeneratedWideStream(rows, [&](size_t row) {
                    if (!row) setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                    return std::vector<NUdf::TUnboxedValue>{NUdf::TUnboxedValuePod(ui32(row)),
                        indirect && row == 500 ? custom : direct};
                })));
            auto stream = graph->GetValue();
            NUdf::TUnboxedValue output[2];
            size_t outputRows = 0;
            for (;;) {
                const auto status = stream.WideFetch(output, 2);
                if (status == NUdf::EFetchStatus::Finish) break;
                if (status == NUdf::EFetchStatus::Yield) continue;
                const auto field = output[1].GetElement(0);
                UNIT_ASSERT_VALUES_EQUAL(TString(field.AsStringRef()), text);
                ++outputRows;
            }
            setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
            UNIT_ASSERT_VALUES_EQUAL(outputRows, rows);
            UNIT_ASSERT(lastState.SpillingBucketsRead > 0);
            UNIT_ASSERT_VALUES_EQUAL(lastState.InputRowMemoryUsageMultiplier.has_value(), !indirect);
            if (!indirect) {
                const size_t rowBytes = 3 * sizeof(NUdf::TUnboxedValuePod) + sizeof(TDirectArrayHolderInplace) +
                    sizeof(*string.AsRawStringValue()) + text.size();
                UNIT_ASSERT_DOUBLES_EQUAL(*lastState.InputRowMemoryUsageMultiplier,
                    double(rowBytes) / (2 * sizeof(NUdf::TUnboxedValuePod)), 1e-12);
            }
        }
    }

    Y_UNIT_TEST_QUAD(TestMemoryEstimationExceptionCleanup, UseLLVM, UseFlow) {
        for (const bool spilling : {false, true}) {
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            auto& pb = setup.GetDqProgramBuilder();
            auto* stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
            auto* compositeType = pb.NewTupleType({stringType, stringType});
            const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
                pb.NewStreamType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<ui32>::Id), compositeType}))).Build();
            TRuntimeNode input(source, false);
            if (UseFlow) input = pb.ToFlow(input, {});
            auto root = GetOperatorNode(pb, spilling, spilling, 16_MB, input,
                [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
                [](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1]}; },
                [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
                [](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys[0], state[0]}; });
            if (UseFlow) root = pb.FromFlow(root);

            TMemoryUsageInfo memInfo("MemoryEstimationExceptionCleanup");
            THolderFactory factory(setup.Alloc.Ref(), memInfo);
            TDefaultValueBuilder builder(factory);
            NUdf::TUnboxedValue string = NUdf::TUnboxedValuePod(NUdf::TStringValue("heap-backed field in a short composite"));
            NUdf::TUnboxedValue* items = nullptr;
            NUdf::TUnboxedValue valid = builder.NewArray(2, items);
            items[0] = items[1] = string;
            NUdf::TUnboxedValue shortHolder = builder.NewArray(1, items);
            items[0] = string;
            auto graph = setup.BuildGraph(root, {source});
            auto storage = std::make_shared<TPreallocatedSpillerFactory>(1_MB);
            graph->GetContext().SpillerFactory = storage;
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
                new TGeneratedWideStream(4096, [&](size_t row) {
                    if (!row) setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                    return std::vector<NUdf::TUnboxedValue>{NUdf::TUnboxedValuePod(ui32(row / 2)),
                        spilling && row < 500 ? valid : shortHolder};
                })));
            auto stream = graph->GetValue();
            NUdf::TUnboxedValue output[2];
            const auto fetch = [&] {
                while (stream.WideFetch(output, 2) == NUdf::EFetchStatus::Yield) {
                }
            };
            UNIT_ASSERT_EXCEPTION_CONTAINS(fetch(), yexception, "Composite holder has fewer elements than its type");
            setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
            UNIT_ASSERT_VALUES_EQUAL(!storage->GetCreatedSpillers().empty(), spilling);
            stream = {};
            graph.Destroy();
            UNIT_ASSERT_VALUES_EQUAL(valid.RefCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(shortHolder.RefCount(), 1);
            valid = {};
            shortHolder = {};
            UNIT_ASSERT_VALUES_EQUAL(string.RefCount(), 1);
        }
    }

    Y_UNIT_TEST_QUAD(TestFixedSizeBypass, UseLLVM, UseFlow) {
        for (const bool blocks : {false, true}) {
            for (const bool optionalKey : {false, true}) {
                for (const bool optionalState : {false, true}) {
                    for (const ui32 scenario : {0, 1, 2}) {
                        TDqSetup<UseLLVM> setup(GetDqNodeFactory());
                        auto& pb = setup.GetDqProgramBuilder();
                        auto* number = pb.NewDataType(NUdf::TDataType<ui64>::Id);
                        auto* keyType = optionalKey ? pb.NewOptionalType(number) : number;
                        auto* stateType = optionalState ? pb.NewOptionalType(number) : number;
                        const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
                            pb.NewStreamType(pb.NewMultiType({keyType, stateType}))).Build();
                        TRuntimeNode input(source, false);
                        if (blocks) input = pb.WideToBlocks(input);
                        if (UseFlow) input = pb.ToFlow(input, {});
                        auto root = pb.DqHashCombine(input, 64_KB,
                            [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
                            [](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1]}; },
                            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                                return {pb.AggrAdd(state[0], items[1])};
                            },
                            [](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys[0], state[0]}; });
                        if (UseFlow) root = pb.FromFlow(root);
                        if (blocks) root = pb.WideFromBlocks(root);
                        auto graph = setup.BuildGraph(root, {source});
                        TOperatorEndState endState;
                        SetTestEndStateUpdater(graph, endState);
                        std::map<ui64, ui64> expected;
                        const size_t rows = scenario == 2 ? 128 : 32768;
                        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
                            new TGeneratedWideStream(rows, [&](size_t row) {
                                const ui64 key = scenario == 1 && row < 8192 ? row / 4 : row;
                                const bool nullKey = optionalKey && key % 257 == 0;
                                const bool nullState = optionalState && row % 5 == 0;
                                expected[nullKey ? Max<ui64>() : key] += !nullState;
                                return std::vector<NUdf::TUnboxedValue>{
                                    nullKey ? NUdf::TUnboxedValuePod{} : NUdf::TUnboxedValuePod(key),
                                    nullState ? NUdf::TUnboxedValuePod{} : NUdf::TUnboxedValuePod(ui64{1})};
                            })));
                        auto stream = graph->GetValue();
                        NUdf::TUnboxedValue output[2];
                        std::map<ui64, ui64> actual;
                        for (;;) {
                            const auto status = stream.WideFetch(output, 2);
                            if (status == NUdf::EFetchStatus::Finish) break;
                            UNIT_ASSERT_VALUES_EQUAL(status, NUdf::EFetchStatus::Ok);
                            UNIT_ASSERT(optionalKey || output[0]);
                            UNIT_ASSERT(optionalState || output[1]);
                            if (output[1]) UNIT_ASSERT(output[1].Get<ui64>() > 0);
                            actual[output[0] ? output[0].Get<ui64>() : Max<ui64>()] += output[1] ? output[1].Get<ui64>() : 0;
                        }
                        UNIT_ASSERT(actual == expected);
                        UNIT_ASSERT_VALUES_EQUAL(endState.WasBypassActive, scenario == 0);
                    }
                }
            }
        }
    }


    Y_UNIT_TEST_QUAD(TestRequiredNativeStateRejectsEmptyValue, UseLLVM, UseFlow) {
        // These exceptions are only thrown in assertions-enabled builds
        for (const ui32 bits : {16, 32, 64}) {
            for (const bool failInit : {false, true}) {
                for (const bool isAggregator : {false, true}) {
                    for (const ui32 shape : {0, 1, 2}) {
                        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
                        auto& pb = setup.GetDqProgramBuilder();
                        auto* type = pb.NewDataType(bits == 16 ? NUdf::TDataType<ui16>::Id :
                            bits == 32 ? NUdf::TDataType<ui32>::Id : NUdf::TDataType<ui64>::Id);
                        const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
                            pb.NewStreamType(pb.NewMultiType({type}))).Build();
                        auto results = [&](TRuntimeNode::TList items) {
                            if (shape == 1) {
                                items.push_back(pb.template NewDataLiteral<NUdf::EDataSlot::String>("inline"));
                            } else if (shape == 2) {
                                items.push_back(pb.NewEmptyOptional(pb.NewOptionalType(type)));
                            }
                            return items;
                        };
                        TRuntimeNode input(source, false);
                        if (UseFlow) {
                            input = pb.ToFlow(input, {});
                        }
                        auto root = GetOperatorNode(pb, isAggregator, false, 128_MB, input,
                            [&](TRuntimeNode::TList) -> TRuntimeNode::TList {
                                return {pb.template NewDataLiteral<ui32>(0)};
                            },
                            [&](TRuntimeNode::TList, TRuntimeNode::TList items) { return results(items); },
                            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList) { return results(items); },
                            [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; });
                        if (UseFlow) {
                            root = pb.FromFlow(root);
                        }
                        auto graph = setup.BuildGraph(root, {source});
                        // Deliberately violate the declared input type to exercise required-state validation
                        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
                            new TGeneratedWideStream(failInit ? 1 : 2, [&](size_t row) {
                                return std::vector<NUdf::TUnboxedValue>{
                                    row == 0 && !failInit ? NUdf::TUnboxedValuePod(ui64{0}) : NUdf::TUnboxedValuePod{}};
                            })));
                        auto stream = graph->GetValue();
                        std::vector<NUdf::TUnboxedValue> output(shape ? 2 : 1);
                        UNIT_ASSERT_EXCEPTION_CONTAINS(stream.WideFetch(output.data(), output.size()), yexception,
                            "Empty value for required native aggregation state");
                    }
                }
            }
        }
    }


    Y_UNIT_TEST_QUAD(TestHomogeneousOptionalState, UseLLVM, UseFlow) {
        for (const ui32 bits : {16, 32, 64}) {
            for (const size_t width : {1, 5, 32, 33}) {
                for (const bool isAggregator : {false, true}) {
                    TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
                    auto& pb = setup.GetDqProgramBuilder();
                    const auto optionalType = pb.NewOptionalType(pb.NewDataType(bits == 16 ? NUdf::TDataType<i16>::Id : bits == 32 ?
                        NUdf::TDataType<i32>::Id : NUdf::TDataType<i64>::Id));
                    const auto inputType = pb.NewStreamType(pb.NewMultiType({
                        pb.NewDataType(NUdf::TDataType<ui32>::Id),
                        pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id)),
                        pb.NewDataType(NUdf::TDataType<ui64>::Id),
                        pb.NewDataType(NUdf::TDataType<char*>::Id),
                    }));
                    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", inputType).Build();
                    TRuntimeNode input(source, false);
                    if (UseFlow) {
                        input = pb.ToFlow(input, {});
                    }
                    auto root = GetOperatorNode(
                        pb, isAggregator, false, 128ull << 20, input,
                        [&](TRuntimeNode::TList) -> TRuntimeNode::TList {
                            return {pb.template NewDataLiteral<ui32>(0)};
                        },
                        [&](TRuntimeNode::TList, TRuntimeNode::TList) -> TRuntimeNode::TList {
                            TRuntimeNode::TList state;
                            for (size_t i = 0; i < width; ++i) {
                                state.push_back(i % 2 ? pb.NewEmptyOptional(optionalType) :
                                    pb.Convert(pb.NewOptional(pb.template NewDataLiteral<i64>(-i64(i) - 1)), optionalType));
                            }
                            return state;
                        },
                        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) {
                            std::rotate(state.begin(), state.begin() + 1, state.end());
                            state.back() = pb.Convert(items[1], optionalType);
                            return state;
                        },
                        [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; });
                    if (UseFlow) {
                        root = pb.FromFlow(root);
                    }
                    auto graph = setup.BuildGraph(root, {source});
                    TMixedReference reference;
                    const size_t rowCount = 2 * width + 5;
                    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(),
                        NUdf::TUnboxedValuePod(new TMixedWideStream(rowCount, reference)));
                    auto stream = graph->GetValue();
                    std::vector<NUdf::TUnboxedValue> output(width);
                    UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Ok);
                    for (size_t i = 0; i < width; ++i) {
                        const auto row = rowCount - width + i;
                        UNIT_ASSERT_VALUES_EQUAL(bool(output[i]), row % 5 != 0);
                        if (output[i]) {
                            UNIT_ASSERT_VALUES_EQUAL((bits == 16 ? output[i].Get<i16>() : bits == 32 ? output[i].Get<i32>() : output[i].Get<i64>()), i64(row % 11));
                        }
                    }
                    UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Finish);
                }
            }
        }
    }

    Y_UNIT_TEST_QUAD(TestMixedWidthState, UseLLVM, UseFlow) {
        for (const bool required : {false, true}) {
            for (const size_t width : {2, 32, 33, 65}) {
                for (const bool isAggregator : {false, true}) {
                    TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
                    auto& pb = setup.GetDqProgramBuilder();
                    const auto int32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
                    const auto int64Type = pb.NewDataType(NUdf::TDataType<i64>::Id);
                    const auto optional16 = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i16>::Id));
                    const auto optional32 = pb.NewOptionalType(int32Type);
                    const auto optional64 = pb.NewOptionalType(int64Type);
                    const auto inputType = pb.NewStreamType(pb.NewMultiType({
                        pb.NewDataType(NUdf::TDataType<ui32>::Id), optional64,
                        pb.NewDataType(NUdf::TDataType<ui64>::Id),
                        pb.NewDataType(NUdf::TDataType<char*>::Id),
                    }));
                    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", inputType).Build();
                    TRuntimeNode input(source, false);
                    if (UseFlow) {
                        input = pb.ToFlow(input, {});
                    }
                    const size_t prefix = required ? 4 : 0;
                    auto root = GetOperatorNode(
                        pb, isAggregator, false, 128ull << 20, input,
                        [&](TRuntimeNode::TList) -> TRuntimeNode::TList {
                            return {pb.template NewDataLiteral<ui32>(0)};
                        },
                        [&](TRuntimeNode::TList, TRuntimeNode::TList) -> TRuntimeNode::TList {
                            TRuntimeNode::TList state;
                            if (required) {
                                state = {pb.template NewDataLiteral<i32>(17),
                                    pb.NewTuple({pb.template NewDataLiteral<i64>(42)}),
                                    pb.template NewDataLiteral<i64>(99),
                                    pb.template NewDataLiteral<NUdf::EDataSlot::String>("initial")};
                            }
                            for (size_t i = 0; i < width; ++i) {
                                auto* type = i % 3 == 0 ? optional16 : i % 3 == 1 ? optional32 : optional64;
                                state.push_back(i % 3 ? pb.NewEmptyOptional(type) :
                                    pb.Convert(pb.NewOptional(pb.template NewDataLiteral<i64>(-i64(i) - 1)), type));
                            }
                            return state;
                        },
                        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) {
                            auto result = state;
                            if (required) {
                                result[0] = pb.Convert(pb.Nth(state[1], 0), int32Type);
                                result[1] = pb.NewTuple({state[2]});
                                result[2] = pb.Convert(state[0], int64Type);
                                result[3] = pb.ToString(state[0]);
                            }
                            for (size_t i = 0; i < width; ++i) {
                                result[prefix + i] = pb.Convert(i + 1 == width ? items[1] : state[prefix + i + 1],
                                    i % 3 == 0 ? optional16 : i % 3 == 1 ? optional32 : optional64);
                            }
                            return result;
                        },
                        [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; });
                    if (UseFlow) {
                        root = pb.FromFlow(root);
                    }
                    auto graph = setup.BuildGraph(root, {source});
                    TMixedReference reference;
                    const size_t rowCount = 2 * width + 5;
                    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(),
                        NUdf::TUnboxedValuePod(new TMixedWideStream(rowCount, reference)));
                    auto stream = graph->GetValue();
                    std::vector<NUdf::TUnboxedValue> output(prefix + width);
                    UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Ok);
                    if (required) {
                        const i64 initial[] = {17, 42, 99};
                        UNIT_ASSERT_VALUES_EQUAL(output[0].Get<i32>(), initial[(rowCount - 1) % 3]);
                        UNIT_ASSERT_VALUES_EQUAL(output[1].GetElement(0).Get<i64>(), initial[rowCount % 3]);
                        UNIT_ASSERT_VALUES_EQUAL(output[2].Get<i64>(), initial[(rowCount + 1) % 3]);
                        UNIT_ASSERT_VALUES_EQUAL(std::string(output[3].AsStringRef()), std::to_string(initial[(rowCount - 2) % 3]));
                    }
                    for (size_t i = 0; i < width; ++i) {
                        const auto row = rowCount - width + i;
                        const auto& value = output[prefix + i];
                        UNIT_ASSERT_VALUES_EQUAL(bool(value), row % 5 != 0);
                        if (value) {
                            UNIT_ASSERT_VALUES_EQUAL((i % 3 == 0 ? value.Get<i16>() : i % 3 == 1 ? value.Get<i32>() : value.Get<i64>()), i64(row % 11));
                        }
                    }
                    UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Finish);
                }
            }
        }
    }

    Y_UNIT_TEST_QUAD(TestNative32Payloads, UseLLVM, UseFlow) {
        for (const bool optional : {false, true}) {
            for (const bool isAggregator : {false, true}) {
                TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
                auto& pb = setup.GetDqProgramBuilder();
                const auto inputType = pb.NewStreamType(pb.NewMultiType({
                    pb.NewDataType(NUdf::TDataType<ui32>::Id),
                    pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id)),
                    pb.NewDataType(NUdf::TDataType<ui64>::Id),
                    pb.NewDataType(NUdf::TDataType<char*>::Id),
                }));
                const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", inputType).Build();
                TRuntimeNode input(source, false);
                if (UseFlow) {
                    input = pb.ToFlow(input, {});
                }
                const ui32 nanBits = 0x7fc01234;
                auto root = GetOperatorNode(
                    pb, isAggregator, false, 128ull << 20, input,
                    [&](TRuntimeNode::TList) -> TRuntimeNode::TList {
                        return {pb.template NewDataLiteral<ui32>(0)};
                    },
                    [&](TRuntimeNode::TList, TRuntimeNode::TList) -> TRuntimeNode::TList {
                        TRuntimeNode::TList state = {
                            pb.template NewDataLiteral<i32>(-7),
                            pb.template NewDataLiteral<i32>(std::numeric_limits<i32>::min()),
                            pb.template NewDataLiteral<ui32>(std::numeric_limits<ui32>::max()),
                            pb.template NewDataLiteral<float>(-0.0f),
                            pb.template NewDataLiteral<float>(std::bit_cast<float>(nanBits)),
                        };
                        if (optional) {
                            for (auto& value : state) {
                                value = pb.NewOptional(value);
                            }
                        }
                        return state;
                    },
                    [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                        return {state[1], state[0], state[2], state[4], state[3]};
                    },
                    [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; });
                if (UseFlow) {
                    root = pb.FromFlow(root);
                }
                auto graph = setup.BuildGraph(root, {source});
                TMixedReference reference;
                graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(),
                    NUdf::TUnboxedValuePod(new TMixedWideStream(6, reference)));
                auto stream = graph->GetValue();
                NUdf::TUnboxedValue output[5];
                UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output, 5), NUdf::EFetchStatus::Ok);
                UNIT_ASSERT_VALUES_EQUAL(output[0].Get<i32>(), std::numeric_limits<i32>::min());
                UNIT_ASSERT_VALUES_EQUAL(output[1].Get<i32>(), -7);
                UNIT_ASSERT_VALUES_EQUAL(output[2].Get<ui32>(), std::numeric_limits<ui32>::max());
                UNIT_ASSERT_VALUES_EQUAL(std::bit_cast<ui32>(output[3].Get<float>()), nanBits);
                UNIT_ASSERT_VALUES_EQUAL(std::bit_cast<ui32>(output[4].Get<float>()), ui32{1} << 31);
                UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output, 5), NUdf::EFetchStatus::Finish);
            }
        }
    }

    Y_UNIT_TEST_QUAD(TestCrossDependentUnboxedState, UseLLVM, UseFlow) {
        for (const bool isAggregator : {false, true}) {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            auto& pb = setup.GetDqProgramBuilder();
            const auto inputType = pb.NewStreamType(pb.NewMultiType({
                pb.NewDataType(NUdf::TDataType<ui32>::Id),
                pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id)),
                pb.NewDataType(NUdf::TDataType<ui64>::Id),
                pb.NewDataType(NUdf::TDataType<char*>::Id),
            }));
            const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", inputType).Build();
            TRuntimeNode input(source, false);
            if (UseFlow) {
                input = pb.ToFlow(input, {});
            }
            auto root = GetOperatorNode(
                pb, isAggregator, false, 128ull << 20, input,
                [&](TRuntimeNode::TList) -> TRuntimeNode::TList {
                    return {pb.template NewDataLiteral<ui32>(0)};
                },
                [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                    auto shortString = pb.template NewDataLiteral<NUdf::EDataSlot::String>("short");
                    return {items[3], shortString, pb.NewTuple({items[3]}), pb.NewTuple({shortString})};
                },
                [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                    return {state[1], state[0], state[3], state[2]};
                },
                [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; });
            if (UseFlow) {
                root = pb.FromFlow(root);
            }
            auto graph = setup.BuildGraph(root, {source});
            TMixedReference reference;
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(),
                NUdf::TUnboxedValuePod(new TMixedWideStream(6, reference)));
            auto stream = graph->GetValue();
            std::vector<NUdf::TUnboxedValue> output(4);
            UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(std::string(output[0].AsStringRef()), "short");
            UNIT_ASSERT_VALUES_EQUAL(std::string(output[1].AsStringRef()), "long-state-value-00000000");
            const auto firstElement = output[2].GetElement(0);
            const auto secondElement = output[3].GetElement(0);
            UNIT_ASSERT_VALUES_EQUAL(std::string(firstElement.AsStringRef()), "short");
            UNIT_ASSERT_VALUES_EQUAL(std::string(secondElement.AsStringRef()), "long-state-value-00000000");
            UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Finish);
        }
    }

    Y_UNIT_TEST_QUAD(TestCrossDependentMixedState, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
        auto& pb = setup.GetDqProgramBuilder();
        const auto inputType = pb.NewStreamType(pb.NewMultiType({
            pb.NewDataType(NUdf::TDataType<ui32>::Id),
            pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id)),
            pb.NewDataType(NUdf::TDataType<ui64>::Id),
            pb.NewDataType(NUdf::TDataType<char*>::Id),
        }));
        const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", inputType).Build();
        TRuntimeNode input(source, false);
        if (UseFlow) {
            input = pb.ToFlow(input, {});
        }
        auto root = GetOperatorNode(
            pb, true, false, 128ull << 20, input,
            [&](TRuntimeNode::TList) -> TRuntimeNode::TList {
                return {pb.template NewDataLiteral<ui32>(0)};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {pb.template NewDataLiteral<ui64>(17), items[3], pb.template NewDataLiteral<ui64>(42), items[1]};
            },
            [](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {state[2], items[3], state[0], items[1]};
            },
            [](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return state;
            });
        if (UseFlow) {
            root = pb.FromFlow(root);
        }
        auto graph = setup.BuildGraph(root, {source});
        TMixedReference reference;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(),
            NUdf::TUnboxedValuePod(new TMixedWideStream(6, reference)));
        auto stream = graph->GetValue();
        std::vector<NUdf::TUnboxedValue> output(4);
        UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(output[0].Get<ui64>(), 42);
        UNIT_ASSERT_VALUES_EQUAL(std::string(output[1].AsStringRef()), "long-state-value-00000005");
        UNIT_ASSERT_VALUES_EQUAL(output[2].Get<ui64>(), 17);
        UNIT_ASSERT(!output[3]);
        UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(output.data(), output.size()), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_QUAD(TestMixedNativeAndStringValues, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunMixedWideTest(setup, UseFlow, true, false);
        }
        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunMixedWideTest(setup, UseFlow, true, true);
        }
        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunMixedWideTest(setup, UseFlow, false, true);
        }
    }

    Y_UNIT_TEST_QUAD(TestMixedNativeAndStringValuesWithSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunMixedWideTest(setup, UseFlow, true, false);
    }

    Y_UNIT_TEST_QUAD(TestFastFinalizeWithSpilling, UseLLVM, UseFlow) {
        RunFinalizeSpillingTest<UseLLVM>(UseFlow, false, false);
    }

    Y_UNIT_TEST_QUAD(TestBlockFastFinalizeWithSpilling, UseLLVM, UseFlow) {
        RunFinalizeSpillingTest<UseLLVM>(UseFlow, true, false);
    }

    Y_UNIT_TEST_QUAD(TestFinalizeExpressionWithSpilling, UseLLVM, UseFlow) {
        RunFinalizeSpillingTest<UseLLVM>(UseFlow, false, true);
        RunFinalizeSpillingTest<UseLLVM>(UseFlow, true, true);
    }

    Y_UNIT_TEST_QUAD(TestFastFinalizeEarlyStopAfterSpilling, UseLLVM, UseFlow) {
        RunFinalizeSpillingTest<UseLLVM>(UseFlow, false, false, true);
        RunFinalizeSpillingTest<UseLLVM>(UseFlow, true, false, true);
    }

    Y_UNIT_TEST_QUAD(TestRequiredNative16State, UseLLVM, UseFlow) {
        for (bool blocks : {false, true}) {
            RunTemporalAggregationTest<UseLLVM>(UseFlow, blocks, false, false, true);
            RunTemporalAggregationTest<UseLLVM>(UseFlow, blocks, true, false, true);
        }
    }

    Y_UNIT_TEST_QUAD(TestTemporalAggregation, UseLLVM, UseFlow) {
        RunTemporalAggregationTest<UseLLVM>(UseFlow, false, false, false);
        RunTemporalAggregationTest<UseLLVM>(UseFlow, false, true, false);
    }

    Y_UNIT_TEST_QUAD(TestBlockTemporalAggregation, UseLLVM, UseFlow) {
        RunTemporalAggregationTest<UseLLVM>(UseFlow, true, false, false);
        RunTemporalAggregationTest<UseLLVM>(UseFlow, true, true, false);
    }

    Y_UNIT_TEST_QUAD(TestTemporalAggregationWithSpilling, UseLLVM, UseFlow) {
        RunTemporalAggregationTest<UseLLVM>(UseFlow, false, true, true);
        RunTemporalAggregationTest<UseLLVM>(UseFlow, true, true, true);
    }

    Y_UNIT_TEST_QUAD(TestNativeStateExceptions, UseLLVM, UseFlow) {
        for (const bool init : {false, true}) {
            for (const ui32 bits : {16, 32, 64}) {
                RunThrowingStateTest<UseLLVM>(UseFlow, bits, 0, init);
            }
            for (const size_t width : {6, 36}) {
                RunThrowingStateTest<UseLLVM>(UseFlow, 64, width, init);
            }
        }
    }

    Y_UNIT_TEST_QUAD(TestStateExceptionDuringSpillReplay, UseLLVM, UseFlow) {
        RunThrowingStateTest<UseLLVM>(UseFlow, 64, 0, false, true);
    }

    Y_UNIT_TEST_QUAD(TestPartialKeyException, UseLLVM, UseFlow) {
        RunThrowingKeyTest<UseLLVM>(UseFlow, true);
        RunThrowingKeyTest<UseLLVM>(UseFlow, false);
    }

    Y_UNIT_TEST_QUAD(TestWideNullableKeyGrouping, UseLLVM, UseFlow) {
        for (const bool blocks : {false, true}) {
            RunKeyGroupingTest<UseLLVM>(UseFlow, blocks, false, false);
            RunKeyGroupingTest<UseLLVM>(UseFlow, blocks, false, true);
        }
    }

    Y_UNIT_TEST_QUAD(TestFloatingKeyGrouping, UseLLVM, UseFlow) {
        for (const bool blocks : {false, true}) {
            RunKeyGroupingTest<UseLLVM>(UseFlow, blocks, true, false);
            RunKeyGroupingTest<UseLLVM>(UseFlow, blocks, true, true);
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeNoInput, UseLLVM, UseFlow) {
        RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 0, 0, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeSingleRow, UseLLVM, UseFlow) {
        RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeMultiRows, UseLLVM, UseFlow) {
        auto endState = RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 20000, 5, columnTypes, keyWidth, refMap);
        });
        UNIT_ASSERT_C(!endState.WasBypassActive, "Bypass should NOT have been activated");
    }

    Y_UNIT_TEST_QUAD(TestWideModeBypass, UseLLVM, UseFlow) {
        auto endState = RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 1, columnTypes, keyWidth, refMap);
        });
        UNIT_ASSERT_C(endState.WasBypassActive, "Bypass should have been activated");

        endState = RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 1, columnTypes, keyWidth, refMap);
        });
        UNIT_ASSERT_C(endState.WasBypassActive, "Bypass should have been activated");
    }

    Y_UNIT_TEST_QUAD(TestBlockModeNoInput, UseLLVM, UseFlow) {
        RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 0, 0, 8192, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeSingleRow, UseLLVM, UseFlow) {
        RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 1, 1, 8192, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeMultiBlocks, UseLLVM, UseFlow) {
        auto endState = RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 20000, 10, 40000, columnTypes, keyWidth, refMap);
        });
        UNIT_ASSERT_C(!endState.WasBypassActive, "Bypass should NOT have been activated");
    }

    Y_UNIT_TEST_QUAD(TestBlockModeBypass, UseLLVM, UseFlow) {
        auto endState = RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 200000, 1, 40000, columnTypes, keyWidth, refMap);
        }, 2, false);
        UNIT_ASSERT_C(endState.WasBypassActive, "Bypass should have been activated");

        endState = RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 200000, 1, 40000, columnTypes, keyWidth, refMap);
        }, 2, true);
        UNIT_ASSERT_C(endState.WasBypassActive, "Bypass should have been activated");
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationNoInput, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());
            RunDqAggregateWideTest<UseLLVM, true>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 0, 0, columnTypes, keyWidth, refMap);
            });
        }

        {
            TDqSetup<UseLLVM, false> setup(GetHashCombineNodeFactory());
            RunDqAggregateWideTest<UseLLVM>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 0, 0, columnTypes, keyWidth, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationSingleRow, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());
            RunDqAggregateWideTest<UseLLVM, true>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
        {
            TDqSetup<UseLLVM, false> setup(GetHashCombineNodeFactory());
            RunDqAggregateWideTest<UseLLVM, false>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationZeroWidth, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());
            RunDqAggregateZeroWidthTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
        {
            TDqSetup<UseLLVM, false> setup(GetHashCombineNodeFactory());
            RunDqAggregateZeroWidthTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationWithSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, keyWidth, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationWithSpillingNonPassthrough, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, keyWidth, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        }, 2, true);
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationMultiRowNoSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetHashCombineNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeAggregationWithSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());
        RunDqAggregateBlockTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 100000, 5, 8192, columnTypes, keyWidth, refMap, [&](const size_t rowNum) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeAggregationMultiRowNoSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetHashCombineNodeFactory());
        RunDqAggregateBlockTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 10000, 5, 8192, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeAggregationPrefetchAcrossBlocks, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetHashCombineNodeFactory());
        RunDqAggregateBlockTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(
                ctx, DqAggregationPrefetchBatchSize + 2, 2, DqAggregationPrefetchBatchSize / 2 + 2, columnTypes, keyWidth, refMap
            );
        });
    }

    Y_UNIT_TEST_QUAD(TestEarlyStop, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetHashCombineNodeFactory());
        size_t lineCount = 0;

        auto streamCreator = [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100, 1, columnTypes, keyWidth, refMap);
        };

        auto streamChecker = [&lineCount](NUdf::EFetchStatus fetchStatus) -> bool {
            if (fetchStatus == NUdf::EFetchStatus::Ok) {
                ++lineCount;
            }
            return lineCount < 90;
        };

        RunDqAggregateEarlyStopTest(
            setup,
            UseFlow,
            streamCreator,
            streamChecker
        );
    }

    Y_UNIT_TEST_QUAD(TestEarlyStopInSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());

        bool stopping = false;

        auto streamCreator = [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 1000, 1, columnTypes, keyWidth, refMap, [&](const size_t rowNum, bool& yield) {
                if (rowNum == 100) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                } else if (rowNum == 200) {
                    stopping = true;
                    yield = true;
                }
            });
        };

        auto streamChecker = [&stopping](NUdf::EFetchStatus fetchStatus) -> bool {
            return !(fetchStatus == NUdf::EFetchStatus::Yield && stopping);
        };

        RunDqAggregateEarlyStopTest(
            setup,
            UseFlow,
            streamCreator,
            streamChecker
        );
    }

    Y_UNIT_TEST_QUAD(TestTeardownDuringStateSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());

        auto streamCreator = [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 1000, 1, columnTypes, keyWidth, refMap, [&](const size_t rowNum, bool&) {
                if (rowNum == 500) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        };

        auto streamChecker = [](NUdf::EFetchStatus fetchStatus) -> bool {
            return fetchStatus != NUdf::EFetchStatus::Yield;
        };

        // The never-completing spiller parks the state-spill coroutine on a pending write
        // after some buckets were already written out and released; destroying the graph
        // in this position must not double-release values still referenced from the arena
        // (https://github.com/ydb-platform/ydb/issues/40326, run under ASAN).
        RunDqAggregateEarlyStopTest(
            setup,
            UseFlow,
            streamCreator,
            streamChecker,
            std::make_shared<TPendingSpillerFactory>()
        );
    }

    Y_UNIT_TEST_QUAD(TestEarlyStopAfterSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetHashCombineNodeFactory());
        size_t lineCount = 0;

        auto streamCreator = [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 1, columnTypes, keyWidth, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        };

        auto streamChecker = [&lineCount](NUdf::EFetchStatus fetchStatus) -> bool {
            if (fetchStatus == NUdf::EFetchStatus::Ok) {
                ++lineCount;
            }
            return lineCount < 90;
        };

        RunDqAggregateEarlyStopTest(
            setup,
            UseFlow,
            streamCreator,
            streamChecker
        );
    }
} // Y_UNIT_TEST_SUITE

}
}
