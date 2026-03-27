#include "utils/dq_setup.h"
#include "utils/dq_factories.h"
#include "utils/preallocated_spiller.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>

#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

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

size_t UpdateMapFromBlocks(std::unordered_map<std::string, std::vector<ui64>>& map, const TArrayRef<const NYql::NUdf::TUnboxedValue>& values)
{
    UNIT_ASSERT(values.size() >= 3);
    size_t valuesCount = values.size() - 2; // exclude the key, exclude the block height column

    auto datumKey = TArrowBlock::From(values[0]).GetDatum();
    std::vector<std::shared_ptr<arrow::UInt64Array>> valueDatums;

    for (size_t i = 0; i < valuesCount; ++i) {
        valueDatums.push_back(TArrowBlock::From(values[i+1]).GetDatum().array_as<arrow::UInt64Array>());
    }

    int64_t valueOffset = 0;

    size_t numRows = 0;

    for (const auto& chunk : datumKey.chunks()) {
        auto* barray = dynamic_cast<arrow::BinaryArray*>(chunk.get());
        UNIT_ASSERT(barray != nullptr);
        for (int64_t i = 0; i < barray->length(); ++i) {
            auto key = barray->GetString(i);
            std::vector<ui64> values;
            values.reserve(valuesCount);
            for (size_t col = 0; col < valuesCount; ++col) {
                values.push_back(valueDatums[col]->Value(valueOffset));
            }
            AddRowToMap(map, key, values);
            ++valueOffset;
            ++numRows;
        }
    }

    return numRows;
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

    TWideStream(const TComputationContext& ctx, size_t numKeys, size_t iterations, const std::vector<TType*>& types, TRefMap& reference)
        : Context(ctx)
        , MaxIterations(iterations)
        , CurrKey(0)
        , NumKeys(numKeys)
        , Types(types)
        , Reference(reference)
    {
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) override = 0;

protected:
    bool IsAtTheEnd() const {
        return (CurrKey == NumKeys) && (Iteration + 1 >= MaxIterations);
    }

    ui64 NextSample() {
        if (CurrKey == NumKeys) {
            ++Iteration;
            if (Iteration >= MaxIterations) {
                ythrow yexception() << "out of samples" << Endl;
            }
            CurrKey = 0;
        }
        return CurrKey++;
    }

    static std::string FormatKey(size_t nextKey) {
        return Sprintf("%08u.%08u.%08u.", nextKey, nextKey, nextKey);
    }

    const TComputationContext& Context;
    const size_t MaxIterations;
    ui64 CurrKey;
    ui64 NumKeys;
    size_t Iteration = 0;
    const std::vector<TType*> Types;
    TRefMap& Reference;
};

class TBlockKVStream : public TWideStream {
public:
    using TCallback = std::function<void(const size_t rowNum)>;

    TBlockKVStream(const TComputationContext& ctx, size_t numKeys, size_t iterations, size_t blockSize, const std::vector<TType*>& types,
        TRefMap& reference, TCallback callback = {})
        : TWideStream(ctx, numKeys, iterations, types, reference)
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

        TVector<std::unique_ptr<IArrayBuilder>> builders;
        std::transform(Types.cbegin(), Types.cend(), std::back_inserter(builders),
        [&](const auto& type) {
            return MakeArrayBuilder(TTypeInfoHelper(), type, Context.ArrowMemoryPool, BlockSize, &Context.Builder->GetPgBuilder());
        });

        size_t count = 0;
        for (; count < BlockSize; ++count) {
            if (IsAtTheEnd()) {
                break;
            }
            ui64 nextKey = NextSample();

            NUdf::TUnboxedValuePod keyUV;
            std::string strKey = FormatKey(nextKey);
            NativeToUnboxed<false>(strKey, keyUV);
            builders[0]->Add(keyUV);
            keyUV.DeleteUnreferenced();

            std::vector<ui64> refValues;
            refValues.reserve(Types.size() - 1);
            for (ui64 i = 1; i < Types.size(); ++i) {
                NUdf::TUnboxedValuePod valueUV;
                ui64 val = (nextKey % 1000) + i;
                refValues.push_back(val);
                NativeToUnboxed<false>(val, valueUV);
                builders[1]->Add(valueUV);
                valueUV.DeleteUnreferenced();
            }

            AddRowToMap(Reference, strKey, refValues);
            ++ItemCount;
            if (Callback) {
                Callback(ItemCount);
            }
        }

        if (count > 0) {
            const bool finish = IsAtTheEnd();
            for (size_t i = 0; i < Types.size(); ++i) {
                result[i] = Context.HolderFactory.CreateArrowBlock(builders[i]->Build(finish));
            }
            result[Types.size()] = Context.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(count)));
            return NUdf::EFetchStatus::Ok;
        } else {
            return NUdf::EFetchStatus::Finish;
        }
    }
};

class TWideKVStream : public TWideStream {
public:
    using TCallback = std::function<void(const size_t rowCount, bool& yield)>;

    TWideKVStream(const TComputationContext& ctx, size_t numKeys, size_t iterations, const std::vector<TType*>& types,
        TRefMap& reference, TCallback callback = {})
        : TWideStream(ctx, numKeys, iterations, types, reference)
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

        std::string strKey = FormatKey(nextKey);
        NYql::NUdf::TUnboxedValuePod keyUV;
        NativeToUnboxed<false>(strKey, keyUV);
        result[0] = keyUV;

        std::vector<ui64> refValues;
        refValues.reserve(Types.size() - 1);
        for (ui64 i = 1; i < Types.size(); ++i) {
            NYql::NUdf::TUnboxedValuePod valueUV;
            ui64 val = (nextKey % 1000) + i;
            refValues.push_back(val);
            NativeToUnboxed<false>(val, valueUV);
            result[i] = valueUV;
        }

        AddRowToMap(Reference, strKey, refValues);

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
THolder<IComputationGraph> BuildBlockGraph(TDqSetup<UseLLVM, Spilling>& setup, bool useFlow, bool isAggregator, const size_t memLimit, std::vector<TType*>& columnTypes) {
    auto& pb = setup.GetDqProgramBuilder();

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto valueBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto keyBlockType = pb.NewBlockType(keyBaseType, TBlockType::EShape::Many);
    auto valueBlockType = pb.NewBlockType(valueBaseType, TBlockType::EShape::Many);
    auto blockSizeType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto blockSizeBlockType = pb.NewBlockType(blockSizeType, TBlockType::EShape::Scalar);
    const auto streamItemType = pb.NewMultiType({keyBlockType, valueBlockType, blockSizeBlockType});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamResultItemType = pb.NewMultiType({keyBlockType, valueBlockType, blockSizeBlockType});
    [[maybe_unused]] const auto streamResultType = pb.NewStreamType(streamResultItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();

    columnTypes = {keyBaseType, valueBaseType};

    TRuntimeNode rootNode;
    if (useFlow) {
        rootNode = pb.FromFlow(
            GetOperatorNode(
                pb,
                isAggregator,
                Spilling,
                memLimit,
                pb.ToFlow(TRuntimeNode(streamCallable, false)),
                [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
                [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
                [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                    return {pb.AggrAdd(state.front(), items.back())};
                },
                [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                    return {keys.front(), state.front()};
                }
            )
        );
    } else {
        rootNode = GetOperatorNode(
            pb,
            isAggregator,
            Spilling,
            memLimit,
            TRuntimeNode(streamCallable, false),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back())};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {keys.front(), state.front()};
            }
        );
    }

    return setup.BuildGraph(rootNode, {streamCallable});
}

template<bool LLVM, bool Spilling = false>
THolder<IComputationGraph> BuildWideGraph(
    TDqSetup<LLVM, Spilling>& setup, const bool useFlow, const bool isAggregator,
    const size_t memLimit, std::vector<TType*>& columnTypes)
{
    auto& pb = setup.GetDqProgramBuilder();

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto valueBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto streamItemType = pb.NewMultiType({keyBaseType, valueBaseType});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamResultItemType = pb.NewMultiType({keyBaseType, valueBaseType});
    [[maybe_unused]] const auto streamResultType = pb.NewStreamType(streamResultItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();

    columnTypes = {keyBaseType, valueBaseType};

    TRuntimeNode input = TRuntimeNode(streamCallable, false);
    if (useFlow) {
        input = pb.ToFlow(input);
    }

    TRuntimeNode opNode = GetOperatorNode(
        pb,
        isAggregator,
        Spilling,
        memLimit,
        input,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {pb.AggrAdd(state.front(), items.back())};
        },
        [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {keys.front(), state.front()};
        }
    );

    TRuntimeNode rootNode;
    if (useFlow) {
        opNode = pb.FromFlow(opNode);
    }
    rootNode = opNode;

    return setup.BuildGraph(rootNode, {streamCallable});
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

    TRuntimeNode rootNode;
    if (useFlow) {
        rootNode = pb.FromFlow(
            GetOperatorNode(
                pb,
                isAggregator,
                Spilling,
                memLimit,
                pb.ToFlow(TRuntimeNode(streamCallable, false)),
                [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
                [&](TRuntimeNode::TList, [[maybe_unused]] TRuntimeNode::TList items) -> TRuntimeNode::TList { return { } ; },
                [&](TRuntimeNode::TList, [[maybe_unused]] TRuntimeNode::TList items, [[maybe_unused]] TRuntimeNode::TList state) -> TRuntimeNode::TList {
                    return {};
                },
                [&]([[maybe_unused]] TRuntimeNode::TList keys, [[maybe_unused]] TRuntimeNode::TList state) -> TRuntimeNode::TList {
                    return {};
                }
            )
        );
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
            [&]([[maybe_unused]] TRuntimeNode::TList keys, [[maybe_unused]] TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {};
            }
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

template<typename TMap>
size_t CollectStreamOutputs(const NUdf::TUnboxedValue& wideStream, const ui32 resultWidth, TMap& resultMap, const bool useBlocks, const bool sleepOnYield)
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
            lineCount += UpdateMapFromBlocks(resultMap, fetchedValues);
        } else {
            std::vector<ui64> valuesVec;
            valuesVec.reserve(resultWidth);
            TArrayRef aggregates(fetchedValues.data() + 1, resultWidth - 1);
            for (const NYql::NUdf::TUnboxedValue& value : aggregates) {
                valuesVec.push_back(UnboxedToNative<ui64>(value));
            }
            AddRowToMap(resultMap, UnboxedToNative<std::string>(fetchedValues[0]), valuesVec);
            ++lineCount;
        }
    }

    return lineCount;
}

template<bool UseLLVM, typename StreamCreator>
void RunDqCombineBlockTest(const bool useFlow, StreamCreator streamCreator)
{
    TDqSetup<UseLLVM> setup(GetDqNodeFactory());

    std::vector<TType*> columnTypes;

    auto graph = BuildBlockGraph(setup, useFlow, false, 128ull << 20, columnTypes);

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    CollectStreamOutputs(resultStream, columnTypes.size() + 1, graphResult, true, true);

    AssertMapsEqual(refResult, graphResult);
}

template<bool UseLLVM, typename StreamCreator>
void RunDqCombineWideTest(const bool useFlow, StreamCreator streamCreator)
{
    TDqSetup<UseLLVM> setup(GetDqNodeFactory());

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, false, 128ull << 20, columnTypes);

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    CollectStreamOutputs(resultStream, columnTypes.size(), graphResult, false, true);

    AssertMapsEqual(refResult, graphResult);
}

void DisableDehydration(THolder<IComputationGraph>& graph)
{
    for (auto& node : graph->GetNodes()) {
        auto* testPoints = dynamic_cast<TDqHashCombineTestPoints*>(node.Get());
        if (!testPoints) {
            continue;
        }
        testPoints->DisableStateDehydration(true);
        return;
    }
    UNIT_ASSERT_C(false, "Couldn't find a DqHashCombine node wrapper in the graph");
}

template<bool UseLLVM, bool Spilling, typename StreamCreator, typename StreamChecker>
void RunDqAggregateEarlyStopTest(TDqSetup<UseLLVM, Spilling>& setup, const bool useFlow,
    StreamCreator streamCreator, StreamChecker streamChecker, const bool disableDehydration)
{
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, true, 0, columnTypes);

    if (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    if (disableDehydration) {
        DisableDehydration(graph);
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
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
void RunDqAggregateBlockTest(TDqSetup<UseLLVM, Spilling>& setup, const bool useFlow, StreamCreator streamCreator)
{
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildBlockGraph(setup, useFlow, true, 0, columnTypes);

    if (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    size_t numResultRows = CollectStreamOutputs(resultStream, columnTypes.size() + 1, graphResult, true, true);

    UNIT_ASSERT(numResultRows == refResult.size());
    AssertMapsEqual(refResult, graphResult);
}

template<bool LLVM, bool Spilling, typename StreamCreator>
void RunDqAggregateWideTest(TDqSetup<LLVM, Spilling>& setup, const bool useFlow, StreamCreator streamCreator, const bool disableDehydration = false)
{
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, true, 0, columnTypes);

    if (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    if (disableDehydration) {
        DisableDehydration(graph);
    }

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    size_t numResultItems = CollectStreamOutputs(resultStream, columnTypes.size(), graphResult, false, true);

    UNIT_ASSERT(numResultItems == refResult.size());
    AssertMapsEqual(refResult, graphResult);
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

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultStream = graph->GetValue();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;
    size_t numResultItems = CollectStreamOutputs(resultStream, 0, graphResult, false, true);

    UNIT_ASSERT(numResultItems == refResult.size());
}


} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqHashCombineTest) {

    Y_UNIT_TEST_QUAD(TestBlockModeNoInput, UseLLVM, UseFlow) {
        RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 0, 0, 8192, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeSingleRow, UseLLVM, UseFlow) {
        RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 1, 1, 8192, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeMultiBlocks, UseLLVM, UseFlow) {
        RunDqCombineBlockTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 20000, 5, 8192, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeNoInput, UseLLVM, UseFlow) {
        RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 0, 0, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeSingleRow, UseLLVM, UseFlow) {
        RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 1, 1, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeMultiRows, UseLLVM, UseFlow) {
        RunDqCombineWideTest<UseLLVM>(UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 20000, 5, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationNoInput, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM, true>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
                return new TWideKVStream(ctx, 0, 0, columnTypes, refMap);
            });
        }

        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
                return new TWideKVStream(ctx, 0, 0, columnTypes, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationSingleRow, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM, true>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, refMap);
            });
        }
        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM, false>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationZeroWidth, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            RunDqAggregateZeroWidthTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, refMap);
            });
        }
        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunDqAggregateZeroWidthTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationWithSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationWithSpillingNonDehydrated, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        }, true);
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationMultiRowNoSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeAggregationWithSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunDqAggregateBlockTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 100000, 5, 8192, columnTypes, refMap, [&](const size_t rowNum) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeAggregationMultiRowNoSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
        RunDqAggregateBlockTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 10000, 5, 8192, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestEarlyStop, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
        size_t lineCount = 0;

        auto streamCreator = [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 100, 1, columnTypes, refMap);
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
            streamChecker,
            false
        );

        lineCount = 0;

        RunDqAggregateEarlyStopTest(
            setup,
            UseFlow,
            streamCreator,
            streamChecker,
            true
        );
    }

    Y_UNIT_TEST_QUAD(TestEarlyStopInSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());

        bool stopping = false;

        auto streamCreator = [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 1000, 1, columnTypes, refMap, [&](const size_t rowNum, bool& yield) {
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
            streamChecker,
            false
        );

        stopping = false;

        RunDqAggregateEarlyStopTest(
            setup,
            UseFlow,
            streamCreator,
            streamChecker,
            true
        );
    }

    Y_UNIT_TEST_QUAD(TestEarlyStopAfterSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        size_t lineCount = 0;

        auto streamCreator = [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 1, columnTypes, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
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
            streamChecker,
            false
        );

        lineCount = 0;

        RunDqAggregateEarlyStopTest(
            setup,
            UseFlow,
            streamCreator,
            streamChecker,
            true
        );
    }
} // Y_UNIT_TEST_SUITE

}
}