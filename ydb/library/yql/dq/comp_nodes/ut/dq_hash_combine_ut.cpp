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

void DisableDehydration(THolder<IComputationGraph>& graph)
{
    ApplyTestPoint(graph, [](TDqHashCombineTestPoints& tp) {
        tp.DisableStateDehydration(true);
    });
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
    auto finish = [](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
        TRuntimeNode::TList result = keys;
        result.insert(result.end(), state.begin(), state.end());
        return result;
    };

    TRuntimeNode rootNode;
    if (useFlow) {
        rootNode = pb.FromFlow(
            GetOperatorNode(
                pb,
                isAggregator,
                Spilling,
                memLimit,
                pb.ToFlow(TRuntimeNode(streamCallable, false), {}),
                keyExtractor,
                initState,
                updateState,
                finish
            )
        );
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
            return result;
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
                pb.ToFlow(TRuntimeNode(streamCallable, false), {}),
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

class TPendingReadSpiller: public TPreallocatedSpiller {
public:
    TPendingReadSpiller(TBuffer& dataStore, size_t pauseOnRead)
        : TPreallocatedSpiller(dataStore)
        , PauseOnRead(pauseOnRead)
    {}

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) override {
        if (++ReadCount == PauseOnRead) {
            PendingKey = key;
            return PendingRead.GetFuture();
        }
        return TPreallocatedSpiller::Get(key);
    }

    size_t GetReadCount() const {
        return ReadCount;
    }

    void ResumeRead(bool missing) {
        PendingRead.SetValue(missing ? std::nullopt : TPreallocatedSpiller::Get(PendingKey).ExtractValueSync());
    }

private:
    const size_t PauseOnRead;
    size_t ReadCount = 0;
    TKey PendingKey = 0;
    NThreading::TPromise<std::optional<NYql::TChunkedBuffer>> PendingRead =
        NThreading::NewPromise<std::optional<NYql::TChunkedBuffer>>();
};

class TPendingReadSpillerFactory: public ISpillerFactory {
public:
    explicit TPendingReadSpillerFactory(size_t pauseOnRead)
        : DataStore(4_MB)
        , Spiller(std::make_shared<TPendingReadSpiller>(DataStore, pauseOnRead))
    {}

    void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>&) override {
    }
    void SetMemoryReportingCallbacks(ISpiller::TMemoryReportCallback, ISpiller::TMemoryReportCallback) override {
    }
    ISpiller::TPtr CreateSpiller() override {
        return Spiller;
    }
    size_t GetReadCount() const {
        return Spiller->GetReadCount();
    }
    void ResumeRead(bool missing) {
        Spiller->ResumeRead(missing);
    }

private:
    TBuffer DataStore;
    std::shared_ptr<TPendingReadSpiller> Spiller;
};

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

template<bool LLVM>
void RunTeardownDuringStateReadBackTest(bool useFlow, bool disableDehydration, size_t pauseOnRead) {
    TDqSetup<LLVM, true> setup(GetDqNodeFactory());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    auto& pb = setup.GetDqProgramBuilder();
    const size_t inputWidth = 16;
    const size_t rowCount = 4096;
    std::vector<TType*> types(inputWidth, pb.NewDataType(NUdf::TDataType<ui64>::Id));
    types[0] = pb.NewDataType(NUdf::TDataType<char*>::Id);
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
        pb.NewStreamType(pb.NewMultiType(types))).Build();
    TRuntimeNode input(source, false);
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto root = pb.DqHashAggregate(input, true,
        [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
        [](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; },
        [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
        [](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys[0], state[0]}; });
    if (useFlow) {
        root = pb.FromFlow(root);
    }
    auto graph = setup.BuildGraph(root, {source});
    auto spiller = std::make_shared<TPendingReadSpillerFactory>(pauseOnRead);
    graph->GetContext().SpillerFactory = spiller;
    if (disableDehydration) {
        DisableDehydration(graph);
    }
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
        new TGeneratedWideStream(rowCount, [&](size_t row) {
            // Spill the complete state so each Get starts a new bucket
            if (row + 1 == rowCount) {
                setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
            }
            std::vector<NUdf::TUnboxedValue> values(inputWidth, NUdf::TUnboxedValuePod(ui64{1}));
            values[0] = NUdf::TUnboxedValuePod(NUdf::TStringValue(Sprintf("read-back heap string key %08zu", row)));
            return values;
        })));
    auto stream = graph->GetValue();
    std::vector<NUdf::TUnboxedValue> output(2);
    std::vector<NUdf::TUnboxedValue> drainedKeys;
    NUdf::EFetchStatus status;
    while ((status = stream.WideFetch(output.data(), output.size())) == NUdf::EFetchStatus::Ok) {
        drainedKeys.push_back(output[0]);
    }
    UNIT_ASSERT_VALUES_EQUAL(status, NUdf::EFetchStatus::Yield);
    UNIT_ASSERT_VALUES_EQUAL(spiller->GetReadCount(), pauseOnRead);
    UNIT_ASSERT_VALUES_EQUAL(drainedKeys.empty(), pauseOnRead == 1);
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    output.clear();
    stream = {};
    graph.Destroy();
    for (const auto& key : drainedKeys) {
        UNIT_ASSERT_VALUES_EQUAL(key.RefCount(), 1);
    }
}

template<bool LLVM>
void RunPassthroughKeyInitFailureTest(bool useFlow, bool useBlocks, bool stringState) {
    TDqSetup<LLVM> setup(GetDqNodeFactory());
    auto& pb = setup.GetDqProgramBuilder();
    auto* stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto* optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui64>::Id));
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
        pb.NewStreamType(pb.NewMultiType({stringType, stringType, optionalType}))).Build();
    TRuntimeNode input(source, false);
    if (useBlocks) {
        input = pb.WideToBlocks(input);
    }
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto root = pb.DqHashAggregate(input, false,
        [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
            return {
                stringState ? items[1] : pb.template NewDataLiteral<ui64>(1),
                pb.Unwrap(items[2], pb.template NewDataLiteral<NUdf::EDataSlot::String>("passthrough key init failure"),
                    __FILE__, __LINE__, 0)
            };
        },
        [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
        [](TRuntimeNode::TList keys, TRuntimeNode::TList) { return keys; });
    if (useFlow) {
        root = pb.FromFlow(root);
    }
    if (useBlocks) {
        root = pb.WideFromBlocks(root);
    }
    NUdf::TUnboxedValue key = NUdf::TUnboxedValuePod(NUdf::TStringValue("passthrough heap string key"));
    NUdf::TUnboxedValue state = NUdf::TUnboxedValuePod(NUdf::TStringValue("partially initialized heap string state"));
    auto graph = setup.BuildGraph(root, {source});
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
        new TGeneratedWideStream(1, [&](size_t) {
            return std::vector<NUdf::TUnboxedValue>{key, state, NUdf::TUnboxedValuePod{}};
        })));
    auto stream = graph->GetValue();
    NUdf::TUnboxedValue output;
    {
        TThrowingBindTerminator terminator;
        UNIT_ASSERT_EXCEPTION_CONTAINS(stream.WideFetch(&output, 1), TTerminateException, "passthrough key init failure");
    }
    stream = {};
    graph.Destroy();
    UNIT_ASSERT_VALUES_EQUAL(key.RefCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(state.RefCount(), 1);
}

enum class ESpillReplayStop {
    PendingRead,
    MissingInput,
    ThrowingUpdate,
    Output,
};

template<bool LLVM>
void RunSpillReplayTeardownTest(bool useFlow, bool useBlocks, ESpillReplayStop stop) {
    TDqSetup<LLVM, true> setup(GetDqNodeFactory());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    auto& pb = setup.GetDqProgramBuilder();
    const bool throwOnUpdate = stop == ESpillReplayStop::ThrowingUpdate;
    auto* stringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto* keyType = throwOnUpdate ? pb.NewDataType(NUdf::TDataType<ui64>::Id) : stringType;
    auto* optionalType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<ui64>::Id));
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
        pb.NewStreamType(pb.NewMultiType({keyType, stringType, optionalType}))).Build();
    TRuntimeNode input(source, false);
    if (useBlocks) {
        input = pb.WideToBlocks(input);
    }
    if (useFlow) {
        input = pb.ToFlow(input, {});
    }
    auto root = pb.DqHashAggregate(input, true,
        [](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[0]}; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
            return {pb.NewTuple({items[1]}), pb.template NewDataLiteral<ui64>(1)};
        },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            if (throwOnUpdate) {
                return {pb.NewTuple({items[1]}),
                    pb.Unwrap(items[2], pb.template NewDataLiteral<NUdf::EDataSlot::String>("spill replay update failure"),
                        __FILE__, __LINE__, 0)};
            }
            return state;
        },
        [](TRuntimeNode::TList keys, TRuntimeNode::TList) { return keys; });
    if (useFlow) {
        root = pb.FromFlow(root);
    }
    if (useBlocks) {
        root = pb.WideFromBlocks(root);
    }
    auto graph = setup.BuildGraph(root, {source});
    auto spiller = std::make_shared<TPendingReadSpillerFactory>(2);
    graph->GetContext().SpillerFactory = spiller;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), NUdf::TUnboxedValuePod(
        new TGeneratedWideStream(3, [&](size_t) {
            setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
            return std::vector<NUdf::TUnboxedValue>{
                throwOnUpdate ? NUdf::TUnboxedValuePod(ui64{1}) :
                    NUdf::TUnboxedValuePod(NUdf::TStringValue("heap string key for spill replay")),
                NUdf::TUnboxedValuePod(NUdf::TStringValue("heap string inside boxed aggregation state")),
                NUdf::TUnboxedValuePod{}};
        })));
    auto stream = graph->GetValue();
    NUdf::TUnboxedValue output;
    UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(&output, 1), NUdf::EFetchStatus::Yield);
    UNIT_ASSERT_VALUES_EQUAL(spiller->GetReadCount(), 2);
    if (stop != ESpillReplayStop::PendingRead) {
        spiller->ResumeRead(stop == ESpillReplayStop::MissingInput);
        if (stop == ESpillReplayStop::MissingInput) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(stream.WideFetch(&output, 1), yexception,
                "A spilled blob is missing while reading back spilled input rows");
        } else if (throwOnUpdate) {
            TThrowingBindTerminator terminator;
            UNIT_ASSERT_EXCEPTION_CONTAINS(stream.WideFetch(&output, 1), TTerminateException, "spill replay update failure");
        } else {
            UNIT_ASSERT_VALUES_EQUAL(stream.WideFetch(&output, 1), NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(TString(output.AsStringRef()), "heap string key for spill replay");
        }
    }

    auto memInfo = TIntrusivePtr<TMemoryUsageInfo>(&graph->GetMemInfo());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    output = {};
    stream = {};
    graph.Destroy();
    // Boxed allocation accounting is enabled in assertions-enabled builds
    UNIT_ASSERT_VALUES_EQUAL(memInfo->GetUsage(), 0);
}

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
    TDqSetup<UseLLVM> setup(GetDqNodeFactory());

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
    TDqSetup<UseLLVM> setup(GetDqNodeFactory());

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
    StreamCreator streamCreator, StreamChecker streamChecker, const bool disableDehydration,
    std::shared_ptr<ISpillerFactory> spillerFactory = {})
{
    const ui32 keyWidth = 2;

    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, true, 0, columnTypes, keyWidth);

    if (Spilling) {
        graph->GetContext().SpillerFactory = spillerFactory ? spillerFactory : CreateSpillerFactory();
    }

    if (disableDehydration) {
        DisableDehydration(graph);
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
void RunDqAggregateWideTest(TDqSetup<LLVM, Spilling>& setup, const bool useFlow, StreamCreator streamCreator, const ui32 keyWidth = 2, const bool disableDehydration = false, const bool disableKeyPassthrough = false)
{
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, useFlow, true, 0, columnTypes, keyWidth);
    TOperatorEndState endState;
    SetTestEndStateUpdater(graph, endState);

    if (Spilling) {
        graph->GetContext().SpillerFactory = CreateSpillerFactory();
    }

    if (disableDehydration) {
        DisableDehydration(graph);
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
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM, true>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 0, 0, columnTypes, keyWidth, refMap);
            });
        }

        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 0, 0, columnTypes, keyWidth, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationSingleRow, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM, true>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunDqAggregateWideTest<UseLLVM, false>(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationZeroWidth, UseLLVM, UseFlow) {
        {
            TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
            RunDqAggregateZeroWidthTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
        {
            TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
            RunDqAggregateZeroWidthTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
                return new TWideKVStream(ctx, 1, 1, columnTypes, keyWidth, refMap);
            });
        }
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationWithSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, keyWidth, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        });
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationWithSpillingNonDehydrated, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, keyWidth, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        }, 2, true, false);
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationWithSpillingNonPassthrough, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, keyWidth, refMap, [&](const size_t rowNum, [[maybe_unused]] bool& yield) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        }, 2, false, true);
    }

    Y_UNIT_TEST_QUAD(TestWideModeAggregationMultiRowNoSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
        RunDqAggregateWideTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TWideKVStream(ctx, 100000, 10, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeAggregationWithSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
        RunDqAggregateBlockTest(setup, UseFlow, [&](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 100000, 5, 8192, columnTypes, keyWidth, refMap, [&](const size_t rowNum) {
                if (rowNum == 100000) {
                    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(true);
                }
            });
        });
    }

    Y_UNIT_TEST_QUAD(TestBlockModeAggregationMultiRowNoSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
        RunDqAggregateBlockTest(setup, UseFlow, [](TComputationContext& ctx, std::vector<TType*>& columnTypes, ui32 keyWidth, auto& refMap) {
            return new TBlockKVStream(ctx, 10000, 5, 8192, columnTypes, keyWidth, refMap);
        });
    }

    Y_UNIT_TEST_QUAD(TestEarlyStop, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, false> setup(GetDqNodeFactory());
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

    Y_UNIT_TEST_QUAD(TestTeardownDuringStateSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());

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
            false,
            std::make_shared<TPendingSpillerFactory>()
        );
    }

    Y_UNIT_TEST_QUAD(TestTeardownDuringStateReadBack, UseLLVM, UseFlow) {
        for (bool disableDehydration : {false, true}) {
            for (size_t pauseOnRead : {1, 2}) {
                RunTeardownDuringStateReadBackTest<UseLLVM>(UseFlow, disableDehydration, pauseOnRead);
            }
        }
    }

    Y_UNIT_TEST_QUAD(TestPassthroughKeyInitFailure, UseLLVM, UseFlow) {
        for (bool useBlocks : {false, true}) {
            for (bool stringState : {false, true}) {
                RunPassthroughKeyInitFailureTest<UseLLVM>(UseFlow, useBlocks, stringState);
            }
        }
    }

    Y_UNIT_TEST_QUAD(TestTeardownDuringSpilledInputRead, UseLLVM, UseFlow) {
        for (bool useBlocks : {false, true}) {
            RunSpillReplayTeardownTest<UseLLVM>(UseFlow, useBlocks, ESpillReplayStop::PendingRead);
            RunSpillReplayTeardownTest<UseLLVM>(UseFlow, useBlocks, ESpillReplayStop::MissingInput);
        }
    }

    Y_UNIT_TEST_QUAD(TestTeardownAfterSpillReplayException, UseLLVM, UseFlow) {
        for (bool useBlocks : {false, true}) {
            RunSpillReplayTeardownTest<UseLLVM>(UseFlow, useBlocks, ESpillReplayStop::ThrowingUpdate);
        }
    }

    Y_UNIT_TEST_QUAD(TestTeardownAfterSpilledBucketDrain, UseLLVM, UseFlow) {
        for (bool useBlocks : {false, true}) {
            RunSpillReplayTeardownTest<UseLLVM>(UseFlow, useBlocks, ESpillReplayStop::Output);
        }
    }

    Y_UNIT_TEST_QUAD(TestEarlyStopAfterSpilling, UseLLVM, UseFlow) {
        TDqSetup<UseLLVM, true> setup(GetDqNodeFactory());
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
