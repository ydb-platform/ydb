#include "dq_setup.h"
#include "dq_factories.h"

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

void UpdateMapFromBlocks(std::unordered_map<std::string, std::vector<ui64>>& map, const TArrayRef<const NYql::NUdf::TUnboxedValue>& values)
{
    UNIT_ASSERT(values.size() >= 3);
    size_t valuesCount = values.size() - 2; // exclude the key, exclude the block height column

    auto datumKey = TArrowBlock::From(values[0]).GetDatum();
    std::vector<std::shared_ptr<arrow::UInt64Array>> valueDatums;

    for (size_t i = 0; i < valuesCount; ++i) {
        valueDatums.push_back(TArrowBlock::From(values[i+1]).GetDatum().array_as<arrow::UInt64Array>());
    }

    int64_t valueOffset = 0;

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
        }
    }
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
    TBlockKVStream(const TComputationContext& ctx, size_t numKeys, size_t iterations, size_t blockSize, const std::vector<TType*>& types, TRefMap& reference)
        : TWideStream(ctx, numKeys, iterations, types, reference)
        , BlockSize(blockSize)
    {
    }

private:
    size_t BlockSize;

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
    TWideKVStream(const TComputationContext& ctx, size_t numKeys, size_t iterations, const std::vector<TType*>& types, TRefMap& reference)
        : TWideStream(ctx, numKeys, iterations, types, reference)
    {
    }

    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final override {
        const size_t expectedWidth = Types.size();
        if (width != expectedWidth) {
            ythrow yexception() << "width " << expectedWidth << " expected";
        }

        if (IsAtTheEnd()) {
            return NUdf::EFetchStatus::Finish;;
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
};


template<bool LLVM>
THolder<IComputationGraph> BuildBlockGraph(TDqSetup<LLVM>& setup, const size_t memLimit, std::vector<TType*>& columnTypes) {
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

    auto rootNode = pb.Collect(pb.NarrowMap(pb.ToFlow(
        pb.DqHashCombine(
            TRuntimeNode(streamCallable, false),
            memLimit,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back())};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {keys.front(), state.front()};
            }
        )),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); } // NarrowMap handler
    ));

    return setup.BuildGraph(rootNode, {streamCallable});
}

template<bool LLVM>
THolder<IComputationGraph> BuildWideGraph(TDqSetup<LLVM>& setup, const size_t memLimit, std::vector<TType*>& columnTypes) {
    auto& pb = setup.GetDqProgramBuilder();

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto valueBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto streamItemType = pb.NewMultiType({keyBaseType, valueBaseType});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamResultItemType = pb.NewMultiType({keyBaseType, valueBaseType});
    [[maybe_unused]] const auto streamResultType = pb.NewStreamType(streamResultItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", streamType).Build();

    columnTypes = {keyBaseType, valueBaseType};

    const auto rootNode = pb.Collect(pb.NarrowMap(pb.ToFlow(
        pb.DqHashCombine(
            TRuntimeNode(streamCallable, false),
            memLimit,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back())};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {keys.front(), state.front()};
            }
        )),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); } // NarrowMap handler
    ));

    return setup.BuildGraph(rootNode, {streamCallable});
}

template<typename StreamCreator>
void RunDqCombineBlockTest(StreamCreator streamCreator)
{
    TDqSetup<false> setup(GetDqNodeFactory());

    std::vector<TType*> columnTypes;

    auto graph = BuildBlockGraph(setup, 128ull << 20, columnTypes);

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultList = graph->GetValue();

    size_t numResultItems = resultList.GetListLength();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;

    const auto ptr = resultList.GetElements();
    for (size_t i = 0ULL; i < numResultItems; ++i) {
        UNIT_ASSERT(ptr[i].GetListLength() >= 2);

        const auto elements = ptr[i].GetElements();
        TArrayRef blocks(elements, ptr[i].GetListLength());

        UpdateMapFromBlocks(graphResult, blocks);
    }

    AssertMapsEqual(refResult, graphResult);
}

template<typename StreamCreator>
void RunDqCombineWideTest(StreamCreator streamCreator)
{
    TDqSetup<false> setup(GetDqNodeFactory());

    std::vector<TType*> columnTypes;

    auto graph = BuildWideGraph(setup, 128ull << 20, columnTypes);

    std::unordered_map<std::string, std::vector<ui64>> refResult;

    auto stream = NUdf::TUnboxedValuePod(streamCreator(graph->GetContext(), columnTypes, refResult));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    auto resultList = graph->GetValue();

    size_t numResultItems = resultList.GetListLength();

    std::unordered_map<std::string, std::vector<ui64>> graphResult;

    const auto ptr = resultList.GetElements();
    for (size_t i = 0ULL; i < numResultItems; ++i) {
        UNIT_ASSERT(ptr[i].GetListLength() >= 2);

        const auto elements = ptr[i].GetElements();

        TArrayRef values(elements+1, ptr[i].GetListLength()-1);
        std::vector<ui64> valuesVec;
        valuesVec.reserve(values.size());
        for (const NYql::NUdf::TUnboxedValue& value : values) {
            valuesVec.push_back(UnboxedToNative<ui64>(value));
        }
        AddRowToMap(graphResult, UnboxedToNative<std::string>(elements[0]), valuesVec);
    }

    AssertMapsEqual(refResult, graphResult);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqHashCombineTest) {

    Y_UNIT_TEST(TestBlockModeNoInput) {
        RunDqCombineBlockTest([](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 0, 0, 8192, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST(TestBlockModeSingleRow) {
        RunDqCombineBlockTest([](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 1, 1, 8192, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST(TestBlockModeMultiBlocks) {
        RunDqCombineBlockTest([](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TBlockKVStream(ctx, 10000, 5, 8192, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST(TestWideModeNoInput) {
        RunDqCombineWideTest([](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 0, 0, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST(TestWideModeSingleRow) {
        RunDqCombineWideTest([](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 1, 1, columnTypes, refMap);
        });
    }

    Y_UNIT_TEST(TestWideModeMultiRows) {
        RunDqCombineWideTest([](TComputationContext& ctx, std::vector<TType*>& columnTypes, auto& refMap) {
            return new TWideKVStream(ctx, 10000, 5, columnTypes, refMap);
        });
    }

} // Y_UNIT_TEST_SUITE


}
}