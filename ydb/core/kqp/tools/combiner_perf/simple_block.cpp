#include "simple_block.h"

#include "factories.h"
#include "streams.h"
#include "printout.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {

template<typename K>
void UpdateMapFromBlocks(const NUdf::TUnboxedValue& key, const NUdf::TUnboxedValue& value, std::unordered_map<K, uint64_t>& result);

template<>
void UpdateMapFromBlocks(const NUdf::TUnboxedValue& key, const NUdf::TUnboxedValue& value, std::unordered_map<uint64_t, uint64_t>& result)
{
    auto datumKey = TArrowBlock::From(key).GetDatum();
    auto datumValue = TArrowBlock::From(value).GetDatum();
    UNIT_ASSERT(datumKey.is_array());
    UNIT_ASSERT(datumValue.is_array());

    const auto ui64keys = datumKey.template array_as<arrow::UInt64Array>();
    const auto ui64values = datumValue.template array_as<arrow::UInt64Array>();
    UNIT_ASSERT(!!ui64keys);
    UNIT_ASSERT(!!ui64values);
    UNIT_ASSERT_VALUES_EQUAL(ui64keys->length(), ui64values->length());

    const size_t length = ui64keys->length();
    for (size_t i = 0; i < length; ++i) {
        result[ui64keys->Value(i)] += ui64values->Value(i);
    }
}

template<>
void UpdateMapFromBlocks(const NUdf::TUnboxedValue& key, const NUdf::TUnboxedValue& value, std::unordered_map<std::string, uint64_t>& result)
{
    auto datumKey = TArrowBlock::From(key).GetDatum();
    auto datumValue = TArrowBlock::From(value).GetDatum();
    UNIT_ASSERT(datumKey.is_arraylike());
    UNIT_ASSERT(datumValue.is_array());

    const auto ui64values = datumValue.template array_as<arrow::UInt64Array>();
    UNIT_ASSERT(!!ui64values);

    int64_t valueOffset = 0;

    for (const auto& chunk : datumKey.chunks()) {
        auto* barray = dynamic_cast<arrow::BinaryArray*>(chunk.get());
        UNIT_ASSERT(barray != nullptr);
        for (int64_t i = 0; i < barray->length(); ++i) {
            auto key = barray->GetString(i);
            auto val = ui64values->Value(valueOffset);
            result[key] += val;
            ++valueOffset;
        }
    }
}


template<typename TStream, typename TMap>
void CalcRefResult(TStream& refStream, TMap& refResult)
{
    NUdf::TUnboxedValue columns[3];

    while (refStream->WideFetch(columns, 3) == NUdf::EFetchStatus::Ok) {
        UpdateMapFromBlocks(columns[0], columns[1], refResult);
    }
}


template<bool LLVM, bool Spilling>
void RunTestBlockCombineHashedSimple(const TRunParams& params, TTestResultCollector& printout)
{
    TSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    printout.SubmitTestNameAndParams(params, __func__, LLVM, Spilling);

    auto samples = MakeKeyedString64Samples(params.RowsPerRun, params.MaxKey, false);

    TProgramBuilder& pb = *setup.PgmBuilder;

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<char*>::Id);
    auto valueBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto keyBlockType = pb.NewBlockType(keyBaseType, TBlockType::EShape::Many);
    auto valueBlockType = pb.NewBlockType(valueBaseType, TBlockType::EShape::Many);
    auto blockSizeType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto blockSizeBlockType = pb.NewBlockType(blockSizeType, TBlockType::EShape::Scalar);
    const auto streamItemType = pb.NewMultiType({keyBlockType, valueBlockType, blockSizeBlockType});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamResultItemType = pb.NewMultiType({keyBlockType, valueBlockType, blockSizeBlockType});
    const auto streamResultType = pb.NewStreamType(streamResultItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    ui32 keys[] = {0};
    TAggInfo aggs[] = {
        TAggInfo{
            .Name = "sum",
            .ArgsColumns = {1u},
        }
    };
    auto pgmReturn = pb.Collect(pb.NarrowMap(pb.ToFlow(
        pb.BlockCombineHashed(
            TRuntimeNode(streamCallable, false),
            {},
            TArrayRef(keys, 1),
            TArrayRef(aggs, 1),
            streamResultType
        )),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); } // NarrowMap handler
    ));
    const auto graph = setup.BuildGraph(pgmReturn, {streamCallable});

    auto streamMaker = [&]() -> auto {
        return std::unique_ptr<TBlockKVStream<std::string, uint64_t>>(new TBlockKVStream<std::string, uint64_t>(
            graph->GetContext(),
            samples,
            params.NumRuns,
            params.BlockSize,
            {keyBaseType, valueBaseType}
        ));
    };

    // Compute results directly from raw samples to test the input stream implementation
    std::unordered_map<std::string, uint64_t> rawResult;
    for (const auto& tuple : samples) {
        rawResult[tuple.first] += (tuple.second * params.NumRuns);
    }

    // Measure the input stream run time
    const auto devnullStream = streamMaker();
    const auto devnullStart = TInstant::Now();
    {
        NUdf::TUnboxedValue columns[3];
        while (devnullStream->WideFetch(columns, 3) == NUdf::EFetchStatus::Ok) {
        }
    }
    const auto devnullTime = TInstant::Now() - devnullStart;

    // Reference implementation (sum via an std::unordered_map)
    std::unordered_map<std::string, uint64_t> refResult;
    const auto refStream = streamMaker();
    const auto cppStart = TInstant::Now();
    CalcRefResult(refStream, refResult);
    const auto cppTime = TInstant::Now() - cppStart;

    // Verify the reference stream implementation against the raw samples
    UNIT_ASSERT_VALUES_EQUAL(refResult.size(), rawResult.size());
    for (const auto& tuple : rawResult) {
        auto otherIt = refResult.find(tuple.first);
        UNIT_ASSERT(otherIt != refResult.end());
        UNIT_ASSERT_VALUES_EQUAL(tuple.second, otherIt->second);
    }

    // Compute graph implementation
    auto stream = NUdf::TUnboxedValuePod(streamMaker().release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    const auto graphStart = TInstant::Now();
    const auto& resultList = graph->GetValue();
    const auto graphTime = TInstant::Now() - graphStart;

    size_t numResultItems = resultList.GetListLength();
    Cerr << "Result block count: " << numResultItems << Endl;

    // Verification
    std::unordered_map<std::string, uint64_t> graphResult;

    const auto ptr = resultList.GetElements();
    for (size_t i = 0ULL; i < numResultItems; ++i) {
        UNIT_ASSERT(ptr[i].GetListLength() >= 2);

        const auto elements = ptr[i].GetElements();

        UpdateMapFromBlocks(elements[0], elements[1], graphResult);
    }

    UNIT_ASSERT_VALUES_EQUAL(refResult.size(), graphResult.size());
    for (const auto& tuple : refResult) {
        auto graphIt = graphResult.find(tuple.first);
        UNIT_ASSERT(graphIt != graphResult.end());
        UNIT_ASSERT_VALUES_EQUAL(tuple.second, graphIt->second);
    }

    Cerr << "Graph time raw: " << graphTime << Endl;
    Cerr << "CPP time raw: " << cppTime << Endl;
    printout.SubmitTimings(graphTime, cppTime, devnullTime);
}

template void RunTestBlockCombineHashedSimple<false, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestBlockCombineHashedSimple<false, true>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestBlockCombineHashedSimple<true, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestBlockCombineHashedSimple<true, true>(const TRunParams& params, TTestResultCollector& printout);


}
}