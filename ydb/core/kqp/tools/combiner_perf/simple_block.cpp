#include "simple_block.h"

#include "factories.h"
#include "converters.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {


using T6464Samples = std::vector<std::pair<uint64_t, uint64_t>>;
using TString64Samples = std::vector<std::pair<std::string, uint64_t>>;

T6464Samples MakeKeyed6464SamplesForBlocks(const size_t numSamples, const unsigned int maxKey) {
    std::default_random_engine eng;
    std::uniform_int_distribution<unsigned int> keys(0, maxKey);
    std::uniform_int_distribution<uint64_t> unif(0, 100000.0);

    T6464Samples samples(numSamples);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(),
        [&]() -> auto {
            return std::make_pair<uint64_t, uint64_t>(keys(eng), unif(eng));
        }
    );

    return samples;
}

struct IWideBlockStream : public NUdf::TBoxedValue
{
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final override {
        Y_UNUSED(result);
        ythrow yexception() << "only WideFetch is supported here";
    }
};

template<typename K, typename V>
struct TBlockStream : public IWideBlockStream {
    using TSamples = std::vector<std::pair<K, V>>;

    TBlockStream(TComputationContext& ctx, const TSamples& samples, size_t iterations, size_t blockSize, const std::vector<TType*> types)
        : Context(ctx)
        , Samples(samples)
        , End(Samples.end())
        , MaxIterations(iterations)
        , CurrSample(Samples.begin())
        , BlockSize(blockSize)
        , Types(types)
    {
        Y_ENSURE(samples.size() > 0);
    }

private:
    TComputationContext& Context;
    const TSamples& Samples;
    const TSamples::const_iterator End;
    const size_t MaxIterations;
    TSamples::const_iterator CurrSample;
    size_t Iteration = 0;
    size_t BlockSize = 1;
    const std::vector<TType*> Types;

    const TSamples::value_type* NextSample() {
        if (CurrSample == End) {
            ++Iteration;
            if (Iteration >= MaxIterations) {
                return nullptr;
            }
            CurrSample = Samples.begin();
        }
        return CurrSample++;
    }

    bool IsAtTheEnd() const {
        return (CurrSample == End) && (Iteration >= MaxIterations);
    }

public:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final override {        
        if (width != 3) {
            ythrow yexception() << "width 3 expected";
        }
        
        TVector<std::unique_ptr<IArrayBuilder>> builders;
        std::transform(Types.cbegin(), Types.cend(), std::back_inserter(builders),
        [&](const auto& type) {
            return MakeArrayBuilder(TTypeInfoHelper(), type, Context.ArrowMemoryPool, BlockSize, &Context.Builder->GetPgBuilder());
        });

        size_t count = 0;
        for (; count < BlockSize; ++count) {
            const auto nextTuple = NextSample();
            if (!nextTuple) {
                break;
            }

            NUdf::TUnboxedValue key;
            NativeToUnboxed<false>(nextTuple->first, key);
            builders[0]->Add(key);

            NUdf::TUnboxedValue value;
            NativeToUnboxed<false>(nextTuple->second, value);            
            builders[1]->Add(value);
        }

        if (count > 0) {
            const bool finish = IsAtTheEnd();
            result[0] = Context.HolderFactory.CreateArrowBlock(builders[0]->Build(finish));
            result[1] = Context.HolderFactory.CreateArrowBlock(builders[1]->Build(finish));
            result[2] = Context.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(count)));

            return NUdf::EFetchStatus::Ok;
        } else {
            return NUdf::EFetchStatus::Finish;
        }
    }
};

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

template<bool LLVM, bool Spilling>
void RunTestBlockCombineHashedSimple(const TRunParams& params)
{
    TSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    Cerr << "Data rows total: " << params.RowsPerRun << " x " << params.NumRuns << Endl;
    Cerr << (params.MaxKey + 1) << " distinct numeric keys" << Endl;
    Cerr << "Block size: " << params.BlockSize << Endl;
    
    auto samples = MakeKeyed6464SamplesForBlocks(params.RowsPerRun, params.MaxKey);

    TProgramBuilder& pb = *setup.PgmBuilder;

    // const auto streamItemType = pb.NewMultiType({pb.NewDataType(NUdf::TDataType<ui64>::Id), pb.NewDataType(NUdf::TDataType<ui64>::Id)});

    auto keyBaseType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
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
        return std::unique_ptr<TBlockStream<uint64_t, uint64_t>>(new TBlockStream<uint64_t, uint64_t>(
            graph->GetContext(),
            samples,
            params.NumRuns,
            params.BlockSize,
            {keyBaseType, valueBaseType}
        ));
    };


    std::unordered_map<uint64_t, uint64_t> rawResult;
    for (const auto& tuple : samples) {
        rawResult[tuple.first] += (tuple.second * params.NumRuns); 
    }

    std::unordered_map<uint64_t, uint64_t> refResult;
    const auto refStream = streamMaker();

    const auto cppStart = TInstant::Now();
    {
        NUdf::TUnboxedValue columns[3];
        
        while (refStream->WideFetch(columns, 3) == NUdf::EFetchStatus::Ok) {
            UpdateMapFromBlocks(columns[0], columns[1], refResult);
        }
    }
    const auto cppTime = TInstant::Now() - cppStart;

    UNIT_ASSERT_VALUES_EQUAL(refResult.size(), rawResult.size());
    for (const auto& tuple : rawResult) {
        auto otherIt = refResult.find(tuple.first);
        UNIT_ASSERT(otherIt != refResult.end());
        UNIT_ASSERT_VALUES_EQUAL(tuple.second, otherIt->second);
    }

    auto stream = NUdf::TUnboxedValuePod(streamMaker().release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));
    const auto graphStart = TInstant::Now();
    const auto& resultList = graph->GetValue();
    const auto graphTime = TInstant::Now() - graphStart;

    size_t numResultItems = resultList.GetListLength();

    std::unordered_map<uint64_t, uint64_t> graphResult;

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

    Cerr << "BlockCombineHashed graph runtime is: " << graphTime << " vs. reference C++ implementation: " << cppTime << "" << Endl << Endl;
}

template void RunTestBlockCombineHashedSimple<false, false>(const TRunParams& params);
template void RunTestBlockCombineHashedSimple<false, true>(const TRunParams& params);
template void RunTestBlockCombineHashedSimple<true, false>(const TRunParams& params);
template void RunTestBlockCombineHashedSimple<true, true>(const TRunParams& params);


}
}