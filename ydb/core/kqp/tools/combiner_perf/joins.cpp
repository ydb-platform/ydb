#include "joins.h"
#include "construct_join_graph.h"
#include "factories.h"
#include <ranges>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace {
TVector<ui64> GenerateIntegerKeyColumn(i32 size, i32 seed) {
    std::default_random_engine eng;
    std::uniform_int_distribution<uint64_t> unif(0, size / 2);
    eng.seed(seed);
    TVector<ui64> keyCoumn;
    std::generate_n(std::back_inserter(keyCoumn), size, [&]() { return unif(eng); });
    return keyCoumn;
}

TVector<TString> GenerateStringKeyColumn(i32 size, i32 seed) {
    TVector<ui64> ints = GenerateIntegerKeyColumn(size, seed);
    TVector<TString> strings;
    strings.reserve(ints.size());
    for (ui64 num : ints) {
        num += 1234567;
        strings.push_back(Sprintf("%08u.%08u.%08u.", num, num, num));
    }
    return strings;
}

template <typename KeyType>
NKikimr::NMiniKQL::TInnerJoinDescription PrepareDescription(NKikimr::NMiniKQL::TDqSetup<false>* setup,
                                                            TVector<KeyType> leftKeys, TVector<KeyType> rightKeys) {
    const int leftSize = std::ssize(leftKeys);
    const int rightSize = std::ssize(rightKeys);
    NKikimr::NMiniKQL::TInnerJoinDescription descr;
    descr.Setup = setup;
    std::tie(descr.LeftSource.ColumnTypes, descr.LeftSource.ValuesList) = ConvertVectorsToRuntimeTypesAndValue(
        *setup, std::move(leftKeys), TVector<ui64>(leftSize, 111), TVector<TString>(leftSize, "meow"));
    std::tie(descr.RightSource.ColumnTypes, descr.RightSource.ValuesList) =
        ConvertVectorsToRuntimeTypesAndValue(*setup, std::move(rightKeys), TVector<TString>(rightSize, "woo"));
    return descr;
}

struct TTestResult {
    TRunResult Run;
    TString TestName;
};

int LineSize(NKikimr::NMiniKQL::ETestedJoinAlgo algo, std::span<const NYql::NUdf::TUnboxedValue> line) {
    if (NKikimr::NMiniKQL::IsBlockJoin(algo)) {
        return NKikimr::NMiniKQL::TArrowBlock::From(line.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    } else {
        return 1;
    }
}

} // namespace

void NKikimr::NMiniKQL::RunJoinsBench(const TRunParams& params, TTestResultCollector& printout) {
    Y_UNUSED(params);
    namespace NYKQL = NKikimr::NMiniKQL;
    TRunResult finalResult;
    NKikimr::NMiniKQL::TDqSetup<false> setup{NKikimr::NMiniKQL::GetPerfTestFactory()};

    const TVector<const ui32> keyColumns{0};

    TVector<std::pair<NYKQL::ETestedJoinAlgo, std::string_view>> algos = {
        {NYKQL::ETestedJoinAlgo::kScalarGrace, "ScalarGrace"}, {NYKQL::ETestedJoinAlgo::kScalarMap, "ScalarMap"},
        {NYKQL::ETestedJoinAlgo::kBlockMap, "BlockMap"},
        //         {NYKQL::ETestedJoinAlgo::kScalarHash, "ScalarHash"}, // hash joins are not ready yet - they require
        //         {NYKQL::ETestedJoinAlgo::kBlockHash, "BlockHash"}, // same schema for left and right tables as they
        //         just spit left then right
    };
    const int bigSize = 1 << 9;
    const int smallSize = bigSize >> 7;
    auto addStringAndIntInputs = [&](TVector<std::pair<NYKQL::TInnerJoinDescription, std::string>>& all, int leftSize,
                                     int rightSize, std::string name) {
        Cout << "Adding " << name << "test cases" << Endl;
        all.emplace_back(PrepareDescription(&setup, GenerateIntegerKeyColumn(leftSize, 123),
                                            GenerateIntegerKeyColumn(rightSize, 111)), name + "_Integer");
        all.emplace_back(PrepareDescription(&setup, GenerateStringKeyColumn(leftSize, 123),
                                            GenerateStringKeyColumn(rightSize, 111)), name + "_String");
    };

    TVector<std::pair<NYKQL::TInnerJoinDescription, std::string>> scaled_inputs;
    for (int scale_log : std::views::iota(1) | std::views::take(2)) {
        int scale = 1 << scale_log;
        int leftSize = bigSize * scale;
        // todo(becalm): there is a lot of input generation which can be optimised like: generate only the biggest list
        // and add node that takes only first k lines of list for input size of k. it introduces some runtime overhead
        // in benchmark tho.
        addStringAndIntInputs(scaled_inputs, leftSize, leftSize, Sprintf("SameSize_%i", leftSize));
        addStringAndIntInputs(scaled_inputs, leftSize, smallSize * scale, Sprintf("BigLeft_%i", leftSize));
    }

    for (auto [algo, algo_name] : algos) {
        for (auto [descr, descr_name] : scaled_inputs) {
            descr.LeftSource.KeyColumnIndexes = keyColumns;
            descr.RightSource.KeyColumnIndexes = keyColumns;

            THolder<NKikimr::NMiniKQL::IComputationGraph> wideStreamGraph = ConstructInnerJoinGraphStream(algo, descr);
            NYql::NUdf::TUnboxedValue wideStream = wideStreamGraph->GetValue();
            std::vector<NYql::NUdf::TUnboxedValue> fetchBuff;
            ui32 cols = NKikimr::NMiniKQL::ResultColumnCount(algo, descr);
            fetchBuff.resize(cols);
            Cerr << "Compute graph result for algorithm '" << algo_name << "' and input data '" << descr_name << "'";

            NYql::NUdf::EFetchStatus fetchStatus;
            i64 lineCount = 0;
            const auto graphTimeStart = GetThreadCPUTime();

            while ((fetchStatus = wideStream.WideFetch(fetchBuff.data(), cols)) != NYql::NUdf::EFetchStatus::Finish) {
                if (fetchStatus == NYql::NUdf::EFetchStatus::Ok) {
                    lineCount += LineSize(algo, {fetchBuff.data(), cols});
                }
            }
            TRunResult thisNodeResult;

            thisNodeResult.ResultTime = GetThreadCPUTimeDelta(graphTimeStart);
            Cerr << ". Output line count(block considered to be 1 line): " << lineCount << Endl;
            std::string testname = std::string{algo_name} + "_" + std::string{descr_name};
            printout.SubmitMetrics(params, thisNodeResult, testname.data(), false, false);
        }
    }
}