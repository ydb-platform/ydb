#include "joins.h"
#include "construct_join_graph.h"
#include "factories.h"
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace {
TVector<ui64> GenerateKeyColumn(i32 size, i32 seed) {
    std::default_random_engine eng;
    std::uniform_int_distribution<uint64_t> unif(0, size / 2);
    eng.seed(seed);
    TVector<ui64> keyCoumn;
    std::generate_n(std::back_inserter(keyCoumn), size, [&]() { return unif(eng); });
    return keyCoumn;
}

NKikimr::NMiniKQL::TInnerJoinDescription PrepareSameSizeTables(NKikimr::NMiniKQL::TDqSetup<false>* setup) {
    NKikimr::NMiniKQL::TInnerJoinDescription descr;
    descr.Setup = setup;
    const int size = 1 << 16;

    std::tie(descr.LeftSource.ColumnTypes, descr.LeftSource.ValuesList) = ConvertVectorsToRuntimeTypesAndValue(
        *setup, GenerateKeyColumn(size, 123), TVector<ui64>(size, 111), TVector<TString>(size, "meow"));
    std::tie(descr.RightSource.ColumnTypes, descr.RightSource.ValuesList) =
        ConvertVectorsToRuntimeTypesAndValue(*setup, GenerateKeyColumn(size, 111), TVector<TString>(size, "woo"));
    return descr;
}

NKikimr::NMiniKQL::TInnerJoinDescription PrepareSmallRightTable(NKikimr::NMiniKQL::TDqSetup<false>* setup) {
    NKikimr::NMiniKQL::TInnerJoinDescription descr;
    descr.Setup = setup;
    const int leftSize = 1 << 16;
    const int rightSize = leftSize >> 7;
    std::tie(descr.LeftSource.ColumnTypes, descr.LeftSource.ValuesList) = ConvertVectorsToRuntimeTypesAndValue(
        *setup, GenerateKeyColumn(leftSize, 123), TVector<ui64>(leftSize, 111), TVector<TString>(leftSize, "meow"));
    std::tie(descr.RightSource.ColumnTypes, descr.RightSource.ValuesList) = ConvertVectorsToRuntimeTypesAndValue(
        *setup, GenerateKeyColumn(rightSize, 111), TVector<TString>(rightSize, "woo"));
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

    TVector<std::pair<NYKQL::ETestedJoinAlgo, std::string_view>> cases = {
        {NYKQL::ETestedJoinAlgo::kScalarGrace, "ScalarGrace"},
        {NYKQL::ETestedJoinAlgo::kScalarMap, "ScalarMap"},
        {NYKQL::ETestedJoinAlgo::kBlockMap, "BlockMap"},
    };
    TVector<std::pair<NYKQL::TInnerJoinDescription, std::string_view>> inputs = {
        {PrepareSameSizeTables(&setup), "SameSizeTables"},
        {PrepareSmallRightTable(&setup), "SmallRight"},
    };

    for (auto [algo, algo_name] : cases) {
        for (auto [descr, descr_name] : inputs) {
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