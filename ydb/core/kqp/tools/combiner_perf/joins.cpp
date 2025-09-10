#include "joins.h"
#include "construct_join_graph.h"
#include "factories.h"
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>
namespace {
TVector<ui64> GenerateKeyColumn(i32 size, i32 seed){
    std::default_random_engine eng;
    std::uniform_int_distribution<uint64_t> unif(0, size/2);
    eng.seed(seed);
    TVector<ui64> keyCoumn;
    std::generate_n(std::back_inserter(keyCoumn), size,
        [&]() {
            return unif(eng);
        }
    );
    return keyCoumn;
}


NKikimr::NMiniKQL::TInnerJoinDescription PrepareCommonDescription(NKikimr::NMiniKQL::TDqSetup<false>* setup){
    NKikimr::NMiniKQL::TInnerJoinDescription descr;
    descr.Setup = setup;
    const int size = 1<<14;


    std::tie(descr.LeftSource.ColumnTypes, descr.LeftSource.ValuesList) =
    ConvertVectorsToRuntimeTypesAndValue(*setup, GenerateKeyColumn(size, 123),
        TVector<ui64>(size, 111), TVector<TString>(size, "meow")
    );
    std::tie(descr.RightSource.ColumnTypes, descr.RightSource.ValuesList) = 
    ConvertVectorsToRuntimeTypesAndValue(*setup, GenerateKeyColumn(size, 111),
        TVector<TString>(size, "woo")
    );
    return descr;
}
struct TTestResult{
    TRunResult Run;
    TString TestName;
};
}
void NKikimr::NMiniKQL::RunJoinsBench(const TRunParams &params, TTestResultCollector &printout){
    Y_UNUSED(params);
    namespace NYKQL = NKikimr::NMiniKQL;
    TRunResult finalResult;
    NKikimr::NMiniKQL::TDqSetup<false> setup{NKikimr::NMiniKQL::GetPerfTestFactory()};

    const TVector<const ui32> keyColumns{0};

    TVector<std::pair<NYKQL::ETestedJoinAlgo, std::string_view>> cases = {{NYKQL::ETestedJoinAlgo::kScalarGrace, "ScalarGrace"}, {NYKQL::ETestedJoinAlgo::kBlockMap, "BlockMap"}};

    for(auto [algo, name]: cases){
        NYKQL::TInnerJoinDescription descr = PrepareCommonDescription(&setup);
        TRunResult thisNodeResult;
        descr.LeftSource.KeyColumnIndexes = keyColumns;
        descr.RightSource.KeyColumnIndexes = keyColumns;
        THolder<NKikimr::NMiniKQL::IComputationGraph> wideStreamGraph = ConstructInnerJoinGraphStream(algo, descr);
        NYql::NUdf::TUnboxedValue wideStream = wideStreamGraph->GetValue();
        std::vector<NYql::NUdf::TUnboxedValue> fetchBuff;
        i32 cols = NKikimr::NMiniKQL::ResultColumnCount(algo,descr);
        fetchBuff.resize(cols);
        Cerr << "Compute graph result for algorithm '" << name << "'"; 

        NYql::NUdf::EFetchStatus fetchStatus;
        i64 lineCount = 0;
        const auto graphTimeStart = GetThreadCPUTime();

        while ((fetchStatus = wideStream.WideFetch(fetchBuff.data(), cols)) != NYql::NUdf::EFetchStatus::Finish) {
            if (fetchStatus == NYql::NUdf::EFetchStatus::Ok) {
                ++lineCount;
            }
        }
        thisNodeResult.ResultTime = GetThreadCPUTimeDelta(graphTimeStart);
        Cerr << ". Output line count(block considered to be 1 line): " << lineCount << Endl;
        printout.SubmitMetrics(params, thisNodeResult,name.data(), false, false);
    }

}