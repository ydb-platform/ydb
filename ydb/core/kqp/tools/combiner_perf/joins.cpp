#include "joins.h"
#include "construct_join_graph.h"
#include "factories.h"
#include <ydb/library/yql/dq/comp_nodes/ut/utils.h>
namespace {
TVector<ui64> GenerateKeyColumn(i32 size, i32 seed){
    std::default_random_engine eng;
    std::uniform_int_distribution<uint64_t> unif(0, size/2);
    eng.seed(seed);
    TVector<ui64> keyCoumn;
    std::generate(keyCoumn.begin(), keyCoumn.end(),
        [&]() -> auto {
            return unif(eng);
        }
    );
    return keyCoumn;
}


NKikimr::NMiniKQL::InnerJoinDescription PrepareCommonDescription(NKikimr::NMiniKQL::TDqSetup<false>* setup){
    NKikimr::NMiniKQL::InnerJoinDescription descr;
    descr.Setup = setup;
    const int size = 1<<12;


    std::tie(descr.LeftSource.ColumnTypes, descr.LeftSource.Values) =
    ConvertVectorsToRuntimeTypesAndValue(*setup, GenerateKeyColumn(size, 123),
        TVector<ui64>(size, 111), TVector<TString>(size, "meow")
    );
    // descr.LeftSource.ColumnTypes = AS_TYPE(NKikimr::NMiniKQL::TListType, descr.LeftSource.ColumnTypes)->GetItemType();
    std::tie(descr.RightSource.ColumnTypes, descr.RightSource.Values) = 
    ConvertVectorsToRuntimeTypesAndValue(*setup, GenerateKeyColumn(size, 111),
        TVector<TString>(size, "woo")
    );
    // descr.RightSource.ItemType = AS_TYPE(NKikimr::NMiniKQL::TListType, descr.RightSource.ColumnTypes)->GetItemType();
    return descr;
}
struct TTestResult{
    TRunResult Run;
    TString TestName;
};

TTestResult DoRunJoinsBench(const NKikimr::NMiniKQL::TRunParams &params){
    Y_UNUSED(params);
    namespace NYKQL = NKikimr::NMiniKQL;
    TRunResult finalResult;
    NKikimr::NMiniKQL::TDqSetup<false> setup{NKikimr::NMiniKQL::GetPerfTestFactory()};

    NYKQL::InnerJoinDescription descr = PrepareCommonDescription(&setup);
    TVector<const ui32> keyColumns;
    keyColumns.push_back(0);
    descr.LeftSource.KeyColumnIndexes = keyColumns;
    descr.RightSource.KeyColumnIndexes = keyColumns;

    struct JoinTestResult{
        i64 LineCount;
        TDuration BenchDuration;
    };
    TMap<NYKQL::ETestedJoinAlgo, JoinTestResult> results;

    for(NYKQL::ETestedJoinAlgo algo: {NYKQL::ETestedJoinAlgo::kScalarGrace, NYKQL::ETestedJoinAlgo::kBlockMap, }){
        NYql::NUdf::TUnboxedValue wideStream = ConstructInnerJoinGraphStream(algo, descr);
        std::vector<NYql::NUdf::TUnboxedValue> fetchBuff;
        i32 cols = 
        // 6;
        NKikimr::NMiniKQL::ResultColumnCount(algo,descr);
        fetchBuff.resize(cols);
        Cerr << "Compute graph result" << Endl; 

        NYql::NUdf::EFetchStatus fetchStatus;
        JoinTestResult thisResults{};
        const auto graphTimeStart = GetThreadCPUTime();

        while ((fetchStatus = wideStream.WideFetch(fetchBuff.data(), cols)) != NYql::NUdf::EFetchStatus::Finish) {
            if (fetchStatus == NYql::NUdf::EFetchStatus::Ok) {
                ++thisResults.LineCount;
            }
        }
        Cout << fetchBuff[0];
        thisResults.BenchDuration = GetThreadCPUTimeDelta(graphTimeStart);
        Cerr << ToString(thisResults.LineCount);
        results.emplace(algo, thisResults);
    }

    return {finalResult, "allJoins"};

}
}

void NKikimr::NMiniKQL::RunJoinsBench(const TRunParams &params, TTestResultCollector &printout){
    auto res = DoRunJoinsBench(params);
    printout.SubmitMetrics(params, res.Run,res.TestName.c_str(), false, false);
}