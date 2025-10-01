#include "joins.h"
#include "construct_join_graph.h"
#include <util/system/datetime.h>
#include <ydb/core/kqp/tools/combiner_perf/factories.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
using namespace NKikimr::NMiniKQL;

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

// struct TTestResult {
//     TRunResult Run;
//     TString TestName;
// };

int LineSize(NKikimr::NMiniKQL::ETestedJoinAlgo algo, std::span<const NYql::NUdf::TUnboxedValue> line) {
    if (NKikimr::NMiniKQL::IsBlockJoin(algo)) {
        return NKikimr::NMiniKQL::TArrowBlock::From(line.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    } else {
        return 1;
    }
}

} // namespace

TVector<TBenchmarkCaseResult> NKikimr::NMiniKQL::RunJoinsBench(const TBenchmarkSettings& params) {
    TVector<TBenchmarkCaseResult> ret;
    const TVector<const ui32> keyColumns{0};

    for (auto keyType : params.KeyTypes) {
        for (auto keyPreset : params.Presets) {
            for (auto sizes : keyPreset.Cases) {
                NKikimr::NMiniKQL::TDqSetup<false> setup{NKikimr::NMiniKQL::GetPerfTestFactory()};
                TInnerJoinDescription descr = [&] {
                    using enum ETestedJoinKeyType;
                    switch (keyType) {
                    case kString: {
                        return PrepareDescription(&setup, GenerateStringKeyColumn(sizes.Left, 123),
                                                  GenerateStringKeyColumn(sizes.Right, 111));
                    }
                    case kInteger: {
                        return PrepareDescription(&setup, GenerateIntegerKeyColumn(sizes.Left, 123),
                                                  GenerateIntegerKeyColumn(sizes.Right, 111));
                    }
                    default:
                        Y_ABORT("unreachable");
                    }
                }();
                descr.LeftSource.KeyColumnIndexes = keyColumns;
                descr.RightSource.KeyColumnIndexes = keyColumns;
                for (auto algo : params.Algorithms) {

                    TBenchmarkCaseResult result;
                    result.CaseName = CaseName(algo, keyType, keyPreset, sizes);

                    THolder<NKikimr::NMiniKQL::IComputationGraph> wideStreamGraph =
                        ConstructInnerJoinGraphStream(algo, descr);
                    NYql::NUdf::TUnboxedValue wideStream = wideStreamGraph->GetValue();
                    std::vector<NYql::NUdf::TUnboxedValue> fetchBuff;
                    ui32 cols = NKikimr::NMiniKQL::ResultColumnCount(algo, descr);
                    fetchBuff.resize(cols);
                    Cerr << "Compute graph result for case '" << result.CaseName << "'";

                    NYql::NUdf::EFetchStatus fetchStatus;
                    i64 lineCount = 0;
                    const ui64 timeStartMicroSeconds = ThreadCPUTime();

                    while ((fetchStatus = wideStream.WideFetch(fetchBuff.data(), cols)) !=
                           NYql::NUdf::EFetchStatus::Finish) {
                        if (fetchStatus == NYql::NUdf::EFetchStatus::Ok) {
                            lineCount += LineSize(algo, {fetchBuff.data(), cols});
                        }
                    }

                    result.RunDuration = TDuration::MicroSeconds(ThreadCPUTime() - timeStartMicroSeconds);
                    Cerr << ". Output line count: " << lineCount << Endl;
                    ret.push_back(result);
                }
            }
        }
    }
    return ret;
}