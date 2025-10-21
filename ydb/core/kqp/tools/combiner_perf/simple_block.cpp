#include "simple_block.h"

#include "factories.h"
#include "streams.h"
#include "printout.h"
#include "subprocess.h"
#include "kqp_setup.h"

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

namespace {

template<bool LLVM, bool Spilling>
TRunResult RunTestOverGraph(const TRunParams& params, const bool measureReferenceMemory)
{
    TKqpSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    auto sampler = CreateBlockSamplerFromParams(params);

    TDqProgramBuilder& pb = setup.GetKqpBuilder();

    auto keyBaseType = sampler->BuildKeyType(*setup.Env);
    auto valueBaseType = sampler->BuildValueType(*setup.Env);
    auto keyBlockType = pb.NewBlockType(keyBaseType, TBlockType::EShape::Many);
    auto valueBlockType = pb.NewBlockType(valueBaseType, TBlockType::EShape::Many);
    auto blockSizeType = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    auto blockSizeBlockType = pb.NewBlockType(blockSizeType, TBlockType::EShape::Scalar);
    const auto streamItemType = pb.NewMultiType({keyBlockType, valueBlockType, blockSizeBlockType});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamResultItemType = pb.NewMultiType({keyBlockType, valueBlockType, blockSizeBlockType});
    [[maybe_unused]] const auto streamResultType = pb.NewStreamType(streamResultItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    // TODO: This should be generated in the sampler
    [[maybe_unused]] ui32 keys[] = {0};
    TAggInfo aggs[] = {
        TAggInfo{
            .Name = "sum",
            .ArgsColumns = {1u},
        }
    };

    /*
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
    */

    auto pgmReturn = pb.Collect(pb.NarrowMap(
        pb.DqHashCombine(
            pb.ToFlow(TRuntimeNode(streamCallable, false)),
            params.WideCombinerMemLimit,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back())};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {keys.front(), state.front()};
            }
        ),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); } // NarrowMap handler
    ));

    auto measureGraphTime = [&](auto& computeGraphPtr, NUdf::TUnboxedValue& resultList) {
        // Compute graph implementation
        auto stream = NUdf::TUnboxedValuePod(sampler->MakeStream(computeGraphPtr->GetContext()).Release());
        computeGraphPtr->GetEntryPoint(0, true)->SetValue(computeGraphPtr->GetContext(), std::move(stream));
        const auto graphStart = GetThreadCPUTime();
        resultList = computeGraphPtr->GetValue();
        return GetThreadCPUTimeDelta(graphStart);
    };

    auto measureRefTime = [&](auto& computeGraphPtr) {
        // Reference implementation (sum via an std::unordered_map)
        const auto cppStart = GetThreadCPUTime();
        sampler->ComputeReferenceResult(computeGraphPtr->GetContext());
        return GetThreadCPUTimeDelta(cppStart);
    };

    auto measureGeneratorTime = [&](auto& computeGraphPtr) {
        // Generator-only loop
        const auto devnullStreamPtr = sampler->MakeStream(computeGraphPtr->GetContext());
        auto& devnullStream = *devnullStreamPtr;
        size_t numBlocks = 0;
        const auto timeStart = GetThreadCPUTime();
        {
            NUdf::TUnboxedValue columns[3];
            while (devnullStream.WideFetch(columns, 3) == NUdf::EFetchStatus::Ok) {
                ++numBlocks;
            }
        }
        auto duration = GetThreadCPUTimeDelta(timeStart);
        Cerr << "Blocks generated: " << numBlocks << Endl;
        return duration;
    };

    const auto graph = setup.BuildGraph(pgmReturn, {streamCallable});

    TRunResult result;

    if (measureReferenceMemory || params.TestMode == ETestMode::RefOnly) {
        long maxRssBefore = GetMaxRSS();
        result.ReferenceTime = measureRefTime(graph);
        result.ReferenceMaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
        return result;
    }

    NUdf::TUnboxedValue resultList;

    if (params.TestMode == ETestMode::GraphOnly || params.TestMode == ETestMode::Full) {
        long maxRssBefore = GetMaxRSS();
        result.ResultTime = measureGraphTime(graph, resultList);
        result.MaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
        if (params.TestMode == ETestMode::GraphOnly) {
            return result;
        }
    }

    // Measure the input stream own run time
    if (params.TestMode == ETestMode::GeneratorOnly || params.TestMode == ETestMode::Full) {
        result.GeneratorTime = measureGeneratorTime(graph);
        if (params.TestMode == ETestMode::GeneratorOnly) {
            return result;
        }
    }

    result.ReferenceTime = measureRefTime(graph);

    // Compute results directly from raw samples
    sampler->ComputeRawResult();

    // Verify the reference result against the raw samples to test the input stream implementation
    sampler->VerifyReferenceResultAgainstRaw();

    // Verify the compute graph result value against the reference implementation
    sampler->VerifyGraphResultAgainstReference(resultList);

    return result;
}

}


template<bool LLVM, bool Spilling>
void RunTestBlockCombineHashedSimple(const TRunParams& params, TTestResultCollector& printout)
{
    std::optional<TRunResult> finalResult;

    Cerr << "======== " << __func__ << ", keys: " << params.NumKeys << ", block size: " << params.BlockSize << ", llvm: " << LLVM << Endl;

    if (params.NumAttempts <= 1 && !params.MeasureReferenceMemory && !params.AlwaysSubprocess) {
        finalResult = RunTestOverGraph<LLVM, Spilling>(params, false);
    } else {
        for (int i = 1; i <= params.NumAttempts; ++i) {
            Cerr << "------ : run " << i << " of " << params.NumAttempts << Endl;

            TRunResult runResult = RunForked([&]() {
                return RunTestOverGraph<LLVM, Spilling>(params, false);
            });

            if (finalResult.has_value()) {
                MergeRunResults(runResult, *finalResult);
            } else {
                finalResult.emplace(runResult);
            }
        }
    }

    if (params.MeasureReferenceMemory) {
        Cerr << "------ Reference memory measurement run" << Endl;
        TRunResult runResult = RunForked([&]() {
            return RunTestOverGraph<LLVM, Spilling>(params, true);
        });
        Y_ENSURE(finalResult.has_value());
        finalResult->ReferenceMaxRSSDelta = runResult.ReferenceMaxRSSDelta;
    }

    printout.SubmitMetrics(params, *finalResult, "BlockCombineHashed", LLVM, Spilling);
}

template void RunTestBlockCombineHashedSimple<false, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestBlockCombineHashedSimple<false, true>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestBlockCombineHashedSimple<true, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestBlockCombineHashedSimple<true, true>(const TRunParams& params, TTestResultCollector& printout);


}
}
