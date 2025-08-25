#include "simple_last.h"

#include "factories.h"
#include "subprocess.h"
#include "streams.h"
#include "printout.h"
#include "preallocated_spiller.h"
#include "kqp_setup.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>
#include <util/stream/file.h>

#include <sys/wait.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

TDuration MeasureGeneratorTime(IComputationGraph& graph, const IDataSampler& sampler)
{
    const auto devnullStream = sampler.MakeStream(graph.GetHolderFactory());
    const auto devnullStart = GetThreadCPUTime();
    {
        NUdf::TUnboxedValue columns[2];
        while (devnullStream->WideFetch(columns, 2) == NUdf::EFetchStatus::Ok) {
        }
    }
    return GetThreadCPUTimeDelta(devnullStart);
}

template<bool LLVM, bool Spilling>
THolder<IComputationGraph> BuildGraph(TKqpSetup<LLVM, Spilling>& setup, std::shared_ptr<ISpillerFactory> spillerFactory, IDataSampler& sampler)
{
    TKqpProgramBuilder& pb = setup.GetKqpBuilder();

    const auto streamItemType = pb.NewMultiType({sampler.GetKeyType(pb), pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    /*
    flow: generate a flow of wide MultiType values ({ key, value } pairs) from the input stream
    extractor: get key from an item
    init: initialize the state with the item value
    update: state += value
    finish: return key + state
    */
    const auto pgmReturn = pb.FromFlow(WideLastCombiner<Spilling>(
        pb,
        pb.ToFlow(TRuntimeNode(streamCallable, false)),
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; }, // extractor
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; }, // init
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {pb.AggrAdd(state.front(), items.back())};
        }, // update
        [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), state.front()}; } // finish
    ));

    auto graph = setup.BuildGraph(pgmReturn, {streamCallable});
    if (Spilling) {
        graph->GetContext().SpillerFactory = spillerFactory;
    }

    auto myStream = NUdf::TUnboxedValuePod(sampler.MakeStream(graph->GetHolderFactory()).Release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(myStream));

    return graph;
}

template<bool LLVM, bool Spilling>
TRunResult RunTestOverGraph(const TRunParams& params, const bool needsVerification, const bool measureReferenceMemory)
{
    TKqpSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    NYql::NLog::InitLogger("cerr", false);

    THolder<IDataSampler> sampler = CreateWideSamplerFromParams(params);
    Cerr << "Sampler type: " << sampler->Describe() << Endl;

    std::shared_ptr<ISpillerFactory> spillerFactory = Spilling ? std::make_shared<TPreallocatedSpillerFactory>() : nullptr;

    auto measureGraphTime = [&](auto& computeGraphPtr) {
        // Compute node implementation

        Cerr << "Compute graph result" << Endl;
        const auto graphTimeStart = GetThreadCPUTime();
        size_t lineCount = CountWideStreamOutputs<2>(computeGraphPtr->GetValue());
        Cerr << lineCount << Endl;

        return GetThreadCPUTimeDelta(graphTimeStart);
    };

    auto measureRefTime = [&](auto& computeGraphPtr, IDataSampler& sampler) {
        // Reference implementation (sum via an std::unordered_map)

        Cerr << "Compute reference result" << Endl;
        auto referenceStream = sampler.MakeStream(computeGraphPtr->GetHolderFactory());
        const auto cppTimeStart = GetThreadCPUTime();
        sampler.ComputeReferenceResult(*referenceStream);

        return GetThreadCPUTimeDelta(cppTimeStart);
    };

    auto graphRun1 = BuildGraph(setup, spillerFactory, *sampler);

    TRunResult result;

    if (measureReferenceMemory || params.TestMode == ETestMode::RefOnly) {
        long maxRssBefore = GetMaxRSS();
        result.ReferenceTime = measureRefTime(graphRun1, *sampler);
        result.ReferenceMaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
        return result;
    }

    if (params.TestMode == ETestMode::GraphOnly || params.TestMode == ETestMode::Full) {
        long maxRssBefore = GetMaxRSS();
        Cerr << "Raw MaxRSS before: " << maxRssBefore << Endl;
        result.ResultTime = measureGraphTime(graphRun1);
        result.MaxRSSDelta = GetMaxRSSDelta(maxRssBefore);

        Cerr << "Raw MaxRSS after: " << GetMaxRSS() << Endl;
        Cerr << "MaxRSS delta, bytes: " << result.MaxRSSDelta << Endl;

        if (params.TestMode == ETestMode::GraphOnly) {
            return result;
        }

        if (spillerFactory) {
            for (const auto& spiller : static_cast<const TPreallocatedSpillerFactory*>(spillerFactory.get())->GetCreatedSpillers()) {
                const auto& putSizes = static_cast<TPreallocatedSpiller*>(spiller.get())->GetPutSizes();
                size_t allocSize = 0;
                for (const auto size : putSizes) {
                    allocSize += size;
                }
                Cerr << "Spiller: " << putSizes.size() << " allocations, " << allocSize / (1024*1024) << " MB total size" << Endl;
            }
        }
    }

    if (params.TestMode == ETestMode::GeneratorOnly || params.TestMode == ETestMode::Full) {
        // Measure the input stream run time

        Cerr << "Generator run" << Endl;
        result.GeneratorTime = MeasureGeneratorTime(*graphRun1, *sampler);

        if (params.TestMode == ETestMode::GeneratorOnly) {
            return result;
        }
    }

    result.ReferenceTime = measureRefTime(graphRun1, *sampler);

    // Verification (re-run the graph again but actually collect the output values into a map)
    if (needsVerification) {
        spillerFactory = Spilling ? std::make_shared<TMockSpillerFactory>() : nullptr;
        Cerr << "Compute and verify the graph result" << Endl;
        auto graphRun2 = BuildGraph(setup, spillerFactory, *sampler);
        sampler->VerifyStreamVsReference(graphRun2->GetValue());
    }

    return result;
}

}

template<bool LLVM, bool Spilling>
void RunTestCombineLastSimple(const TRunParams& params, TTestResultCollector& printout)
{
    std::optional<TRunResult> finalResult;

    Cerr << "======== " << __func__ << ", keys: " << params.NumKeys << ", llvm: " << LLVM << ", spilling: " << Spilling << Endl;

    if (params.NumAttempts <= 1 && !params.MeasureReferenceMemory && !params.AlwaysSubprocess) {
        finalResult = RunTestOverGraph<LLVM, Spilling>(params, params.EnableVerification, false);
    }
    else {
        for (int i = 1; i <= params.NumAttempts; ++i) {
            Cerr << "------ Run " << i << " of " << params.NumAttempts << Endl;

            const bool needsVerification = (i == 1) && params.EnableVerification;
            TRunResult runResult = RunForked([&]() {
                return RunTestOverGraph<LLVM, Spilling>(params, needsVerification, false);
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
            return RunTestOverGraph<LLVM, Spilling>(params, false, true);
        });
        Y_ENSURE(finalResult.has_value());
        finalResult->ReferenceMaxRSSDelta = runResult.ReferenceMaxRSSDelta;
    }

    printout.SubmitMetrics(params, *finalResult, "WideLastCombiner", LLVM, Spilling);
}

template void RunTestCombineLastSimple<false, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestCombineLastSimple<false, true>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestCombineLastSimple<true, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestCombineLastSimple<true, true>(const TRunParams& params, TTestResultCollector& printout);

}
}
