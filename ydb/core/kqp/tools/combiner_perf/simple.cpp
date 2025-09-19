#include "simple.h"

#include "factories.h"
#include "streams.h"
#include "printout.h"
#include "subprocess.h"
#include "kqp_setup.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>


namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool LLVM>
THolder<IComputationGraph> BuildGraph(TKqpSetup<LLVM>& setup, IDataSampler& sampler, size_t memLimit)
{
    TKqpProgramBuilder& pb = setup.GetKqpBuilder();

    const auto streamItemType = pb.NewMultiType({sampler.GetKeyType(pb), pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    const auto pgmReturn = pb.FromFlow(pb.WideCombiner(
        pb.ToFlow(TRuntimeNode(streamCallable, false)),
        memLimit,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {pb.AggrAdd(state.front(), items.back())};
        },
        [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {keys.front(), state.front()};
        }));

    auto graph = setup.BuildGraph(pgmReturn, {streamCallable});
    auto myStream = NUdf::TUnboxedValuePod(sampler.MakeStream(graph->GetHolderFactory()).Release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(myStream));

    return graph;
}

template<bool LLVM>
TRunResult RunTestOverGraph(const TRunParams& params, const bool needsVerification, const bool measureReferenceMemory)
{
    TKqpSetup<LLVM> setup(GetPerfTestFactory());

    NYql::NLog::InitLogger("cerr", false);

    THolder<IDataSampler> sampler = CreateWideSamplerFromParams(params);
    Cerr << "Sampler type: " << sampler->Describe() << Endl;

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

    auto graphRun1 = BuildGraph(setup, *sampler, params.WideCombinerMemLimit);

    TRunResult result;

    if (measureReferenceMemory || params.TestMode == ETestMode::RefOnly) {
        long maxRssBefore = GetMaxRSS();
        result.ReferenceTime = measureRefTime(graphRun1, *sampler);
        result.ReferenceMaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
        return result;
    }

    if (params.TestMode == ETestMode::GraphOnly || params.TestMode == ETestMode::Full) {
        long maxRssBefore = GetMaxRSS();
        result.ResultTime = measureGraphTime(graphRun1);
        result.MaxRSSDelta = GetMaxRSSDelta(maxRssBefore);

        if (params.TestMode == ETestMode::GraphOnly) {
            return result;
        }
    }

    if (params.TestMode == ETestMode::GeneratorOnly || params.TestMode == ETestMode::Full) {
        // Measure the input stream run time
        Cerr << "Generator run" << Endl;
        const auto devnullStream = sampler->MakeStream(graphRun1->GetHolderFactory());
        const auto devnullStart = GetThreadCPUTime();
        {
            NUdf::TUnboxedValue columns[2];
            while (devnullStream->WideFetch(columns, 2) == NUdf::EFetchStatus::Ok) {
            }
        }
        result.GeneratorTime = GetThreadCPUTimeDelta(devnullStart);

        if (params.TestMode == ETestMode::GeneratorOnly) {
            return result;
        }
    }

    result.ReferenceTime = measureRefTime(graphRun1, *sampler);

    // Verification (re-run the graph again but actually collect the output values into a map)
    if (needsVerification) {
        Cerr << "Compute and verify the graph result" << Endl;
        auto graphRun2 = BuildGraph(setup, *sampler, params.WideCombinerMemLimit);
        {
            sampler->VerifyStreamVsReference(graphRun2->GetValue());
        }
    }

    return result;
}

}

template<bool LLVM>
void RunTestSimple(const TRunParams& params, TTestResultCollector& printout)
{
    std::optional<TRunResult> finalResult;

    Cerr << "======== " << __func__ << ", keys: " << params.NumKeys << ", llvm: " << LLVM << ", mem limit: " << params.WideCombinerMemLimit << Endl;

    if (params.NumAttempts <= 1 && !params.MeasureReferenceMemory && !params.AlwaysSubprocess) {
        finalResult = RunTestOverGraph<LLVM>(params, true, false);
    }
    else {
        for (int i = 1; i <= params.NumAttempts; ++i) {
            Cerr << "------ Run " << i << " of " << params.NumAttempts << Endl;

            const bool needsVerification = (i == 1);

            TRunResult runResult = RunForked([&]() {
                return RunTestOverGraph<LLVM>(params, needsVerification, false);
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
            return RunTestOverGraph<LLVM>(params, false, true);
        });
        Y_ENSURE(finalResult.has_value());
        finalResult->ReferenceMaxRSSDelta = runResult.ReferenceMaxRSSDelta;
    }

    printout.SubmitMetrics(params, *finalResult, "WideCombiner", LLVM);
}

template void RunTestSimple<false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestSimple<true>(const TRunParams& params, TTestResultCollector& printout);

}
}
