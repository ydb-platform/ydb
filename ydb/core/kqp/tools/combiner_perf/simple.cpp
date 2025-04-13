#include "simple.h"

#include "factories.h"
#include "streams.h"
#include "printout.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {

template<bool LLVM>
void RunTestSimple(const TRunParams& params, TTestResultCollector& printout)
{
    TSetup<LLVM> setup(GetPerfTestFactory());

    printout.SubmitTestNameAndParams(params, __func__, LLVM);

    TString64DataSampler sampler(params.RowsPerRun, params.MaxKey, params.NumRuns, params.LongStringKeys);
    // or T6464DataSampler sampler(numSamples, maxKey, numIters); -- maybe make selectable from params
    Cerr << "Sampler type: " << sampler.Describe() << Endl;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto streamItemType = pb.NewMultiType({sampler.GetKeyType(pb), pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(
        pb.ToFlow(TRuntimeNode(streamCallable, false)),
        0ULL,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.front() }; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return { items.back() } ; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {pb.AggrAdd(state.front(), items.back())};
        },
        [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            return {keys.front(), state.front()};
        }),
        [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
    ));

    const auto graph = setup.BuildGraph(pgmReturn, {streamCallable});

    // Measure the input stream run time
    const auto devnullStream = sampler.MakeStream(graph->GetHolderFactory());
    const auto devnullStart = TInstant::Now();
    {
        NUdf::TUnboxedValue columns[2];
        while (devnullStream->WideFetch(columns, 2) == NUdf::EFetchStatus::Ok) {
        }
    }
    const auto devnullTime = TInstant::Now() - devnullStart;

    // Reference implementation (sum via an std::unordered_map)
    auto referenceStream = sampler.MakeStream(graph->GetHolderFactory());
    const auto t = TInstant::Now();
    sampler.ComputeReferenceResult(*referenceStream);
    const auto cppTime = TInstant::Now() - t;

    // Compute graph implementation
    auto myStream = NUdf::TUnboxedValuePod(sampler.MakeStream(graph->GetHolderFactory()).Release());
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(myStream));

    const auto graphTimeStart = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto graphTime = TInstant::Now() - graphTimeStart;

    // Verification
    sampler.VerifyComputedValueVsReference(value);

    printout.SubmitTimings(graphTime, cppTime, devnullTime);
}

template void RunTestSimple<false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestSimple<true>(const TRunParams& params, TTestResultCollector& printout);

}
}
