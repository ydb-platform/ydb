#include "simple_last.h"

#include "factories.h"
#include "converters.h"
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

template<bool LLVM, bool Spilling>
void RunTestCombineLastSimple(const TRunParams& params, TTestResultCollector& printout)
{
    TSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    printout.SubmitTestNameAndParams(params, __func__, LLVM, Spilling);

    const size_t numSamples = params.RowsPerRun;
    const size_t numIters = params.NumRuns; // Will process numSamples * numIters items
    const size_t maxKey = params.MaxKey; // maxKey + 1 distinct keys, each key multiplied by 64 for some reason

    TString64DataSampler sampler(numSamples, maxKey, numIters, params.LongStringKeys);
    // or T6464DataSampler sampler(numSamples, maxKey, numIters); -- maybe make selectable from params
    Cerr << "Sampler type: " << sampler.Describe() << Endl;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto streamItemType = pb.NewMultiType({sampler.GetKeyType(pb), pb.NewDataType(NUdf::TDataType<ui64>::Id)});
    const auto streamType = pb.NewStreamType(streamItemType);
    const auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    /*
    flow: generate a flow of wide MultiType values (uint64 { key, value } pairs) from the input stream
    extractor: get key from an item
    init: initialize the state with the item value
    update: state += value (why AggrAdd though?)
    finish: return key + state
    */
    const auto pgmReturn = pb.Collect(pb.NarrowMap(
        WideLastCombiner<Spilling>(
            pb,
            pb.ToFlow(TRuntimeNode(streamCallable, false)),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; }, // extractor
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; }, // init
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back())};
            }, // update
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), state.front()}; } // finish
        ), // NarrowMap flow
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); } // NarrowMap handler
    ));

    const auto graph = setup.BuildGraph(pgmReturn, {streamCallable});
    if (Spilling) {
        graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
    }

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

    const auto t1 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto graphTime = TInstant::Now() - t1;

    // Verification
    sampler.VerifyComputedValueVsReference(value);

    printout.SubmitTimings(graphTime, cppTime, devnullTime);
}

template void RunTestCombineLastSimple<false, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestCombineLastSimple<false, true>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestCombineLastSimple<true, false>(const TRunParams& params, TTestResultCollector& printout);
template void RunTestCombineLastSimple<true, true>(const TRunParams& params, TTestResultCollector& printout);

}
}
