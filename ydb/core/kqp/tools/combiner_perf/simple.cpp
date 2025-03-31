#include "simple.h"
#include "factories.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {

std::vector<std::pair<i8, double>> MakeSamples() {
    constexpr auto total_samples = 100'000'000ULL;

    std::default_random_engine eng;
    std::uniform_int_distribution<int> keys(-100, +100);
    std::uniform_real_distribution<double> unif(-999.0, +999.0);

    std::vector<std::pair<i8, double>> samples(total_samples);

    eng.seed(std::time(nullptr));
    std::generate(samples.begin(), samples.end(), std::bind(&std::make_pair<i8, double>, std::bind(std::move(keys), std::move(eng)), std::bind(std::move(unif), std::move(eng))));
    return samples;
}

const std::vector<std::pair<i8, double>> I8Samples = MakeSamples();

template<bool LLVM>
void RunTestSimple()
{
    TSetup<LLVM> setup(GetPerfTestFactory());

    Cerr << "Simple i8 sample has " << I8Samples.size() << " rows" << Endl;

    double positive = 0.0, negative = 0.0;
    const auto t = TInstant::Now();
    for (const auto& sample : I8Samples) {
        (sample.second > 0.0 ? positive : negative) += sample.second;
    }
    const auto cppTime = TInstant::Now() - t;

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
        [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }), 0ULL,
        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.AggrGreater(items.front(), pb.NewDataLiteral(0.0))}; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return items; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.front())}; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList { return state; }),
        [&](TRuntimeNode::TList items) { return items.front(); }
    ));

    const auto graph = setup.BuildGraph(pgmReturn, {list});
    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
    std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

    const auto t1 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto first = value.GetElement(0);
    const auto second = value.GetElement(1);
    const auto t2 = TInstant::Now();

    if (first.template Get<double>() > 0.0) {
        UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), positive);
        UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), negative);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), negative);
        UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), positive);
    }

    Cerr << "WideCombiner graph runtime is: " << t2 - t1 << " vs. reference C++ implementation: " << cppTime << Endl << Endl;
}

template void RunTestSimple<false>();
template void RunTestSimple<true>();

}
}
