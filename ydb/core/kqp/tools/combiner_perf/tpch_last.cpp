#include "tpch_last.h"
#include "factories.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mock_spiller_factory_ut.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>

namespace NKikimr {
namespace NMiniKQL {

constexpr auto TpchDateBorder = 9124596000000000ULL;

std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> MakeTpchSamples() {
    constexpr auto total_samples = 5000000ULL;

    std::default_random_engine eng;
    std::uniform_int_distribution<ui64> dates(694303200000000ULL, 9124596000000005ULL);
    std::uniform_int_distribution<int> keys(0U, 3U);
    std::uniform_real_distribution<double> prices(900., 105000.0);
    std::uniform_real_distribution<double> taxes(0., 0.08);
    std::uniform_real_distribution<double> discs(0., 0.1);
    std::uniform_real_distribution<double> qntts(1., 50.);

    std::random_device rd;
    std::mt19937 gen(rd());

    std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> samples(total_samples);

    eng.seed(std::time(nullptr));

    std::generate(samples.begin(), samples.end(), [&]() {
        switch(keys(gen)) {
            case 0U: return std::make_tuple(dates(gen), "N", "O", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 1U: return std::make_tuple(dates(gen), "A", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 2U: return std::make_tuple(dates(gen), "N", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
            case 3U: return std::make_tuple(dates(gen), "R", "F", prices(gen), taxes(gen), discs(gen), qntts(gen));
        }
        Y_ABORT("Unexpected");
    });
    return samples;
}


template<bool LLVM, bool Spilling>
void RunTestLastTpch()
{
    TSetup<LLVM, Spilling> setup(GetPerfTestFactory());

    const std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> tpchSamples = MakeTpchSamples();

    Cerr << "tpc-h sample has " << tpchSamples.size() << " rows" << Endl;

    struct TPairHash { size_t operator()(const std::pair<std::string_view, std::string_view>& p) const { return CombineHashes(std::hash<std::string_view>()(p.first), std::hash<std::string_view>()(p.second)); } };

    std::unordered_map<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>, TPairHash> expects;
    const auto t = TInstant::Now();
    for (auto& sample : tpchSamples) {
        if (std::get<0U>(sample) <= TpchDateBorder) {
            const auto& ins = expects.emplace(std::pair<std::string_view, std::string_view>{std::get<1U>(sample), std::get<2U>(sample)}, std::pair<ui64, std::array<double, 5U>>{0ULL, {0., 0., 0., 0., 0.}});
            auto& item = ins.first->second;
            ++item.first;
            std::get<0U>(item.second) += std::get<3U>(sample);
            std::get<1U>(item.second) += std::get<5U>(sample);
            std::get<2U>(item.second) += std::get<6U>(sample);
            const auto v = std::get<3U>(sample) * (1. - std::get<5U>(sample));
            std::get<3U>(item.second) += v;
            std::get<4U>(item.second) += v * (1. + std::get<4U>(sample));
        }
    }
    for (auto& item : expects) {
        std::get<1U>(item.second.second) /= item.second.first;
    }
    const auto cppTime = TInstant::Now() - t;

    std::vector<std::pair<std::pair<std::string, std::string>, std::pair<ui64, std::array<double, 5U>>>> one, two;
    one.reserve(expects.size());
    two.reserve(expects.size());

    one.insert(one.cend(), expects.cbegin(), expects.cend());
    std::sort(one.begin(), one.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });

    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto listType = pb.NewListType(pb.NewTupleType({
        pb.NewDataType(NUdf::TDataType<ui64>::Id),
        pb.NewDataType(NUdf::TDataType<const char*>::Id),
        pb.NewDataType(NUdf::TDataType<const char*>::Id),
        pb.NewDataType(NUdf::TDataType<double>::Id),
        pb.NewDataType(NUdf::TDataType<double>::Id),
        pb.NewDataType(NUdf::TDataType<double>::Id),
        pb.NewDataType(NUdf::TDataType<double>::Id)
    }));
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

    const auto pgmReturn = pb.Collect(pb.NarrowMap(WideLastCombiner<Spilling>(
        pb,
        pb.WideFilter(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U), pb.Nth(item, 4U), pb.Nth(item, 5U), pb.Nth(item, 6U)}; }),
            [&](TRuntimeNode::TList items) { return pb.AggrLessOrEqual(items.front(), pb.NewDataLiteral<ui64>(TpchDateBorder)); }
        ),
        [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return {item[1U], item[2U]}; },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
            const auto price = items[3U];
            const auto disco = items[5U];
            const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
            return {pb.NewDataLiteral<ui64>(1ULL), price, disco, items[6U], v, pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), items[4U]))};
        },
        [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
            const auto price = items[3U];
            const auto disco = items[5U];
            const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
            return {pb.Increment(state[0U]), pb.AggrAdd(state[1U], price), pb.AggrAdd(state[2U], disco), pb.AggrAdd(state[3U], items[6U]), pb.AggrAdd(state[4U], v), pb.AggrAdd(state[5U], pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), items[4U])))};
        },
        [&](TRuntimeNode::TList key, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {key.front(), key.back(), state[0U], state[1U], pb.Div(state[2U], state[0U]), state[3U], state[4U], state[5U]}; }),
        [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
    ));

    const auto graph = setup.BuildGraph(pgmReturn, {list});
    if (Spilling) {
        graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
    }

    NUdf::TUnboxedValue* items = nullptr;
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(TpchSamples.size(), items));
    for (const auto& sample : TpchSamples) {
        NUdf::TUnboxedValue* elements = nullptr;
        *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(7U, elements);
        elements[0] = NUdf::TUnboxedValuePod(std::get<0U>(sample));
        elements[1] = NUdf::TUnboxedValuePod::Embedded(std::get<1U>(sample));
        elements[2] = NUdf::TUnboxedValuePod::Embedded(std::get<2U>(sample));
        elements[3] = NUdf::TUnboxedValuePod(std::get<3U>(sample));
        elements[4] = NUdf::TUnboxedValuePod(std::get<4U>(sample));
        elements[5] = NUdf::TUnboxedValuePod(std::get<5U>(sample));
        elements[6] = NUdf::TUnboxedValuePod(std::get<6U>(sample));
    }

    const auto t1 = TInstant::Now();
    const auto& value = graph->GetValue();
    const auto t2 = TInstant::Now();

    //UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

    const auto ptr = value.GetElements();
    for (size_t i = 0ULL; i < expects.size(); ++i) {
        const auto elements = ptr[i].GetElements();
        two.emplace_back(std::make_pair(elements[0].AsStringRef(), elements[1].AsStringRef()), std::pair<ui64, std::array<double, 5U>>{elements[2].template Get<ui64>(), {elements[3].template Get<double>(), elements[4].template Get<double>(), elements[5].template Get<double>(), elements[6].template Get<double>(), elements[7].template Get<double>()}});
    }

    std::sort(two.begin(), two.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });
    UNIT_ASSERT_VALUES_EQUAL(one, two);

    Cerr << "WideLastCombiner graph runtime is: " << t2 - t1 << " vs. reference C++ implementation: " << cppTime << Endl << Endl;
}

template void RunTestLastTpch<false, false>();
template void RunTestLastTpch<false, true>();
template void RunTestLastTpch<true, false>();
template void RunTestLastTpch<true, true>();


} // namespace NMiniKQL
} // namespace NKikimr
