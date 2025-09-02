#include "../mkql_time_order_recover.h"
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_graph_saveload.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {
namespace {
    TIntrusivePtr<IRandomProvider> CreateRandomProvider() {
        return CreateDeterministicRandomProvider(1);
    }

    TIntrusivePtr<ITimeProvider> CreateTimeProvider() {
        return CreateDeterministicTimeProvider(10000000);
    }

    struct TSetup {
        TSetup(TScopedAlloc& alloc)
            : Alloc(alloc)
        {
            FunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
            RandomProvider = CreateRandomProvider();
            TimeProvider = CreateTimeProvider();

            Env.Reset(new TTypeEnvironment(Alloc));
            PgmBuilder.Reset(new TProgramBuilder(*Env, *FunctionRegistry));
        }

        THolder<IComputationGraph> BuildGraph(TRuntimeNode pgm, const std::vector<TNode*>& entryPoints = std::vector<TNode*>()) {
            Explorer.Walk(pgm.GetNode(), *Env);
            TComputationPatternOpts opts(
            Alloc.Ref(),
            *Env, GetBuiltinFactory(),
            FunctionRegistry.Get(),
            NUdf::EValidateMode::None,
            NUdf::EValidatePolicy::Fail, "OFF", EGraphPerProcess::Multi);
            Pattern = MakeComputationPattern(Explorer, pgm, entryPoints, opts);
            TComputationOptsFull compOpts = opts.ToComputationOptions(*RandomProvider, *TimeProvider);
            return Pattern->Clone(compOpts);
        }

        TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
        TIntrusivePtr<IRandomProvider> RandomProvider;
        TIntrusivePtr<ITimeProvider> TimeProvider;
        TScopedAlloc& Alloc;
        THolder<TTypeEnvironment> Env;
        THolder<TProgramBuilder> PgmBuilder;
        TExploringNodeVisitor Explorer;
        IComputationPattern::TPtr Pattern;
    };

    using TTestInputData = std::vector<std::tuple<i64, std::string, ui32, std::string>>;

    THolder<IComputationGraph> BuildGraph(
        TSetup& setup,
        bool streamingMode,
        const TTestInputData& input) {
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        const auto structType = pgmBuilder.NewStructType({
            {"time", pgmBuilder.NewDataType(NUdf::EDataSlot::Int64)},
            {"key", pgmBuilder.NewDataType(NUdf::EDataSlot::String)},
            {"sum", pgmBuilder.NewDataType(NUdf::EDataSlot::Uint32)},
            {"part", pgmBuilder.NewDataType(NUdf::EDataSlot::String)}
        });

        TVector<TRuntimeNode> items;
        for (size_t i = 0; i < input.size(); ++i) {
            const auto& [time, key, sum, part] = input[i];
            items.push_back(pgmBuilder.NewStruct({
                {"time", pgmBuilder.NewDataLiteral(time)},
                {"key", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(key)},
                {"sum", pgmBuilder.NewDataLiteral(sum)},
                {"part", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(part)},
            }));
        }

        const auto list = pgmBuilder.NewList(structType, std::move(items));
        auto inputFlow = pgmBuilder.ToFlow(list);
        auto pgmReturn = pgmBuilder.MatchRecognizeCore(
            inputFlow,
            [&](TRuntimeNode item) {
                return pgmBuilder.NewTuple({pgmBuilder.Member(item, "part")});
            },
            {},
            {"key"sv},
            {[&](TRuntimeNode /*measureInputDataArg*/, TRuntimeNode /*matchedVarsArg*/) {
                return pgmBuilder.NewDataLiteral<ui32>(56);
            }},
            {
                {NYql::NMatchRecognize::TRowPatternFactor{"A", 3, 3, false, false, false}}
            },
            {"A"sv},
            {[&](TRuntimeNode /*inputDataArg*/, TRuntimeNode /*matchedVarsArg*/, TRuntimeNode /*currentRowIndexArg*/) {
                return pgmBuilder.NewDataLiteral<bool>(true);
            }},
            streamingMode,
            {NYql::NMatchRecognize::EAfterMatchSkipTo::NextRow, ""},
            NYql::NMatchRecognize::ERowsPerMatch::OneRow
        );

        auto graph = setup.BuildGraph(pgmReturn);
        return graph;
    }
}

Y_UNIT_TEST_SUITE(MatchRecognizeSaveLoadTest) {
    void TestWithSaveLoadImpl(bool streamingMode) {
        TScopedAlloc alloc(__LOCATION__);
        std::vector<std::tuple<ui32, i64, ui32>> result;
        TSetup setup1(alloc);

        const TTestInputData input = {
            {1000, "A", 101, "P"},
            {1001, "B", 102, "P"},
            {1002, "C", 103, "P"},      // <- match end
            {1003, "D", 103, "P"}};     // <- not processed

        auto graph1 = BuildGraph(setup1,streamingMode, input);

        auto value = graph1->GetValue();

        UNIT_ASSERT(!value.IsFinish() && value);
        auto v = value.GetElement(0).Get<ui32>();

        TString graphState = graph1->SaveGraphState();

        graph1.Reset();

        TSetup setup2(alloc);

        auto graph2 = BuildGraph(setup2, streamingMode, TTestInputData{{1003, "D", 103, "P"}});
        graph2->LoadGraphState(graphState);

        value = graph2->GetValue();
        UNIT_ASSERT(!value.IsFinish() && value);
        v = value.GetElement(0).Get<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(56, v);
    }

    Y_UNIT_TEST(StreamingMode) {
        TestWithSaveLoadImpl(true);
    }

    Y_UNIT_TEST(NotStreamingMode) {
        TestWithSaveLoadImpl(false);
    }
}

} // namespace NKikimr::NMiniKQL
