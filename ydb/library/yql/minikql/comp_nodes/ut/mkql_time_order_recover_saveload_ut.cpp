#include "../mkql_time_order_recover.h"
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_graph_saveload.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
    namespace NMiniKQL {

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

            using TTestData = std::vector<std::tuple<ui32, i64, ui32>>;

            THolder<IComputationGraph> BuildGraph(TSetup& setup, const TTestData& input) {
                TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

                auto structType = pgmBuilder.NewStructType({
                    {"key", pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id)},
                    {"time", pgmBuilder.NewDataType(NUdf::TDataType<i64>::Id)},
                    {"sum", pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id)}});

                TVector<TRuntimeNode> items;
                for (size_t i = 0; i < input.size(); ++i)
                {
                    auto key = pgmBuilder.NewDataLiteral<ui32>(std::get<0>(input[i]));
                    auto time = pgmBuilder.NewDataLiteral<i64>(std::get<1>(input[i]));
                    auto sum = pgmBuilder.NewDataLiteral<ui32>(std::get<2>(input[i]));

                    auto item = pgmBuilder.NewStruct(structType,
                        {{"key", key}, {"time", time}, {"sum", sum}});
                    items.push_back(std::move(item));
                }

                const auto list = pgmBuilder.NewList(structType, std::move(items));
                auto inputFlow = pgmBuilder.ToFlow(list);

                i64 delay = -10;
                i64 ahead = 30;
                ui32 rowLimit = 20;

                auto pgmReturn = pgmBuilder.TimeOrderRecover(
                    inputFlow,
                    [&](TRuntimeNode item) {
                        return pgmBuilder.Member(item, "time");
                    },
                    pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&delay, sizeof(delay))),
                    pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&ahead, sizeof(ahead))),
                    pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&rowLimit, sizeof(rowLimit))));

                auto graph = setup.BuildGraph(pgmReturn);
                return graph;
            }
        }

        Y_UNIT_TEST_SUITE(TMiniKQLTimeOrderRecoverSaveLoadTest) {
            void TestWithSaveLoadImpl(
                const TTestData& input,
                const TTestData& expected)
            {
                TScopedAlloc alloc(__LOCATION__);
                std::vector<std::tuple<ui32, i64, ui32>> result;
                TSetup setup1(alloc);
                auto graph1 = BuildGraph(setup1, input);

                auto value = graph1->GetValue();
                UNIT_ASSERT(!value.IsFinish() && value);
                result.emplace_back(
                    value.GetElement(1).Get<ui32>(),
                    value.GetElement(3).Get<i64>(),
                    value.GetElement(2).Get<ui32>());

                TString graphState = graph1->SaveGraphState();
                TSetup setup2(alloc);

                auto graph2 = BuildGraph(setup2, TTestData());
                graph2->LoadGraphState(graphState);

                while (true)
                {
                    value = graph2->GetValue();
                    if (value.IsFinish())
                        break;

                    result.emplace_back(
                        value.GetElement(1).Get<ui32>(),
                        value.GetElement(3).Get<i64>(),
                        value.GetElement(2).Get<ui32>());
                }

                UNIT_ASSERT_EQUAL(result, expected);
            }

            const std::vector<std::tuple<ui32, i64, ui32>> input = {
                // Group; Time; Value
                {1000, 800, 101},
                {1000, 800, 102},
                {1000, 800, 103},
                {1000, 800, 104},
                {1000, 800, 105},
                {2000, 802, 300},
                {3000, 801, 200}};

            const std::vector<std::tuple<ui32, i64, ui32>> expected = {
                // Group; Time; Value
                {1000, 800, 101},
                {1000, 800, 102},
                {1000, 800, 103},
                {1000, 800, 104},
                {1000, 800, 105},
                {3000, 801, 200},
                {2000, 802, 300}};

            Y_UNIT_TEST(Test1) {
                TestWithSaveLoadImpl(input, expected);
            }

        }

    } // namespace NMiniKQL
} // namespace NKikimr
