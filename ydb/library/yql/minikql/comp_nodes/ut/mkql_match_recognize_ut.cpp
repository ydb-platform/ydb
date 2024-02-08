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

            using TTestInputData = std::vector<std::tuple<i64, std::string, ui32, std::string>>;
            using TTestData = std::vector<std::tuple<ui32, i64, ui32>>;

            THolder<IComputationGraph> BuildGraph(TSetup& setup, const TTestInputData& input) {
                TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

                auto structType = pgmBuilder.NewStructType({
                    {"time", pgmBuilder.NewDataType(NUdf::TDataType<i64>::Id)},
                    {"key", pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id)},
                    {"sum", pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id)},
                    {"part", pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id)}});

                TVector<TRuntimeNode> items;
                // constexpr ui64 g_Yield = std::numeric_limits<ui64>::max();
                // items.push_back(pgmBuilder.NewDataLiteral<ui64>(g_Yield));
                for (size_t i = 0; i < input.size(); ++i)
                {
                    auto time = pgmBuilder.NewDataLiteral<i64>(std::get<0>(input[i]));
                    auto key = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NUdf::TStringRef(std::get<1>(input[i])));
                    auto sum = pgmBuilder.NewDataLiteral<ui32>(std::get<2>(input[i]));
                    auto part = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(NUdf::TStringRef(std::get<3>(input[i])));

                    auto item = pgmBuilder.NewStruct(structType,
                        {{"time", time}, {"key", key}, {"sum", sum},  {"part", part}});
                    items.push_back(std::move(item));
                }
                
                const auto list = pgmBuilder.NewList(structType, std::move(items));
                auto inputFlow = pgmBuilder.ToFlow(list);

                i64 delay = -10;
                i64 ahead = 30;
                ui32 rowLimit = 20;

                // MEASURES
                //  LAST(A.dt) as dt_begin
                // ONE ROW PER MATCH
                // PATTERN ( A{3, 3} )
                // DEFINE A as True) 


                TVector<TStringBuf> partitionColumns;// =  {TStringBuf("a")};
                TVector<std::pair<TStringBuf, TProgramBuilder::TBinaryLambda>> getMeasures = {{
                    std::make_pair(
                        TStringBuf("key"),
                        [&](TRuntimeNode measureInputDataArg, TRuntimeNode matchedVarsArg) {
                         //   auto run =  pgmBuilder.Take(measureInputDataArg, pgmBuilder.NewDataLiteral<ui64>(0));
                            return pgmBuilder.NewDataLiteral<ui32>(56);
                        }
                )}};
                TVector<std::pair<TStringBuf, TProgramBuilder::TTernaryLambda>> getDefines = {{
                    std::make_pair(
                        TStringBuf("A"),
                        [&](TRuntimeNode inputDataArg, TRuntimeNode matchedVarsArg, TRuntimeNode currentRowIndexArg) {
                            return pgmBuilder.NewDataLiteral<bool>(true);
                        }
                )}};

                auto pgmReturn = pgmBuilder.MatchRecognizeCore(
                    inputFlow,
                    [&](TRuntimeNode item) {
                        return pgmBuilder.Member(item, "part");
                    },
                    partitionColumns,              // partitionColumns
                    getMeasures,
                    {
                        {NYql::NMatchRecognize::TRowPatternFactor{"A", 3, 3, false, false, false}}
                    },
                    getDefines,
                    true);

                auto graph = setup.BuildGraph(pgmReturn);
                return graph;
            }
        }

        Y_UNIT_TEST_SUITE(TMiniKQLMatchRecognizeSaveLoadTest) {
            void TestWithSaveLoadImpl(
                const TTestInputData& input,
                const TTestData& expected)
            {
                TScopedAlloc alloc(__LOCATION__);
                std::vector<std::tuple<ui32, i64, ui32>> result;
                TSetup setup1(alloc);
                auto graph1 = BuildGraph(setup1, input);

                auto value = graph1->GetValue();

                std::cerr << "IsFinish " << value.IsFinish() << std::endl;
                UNIT_ASSERT(!value.IsFinish() && value);
                auto v = value.GetElement(0).Get<ui32>();
                std::cerr << "GetElement " << v << std::endl;

                TString graphState = graph1->SaveGraphState();

                std::cerr << "----------------------" << std::endl;
                std::cerr << "State size  " << graphState.size() << std::endl;
                TSetup setup2(alloc);

                auto graph2 = BuildGraph(setup2, TTestInputData{{1003, "D", 103, "P"}});
                graph2->LoadGraphState(graphState);

                value = graph2->GetValue();
                UNIT_ASSERT(!value.IsFinish() && value);
                v = value.GetElement(0).Get<ui32>();
                std::cerr << "GetElement " << v << std::endl;
            }

            const TTestInputData input = {
                // Time; Key; Value; PartitionKey
                {1000, "A", 101, "P"},
                {1001, "B", 102, "P"},
                {1002, "C", 103, "P"},      // <- match end
                {1003, "D", 103, "P"}};     // <- not processed

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
