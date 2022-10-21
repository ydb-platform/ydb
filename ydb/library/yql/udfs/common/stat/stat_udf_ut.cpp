#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <util/random/random.h>
#include <util/system/sanitizers.h>
#include <array>

namespace NYql {
using namespace NKikimr::NMiniKQL;

    namespace NUdf {
        extern NUdf::TUniquePtr<NUdf::IUdfModule> CreateStatModule();
    }

    Y_UNIT_TEST_SUITE(TUDFStatTest) {
        Y_UNIT_TEST(SimplePercentile) {
            auto mutableFunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(10000000);
            NUdf::TUniquePtr<NUdf::IUdfModule> module = NUdf::CreateStatModule();
            mutableFunctionRegistry->AddModule("", "Stat", std::move(module));
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, *mutableFunctionRegistry);
            auto udfTDigest_Create = pgmBuilder.Udf("Stat.TDigest_Create");
            auto udfTDigest_AddValue = pgmBuilder.Udf("Stat.TDigest_AddValue");
            auto udfTDigest_GetPercentile = pgmBuilder.Udf("Stat.TDigest_GetPercentile");

            TRuntimeNode pgmDigest;
            {
                auto param1 = pgmBuilder.NewDataLiteral<double>(0.0);
                TVector<TRuntimeNode> params = {param1};
                pgmDigest = pgmBuilder.Apply(udfTDigest_Create, params);
            }

            for (int n = 1; n < 10; n += 1) {
                auto param2 = pgmBuilder.NewDataLiteral((double)n);
                TVector<TRuntimeNode> params = {pgmDigest, param2};
                pgmDigest = pgmBuilder.Apply(udfTDigest_AddValue, params);
            }

            TRuntimeNode pgmReturn;
            {
                auto param2 = pgmBuilder.NewDataLiteral<double>(0.9);
                TVector<TRuntimeNode> params = {pgmDigest, param2};
                pgmReturn = pgmBuilder.Apply(udfTDigest_GetPercentile, params);
            }

            TExploringNodeVisitor explorer;
            explorer.Walk(pgmReturn.GetNode(), env);
            TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(), mutableFunctionRegistry.Get(),
                                            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "", EGraphPerProcess::Multi);
            auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
            auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
            auto value = graph->GetValue();
            UNIT_ASSERT_DOUBLES_EQUAL(value.Get<double>(), 8.5, 0.001);
        }

        Y_UNIT_TEST(SimplePercentileSpecific) {
            auto mutableFunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(1);
            NUdf::TUniquePtr<NUdf::IUdfModule> module = NUdf::CreateStatModule();
            mutableFunctionRegistry->AddModule("", "Stat", std::move(module));
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, *mutableFunctionRegistry);
            auto udfTDigest_Create = pgmBuilder.Udf("Stat.TDigest_Create");
            auto udfTDigest_AddValue = pgmBuilder.Udf("Stat.TDigest_AddValue");
            auto udfTDigest_GetPercentile = pgmBuilder.Udf("Stat.TDigest_GetPercentile");

            TRuntimeNode pgmDigest;
            {
                auto param1 = pgmBuilder.NewDataLiteral<double>(75.0);
                TVector<TRuntimeNode> params = {param1};
                pgmDigest = pgmBuilder.Apply(udfTDigest_Create, params);
            }

            TVector<double> vals = {800, 20, 150};
            for (auto val : vals) {
                auto param2 = pgmBuilder.NewDataLiteral(val);
                TVector<TRuntimeNode> params = {pgmDigest, param2};
                pgmDigest = pgmBuilder.Apply(udfTDigest_AddValue, params);
            }

            TRuntimeNode pgmReturn;
            {
                auto param2 = pgmBuilder.NewDataLiteral<double>(0.5);
                TVector<TRuntimeNode> params = {pgmDigest, param2};
                pgmReturn = pgmBuilder.Apply(udfTDigest_GetPercentile, params);
            }

            TExploringNodeVisitor explorer;
            explorer.Walk(pgmReturn.GetNode(), env);
            TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(), mutableFunctionRegistry.Get(),
                                            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "", EGraphPerProcess::Multi);
            auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
            auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
            auto value = graph->GetValue();
            Cerr << value.Get<double>() << Endl;
            //~ UNIT_ASSERT_DOUBLES_EQUAL(value.Get<double>(), 9.0, 0.001);
        }

        Y_UNIT_TEST(SerializedPercentile) {
            auto mutableFunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(1);
            NUdf::TUniquePtr<NUdf::IUdfModule> module = NUdf::CreateStatModule();
            mutableFunctionRegistry->AddModule("", "Stat", std::move(module));
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, *mutableFunctionRegistry);
            auto udfTDigest_Create = pgmBuilder.Udf("Stat.TDigest_Create");
            auto udfTDigest_AddValue = pgmBuilder.Udf("Stat.TDigest_AddValue");
            auto udfTDigest_GetPercentile = pgmBuilder.Udf("Stat.TDigest_GetPercentile");
            auto udfTDigest_Serialize = pgmBuilder.Udf("Stat.TDigest_Serialize");
            auto udfTDigest_Deserialize = pgmBuilder.Udf("Stat.TDigest_Deserialize");

            TRuntimeNode pgmDigest;
            {
                auto param1 = pgmBuilder.NewDataLiteral<double>(0.0);
                TVector<TRuntimeNode> params = {param1};
                pgmDigest = pgmBuilder.Apply(udfTDigest_Create, params);
            }

            for (int n = 1; n < 10; n += 1) {
                auto param2 = pgmBuilder.NewDataLiteral((double)n);
                TVector<TRuntimeNode> params = {pgmDigest, param2};
                pgmDigest = pgmBuilder.Apply(udfTDigest_AddValue, params);
            }

            TRuntimeNode pgmSerializedData;
            {
                TVector<TRuntimeNode> params = {pgmDigest};
                pgmSerializedData = pgmBuilder.Apply(udfTDigest_Serialize, params);
            }

            TRuntimeNode pgmDigest2;
            {
                TVector<TRuntimeNode> params = {pgmSerializedData};
                pgmDigest2 = pgmBuilder.Apply(udfTDigest_Deserialize, params);
            }

            TRuntimeNode pgmReturn;
            {
                auto param2 = pgmBuilder.NewDataLiteral<double>(0.9);
                TVector<TRuntimeNode> params = {pgmDigest2, param2};
                pgmReturn = pgmBuilder.Apply(udfTDigest_GetPercentile, params);
            }

            TExploringNodeVisitor explorer;
            explorer.Walk(pgmReturn.GetNode(), env);
            TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(), mutableFunctionRegistry.Get(),
                                            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "", EGraphPerProcess::Multi);
            auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
            auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
            auto value = graph->GetValue();
            UNIT_ASSERT_DOUBLES_EQUAL(value.Get<double>(), 8.5, 0.001);
        }

        Y_UNIT_TEST(SerializedMergedPercentile) {
            auto mutableFunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(1);
            NUdf::TUniquePtr<NUdf::IUdfModule> module = NUdf::CreateStatModule();
            mutableFunctionRegistry->AddModule("", "Stat", std::move(module));
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, *mutableFunctionRegistry);
            auto udfTDigest_Create = pgmBuilder.Udf("Stat.TDigest_Create");
            auto udfTDigest_AddValue = pgmBuilder.Udf("Stat.TDigest_AddValue");
            auto udfTDigest_GetPercentile = pgmBuilder.Udf("Stat.TDigest_GetPercentile");
            auto udfTDigest_Serialize = pgmBuilder.Udf("Stat.TDigest_Serialize");
            auto udfTDigest_Deserialize = pgmBuilder.Udf("Stat.TDigest_Deserialize");
            auto udfTDigest_Merge = pgmBuilder.Udf("Stat.TDigest_Merge");

            TVector<TRuntimeNode> pgmSerializedDataVector;

            for (int i = 0; i < 100; i += 10) {
                TRuntimeNode pgmDigest;
                {
                    auto param1 = pgmBuilder.NewDataLiteral(double(i) / 10);
                    TVector<TRuntimeNode> params = {param1};
                    pgmDigest = pgmBuilder.Apply(udfTDigest_Create, params);
                }

                for (int n = i + 1; n < i + 10; n += 1) {
                    auto param2 = pgmBuilder.NewDataLiteral(double(n) / 10);
                    TVector<TRuntimeNode> params = {pgmDigest, param2};
                    pgmDigest = pgmBuilder.Apply(udfTDigest_AddValue, params);
                }

                TRuntimeNode pgmSerializedData;
                {
                    TVector<TRuntimeNode> params = {pgmDigest};
                    pgmSerializedData = pgmBuilder.Apply(udfTDigest_Serialize, params);
                }
                pgmSerializedDataVector.push_back(pgmSerializedData);
            }

            TRuntimeNode pgmDigest;
            for (size_t i = 0; i < pgmSerializedDataVector.size(); ++i) {
                TRuntimeNode pgmDigest2;
                {
                    TVector<TRuntimeNode> params = {pgmSerializedDataVector[i]};
                    pgmDigest2 = pgmBuilder.Apply(udfTDigest_Deserialize, params);
                }
                if (!pgmDigest) {
                    pgmDigest = pgmDigest2;
                } else {
                    TVector<TRuntimeNode> params = {pgmDigest, pgmDigest2};
                    pgmDigest = pgmBuilder.Apply(udfTDigest_Merge, params);
                }
            }

            TRuntimeNode pgmReturn;
            {
                auto param2 = pgmBuilder.NewDataLiteral<double>(0.9);
                TVector<TRuntimeNode> params = {pgmDigest, param2};
                pgmReturn = pgmBuilder.Apply(udfTDigest_GetPercentile, params);
            }

            TExploringNodeVisitor explorer;
            explorer.Walk(pgmReturn.GetNode(), env);
            TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(), mutableFunctionRegistry.Get(),
                                            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "", EGraphPerProcess::Multi);
            auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
            auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
            auto value = graph->GetValue();
            UNIT_ASSERT_DOUBLES_EQUAL(value.Get<double>(), 8.95, 0.001);
        }

        static double GetParetoRandomNumber(double a) {
            return 1 / pow(RandomNumber<double>(), double(1) / a);
        }

        Y_UNIT_TEST(BigPercentile) {
            auto mutableFunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(1);
            NUdf::TUniquePtr<NUdf::IUdfModule> module = NUdf::CreateStatModule();
            mutableFunctionRegistry->AddModule("", "Stat", std::move(module));
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, *mutableFunctionRegistry);
            auto udfTDigest_Create = pgmBuilder.Udf("Stat.TDigest_Create");
            auto udfTDigest_AddValue = pgmBuilder.Udf("Stat.TDigest_AddValue");
            auto udfTDigest_GetPercentile = pgmBuilder.Udf("Stat.TDigest_GetPercentile");
            const size_t NUMBERS = 100000;
            const double PERCENTILE = 0.99;
            const double THRESHOLD = 0.0004; // at q=0.99 threshold is 4*delta*0.0099
            TVector<double> randomNumbers1;
            TVector<TRuntimeNode> randomNumbers2;
            randomNumbers1.reserve(NUMBERS);
            randomNumbers2.reserve(NUMBERS);
            for (size_t n = 0; n < NUMBERS; ++n) {
                double randomNumber = GetParetoRandomNumber(10);
                randomNumbers1.push_back(randomNumber);
                randomNumbers2.push_back(pgmBuilder.NewDataLiteral(randomNumber));
            }
            TRuntimeNode bigList = pgmBuilder.AsList(randomNumbers2);
            auto pgmDigest =
                pgmBuilder.Fold1(bigList,
                                    [&](TRuntimeNode item) {
                                        std::array<TRuntimeNode, 1> args;
                                        args[0] = item;
                                        return pgmBuilder.Apply(udfTDigest_Create, args);
                                    },
                                    [&](TRuntimeNode item, TRuntimeNode state) {
                                        std::array<TRuntimeNode, 2> args;
                                        args[0] = state;
                                        args[1] = item;
                                        return pgmBuilder.Apply(udfTDigest_AddValue, args);
                                    });
            TRuntimeNode pgmReturn =
                pgmBuilder.Map(pgmDigest, [&](TRuntimeNode item) {
                    auto param2 = pgmBuilder.NewDataLiteral(PERCENTILE);
                    std::array<TRuntimeNode, 2> args;
                    args[0] = item;
                    args[1] = param2;
                    return pgmBuilder.Apply(udfTDigest_GetPercentile, args);
                });

            TExploringNodeVisitor explorer;
            explorer.Walk(pgmReturn.GetNode(), env);
            TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(), mutableFunctionRegistry.Get(),
                                            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "", EGraphPerProcess::Multi);
            auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
            auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
            auto value = graph->GetValue();
            UNIT_ASSERT(value);
            double digestValue = value.Get<double>();
            std::sort(randomNumbers1.begin(), randomNumbers1.end());
            // This gives us a 1-based index of the last value <= digestValue
            auto index = std::upper_bound(randomNumbers1.begin(), randomNumbers1.end(), digestValue) - randomNumbers1.begin();
            // See https://en.wikipedia.org/wiki/Percentile#First_Variant.2C
            double p = (index - 0.5) / double(randomNumbers1.size());
            UNIT_ASSERT_DOUBLES_EQUAL(p, PERCENTILE, THRESHOLD);
        }

        Y_UNIT_TEST(CentroidPrecision) {
            auto mutableFunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(1);
            NUdf::TUniquePtr<NUdf::IUdfModule> module = NUdf::CreateStatModule();
            mutableFunctionRegistry->AddModule("", "Stat", std::move(module));
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, *mutableFunctionRegistry);
            auto udfTDigest_Create = pgmBuilder.Udf("Stat.TDigest_Create");
            auto udfTDigest_AddValue = pgmBuilder.Udf("Stat.TDigest_AddValue");
            auto udfTDigest_GetPercentile = pgmBuilder.Udf("Stat.TDigest_GetPercentile");
            const size_t NUMBERS = 100000;
            const double PERCENTILE = 0.25;
            const double minValue = 1.0;
            const double maxValue = 100.0;
            const double majorityValue = 50.0;
            TVector<TRuntimeNode> numbers;
            numbers.reserve(NUMBERS);
            for (size_t n = 0; n < NUMBERS - 2; ++n) {
                numbers.push_back(pgmBuilder.NewDataLiteral(majorityValue));
            }
            numbers.push_back(pgmBuilder.NewDataLiteral(minValue));
            numbers.push_back(pgmBuilder.NewDataLiteral(maxValue));
            TRuntimeNode bigList = pgmBuilder.AsList(numbers);
            auto pgmDigest =
                pgmBuilder.Fold1(bigList,
                                    [&](TRuntimeNode item) {
                                        std::array<TRuntimeNode, 1> args;
                                        args[0] = item;
                                        return pgmBuilder.Apply(udfTDigest_Create, args);
                                    },
                                    [&](TRuntimeNode item, TRuntimeNode state) {
                                        std::array<TRuntimeNode, 2> args;
                                        args[0] = state;
                                        args[1] = item;
                                        return pgmBuilder.Apply(udfTDigest_AddValue, args);
                                    });
            TRuntimeNode pgmReturn =
                pgmBuilder.Map(pgmDigest, [&](TRuntimeNode item) {
                    auto param2 = pgmBuilder.NewDataLiteral(PERCENTILE);
                    std::array<TRuntimeNode, 2> args;
                    args[0] = item;
                    args[1] = param2;
                    return pgmBuilder.Apply(udfTDigest_GetPercentile, args);
                });

            TExploringNodeVisitor explorer;
            explorer.Walk(pgmReturn.GetNode(), env);
            TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(), mutableFunctionRegistry.Get(),
                                            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "", EGraphPerProcess::Multi);
            auto pattern = MakeComputationPattern(explorer, pgmReturn, {}, opts);
            auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
            auto value = graph->GetValue();
            UNIT_ASSERT(value);
            double digestValue = value.Get<double>();
            UNIT_ASSERT_EQUAL(digestValue, majorityValue);
        }
    }
}
