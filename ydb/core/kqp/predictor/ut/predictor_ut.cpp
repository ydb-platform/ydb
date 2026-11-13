#include <ydb/core/kqp/predictor/kqp_predictor.h>

#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/info.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(TStagePredictorTest) {
    Y_UNIT_TEST(ThreadsOutsideActorContext) {
        const ui32 expected = Max<ui32>(1, NSystemInfo::NumberOfCpus());
        UNIT_ASSERT_VALUES_EQUAL(TStagePredictor::GetUsableThreads(), expected);
        UNIT_ASSERT_VALUES_EQUAL(TStagePredictor::GetPossibleMaxLimitThreads(), expected);
    }

    Y_UNIT_TEST(ProgramSettingsRoundTrip) {
        NYql::NDqProto::TProgram::TSettings settings;
        settings.SetHasMapJoin(true);
        settings.SetHasSort(true);
        settings.SetHasUdf(true);
        settings.SetHasFinalAggregation(true);
        settings.SetHasStateAggregation(true);
        settings.SetGroupByKeysCount(3);
        settings.SetHasFilter(true);
        settings.SetHasTop(true);
        settings.SetHasRangeScan(true);
        settings.SetHasCondense(true);
        settings.SetHasWatermarkGenerator(true);
        settings.SetWatermarkGeneratorIdleTimeoutUs(12345);
        settings.SetNodesCount(27);
        settings.SetInputDataPrediction(2);
        settings.SetOutputDataPrediction(0.25);
        settings.SetStageLevel(4);
        settings.SetLevelDataPrediction(8);

        TStagePredictor predictor;
        UNIT_ASSERT(predictor.DeserializeFromKqpSettings(settings));
        UNIT_ASSERT(predictor.NeedLLVM());
        NYql::NDqProto::TProgram::TSettings result;
        predictor.SerializeToKqpSettings(result);
        UNIT_ASSERT_VALUES_EQUAL(settings.SerializeAsString(), result.SerializeAsString());
    }

    Y_UNIT_TEST(DeserializeClearsAbsentWatermarkTimeout) {
        NYql::NDqProto::TProgram::TSettings settings;
        settings.SetWatermarkGeneratorIdleTimeoutUs(42);
        TStagePredictor predictor;
        predictor.DeserializeFromKqpSettings(settings);
        UNIT_ASSERT(predictor.HasWatermarkGeneratorIdleTimeoutUs());

        settings.ClearWatermarkGeneratorIdleTimeoutUs();
        predictor.DeserializeFromKqpSettings(settings);
        UNIT_ASSERT(!predictor.HasWatermarkGeneratorIdleTimeoutUs());
    }

    Y_UNIT_TEST(TaskCountRespectsDataShareAndPreviousStage) {
        NYql::NDqProto::TProgram::TSettings settings;
        settings.SetInputDataPrediction(2);
        settings.SetLevelDataPrediction(4);
        TStagePredictor predictor;
        predictor.DeserializeFromKqpSettings(settings);
        UNIT_ASSERT_VALUES_EQUAL(predictor.CalcTasksOptimalCount(8, std::nullopt), 4);
        UNIT_ASSERT_VALUES_EQUAL(predictor.CalcTasksOptimalCount(8, 2), 2);
        UNIT_ASSERT_VALUES_EQUAL(predictor.CalcTasksOptimalCount(0, std::nullopt), 1);
    }

    Y_UNIT_TEST(ScanCollectsDistinctUdfModules) {
        NYql::TExprContext ctx;
        const NYql::TPositionHandle pos;
        auto program = ctx.NewCallable(pos, "AsTuple", {
            ctx.NewCallable(pos, "Udf", {ctx.NewAtom(pos, "Zeta.First")}),
            ctx.NewCallable(pos, "Udf", {ctx.NewAtom(pos, "Alpha.Func")}),
            ctx.NewCallable(pos, "Udf", {ctx.NewAtom(pos, "Zeta.Second")}),
        });
        TStagePredictor predictor;
        predictor.Scan(program);
        UNIT_ASSERT(predictor.GetWasmUdfModules() == TVector<TString>({"Alpha", "Zeta"}));
        NYql::NDqProto::TProgram::TSettings settings;
        predictor.SerializeToKqpSettings(settings);
        UNIT_ASSERT(settings.GetHasUdf());
        UNIT_ASSERT(!predictor.NeedLLVM());
    }
}

} // namespace NKikimr::NKqp
