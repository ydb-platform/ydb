#include <yql/essentials/tools/purebench/lib/benchmark.h>
#include <yql/essentials/tools/purebench/lib/benchmark_program.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace {

class TCountingProgram final: public NYql::NPureBench::IBenchmarkProgram {
public:
    ui64 MeasuredCalls = 0;
    ui64 CalibrationCalls = 0;
    bool EnableCalibration = true;
    TDuration MeasuredElapsed = TDuration::MilliSeconds(4);

    NYql::NPureBench::TBenchmarkPreparationResult PrepareBenchmark(IOutputStream*) override {
        ythrow yexception() << "Unexpected benchmark preparation";
    }

    NYql::NPureBench::TMeasureQueryResult RunMeasureQuery(NYql::NPureBench::EOutputMode outputMode) override {
        UNIT_ASSERT(outputMode == NYql::NPureBench::EOutputMode::Discard);
        ++MeasuredCalls;
        return {.Statistics = {.Elapsed = MeasuredElapsed}, .Output = Nothing()};
    }

    NYql::NPureBench::TCalibrationQueryResult RunCalibrationQuery() override {
        ++CalibrationCalls;
        if (!EnableCalibration) {
            return NYql::NPureBench::TCalibrationDisabled{};
        }
        return NYql::NPureBench::TQueryRunStatistics{.Elapsed = TDuration::MilliSeconds(1)};
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TPureBenchStatistics) {
Y_UNIT_TEST(ExactRunCountWithZeroMinimumDuration) {
    for (const ui32 runs : {0, 1, 7}) {
        ui32 calls = 0;
        const auto result = NYql::NPureBench::Measure(runs, TDuration::Zero(), [&] {
            ++calls;
            return TDuration::Zero();
        });
        UNIT_ASSERT_VALUES_EQUAL(calls, runs);
        UNIT_ASSERT_VALUES_EQUAL(result.Iterations, runs);
        UNIT_ASSERT_VALUES_EQUAL(result.Times.size(), runs);
        UNIT_ASSERT_VALUES_EQUAL(result.TrimmedStatistics.GeometricMean.Defined(), runs != 0);
        if (runs != 0) {
            UNIT_ASSERT_VALUES_EQUAL(*result.TrimmedStatistics.GeometricMean, TDuration::Zero());
        }
        UNIT_ASSERT(!result.TrimmedStatistics.CoefficientOfVariationPercent.Defined());
    }
}

Y_UNIT_TEST(StatisticsAfterDroppingSlowestThird) {
    const auto result = NYql::NPureBench::CalculateTrimmedStatistics(
        {TDuration::MilliSeconds(100), TDuration::MilliSeconds(1), TDuration::MilliSeconds(4)});
    UNIT_ASSERT(result.GeometricMean.Defined());
    UNIT_ASSERT(result.CoefficientOfVariationPercent.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*result.GeometricMean, TDuration::MilliSeconds(2));
    UNIT_ASSERT_DOUBLES_EQUAL(*result.CoefficientOfVariationPercent, 60.0, 1e-12);
}

Y_UNIT_TEST(BenchmarkUsesIndependentRunMinimumsAndReturnsScore) {
    for (const ui32 calibrationRuns : {0, 2}) {
        TCountingProgram program;
        const auto result = NYql::NPureBench::RunBenchmarks(program, {.MinMeasuredRuns = 3, .MinCalibrationRuns = calibrationRuns,
                                                                      .MinMeasurementDuration = TDuration::Zero(),
                                                                      .MinCalibrationDuration = TDuration::Zero()});
        UNIT_ASSERT_VALUES_EQUAL(program.MeasuredCalls, 3);
        UNIT_ASSERT_VALUES_EQUAL(program.CalibrationCalls, calibrationRuns);
        UNIT_ASSERT_VALUES_EQUAL(result.Benchmark.Iterations, 3);
        UNIT_ASSERT_VALUES_EQUAL(result.Calibration.Defined(), calibrationRuns != 0);
        if (result.Calibration) {
            UNIT_ASSERT_VALUES_EQUAL(result.Calibration->Iterations, calibrationRuns);
        }
        UNIT_ASSERT(result.Score.Defined());
        UNIT_ASSERT_DOUBLES_EQUAL(*result.Score, calibrationRuns ? 3.0 : 4.0, 1e-12);
    }
}

Y_UNIT_TEST(DisabledCalibrationReturnsUnadjustedScore) {
    TCountingProgram program;
    program.EnableCalibration = false;
    const auto result = NYql::NPureBench::RunBenchmarks(program, {.MinMeasuredRuns = 1, .MinCalibrationRuns = 1,
                                                                  .MinMeasurementDuration = TDuration::Zero(),
                                                                  .MinCalibrationDuration = TDuration::Seconds(1)});
    UNIT_ASSERT(result.Score.Defined());
    UNIT_ASSERT_VALUES_EQUAL(program.MeasuredCalls, 1);
    UNIT_ASSERT_VALUES_EQUAL(program.CalibrationCalls, 1);
    UNIT_ASSERT(!result.Calibration.Defined());
    UNIT_ASSERT_DOUBLES_EQUAL(*result.Score, 4.0, 1e-12);
}

} // Y_UNIT_TEST_SUITE(TPureBenchStatistics)
