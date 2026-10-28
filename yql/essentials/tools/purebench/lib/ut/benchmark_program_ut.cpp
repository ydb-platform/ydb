#include <yql/essentials/tools/purebench/lib/benchmark.h>
#include <yql/essentials/tools/purebench/lib/benchmark_program.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/monotonic.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace {

NYql::NPureBench::TBenchmarkProgramOptions MakeOptions(TStringBuf blockEngine, ui64 generatorInputRows) {
    NYql::NPureBench::TBenchmarkProgramOptions options;
    options.BlockEngineSettings = blockEngine;
    options.LLVMSettings = "OFF";
    options.GeneratorInputRows = generatorInputRows;
    return options;
}

void CheckStringResult(const NYql::NPureBench::TQueryOutput& result) {
    UNIT_ASSERT_VALUES_EQUAL(result.GetRows().size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(result.GetRows()[0][0].AsString(), "0value");
    UNIT_ASSERT_VALUES_EQUAL(result.GetRows()[2][0].AsString(), "2value");
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowType()[0].AsString(), "StructType");
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowType()[1][0][0].AsString(), "value");
    const auto yson = NYT::NodeFromYsonString(NYql::NPureBench::ToYson(result));
    UNIT_ASSERT(yson["Type"] == result.GetRowType());
    UNIT_ASSERT(yson["Data"].AsList() == result.GetRows());
}

void AssertInputByteStatistics(const NYql::NPureBench::TQueryRunStatistics& statistics,
                               const NYql::NPureBench::TBenchmarkPreparationResult& preparation) {
    UNIT_ASSERT_VALUES_EQUAL(statistics.GeneratorInputBytes, preparation.GeneratorInputBytes);
    UNIT_ASSERT_VALUES_EQUAL(statistics.BenchmarkInputBytes, preparation.BenchmarkInputBytes);
}

void CheckRepeatedRuns(TStringBuf blockEngine, ui64 generatorInputRows) {
    auto program = NYql::NPureBench::CreateBenchmarkProgram(MakeOptions(blockEngine, generatorInputRows));
    const auto preparation = program->PrepareBenchmark(nullptr);
    UNIT_ASSERT_VALUES_EQUAL(preparation.GeneratorInputBytes == 0, generatorInputRows == 0);
    UNIT_ASSERT_VALUES_EQUAL(preparation.BenchmarkInputBytes == 0, generatorInputRows == 0);
    const auto expected = ToString(generatorInputRows);
    for (ui32 iteration = 0; iteration < 3; ++iteration) {
        const auto started = TMonotonic::Now();
        const auto result = program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Discard);
        const auto elapsed = TMonotonic::Now() - started;
        UNIT_ASSERT(result.Statistics.Elapsed <= elapsed);
        UNIT_ASSERT(!result.Output.Defined());
        AssertInputByteStatistics(result.Statistics, preparation);
        AssertInputByteStatistics(std::get<NYql::NPureBench::TQueryRunStatistics>(program->RunCalibrationQuery()), preparation);
        const auto collected = program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Collect);
        UNIT_ASSERT(collected.Output.Defined());
        AssertInputByteStatistics(collected.Statistics, preparation);
        UNIT_ASSERT_VALUES_EQUAL(collected.Output->GetRows()[0][0].AsString(), expected);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TPureBenchFacade) {
Y_UNIT_TEST(RepeatedRunsAndEmptyInput) {
    for (const TStringBuf mode : {"disable", "auto", "force"}) {
        for (const ui64 generatorInputRows : {0, 1, 17}) {
            CheckRepeatedRuns(mode, generatorInputRows);
        }
    }
}

Y_UNIT_TEST(QueryOutputOwnsTypeAndRows) {
    auto options = MakeOptions("force", 3);
    options.GenerationQuery = "SELECT Cast(index AS String) || 'value' AS value FROM Input";
    options.MeasuredQuery = "SELECT value FROM Input ORDER BY value";
    auto program = NYql::NPureBench::CreateBenchmarkProgram(options);
    program->PrepareBenchmark(nullptr);
    const auto firstResult = program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Collect);
    for (ui32 iteration = 0; iteration < 3; ++iteration) {
        program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Discard);
        CheckStringResult(*program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Collect).Output);
    }
    const auto result = program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Collect);
    program.Reset();
    CheckStringResult(*firstResult.Output);
    CheckStringResult(*result.Output);
}

Y_UNIT_TEST(DisabledCalibrationReturnsUnadjustedScore) {
    for (const TStringBuf mode : {"disable", "auto", "force"}) {
        auto options = MakeOptions(mode, 0);
        options.EnableCalibration = false;
        auto program = NYql::NPureBench::CreateBenchmarkProgram(options);
        program->PrepareBenchmark(nullptr);
        UNIT_ASSERT(std::holds_alternative<NYql::NPureBench::TCalibrationDisabled>(program->RunCalibrationQuery()));
        const auto result = NYql::NPureBench::RunBenchmarks(*program, {.MinMeasuredRuns = 1, .MinCalibrationRuns = 1,
                                                                       .MinMeasurementDuration = TDuration::Zero(),
                                                                       .MinCalibrationDuration = TDuration::Zero()});
        UNIT_ASSERT(!result.Calibration.Defined());
        UNIT_ASSERT(result.Score.Defined());
        UNIT_ASSERT_VALUES_EQUAL(*result.Score, *result.Benchmark.TrimmedStatistics.GeometricMean / TDuration::MilliSeconds(1));
    }
}

Y_UNIT_TEST(PreparationWarmsUpMeasuredQueries) {
    for (const TStringBuf mode : {"disable", "auto", "force"}) {
        for (const TStringBuf llvm : {"OFF", "ON"}) {
            for (const TStringBuf query : {
                     "SELECT Ensure(index, index < 0, 'measured query warmup') AS index FROM Input",
                     "SELECT Ensure(count(*), count(*) == 0, 'measured query warmup') AS count FROM Input"}) {
                auto options = MakeOptions(mode, 1);
                options.LLVMSettings = llvm;
                options.MeasuredQuery = query;
                auto program = NYql::NPureBench::CreateBenchmarkProgram(options);
                UNIT_ASSERT_EXCEPTION_CONTAINS(program->PrepareBenchmark(nullptr), yexception, "measured query warmup");
                UNIT_ASSERT_EXCEPTION_CONTAINS(program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Discard), yexception, "PrepareBenchmark()");
                UNIT_ASSERT_EXCEPTION_CONTAINS(program->RunCalibrationQuery(), yexception, "PrepareBenchmark()");
                UNIT_ASSERT_EXCEPTION_CONTAINS(program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Collect), yexception, "PrepareBenchmark()");
            }
        }
    }
}

} // Y_UNIT_TEST_SUITE(TPureBenchFacade)
