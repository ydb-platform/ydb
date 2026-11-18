#include <yql/essentials/tools/purebench/lib/benchmark.h>
#include <yql/essentials/tools/purebench/lib/benchmark_program.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/monotonic.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <utility>

namespace {

namespace NPureBench = NYql::NPureBench;

struct TBenchmarkProgramTestCase {
    TStringBuf Name;
    NPureBench::TBenchmarkProgramOptions Options;
    TStringBuf ExpectedRows;
    TStringBuf FirstColumn = "count";
};

void CheckOutput(const NPureBench::TQueryOutput& result, const TBenchmarkProgramTestCase& testCase) {
    const auto expected = NYT::NodeFromYsonString(testCase.ExpectedRows);
    UNIT_ASSERT_C(result.GetRows() == expected.AsList(), testCase.Name);
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowType()[0].AsString(), "StructType");
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowType()[1][0][0].AsString(), testCase.FirstColumn);
    const auto yson = NYT::NodeFromYsonString(NPureBench::ToYson(result));
    UNIT_ASSERT(yson["Type"] == result.GetRowType());
    UNIT_ASSERT(yson["Data"].AsList() == result.GetRows());
}

void CheckInputBytes(const NPureBench::TQueryRunStatistics& statistics,
                     const NPureBench::TBenchmarkPreparationResult& preparation) {
    UNIT_ASSERT_VALUES_EQUAL(statistics.GeneratorInputBytes, preparation.GeneratorInputBytes);
    UNIT_ASSERT_VALUES_EQUAL(statistics.BenchmarkInputBytes, preparation.BenchmarkInputBytes);
}

NPureBench::TQueryOutput CheckIteration(NPureBench::IBenchmarkProgram& program,
                                        const NPureBench::TBenchmarkPreparationResult& preparation,
                                        const TBenchmarkProgramTestCase& testCase) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(program.PrepareBenchmark(nullptr), yexception, "PrepareBenchmark() can only be called once");
    const auto started = TMonotonic::Now();
    const auto discarded = program.RunMeasureQuery(NPureBench::EOutputMode::Discard);
    UNIT_ASSERT(discarded.Statistics.Elapsed <= TMonotonic::Now() - started);
    UNIT_ASSERT(!discarded.Output.Defined());
    CheckInputBytes(discarded.Statistics, preparation);
    const auto calibration = program.RunCalibrationQuery();
    if (testCase.Options.EnableCalibration) {
        CheckInputBytes(std::get<NPureBench::TQueryRunStatistics>(calibration), preparation);
    } else {
        UNIT_ASSERT(std::holds_alternative<NPureBench::TCalibrationDisabled>(calibration));
    }
    auto collected = program.RunMeasureQuery(NPureBench::EOutputMode::Collect);
    UNIT_ASSERT(collected.Output.Defined());
    CheckInputBytes(collected.Statistics, preparation);
    CheckOutput(*collected.Output, testCase);
    return std::move(*collected.Output);
}

void CheckUnadjustedScore(NPureBench::IBenchmarkProgram& program) {
    const auto result = NPureBench::RunBenchmarks(program, {.MinMeasuredRuns = 1, .MinCalibrationRuns = 1,
                                                            .MinMeasurementDuration = TDuration::Zero(),
                                                            .MinCalibrationDuration = TDuration::Zero()});
    UNIT_ASSERT(!result.Calibration.Defined());
    UNIT_ASSERT(result.Score.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*result.Score, *result.Benchmark.TrimmedStatistics.GeometricMean / TDuration::MilliSeconds(1));
}

void CheckProgram(const TBenchmarkProgramTestCase& testCase, NYql::EBlockEngineMode mode) {
    auto options = testCase.Options;
    options.BlockEngineMode = mode;
    options.LLVMSettings = "OFF";
    auto program = NPureBench::CreateBenchmarkProgram(options);
    const auto preparation = program->PrepareBenchmark(nullptr);
    UNIT_ASSERT_VALUES_EQUAL(preparation.GeneratorInputBytes == 0, options.GeneratorInputRows == 0);
    UNIT_ASSERT_VALUES_EQUAL(preparation.BenchmarkInputBytes == 0, options.GeneratorInputRows == 0);
    TVector<NPureBench::TQueryOutput> outputs;
    for (ui32 iteration = 0; iteration < 3; ++iteration) {
        outputs.push_back(CheckIteration(*program, preparation, testCase));
    }
    if (!options.EnableCalibration) {
        CheckUnadjustedScore(*program);
    }
    program.Reset();
    for (const auto& output : outputs) {
        CheckOutput(output, testCase);
    }
}

void CheckPreparationFailure(const NPureBench::TBenchmarkProgramOptions& options, TStringBuf expectedError) {
    auto program = NPureBench::CreateBenchmarkProgram(options);
    UNIT_ASSERT_EXCEPTION_CONTAINS(program->PrepareBenchmark(nullptr), yexception, expectedError);
    UNIT_ASSERT_EXCEPTION_CONTAINS(program->PrepareBenchmark(nullptr), yexception, "PrepareBenchmark() can only be called once");
    UNIT_ASSERT_EXCEPTION_CONTAINS(program->RunMeasureQuery(NPureBench::EOutputMode::Discard), yexception, "Call PrepareBenchmark()");
    UNIT_ASSERT_EXCEPTION_CONTAINS(program->RunCalibrationQuery(), yexception, "Call PrepareBenchmark()");
    UNIT_ASSERT_EXCEPTION_CONTAINS(program->RunMeasureQuery(NPureBench::EOutputMode::Collect), yexception, "Call PrepareBenchmark()");
}

} // namespace

Y_UNIT_TEST_SUITE(TPureBenchFacade) {
Y_UNIT_TEST(ExecutesConfiguredPrograms) {
    const TVector<TBenchmarkProgramTestCase> cases{
        {.Name = "empty", .Options = {.GeneratorInputRows = 0}, .ExpectedRows = R"([["0"]])"},
        {.Name = "one row", .Options = {.GeneratorInputRows = 1}, .ExpectedRows = R"([["1"]])"},
        {.Name = "several rows", .Options = {.GeneratorInputRows = 17}, .ExpectedRows = R"([["17"]])"},
        {.Name = "owned strings", .Options = {.GeneratorInputRows = 3,
                                              .GenerationQuery = "SELECT Cast(index AS String) || 'value' AS value FROM Input",
                                              .MeasuredQuery = "SELECT value FROM Input ORDER BY value"},
         .ExpectedRows = R"([["0value"]; ["1value"]; ["2value"]])",
         .FirstColumn = "value"},
        {.Name = "multiple blocks", .Options = {.GeneratorInputRows = 100000,
                                                .GenerationQuery = "SELECT index, index + 1 AS other, Cast(index AS String) AS label FROM Input",
                                                .MeasuredQuery = "SELECT count(*) AS count, SUM(other) AS total FROM Input"},
         .ExpectedRows = R"([["100000"; ["5000050000"]]])"},
        {.Name = "calibration disabled", .Options = {.GeneratorInputRows = 0, .EnableCalibration = false}, .ExpectedRows = R"([["0"]])"},
    };
    for (const auto& testCase : cases) {
        for (const auto mode : {NYql::EBlockEngineMode::Disable, NYql::EBlockEngineMode::Auto, NYql::EBlockEngineMode::Force}) {
            CheckProgram(testCase, mode);
        }
    }
}

Y_UNIT_TEST(RejectsInvalidPrograms) {
    const TVector<std::pair<NPureBench::TBenchmarkProgramOptions, TStringBuf>> cases{
        {{.GeneratorInputRows = 0, .GenerationQuery = "SELECT missing_column FROM Input"}, "missing_column"},
        {{.GeneratorInputRows = 1, .MeasuredQuery = "SELECT Ensure(index, index < 0, 'measured query warmup') AS index FROM Input"}, "measured query warmup"},
        {{.GeneratorInputRows = 1, .MeasuredQuery = "SELECT Ensure(count(*), count(*) == 0, 'measured query warmup') AS count FROM Input"}, "measured query warmup"},
    };
    for (const auto& [baseOptions, expectedError] : cases) {
        for (const auto mode : {NYql::EBlockEngineMode::Disable, NYql::EBlockEngineMode::Auto, NYql::EBlockEngineMode::Force}) {
            for (const TStringBuf llvm : {"OFF", "ON"}) {
                auto options = baseOptions;
                options.BlockEngineMode = mode;
                options.LLVMSettings = llvm;
                CheckPreparationFailure(options, expectedError);
            }
        }
    }
}
} // Y_UNIT_TEST_SUITE(TPureBenchFacade)
