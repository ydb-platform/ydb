#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/svnversion/svnversion.h>

#include <yql/essentials/public/udf/udf_version.h>
#include <yql/essentials/tools/purebench/lib/benchmark.h>
#include <yql/essentials/tools/purebench/lib/benchmark_program.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <limits>

namespace {

struct TCommandLineOptions {
    NYql::NPureBench::TBenchmarkProgramOptions Program;
    NYql::NPureBench::TBenchmarkOptions Measurements;
    bool ShowQueryOutput = true;
    bool PrintExpressionToStdout = false;
    TString ExpressionOutputPath;
};

void AddQueryOptions(NLastGetopt::TOpts& opts, TCommandLineOptions& options) {
    opts.AddLongOption('b', "blocks-engine", "Block engine settings").StoreResult(&options.Program.BlockEngineSettings).DefaultValue("disable");
    opts.AddLongOption('c', "count", "number of rows passed to the generation query").StoreResult(&options.Program.GeneratorInputRows).DefaultValue(1000000);
    opts.AddLongOption('g', "gen-sql", "SQL query to generate data").StoreResult(&options.Program.GenerationQuery).DefaultValue("select index from Input");
    opts.AddLongOption('t', "test-sql", "SQL query to benchmark").StoreResult(&options.Program.MeasuredQuery).DefaultValue("select count(*) as count from Input");
    opts.AddLongOption("pg", "use PG syntax for generation query").NoArgument();
    opts.AddLongOption("pt", "use PG syntax for measured query").NoArgument();
    opts.AddLongOption("udfs-dir", "directory with UDFs").StoreResult(&options.Program.UdfsDirectory).DefaultValue("");
    opts.AddLongOption("llvm-settings", "LLVM settings").StoreResult(&options.Program.LLVMSettings).DefaultValue("");
    opts.AddLongOption("langver", "Set current language version").RequiredArgument("VER").Handler1T<TString>([&options](const TString& value) {
        if (value == "unknown") {
            options.Program.LanguageVersion = NYql::UnknownLangVersion;
        } else if (!NYql::ParseLangVersion(value, options.Program.LanguageVersion)) {
            ythrow yexception() << "Failed to parse language version: " << value;
        }
    });
}

void AddMeasurementOptions(NLastGetopt::TOpts& opts, TCommandLineOptions& options) {
    const auto secondsFromString = [](const TString& value) {
        const auto seconds = FromString<ui64>(value);
        YQL_ENSURE(seconds <= TDuration::Max().Seconds(), "Duration in seconds is too large: " << value);
        return TDuration::Seconds(seconds);
    };
    opts.AddLongOption('r', "repeats", "minimum number of measured query runs").StoreResult(&options.Measurements.MinMeasuredRuns).DefaultValue(10);
    opts.AddLongOption('R', "repeat-time", "minimum measurement duration in seconds").StoreMappedResultT<TString, TDuration>(&options.Measurements.MinMeasurementDuration, secondsFromString).DefaultValue(1);
    opts.AddLongOption("calibrate", "minimum number of calibration query runs").StoreResult(&options.Measurements.MinCalibrationRuns).DefaultValue(3);
    opts.AddLongOption("calibrate-time", "minimum calibration duration in seconds").StoreMappedResultT<TString, TDuration>(&options.Measurements.MinCalibrationDuration, secondsFromString).DefaultValue(1);
}

TCommandLineOptions ParseOptions(int argc, const char** argv) {
    TCommandLineOptions options;
    auto opts = NLastGetopt::TOpts::Default();
    opts.AddHelpOption();
    opts.AddLongOption("ndebug", "suppress the ABI version banner when passed as the first argument").NoArgument();
    opts.AddLongOption('w', "show-results", "show results of measured query").StoreResult(&options.ShowQueryOutput).DefaultValue(true);
    opts.AddLongOption("print-expr", "print rebuild AST before execution").NoArgument();
    opts.AddLongOption("expr-file", "print AST to that file instead of stdout").StoreResult(&options.ExpressionOutputPath);
    opts.MutuallyExclusive("print-expr", "expr-file");
    AddQueryOptions(opts, options);
    AddMeasurementOptions(opts, options);
    opts.SetFreeArgsMax(0);
    NLastGetopt::TOptsParseResult result(&opts, argc, argv);
    YQL_ENSURE(!result.Has("expr-file") || !options.ExpressionOutputPath.empty(), "--expr-file requires a non-empty path");
    options.Program.GenerationSyntax = result.Has("pg") ? NYql::NPureBench::ESyntax::PG : NYql::NPureBench::ESyntax::SQL;
    options.Program.MeasuredQuerySyntax = result.Has("pt") ? NYql::NPureBench::ESyntax::PG : NYql::NPureBench::ESyntax::SQL;
    options.PrintExpressionToStdout = result.Has("print-expr");
    return options;
}

TString FormatMilliseconds(const TMaybe<TDuration>& duration) {
    if (!duration) {
        return "nan";
    }
    return TStringBuilder() << Prec(duration->MillisecondsFloat(), 4);
}

void PrintBenchmarkResult(const NYql::NPureBench::TBenchmarkResult& result) {
    Cout << "Benchmark completed: " << result.Benchmark.Iterations << " iterations for " << result.Benchmark.Elapsed << "\n";
    Cerr << "Score calibration...\n";
    Cerr << "Calibration completed: " << (result.Calibration ? result.Calibration->Iterations : 0)
         << " iterations for " << (result.Calibration ? result.Calibration->Elapsed : TDuration::Zero()) << "\n";
    const auto missing = std::numeric_limits<double>::quiet_NaN();
    Cout << "Bench score: " << Prec(result.Score.GetOrElse(missing), 4)
         << "ms (mean wall clock: " << FormatMilliseconds(result.Benchmark.TrimmedStatistics.GeometricMean)
         << "ms, cv: " << Prec(result.Benchmark.TrimmedStatistics.CoefficientOfVariationPercent.GetOrElse(missing), 4) << "%)\n";
}

int Main(int argc, const char** argv) {
    auto options = ParseOptions(argc, argv);
    THolder<TFixedBufferFileOutput> exprFile;
    IOutputStream* exprOutput = nullptr;
    if (options.PrintExpressionToStdout) {
        exprOutput = &Cout;
    } else if (!options.ExpressionOutputPath.empty()) {
        exprFile = MakeHolder<TFixedBufferFileOutput>(options.ExpressionOutputPath);
        exprOutput = exprFile.Get();
    }
    auto program = NYql::NPureBench::CreateBenchmarkProgram(options.Program);
    const auto preparation = program->PrepareBenchmark(exprOutput);
    Cerr << "Input data size: " << preparation.GeneratorInputBytes << "\n";
    Cerr << "Generated data size: " << preparation.BenchmarkInputBytes << "\n";
    if (options.ShowQueryOutput) {
        const auto result = program->RunMeasureQuery(NYql::NPureBench::EOutputMode::Collect);
        Cerr << NYql::NPureBench::ToYson(*result.Output) << '\n';
    }
    Cerr << "Run benchmark...\n";
    PrintBenchmarkResult(NYql::NPureBench::RunBenchmarks(*program, options.Measurements));
    NYql::NLog::CleanupLogger();
    return 0;
}

} // namespace

int main(int argc, const char** argv) {
    if (argc > 1 && TString(argv[1]) != TStringBuf("--ndebug")) {
        Cerr << "purebench ABI version: " << NKikimr::NUdf::CurrentAbiVersionStr() << Endl;
    }
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();
    try {
        return Main(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
