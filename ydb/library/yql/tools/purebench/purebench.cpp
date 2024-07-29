#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/getopt/last_getopt.h>

#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>
#include <ydb/library/yql/public/purecalc/io_specs/arrow/spec.h>
#include <ydb/library/yql/public/purecalc/helpers/stream/stream_from_vector.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_version.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/yson/writer.h>

#include <util/datetime/cputimer.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/null.h>

#include <algorithm>
#include <cmath>

using namespace NYql;
using namespace NYql::NPureCalc;

TStringStream MakeGenInput(ui64 count) {
    TStringStream stream;
    NSkiff::TUncheckedSkiffWriter writer{&stream};
    for (ui64 i = 0; i < count; ++i) {
        writer.WriteVariant16Tag(0);
        writer.WriteInt64(i);
    }
    writer.Finish();
    return stream;
}

template <typename TInputSpec, typename TOutputSpec>
using TRunCallable = std::function<void (const THolder<TPullListProgram<TInputSpec, TOutputSpec>>&)>;

template <typename TOutputSpec>
NYT::TNode RunGenSql(
    const IProgramFactoryPtr factory,
    const TVector<NYT::TNode>& inputSchema,
    const TString& sql,
    ETranslationMode isPg,
    TRunCallable<TSkiffInputSpec, TOutputSpec> runCallable
) {
    auto inputSpec = TSkiffInputSpec(inputSchema);
    auto outputSpec = TOutputSpec({NYT::TNode::CreateEntity()});
    auto program = factory->MakePullListProgram(inputSpec, outputSpec, sql, isPg);

    runCallable(program);

    return program->MakeOutputSchema();
}

template <typename TInputSpec, typename TStream>
void ShowResults(
    const IProgramFactoryPtr factory,
    const TVector<NYT::TNode>& inputSchema,
    const TString& sql,
    ETranslationMode isPg,
    TStream* input
) {
    auto inputSpec = TInputSpec(inputSchema);
    auto outputSpec = TYsonOutputSpec({NYT::TNode::CreateEntity()});
    auto program = factory->MakePullListProgram(inputSpec, outputSpec, sql, isPg);
    auto handle = program->Apply(input);
    TStringStream output;
    handle->Run(&output);
    TStringInput in(output.Str());
    NYson::ReformatYsonStream(&in, &Cerr, NYson::EYsonFormat::Pretty, NYson::EYsonType::ListFragment);
}

template <typename TInputSpec, typename TOutputSpec>
double RunBenchmarks(
    const IProgramFactoryPtr factory,
    const TVector<NYT::TNode>& inputSchema,
    const TString& sql,
    ETranslationMode isPg,
    ui32 repeats,
    TRunCallable<TInputSpec, TOutputSpec> runCallable
) {
    auto inputSpec = TInputSpec(inputSchema);
    auto outputSpec = TOutputSpec({NYT::TNode::CreateEntity()});
    auto program = factory->MakePullListProgram(inputSpec, outputSpec, sql, isPg);

    Cerr << "Dry run of test sql...\n";

    runCallable(program);

    Cerr << "Run benchmark...\n";

    TVector<TDuration> times;
    TSimpleTimer allTimer;
    for (ui32 i = 0; i < repeats; ++i) {
        TSimpleTimer timer;
        runCallable(program);
        times.push_back(timer.Get());
    }

    Cout << "Elapsed: " << allTimer.Get() << "\n";

    Sort(times);
    times.erase(times.end() - times.size() / 3, times.end());

    double sum = std::transform_reduce(times.cbegin(), times.cend(),
        .0, std::plus{}, [](auto t) { return std::log(t.MicroSeconds()); });

    return std::exp(sum / times.size());
}

int Main(int argc, const char *argv[])
{
    Y_UNUSED(NUdf::GetStaticSymbols());
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();
    ui64 count;
    ui32 repeats;
    TString genSql, testSql;
    bool showResults;
    TString udfsDir;
    TString LLVMSettings;
    TString blockEngineSettings;
    TString exprFile;
    opts.AddHelpOption();
    opts.AddLongOption("ndebug", "should be at first argument, do not show debug info in error output").NoArgument();
    opts.AddLongOption('b', "blocks-engine", "Block engine settings").StoreResult(&blockEngineSettings).DefaultValue("disable");
    opts.AddLongOption('c', "count", "count of input rows").StoreResult(&count).DefaultValue(1000000);
    opts.AddLongOption('g', "gen-sql", "SQL query to generate data").StoreResult(&genSql).DefaultValue("select index from Input");
    opts.AddLongOption('t', "test-sql", "SQL query to test").StoreResult(&testSql).DefaultValue("select count(*) as count from Input");
    opts.AddLongOption('r', "repeats", "number of iterations").StoreResult(&repeats).DefaultValue(10);
    opts.AddLongOption('w', "show-results", "show results of test SQL").StoreResult(&showResults).DefaultValue(true);
    opts.AddLongOption("pg", "use PG syntax for generate query").NoArgument();
    opts.AddLongOption("pt", "use PG syntax for test query").NoArgument();
    opts.AddLongOption("udfs-dir", "directory with UDFs").StoreResult(&udfsDir).DefaultValue("");
    opts.AddLongOption("llvm-settings", "LLVM settings").StoreResult(&LLVMSettings).DefaultValue("");
    opts.AddLongOption("print-expr", "print rebuild AST before execution").NoArgument();
    opts.AddLongOption("expr-file", "print AST to that file instead of stdout").StoreResult(&exprFile);
    opts.SetFreeArgsMax(0);
    TOptsParseResult res(&opts, argc, argv);

    auto factoryOptions = TProgramFactoryOptions();
    factoryOptions.SetUDFsDir(udfsDir);
    factoryOptions.SetLLVMSettings(LLVMSettings);
    factoryOptions.SetBlockEngineSettings(blockEngineSettings);

    IOutputStream* exprOut = nullptr;
    THolder<TFixedBufferFileOutput> exprFileHolder;
    if (res.Has("print-expr")) {
        exprOut = &Cout;
    } else if (!exprFile.empty()) {
        exprFileHolder.Reset(new TFixedBufferFileOutput(exprFile));
        exprOut = exprFileHolder.Get();
    }
    factoryOptions.SetExprOutputStream(exprOut);

    auto factory = MakeProgramFactory(factoryOptions);

    NYT::TNode members{NYT::TNode::CreateList()};
    auto typeNode = NYT::TNode::CreateList()
                        .Add("DataType")
                        .Add("Int64");

    members.Add(NYT::TNode::CreateList()
                    .Add("index")
                    .Add(typeNode));
    NYT::TNode schema = NYT::TNode::CreateList()
                            .Add("StructType")
                            .Add(members);

    auto inputGenSchema = TVector<NYT::TNode>{schema};
    auto inputGenStream = MakeGenInput(count);
    Cerr << "Input data size: " << inputGenStream.Size() << "\n";
    ETranslationMode isPgGen = res.Has("pg") ? ETranslationMode::PG : ETranslationMode::SQL;
    ETranslationMode isPgTest = res.Has("pt") ? ETranslationMode::PG : ETranslationMode::SQL;
    double normalizedTime;
    size_t inputBenchSize;

    if (blockEngineSettings == "disable") {
        TStringStream outputGenStream;
        auto outputGenSchema = RunGenSql<TSkiffOutputSpec>(
            factory, inputGenSchema, genSql, isPgGen,
            [&](const auto& program) {
                auto handle = program->Apply(&inputGenStream);
                handle->Run(&outputGenStream);
                Cerr << "Generated data size: " << outputGenStream.Size() << "\n";
            });

        if (showResults) {
            auto inputResStream = TStringStream(outputGenStream);
            ShowResults<TSkiffInputSpec>(
                factory, {outputGenSchema}, testSql, isPgTest, &inputResStream);
        }

        inputBenchSize = outputGenStream.Size();
        normalizedTime = RunBenchmarks<TSkiffInputSpec, TSkiffOutputSpec>(
            factory, {outputGenSchema}, testSql, isPgTest, repeats,
            [&](const auto& program) {
                auto inputBorrowed = TStringStream(outputGenStream);
                auto handle = program->Apply(&inputBorrowed);
                TNullOutput output;
                handle->Run(&output);
            });
    } else {
        auto inputGenSpec = TSkiffInputSpec(inputGenSchema);
        auto outputGenSpec = TArrowOutputSpec({NYT::TNode::CreateEntity()});
        // XXX: <RunGenSql> cannot be used for this case, since all buffers
        // from the Datums in the obtained batches are owned by the worker's
        // allocator. Hence, the program (i.e. worker) object should be created
        // at the very beginning of the block, or at least prior to all the
        // temporary batch storages (mind outputGenStream below).
        auto program = factory->MakePullListProgram(
            inputGenSpec, outputGenSpec, genSql, isPgGen);

        auto handle = program->Apply(&inputGenStream);
        auto outputGenSchema = program->MakeOutputSchema();

        TVector<arrow::compute::ExecBatch> outputGenStream;
        while (arrow::compute::ExecBatch* batch = handle->Fetch()) {
            outputGenStream.push_back(*batch);
        }

        ui64 outputGenSize = std::transform_reduce(
            outputGenStream.cbegin(), outputGenStream.cend(),
            0l, std::plus{}, [](const auto& b) {
                return NYql::NUdf::GetSizeOfArrowExecBatchInBytes(b);
            });

        Cerr << "Generated data size: " << outputGenSize << "\n";

        if (showResults) {
            auto inputResStreamHolder = StreamFromVector(outputGenStream);
            auto inputResStream = inputResStreamHolder.Get();
            ShowResults<TArrowInputSpec>(
                factory, {outputGenSchema}, testSql, isPgTest, inputResStream);
        }

        inputBenchSize = outputGenSize;
        normalizedTime = RunBenchmarks<TArrowInputSpec, TArrowOutputSpec>(
            factory, {outputGenSchema}, testSql, isPgTest, repeats,
            [&](const auto& program) {
                auto handle = program->Apply(StreamFromVector(outputGenStream));
                while (arrow::compute::ExecBatch* batch = handle->Fetch()) {}
            });
    }

    Cout << "Bench score: " << Prec(inputBenchSize / normalizedTime, 4) << "\n";

    NLog::CleanupLogger();
    return 0;
}

int main(int argc, const char *argv[]) {
    if (argc > 1 && TString(argv[1]) != TStringBuf("--ndebug")) {
        Cerr << "purebench ABI version: " << NKikimr::NUdf::CurrentAbiVersionStr() << Endl;
    }

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        return Main(argc, argv);
    } catch (const TCompileError& e) {
        Cerr << e.what() << "\n" << e.GetIssues();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
