#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/getopt/last_getopt.h>

#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/mkql/spec.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_version.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/yson/writer.h>

#include <util/datetime/cputimer.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/null.h>

#include <cmath>

using namespace NYql;
using namespace NYql::NPureCalc;

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

    auto inputSpec1 = TSkiffInputSpec(TVector<NYT::TNode>{schema});
    auto outputSpec1 = TSkiffOutputSpec({NYT::TNode::CreateEntity()});
    auto genProgram = factory->MakePullListProgram(
        inputSpec1,
        outputSpec1,
        genSql,
        res.Has("pg") ? ETranslationMode::PG : ETranslationMode::SQL);

    TStringStream stream;
    NSkiff::TUncheckedSkiffWriter writer{&stream};
    for (ui64 i = 0; i < count; ++i) {
        writer.WriteVariant16Tag(0);
        writer.WriteInt64(i);
    }
    writer.Finish();
    auto input1 = TStringStream(stream);
    Cerr << "Input data size: " << input1.Size() << "\n";
    auto handle1 = genProgram->Apply(&input1);
    TStringStream output1;
    handle1->Run(&output1);
    Cerr << "Generated data size: " << output1.Size() << "\n";

    Cerr << "Dry run of test sql...\n";
    auto inputSpec2 = TSkiffInputSpec(genProgram->MakeOutputSchema());
    auto outputSpec2 = TYsonOutputSpec({NYT::TNode::CreateEntity()});
    auto testProgram = factory->MakePullListProgram(
        inputSpec2,
        outputSpec2,
        testSql,
        res.Has("pt") ? ETranslationMode::PG : ETranslationMode::SQL);
    auto input2 = TStringStream(output1);
    auto handle2 = testProgram->Apply(&input2);
    TStringStream output2;
    handle2->Run(&output2);
    if (showResults) {
        TStringInput in(output2.Str());
        NYson::ReformatYsonStream(&in, &Cerr, NYson::EYsonFormat::Pretty, NYson::EYsonType::ListFragment);
    }

    Cerr << "Run benchmark...\n";
    TVector<TDuration> times;
    TSimpleTimer allTimer;
    for (ui32 i = 0; i < repeats; ++i) {
        TSimpleTimer timer;
        auto input2 = TStringStream(output1);
        auto handle2 = testProgram->Apply(&input2);
        TNullOutput output2;
        handle2->Run(&output2);
        times.push_back(timer.Get());
    }

    Cout << "Elapsed: " << allTimer.Get() << "\n";
    Sort(times);
    times.erase(times.end() - times.size() / 3, times.end());
    double s = 0;
    for (auto t : times) {
        s += std::log(t.MicroSeconds());
    }

    double score = output1.Size() / std::exp(s / times.size());
    Cout << "Bench score: " << Prec(score, 4) << "\n";

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
