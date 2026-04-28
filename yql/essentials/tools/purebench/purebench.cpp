#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/getopt/last_getopt.h>

#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/arrow/spec.h>
#include <yql/essentials/public/purecalc/helpers/stream/stream_from_vector.h>

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_version.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>

#include <library/cpp/yson/writer.h>

#include <util/datetime/cputimer.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/null.h>

#include <algorithm>
#include <cmath>
#include <utility>

using namespace NYql;
using namespace NYql::NPureCalc;
using namespace NKikimr::NMiniKQL;
using namespace NYql::NUdf;

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
struct TPickleInputSpec: public TInputSpecBase {
    explicit TPickleInputSpec(const TVector<NYT::TNode>& schemas)
        : Schemas(schemas)
    {
    }

    const TVector<NYT::TNode>& GetSchemas() const final {
        return Schemas;
    }

    const TVector<NYT::TNode> Schemas;
};

class TPickleListValue final: public TCustomListValue {
public:
    TPickleListValue(
        TMemoryUsageInfo* memInfo,
        const TPickleInputSpec& /* inputSpec */,
        ui32 index,
        IInputStream* underlying,
        IWorker* worker)
        : TCustomListValue(memInfo)
        , Underlying_(underlying)
        , Worker_(worker)
        , ScopedAlloc_(Worker_->GetScopedAlloc())
        , Packer_(false, Worker_->GetInputType(index))
    {
    }

    TUnboxedValue GetListIterator() const override {
        YQL_ENSURE(!HasIterator_, "Only one pass over input is supported");
        HasIterator_ = true;
        return TUnboxedValuePod(const_cast<TPickleListValue*>(this));
    }

    bool Next(TUnboxedValue& result) override {
        ui32 len;
        auto read = Underlying_->Load(&len, sizeof(len));
        if (!read) {
            return false;
        }

        YQL_ENSURE(read == sizeof(len));
        if (len > RecordBuffer_.size()) {
            RecordBuffer_.resize(Max<size_t>(2 * RecordBuffer_.size(), len));
        }

        Underlying_->LoadOrFail(RecordBuffer_.data(), len);
        result = Packer_.Unpack(TStringBuf(RecordBuffer_.data(), len), Worker_->GetGraph().GetHolderFactory());
        return true;
    }

private:
    mutable bool HasIterator_ = false;
    IInputStream* Underlying_;
    IWorker* Worker_;
    TScopedAlloc& ScopedAlloc_;
    TValuePackerGeneric<true> Packer_;
    TVector<char> RecordBuffer_;
};

template <>
struct TInputSpecTraits<TPickleInputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = false;

    static void PreparePullListWorker(const TPickleInputSpec& spec, IPullListWorker* worker, IInputStream* stream) {
        PreparePullListWorker(spec, worker, TVector<IInputStream*>({stream}));
    }

    static void PreparePullListWorker(const TPickleInputSpec& spec, IPullListWorker* worker, const TVector<IInputStream*>& streams) {
        YQL_ENSURE(worker->GetInputsCount() == streams.size(),
                   "number of input streams should match number of inputs provided by spec");

        with_lock (worker->GetScopedAlloc()) {
            auto& holderFactory = worker->GetGraph().GetHolderFactory();
            for (ui32 i = 0; i < streams.size(); i++) {
                auto input = holderFactory.template Create<TPickleListValue>(
                    spec, i, std::move(streams[i]), worker);
                worker->SetInput(std::move(input), i);
            }
        }
    }
};

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
struct TPickleOutputSpec: public TOutputSpecBase {
    explicit TPickleOutputSpec(NYT::TNode schema)
        : Schema(std::move(schema))
    {
    }

    const NYT::TNode& GetSchema() const final {
        return Schema;
    }

    const NYT::TNode Schema;
};

class TStreamOutputHandle: private TMoveOnly {
public:
    virtual NKikimr::NMiniKQL::TType* GetOutputType() const = 0;
    virtual void Run(IOutputStream*) = 0;
    virtual ~TStreamOutputHandle() = default;
};

class TPickleOutputHandle final: public TStreamOutputHandle {
public:
    explicit TPickleOutputHandle(TWorkerHolder<IPullListWorker> worker)
        : Worker_(std::move(worker))
        , Packer_(false, Worker_->GetOutputType())
    {
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const final {
        return const_cast<NKikimr::NMiniKQL::TType*>(Worker_->GetOutputType());
    }

    void Run(IOutputStream* stream) final {
        Y_ENSURE(
            Worker_->GetOutputType()->IsStruct(),
            "Run(IOutputStream*) cannot be used with multi-output programs");

        TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            TUnboxedValue value;
            while (outputIterator.Next(value)) {
                auto buf = Packer_.Pack(value);
                ui32 len = buf.Size();
                stream->Write(&len, sizeof(len));
                stream->Write(buf.Data(), len);
            }
            Worker_->CheckState(true);
        }
    }

private:
    TWorkerHolder<IPullListWorker> Worker_;
    TValuePackerGeneric<true> Packer_;
};

template <>
struct TOutputSpecTraits<TPickleOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = false;

    using TPullListReturnType = THolder<TPickleOutputHandle>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(const TPickleOutputSpec&, TWorkerHolder<IPullListWorker> worker) {
        return MakeHolder<TPickleOutputHandle>(std::move(worker));
    }
};

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
struct TPrintOutputSpec: public TOutputSpecBase {
    explicit TPrintOutputSpec(NYT::TNode schema)
        : Schema(std::move(schema))
    {
    }

    const NYT::TNode& GetSchema() const final {
        return Schema;
    }

    const NYT::TNode Schema;
};

class TPrintOutputHandle final: public TStreamOutputHandle {
public:
    explicit TPrintOutputHandle(TWorkerHolder<IPullListWorker> worker)
        : Worker_(std::move(worker))
    {
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const final {
        return const_cast<NKikimr::NMiniKQL::TType*>(Worker_->GetOutputType());
    }

    void Run(IOutputStream* stream) final {
        Y_ENSURE(
            Worker_->GetOutputType()->IsStruct(),
            "Run(IOutputStream*) cannot be used with multi-output programs");

        TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            TUnboxedValue value;
            while (outputIterator.Next(value)) {
                auto str = NCommon::WriteYsonValue(value, GetOutputType());
                stream->Write(str.data(), str.size());
                stream->Write(';');
            }
        }
    }

private:
    TWorkerHolder<IPullListWorker> Worker_;
};

template <>
struct TOutputSpecTraits<TPrintOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = false;

    using TPullListReturnType = THolder<TPrintOutputHandle>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(const TPrintOutputSpec&, TWorkerHolder<IPullListWorker> worker) {
        return MakeHolder<TPrintOutputHandle>(std::move(worker));
    }
};

template <bool SupportsBlocks>
struct TNopOutputSpec: public TOutputSpecBase {
    explicit TNopOutputSpec(NYT::TNode schema)
        : Schema(std::move(schema))
    {
    }

    const NYT::TNode& GetSchema() const final {
        return Schema;
    }

    bool AcceptsBlocks() const override {
        return SupportsBlocks;
    }

    const NYT::TNode Schema;
};

class TNopScalarOutputHandle final: public TStreamOutputHandle {
public:
    explicit TNopScalarOutputHandle(TWorkerHolder<IPullListWorker> worker)
        : Worker_(std::move(worker))
    {
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const final {
        return const_cast<NKikimr::NMiniKQL::TType*>(Worker_->GetOutputType());
    }

    void Run(IOutputStream* stream) final {
        Y_UNUSED(stream);
        Y_ENSURE(
            Worker_->GetOutputType()->IsStruct(),
            "Run(IOutputStream*) cannot be used with multi-output programs");

        TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            TUnboxedValue value;
            while (outputIterator.Next(value)) {
            }
            Worker_->CheckState(true);
        }
    }

private:
    TWorkerHolder<IPullListWorker> Worker_;
};

class TNopBlockOutputHandle final: public IStream<arrow::compute::ExecBatch*> {
public:
    explicit TNopBlockOutputHandle(TWorkerHolder<IPullListWorker> worker)
        : Worker_(std::move(worker))
    {
    }

    arrow::compute::ExecBatch* Fetch() override {
        TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            TUnboxedValue value;
            if (outputIterator.Next(value)) {
                return &ExecBatchStub_;
            }
            Worker_->CheckState(true);
            return nullptr;
        }
    }

private:
    TWorkerHolder<IPullListWorker> Worker_;
    arrow::compute::ExecBatch ExecBatchStub_;
};

template <bool SupportsBlocks>
struct TOutputSpecTraits<TNopOutputSpec<SupportsBlocks>> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = false;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = false;

    using TPullListReturnType = std::conditional_t<SupportsBlocks, THolder<TNopBlockOutputHandle>, THolder<TNopScalarOutputHandle>>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(const TNopOutputSpec<SupportsBlocks>&, TWorkerHolder<IPullListWorker> worker) {
        if constexpr (SupportsBlocks) {
            return MakeHolder<TNopBlockOutputHandle>(std::move(worker));
        } else {
            return MakeHolder<TNopScalarOutputHandle>(std::move(worker));
        }
    }
};

TStringStream MakeGenInput(ui64 count) {
    TStringStream stream;
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TMemoryUsageInfo memInfo("MakeGenInput");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    auto ui64Type = env.GetUi64Lazy();
    std::pair<TString, NKikimr::NMiniKQL::TType*> member("index", ui64Type);
    auto ui64StructType = TStructType::Create(&member, 1, env);
    TValuePackerGeneric<true> packer(false, ui64StructType);

    TPlainContainerCache cache;
    for (ui64 i = 0; i < count; ++i) {
        TUnboxedValue* items;
        auto array = cache.NewArray(holderFactory, 1, items);
        items[0] = TUnboxedValuePod(i);
        auto buf = packer.Pack(array);
        ui32 len = buf.Size();
        stream.Write(&len, sizeof(len));
        stream.Write(buf.Data(), len);
    }

    return stream;
}

template <typename TOutputSpec, typename TRunProgram>
NYT::TNode RunGenSql(
    const IProgramFactoryPtr factory,
    const TVector<NYT::TNode>& inputSchema,
    const TString& sql,
    ETranslationMode isPg,
    const TRunProgram& runCallable) {
    auto inputSpec = TPickleInputSpec(inputSchema);
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
    TStream* input) {
    auto inputSpec = TInputSpec(inputSchema);
    auto outputSpec = TPrintOutputSpec({NYT::TNode::CreateEntity()});
    auto program = factory->MakePullListProgram(inputSpec, outputSpec, sql, isPg);
    auto handle = program->Apply(input);
    TStringStream output;
    output << "{Type=";
    output << NCommon::WriteTypeToYson(handle->GetOutputType());
    output << ";Data=[";
    handle->Run(&output);
    output << "]}";
    TStringInput in(output.Str());
    NYson::ReformatYsonStream(&in, &Cerr, NYson::EYsonFormat::Pretty, NYson::EYsonType::Node);
    Cerr << "\n";
}

void DropOutliers(TVector<TDuration>& times, size_t portion) {
    Sort(times);
    times.erase(times.end() - times.size() / portion, times.end());
}

double Score(TVector<TDuration> times) {
    double sum = std::transform_reduce(times.cbegin(), times.cend(), 0.0, std::plus{},
                                       [](auto t) { return std::log(t.MicroSeconds()); });
    return std::exp(sum / times.size()) / 1000;
}

double CV(const TVector<TDuration>& times) {
    TVector<ui64> microseconds(times.size());
    std::transform(times.cbegin(), times.cend(), microseconds.begin(),
                   [](auto t) { return t.MicroSeconds(); });
    double sum = std::accumulate(microseconds.cbegin(), microseconds.cend(), 0.0);
    double mean = sum / microseconds.size();
    double sqDiff = std::transform_reduce(microseconds.cbegin(), microseconds.cend(), 0.0, std::plus{},
                                          [mean](auto t) { return std::pow(t - mean, 2); });
    return std::sqrt(sqDiff / microseconds.size()) / mean * 100.0;
}

template <typename TInputSpec, typename TOutputSpec, typename TNopOutputSpec, typename TRunProgram>
std::tuple<double, double, double> RunBenchmarks(
    const IProgramFactoryPtr testFactory,
    const IProgramFactoryPtr benchFactory,
    const TVector<NYT::TNode>& inputSchema,
    const TString& sql,
    ETranslationMode isPg,
    ui32 benchmarkRuns,
    ui32 calibrationRuns,
    TDuration benchmarkTime,
    TDuration calibrationTime,
    const TRunProgram& runCallable) {
    auto inputSpec = TInputSpec(inputSchema);
    auto outputSpec = TOutputSpec({NYT::TNode::CreateEntity()});
    auto nopSpec = TNopOutputSpec({NYT::TNode::CreateEntity()});

    Cerr << "Dry run of test sql...\n";

    auto testProgram = testFactory->MakePullListProgram(inputSpec, outputSpec, sql, isPg);

    runCallable(testProgram);

    Cerr << "Run benchmark...\n";

    auto benchProgram = benchFactory->MakePullListProgram(inputSpec, nopSpec, sql, isPg);

    ui32 benchmarks = 0;
    TVector<TDuration> benchTimes;
    TSimpleTimer benchTimer;
    while (++benchmarks < benchmarkRuns || benchTimer.Get() < benchmarkTime) {
        TSimpleTimer timer;
        runCallable(benchProgram);
        benchTimes.push_back(timer.Get());
    }

    Cout << "Benchmark completed: " << benchmarks << " iterations for " << benchTimer.Get() << "\n";

    Cerr << "Score calibration...\n";

    auto calibrationProgram = benchFactory->MakePullListProgram(inputSpec, nopSpec, "SELECT * FROM Input", isPg);

    ui32 calibrations = 0;
    TVector<TDuration> calibrationTimes;
    TSimpleTimer calibrationTimer;
    while (++calibrations < calibrationRuns || calibrationTimer.Get() < calibrationTime) {
        TSimpleTimer timer;
        runCallable(calibrationProgram);
        calibrationTimes.push_back(timer.Get());
    }

    Cerr << "Calibration completed: " << calibrations << " iterations for " << calibrationTimer.Get() << "\n";

    DropOutliers(benchTimes, 3);
    DropOutliers(calibrationTimes, 3);

    return {Score(benchTimes) - Score(calibrationTimes), Score(benchTimes), CV(benchTimes)};
}

int Main(int argc, const char** argv)
{
    Y_UNUSED(NUdf::GetStaticSymbols());
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();
    ui64 count;
    ui32 benchmarkRuns;
    ui32 calibrationRuns;
    TDuration benchmarkSeconds;
    TDuration calibrationSeconds;
    const auto secondsFromString = [](const TString& str) { return TDuration::Seconds(std::stoul(str)); };
    TString genSql;
    TString testSql;
    bool showResults;
    TString udfsDir;
    TString LLVMSettings;
    TString blockEngineSettings;
    TString exprFile;
    TLangVersion langVer = NYql::GetMaxReleasedLangVersion();
    opts.AddHelpOption();
    opts.AddLongOption("ndebug", "should be at first argument, do not show debug info in error output").NoArgument();
    opts.AddLongOption('b', "blocks-engine", "Block engine settings").StoreResult(&blockEngineSettings).DefaultValue("disable");
    opts.AddLongOption('c', "count", "count of input rows").StoreResult(&count).DefaultValue(1000000);
    opts.AddLongOption('g', "gen-sql", "SQL query to generate data").StoreResult(&genSql).DefaultValue("select index from Input");
    opts.AddLongOption('t', "test-sql", "SQL query to test").StoreResult(&testSql).DefaultValue("select count(*) as count from Input");
    opts.AddLongOption('r', "repeats", "number of iterations").StoreResult(&benchmarkRuns).DefaultValue(10);
    opts.AddLongOption('R', "repeat-time", "total time running benchmark").StoreMappedResultT<TString, TDuration>(&benchmarkSeconds, secondsFromString).DefaultValue(1);
    opts.AddLongOption('w', "show-results", "show results of test SQL").StoreResult(&showResults).DefaultValue(true);
    opts.AddLongOption("pg", "use PG syntax for generate query").NoArgument();
    opts.AddLongOption("pt", "use PG syntax for test query").NoArgument();
    opts.AddLongOption("udfs-dir", "directory with UDFs").StoreResult(&udfsDir).DefaultValue("");
    opts.AddLongOption("llvm-settings", "LLVM settings").StoreResult(&LLVMSettings).DefaultValue("");
    opts.AddLongOption("print-expr", "print rebuild AST before execution").NoArgument();
    opts.AddLongOption("expr-file", "print AST to that file instead of stdout").StoreResult(&exprFile);
    opts.AddLongOption("calibrate", "number of calibrating iterations").StoreResult(&calibrationRuns).DefaultValue(3);
    opts.AddLongOption("calibrate-time", "number of calibrating iterations").StoreMappedResultT<TString, TDuration>(&calibrationSeconds, secondsFromString).DefaultValue(1);
    opts.AddLongOption("langver", "Set current language version").RequiredArgument("VER").Handler1T<TString>([&langVer](const TString& str) {
        if (str == "unknown") {
            langVer = UnknownLangVersion;
        } else if (!ParseLangVersion(str, langVer)) {
            throw yexception() << "Failed to parse language version: " << str;
        }
    });
    opts.SetFreeArgsMax(0);
    TOptsParseResult res(&opts, argc, argv);

    auto factoryOptions = TProgramFactoryOptions();
    factoryOptions.SetUDFsDir(udfsDir);
    factoryOptions.SetLLVMSettings(LLVMSettings);
    factoryOptions.SetBlockEngineSettings(blockEngineSettings);
    factoryOptions.SetLanguageVersion(langVer);

    auto benchFactory = MakeProgramFactory(factoryOptions);

    IOutputStream* exprOut = nullptr;
    THolder<TFixedBufferFileOutput> exprFileHolder;
    if (res.Has("print-expr")) {
        exprOut = &Cout;
    } else if (!exprFile.empty()) {
        exprFileHolder.Reset(new TFixedBufferFileOutput(exprFile));
        exprOut = exprFileHolder.Get();
    }
    factoryOptions.SetExprOutputStream(exprOut);

    auto testFactory = MakeProgramFactory(factoryOptions);

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
    std::tuple<double, double, double> score;

    if (blockEngineSettings == "disable") {
        TStringStream outputGenStream;
        auto outputGenSchema = RunGenSql<TPickleOutputSpec>(
            testFactory, inputGenSchema, genSql, isPgGen,
            [&](const auto& program) {
                auto handle = program->Apply(&inputGenStream);
                handle->Run(&outputGenStream);
                Cerr << "Generated data size: " << outputGenStream.Size() << "\n";
            });

        if (showResults) {
            auto inputResStream = TStringStream(outputGenStream);
            ShowResults<TPickleInputSpec>(
                benchFactory, {outputGenSchema}, testSql, isPgTest, &inputResStream);
        }

        score = RunBenchmarks<TPickleInputSpec, TPickleOutputSpec, TNopOutputSpec<false>>(
            testFactory, benchFactory, {outputGenSchema}, testSql, isPgTest,
            benchmarkRuns, calibrationRuns, benchmarkSeconds, calibrationSeconds,
            [&](const auto& program) {
                auto inputBorrowed = TStringStream(outputGenStream);
                auto handle = program->Apply(&inputBorrowed);
                TNullOutput output;
                handle->Run(&output);
            });
    } else {
        auto inputGenSpec = TPickleInputSpec(inputGenSchema);
        // XXX: Untrack the datums, produced by "gen sql", so they can be
        // preserved for later multiply usage in "test sql".
        auto outputGenSpec = TArrowOutputSpec({NYT::TNode::CreateEntity()}, true);
        // XXX: <RunGenSql> cannot be used for this case, since all buffers
        // from the Datums in the obtained batches are owned by the worker's
        // allocator. Hence, the program (i.e. worker) object should be created
        // at the very beginning of the block, or at least prior to all the
        // temporary batch storages (mind outputGenStream below).
        auto program = testFactory->MakePullListProgram(
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
                benchFactory, {outputGenSchema}, testSql, isPgTest, inputResStream);
        }

        score = RunBenchmarks<TArrowInputSpec, TArrowOutputSpec, TNopOutputSpec<true>>(
            testFactory, benchFactory, {outputGenSchema}, testSql, isPgTest,
            benchmarkRuns, calibrationRuns, benchmarkSeconds, calibrationSeconds,
            [&](const auto& program) {
                auto handle = program->Apply(StreamFromVector(outputGenStream));
                while (/* arrow::compute::ExecBatch* batch = */ handle->Fetch()) {
                }
            });
    }

    Cout << "Bench score: "
         << Prec(std::get<0>(score), 4)
         << "ms (mean wall clock: "
         << Prec(std::get<1>(score), 4)
         << "ms, cv: "
         << Prec(std::get<2>(score), 4)
         << "%)\n";

    NLog::CleanupLogger();
    return 0;
}

int main(int argc, const char** argv) {
    if (argc > 1 && TString(argv[1]) != TStringBuf("--ndebug")) {
        Cerr << "purebench ABI version: " << NKikimr::NUdf::CurrentAbiVersionStr() << Endl;
    }

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        return Main(argc, argv);
    } catch (const TCompileError& e) {
        Cerr << e.what() << "\n"
             << e.GetIssues();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
