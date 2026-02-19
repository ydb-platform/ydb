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

using namespace NYql;
using namespace NYql::NPureCalc;
using namespace NKikimr::NMiniKQL;
using namespace NYql::NUdf;

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

struct TPickleOutputSpec: public TOutputSpecBase {
    explicit TPickleOutputSpec(const NYT::TNode& schema)
        : Schema(schema)
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

struct TPrintOutputSpec: public TOutputSpecBase {
    explicit TPrintOutputSpec(const NYT::TNode& schema)
        : Schema(schema)
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

template <typename TInputSpec, typename TOutputSpec>
using TRunCallable = std::function<void(const THolder<TPullListProgram<TInputSpec, TOutputSpec>>&)>;

template <typename TOutputSpec>
NYT::TNode RunGenSql(
    const IProgramFactoryPtr factory,
    const TVector<NYT::TNode>& inputSchema,
    const TString& sql,
    ETranslationMode isPg,
    TRunCallable<TPickleInputSpec, TOutputSpec> runCallable) {
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

template <typename TInputSpec, typename TOutputSpec>
double RunBenchmarks(
    const IProgramFactoryPtr factory,
    const TVector<NYT::TNode>& inputSchema,
    const TString& sql,
    ETranslationMode isPg,
    ui32 repeats,
    TRunCallable<TInputSpec, TOutputSpec> runCallable) {
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

int Main(int argc, const char** argv)
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
        auto outputGenSchema = RunGenSql<TPickleOutputSpec>(
            factory, inputGenSchema, genSql, isPgGen,
            [&](const auto& program) {
                auto handle = program->Apply(&inputGenStream);
                handle->Run(&outputGenStream);
                Cerr << "Generated data size: " << outputGenStream.Size() << "\n";
            });

        if (showResults) {
            auto inputResStream = TStringStream(outputGenStream);
            ShowResults<TPickleInputSpec>(
                factory, {outputGenSchema}, testSql, isPgTest, &inputResStream);
        }

        inputBenchSize = outputGenStream.Size();
        normalizedTime = RunBenchmarks<TPickleInputSpec, TPickleOutputSpec>(
            factory, {outputGenSchema}, testSql, isPgTest, repeats,
            [&](const auto& program) {
                auto inputBorrowed = TStringStream(outputGenStream);
                auto handle = program->Apply(&inputBorrowed);
                TNullOutput output;
                handle->Run(&output);
            });
    } else {
        auto inputGenSpec = TPickleInputSpec(inputGenSchema);
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
                while (/* arrow::compute::ExecBatch* batch = */ handle->Fetch()) {
                }
            });
    }

    Cout << "Bench score: " << Prec(inputBenchSize / normalizedTime, 4) << "\n";

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
