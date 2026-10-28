#include "benchmark_program.h"

#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/arrow/spec.h>
#include <yql/essentials/public/purecalc/helpers/stream/stream_from_vector.h>

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/udf_registrator.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>

#include <library/cpp/time_provider/monotonic.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>
#include <library/cpp/yson/writer.h>

#include <util/stream/null.h>
#include <util/stream/str.h>

#include <type_traits>
#include <utility>

namespace NYql::NPureBench {
namespace {

namespace NMiniKQL = NKikimr::NMiniKQL;

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
struct TPickleInputSpec: public NPureCalc::TInputSpecBase {
    explicit TPickleInputSpec(const TVector<NYT::TNode>& schemas)
        : Schemas(schemas)
    {
    }

    const TVector<NYT::TNode>& GetSchemas() const final {
        return Schemas;
    }

    const TVector<NYT::TNode> Schemas;
};

class TPickleListValue final: public NMiniKQL::TCustomListValue {
public:
    TPickleListValue(
        NMiniKQL::TMemoryUsageInfo* memInfo,
        const TPickleInputSpec& /* inputSpec */,
        ui32 index,
        IInputStream* underlying,
        NPureCalc::IWorker* worker)
        : NMiniKQL::TCustomListValue(memInfo)
        , Underlying_(underlying)
        , Worker_(worker)
        , ScopedAlloc_(Worker_->GetScopedAlloc())
        , Packer_(/*stable=*/false, Worker_->GetInputType(index))
    {
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        YQL_ENSURE(!HasIterator_, "Only one pass over input is supported");
        HasIterator_ = true;
        return NUdf::TUnboxedValuePod(const_cast<TPickleListValue*>(this));
    }

    bool Next(NUdf::TUnboxedValue& result) override {
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
    NPureCalc::IWorker* Worker_;
    NMiniKQL::TScopedAlloc& ScopedAlloc_;
    NMiniKQL::TValuePackerGeneric<true> Packer_;
    TVector<char> RecordBuffer_;
};

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
struct TPickleOutputSpec: public NPureCalc::TOutputSpecBase {
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
    explicit TPickleOutputHandle(NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> worker)
        : Worker_(std::move(worker))
        , Packer_(/*stable=*/false, Worker_->GetOutputType())
    {
    }

    NKikimr::NMiniKQL::TType* GetOutputType() const final {
        return const_cast<NKikimr::NMiniKQL::TType*>(Worker_->GetOutputType());
    }

    void Run(IOutputStream* stream) final {
        Y_ENSURE(
            Worker_->GetOutputType()->IsStruct(),
            "Run(IOutputStream*) cannot be used with multi-output programs");

        NMiniKQL::TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            NUdf::TUnboxedValue value;
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
    NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> Worker_;
    NMiniKQL::TValuePackerGeneric<true> Packer_;
};

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
struct TPrintOutputSpec: public NPureCalc::TOutputSpecBase {
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
    explicit TPrintOutputHandle(NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> worker)
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

        NMiniKQL::TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            NUdf::TUnboxedValue value;
            while (outputIterator.Next(value)) {
                auto str = NCommon::WriteYsonValue(value, GetOutputType());
                stream->Write(str.data(), str.size());
                stream->Write(';');
            }
            Worker_->CheckState(true);
        }
    }

private:
    NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> Worker_;
};

template <bool SupportsBlocks>
struct TNopOutputSpec: public NPureCalc::TOutputSpecBase {
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
    explicit TNopScalarOutputHandle(NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> worker)
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

        NMiniKQL::TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            NUdf::TUnboxedValue value;
            while (outputIterator.Next(value)) {
            }
            Worker_->CheckState(true);
        }
    }

private:
    NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> Worker_;
};

class TNopBlockOutputHandle final: public NPureCalc::IStream<arrow::compute::ExecBatch*> {
public:
    explicit TNopBlockOutputHandle(NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> worker)
        : Worker_(std::move(worker))
    {
    }

    arrow::compute::ExecBatch* Fetch() override {
        NMiniKQL::TBindTerminator bind(Worker_->GetGraph().GetTerminator());

        with_lock (Worker_->GetScopedAlloc()) {
            const auto outputIterator = Worker_->GetOutputIterator();

            NUdf::TUnboxedValue value;
            if (outputIterator.Next(value)) {
                return &ExecBatchStub_;
            }
            Worker_->CheckState(true);
            return nullptr;
        }
    }

private:
    NPureCalc::TWorkerHolder<NPureCalc::IPullListWorker> Worker_;
    arrow::compute::ExecBatch ExecBatchStub_;
};

NPureCalc::ETranslationMode TranslationMode(ESyntax syntax) {
    switch (syntax) {
        case ESyntax::SQL:
            return NPureCalc::ETranslationMode::SQL;
        case ESyntax::PG:
            return NPureCalc::ETranslationMode::PG;
    }
    ythrow yexception() << "Unsupported query syntax: " << static_cast<int>(syntax);
}

template <typename TInputSpec, typename TOutputSpec>
auto CompileProgram(const NPureCalc::IProgramFactoryPtr& factory,
                    const TInputSpec& inputSpec, const TOutputSpec& outputSpec,
                    const TString& query, ESyntax syntax) {
    try {
        return factory->MakePullListProgram(inputSpec, outputSpec, query, TranslationMode(syntax));
    } catch (const NPureCalc::TCompileError& error) {
        ythrow yexception() << error.what() << '\n'
                            << error.GetIssues();
    }
}

NPureCalc::IProgramFactoryPtr MakeFactory(const TBenchmarkProgramOptions& options, IOutputStream* exprOutput) {
    NPureCalc::TProgramFactoryOptions factoryOptions;
    factoryOptions.SetUDFsDir(options.UdfsDirectory);
    factoryOptions.SetLLVMSettings(options.LLVMSettings);
    factoryOptions.SetBlockEngineSettings(options.BlockEngineSettings);
    factoryOptions.SetLanguageVersion(options.LanguageVersion);
    factoryOptions.SetExprOutputStream(exprOutput);
    factoryOptions.SetUseWorkerPool(true);
    return NPureCalc::MakeProgramFactory(factoryOptions);
}

NYT::TNode MakeSeedSchema() {
    auto type = NYT::TNode::CreateList().Add("DataType").Add("Int64");
    auto member = NYT::TNode::CreateList().Add("index").Add(type);
    return NYT::TNode::CreateList().Add("StructType").Add(NYT::TNode::CreateList().Add(member));
}

TStringStream MakeGenInput(ui64 count) {
    TStringStream stream;
    NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NMiniKQL::TTypeEnvironment env(alloc);
    NMiniKQL::TMemoryUsageInfo memInfo("MakeGenInput");
    NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo);
    auto ui64Type = env.GetUi64Lazy();
    std::pair<TString, NMiniKQL::TType*> member("index", ui64Type);
    auto ui64StructType = NMiniKQL::TStructType::Create(&member, 1, env);
    NMiniKQL::TValuePackerGeneric<true> packer(/*stable=*/false, ui64StructType);

    NMiniKQL::TPlainContainerCache cache;
    for (ui64 i = 0; i < count; ++i) {
        NUdf::TUnboxedValue* items;
        auto array = cache.NewArray(holderFactory, 1, items);
        items[0] = NUdf::TUnboxedValuePod(i);
        auto buf = packer.Pack(array);
        ui32 len = buf.Size();
        stream.Write(&len, sizeof(len));
        stream.Write(buf.Data(), len);
    }

    return stream;
}

TQueryOutput CollectResult(TStreamOutputHandle& handle) {
    TStringStream output;
    output << '[';
    handle.Run(&output);
    output << ']';
    auto type = NYT::NodeFromYsonString(NCommon::WriteTypeToYson(handle.GetOutputType()));
    auto rows = NYT::NodeFromYsonString(output.Str());
    return TQueryOutput(std::move(type), std::move(rows.AsList()));
}

struct TScalarBenchmarkInput {
    using TInputSpec = TPickleInputSpec;
    using TOutputSpec = TPickleOutputSpec;
    static constexpr bool UsesBlocks = false;

    THolder<NPureCalc::TPullListProgram<TPickleInputSpec, TOutputSpec>> GeneratorProgram;
    TStringStream Data;
    NYT::TNode RowSchema;
    ui64 GeneratorInputBytes = 0;

    TScalarBenchmarkInput(const NPureCalc::IProgramFactoryPtr& factory, const TBenchmarkProgramOptions& options);
    void Generate(ui64 generatorInputRows);

    ui64 GetByteSize() const {
        return Data.Size();
    }

    template <typename TRun>
    auto WithInput(const TRun& run) const {
        auto input = TStringStream(Data);
        return run(&input);
    }
};

struct TBlockBenchmarkInput {
    using TInputSpec = NPureCalc::TArrowInputSpec;
    using TOutputSpec = NPureCalc::TArrowOutputSpec;
    static constexpr bool UsesBlocks = true;

    // Batches borrow the generator's allocator, so destroy them before the program.
    THolder<NPureCalc::TPullListProgram<TPickleInputSpec, TOutputSpec>> GeneratorProgram;
    TVector<arrow::compute::ExecBatch> Data;
    NYT::TNode RowSchema;
    ui64 GeneratorInputBytes = 0;

    TBlockBenchmarkInput(const NPureCalc::IProgramFactoryPtr& factory, const TBenchmarkProgramOptions& options);
    void Generate(ui64 generatorInputRows);

    ui64 GetByteSize() const {
        ui64 bytes = 0;
        for (const auto& batch : Data) {
            bytes += NUdf::GetSizeOfArrowExecBatchInBytes(batch);
        }
        return bytes;
    }

    template <typename TRun>
    auto WithInput(const TRun& run) const {
        auto input = NPureCalc::StreamFromVector(Data);
        return run(input.Get());
    }
};

template <typename TBenchmarkInput>
class TBenchmarkProgram final: public IBenchmarkProgram {
    using TInputSpec = typename TBenchmarkInput::TInputSpec;
    using TOutputSpec = typename TBenchmarkInput::TOutputSpec;
    using TDiscardOutputSpec = TNopOutputSpec<TBenchmarkInput::UsesBlocks>;
    using TProgram = NPureCalc::TPullListProgram<TInputSpec, TDiscardOutputSpec>;
    using TProgramWithOutput = NPureCalc::TPullListProgram<TInputSpec, TPrintOutputSpec>;

public:
    explicit TBenchmarkProgram(TBenchmarkProgramOptions options)
        : Options_(std::move(options))
    {
    }

    TBenchmarkPreparationResult PrepareBenchmark(IOutputStream* exprOutput) override {
        PreparationResult_.Clear();
        if (!BenchmarkInput_) {
            CompileGenerationQuery(exprOutput);
        }
        if (!MeasuredProgram_) {
            CompileBenchmarkQueries(exprOutput);
        }
        BenchmarkInput_->Generate(Options_.GeneratorInputRows);
        RunAndMeasureProgram(*MeasuredProgram_);
        RunAndMeasureProgram(*MeasuredProgramWithOutput_);
        if (Options_.EnableCalibration) {
            RunAndMeasureProgram(*CalibrationProgram_);
        }
        PreparationResult_ = TBenchmarkPreparationResult{.GeneratorInputBytes = BenchmarkInput_->GeneratorInputBytes, .BenchmarkInputBytes = BenchmarkInput_->GetByteSize()};
        return *PreparationResult_;
    }

    TMeasureQueryResult RunMeasureQuery(EOutputMode outputMode) override {
        EnsurePrepared();
        if (outputMode == EOutputMode::Discard) {
            return {.Statistics = MakeStatistics(RunAndMeasureProgram(*MeasuredProgram_)), .Output = Nothing()};
        }
        YQL_ENSURE(outputMode == EOutputMode::Collect, "Unsupported output mode");
        const auto started = TMonotonic::Now();
        auto output = BenchmarkInput_->WithInput([&](auto* input) {
            auto handle = MeasuredProgramWithOutput_->Apply(input);
            return CollectResult(*handle);
        });
        return {.Statistics = MakeStatistics(TMonotonic::Now() - started), .Output = std::move(output)};
    }

    TCalibrationQueryResult RunCalibrationQuery() override {
        EnsurePrepared();
        if (!Options_.EnableCalibration) {
            return TCalibrationDisabled{};
        }
        return MakeStatistics(RunAndMeasureProgram(*CalibrationProgram_));
    }

private:
    void CompileGenerationQuery(IOutputStream* exprOutput) {
        YQL_ENSURE(!BenchmarkInput_, "CompileGenerationQuery() has already been called");
        Y_UNUSED(NUdf::GetStaticSymbols());
        BenchmarkInput_ = MakeHolder<TBenchmarkInput>(MakeFactory(Options_, exprOutput), Options_);
    }

    void CompileBenchmarkQueries(IOutputStream* exprOutput) {
        YQL_ENSURE(BenchmarkInput_, "Call CompileGenerationQuery() before CompileBenchmarkQueries()");
        YQL_ENSURE(!MeasuredProgram_, "CompileBenchmarkQueries() has already been called");
        const auto outputSchemaPlaceholder = NYT::TNode::CreateEntity();
        if (exprOutput) {
            CompileProgram(MakeFactory(Options_, exprOutput), TInputSpec({BenchmarkInput_->RowSchema}),
                           TOutputSpec(outputSchemaPlaceholder), Options_.MeasuredQuery, Options_.MeasuredQuerySyntax);
        }
        auto factory = MakeFactory(Options_, /*exprOutput=*/nullptr);
        auto measuredProgram = CompileBenchmarkQuery(factory, TDiscardOutputSpec(outputSchemaPlaceholder), Options_.MeasuredQuery);
        auto measuredProgramWithOutput = CompileBenchmarkQuery(factory, TPrintOutputSpec(outputSchemaPlaceholder), Options_.MeasuredQuery);
        THolder<TProgram> calibrationProgram;
        if (Options_.EnableCalibration) {
            const TString calibrationQuery = Options_.MeasuredQuerySyntax == ESyntax::PG
                                                 ? "SELECT * FROM \"Input\""
                                                 : "SELECT * FROM Input";
            calibrationProgram = CompileBenchmarkQuery(factory, TDiscardOutputSpec(outputSchemaPlaceholder), calibrationQuery);
        }
        MeasuredProgram_ = std::move(measuredProgram);
        MeasuredProgramWithOutput_ = std::move(measuredProgramWithOutput);
        CalibrationProgram_ = std::move(calibrationProgram);
    }

    template <typename TQueryOutputSpec>
    auto CompileBenchmarkQuery(const NPureCalc::IProgramFactoryPtr& factory,
                               const TQueryOutputSpec& outputSpec, const TString& query) const {
        return CompileProgram(factory, TInputSpec({BenchmarkInput_->RowSchema}), outputSpec, query, Options_.MeasuredQuerySyntax);
    }

    void EnsurePrepared() const {
        YQL_ENSURE(PreparationResult_, "Call PrepareBenchmark() before running benchmark queries");
    }

    TQueryRunStatistics MakeStatistics(TDuration elapsed) const {
        return {.Elapsed = elapsed, .GeneratorInputBytes = PreparationResult_->GeneratorInputBytes, .BenchmarkInputBytes = PreparationResult_->BenchmarkInputBytes};
    }

    template <typename TQueryOutputSpec>
    TDuration RunAndMeasureProgram(NPureCalc::TPullListProgram<TInputSpec, TQueryOutputSpec>& program) const {
        const auto started = TMonotonic::Now();
        BenchmarkInput_->WithInput([&](auto* input) {
            auto handle = program.Apply(input);
            if constexpr (std::is_same_v<TQueryOutputSpec, TNopOutputSpec<true>>) {
                while (handle->Fetch()) {
                }
            } else {
                TNullOutput output;
                handle->Run(&output);
            }
        });
        return TMonotonic::Now() - started;
    }

    TBenchmarkProgramOptions Options_;
    TMaybe<TBenchmarkPreparationResult> PreparationResult_;
    THolder<TBenchmarkInput> BenchmarkInput_;
    THolder<TProgram> MeasuredProgram_;
    THolder<TProgramWithOutput> MeasuredProgramWithOutput_;
    THolder<TProgram> CalibrationProgram_;
};

} // namespace

} // namespace NYql::NPureBench

template <>
struct NYql::NPureCalc::TInputSpecTraits<NYql::NPureBench::TPickleInputSpec> {
    static constexpr bool IsPartial = false;

    static constexpr bool SupportPullListMode = true;

    static void PreparePullListWorker(const NYql::NPureBench::TPickleInputSpec& spec, NYql::NPureCalc::IPullListWorker* worker, IInputStream* stream) {
        PreparePullListWorker(spec, worker, TVector<IInputStream*>({stream}));
    }

    static void PreparePullListWorker(const NYql::NPureBench::TPickleInputSpec& spec, NYql::NPureCalc::IPullListWorker* worker, const TVector<IInputStream*>& streams) {
        YQL_ENSURE(worker->GetInputsCount() == streams.size(),
                   "number of input streams should match number of inputs provided by spec");

        with_lock (worker->GetScopedAlloc()) {
            auto& holderFactory = worker->GetGraph().GetHolderFactory();
            for (ui32 i = 0; i < streams.size(); i++) {
                auto input = holderFactory.template Create<NYql::NPureBench::TPickleListValue>(
                    spec, i, streams[i], worker);
                worker->SetInput(input, i);
            }
        }
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<NYql::NPureBench::TPickleOutputSpec> {
    static constexpr bool IsPartial = false;

    static constexpr bool SupportPullListMode = true;

    using TPullListReturnType = THolder<NYql::NPureBench::TPickleOutputHandle>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(const NYql::NPureBench::TPickleOutputSpec&, NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullListWorker> worker) {
        return MakeHolder<NYql::NPureBench::TPickleOutputHandle>(std::move(worker));
    }
};

template <>
struct NYql::NPureCalc::TOutputSpecTraits<NYql::NPureBench::TPrintOutputSpec> {
    static constexpr bool IsPartial = false;

    static constexpr bool SupportPullListMode = true;

    using TPullListReturnType = THolder<NYql::NPureBench::TPrintOutputHandle>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(const NYql::NPureBench::TPrintOutputSpec&, NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullListWorker> worker) {
        return MakeHolder<NYql::NPureBench::TPrintOutputHandle>(std::move(worker));
    }
};

template <bool SupportsBlocks>
struct NYql::NPureCalc::TOutputSpecTraits<NYql::NPureBench::TNopOutputSpec<SupportsBlocks>> {
    static constexpr bool IsPartial = false;

    static constexpr bool SupportPullListMode = true;

    using TPullListReturnType = std::conditional_t<SupportsBlocks, THolder<NYql::NPureBench::TNopBlockOutputHandle>, THolder<NYql::NPureBench::TNopScalarOutputHandle>>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(const NYql::NPureBench::TNopOutputSpec<SupportsBlocks>&, NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullListWorker> worker) {
        if constexpr (SupportsBlocks) {
            return MakeHolder<NYql::NPureBench::TNopBlockOutputHandle>(std::move(worker));
        } else {
            return MakeHolder<NYql::NPureBench::TNopScalarOutputHandle>(std::move(worker));
        }
    }
};

namespace NYql::NPureBench {

TScalarBenchmarkInput::TScalarBenchmarkInput(const NPureCalc::IProgramFactoryPtr& factory, const TBenchmarkProgramOptions& options) {
    GeneratorProgram = CompileProgram(factory, TInputSpec({MakeSeedSchema()}),
                                      TOutputSpec(NYT::TNode::CreateEntity()), options.GenerationQuery, options.GenerationSyntax);
    RowSchema = GeneratorProgram->MakeOutputSchema();
}

void TScalarBenchmarkInput::Generate(ui64 generatorInputRows) {
    auto input = MakeGenInput(generatorInputRows);
    const auto generatorInputBytes = input.Size();
    TStringStream data;
    GeneratorProgram->Apply(&input)->Run(&data);
    Data = std::move(data);
    GeneratorInputBytes = generatorInputBytes;
}

TBlockBenchmarkInput::TBlockBenchmarkInput(const NPureCalc::IProgramFactoryPtr& factory, const TBenchmarkProgramOptions& options) {
    GeneratorProgram = CompileProgram(factory, TPickleInputSpec({MakeSeedSchema()}),
                                      TOutputSpec(NYT::TNode::CreateEntity(), /*untrackBatches=*/true),
                                      options.GenerationQuery, options.GenerationSyntax);
    RowSchema = GeneratorProgram->MakeOutputSchema();
}

void TBlockBenchmarkInput::Generate(ui64 generatorInputRows) {
    auto input = MakeGenInput(generatorInputRows);
    const auto generatorInputBytes = input.Size();
    TVector<arrow::compute::ExecBatch> data;
    auto handle = GeneratorProgram->Apply(&input);
    while (auto* batch = handle->Fetch()) {
        data.push_back(*batch);
    }
    Data = std::move(data);
    GeneratorInputBytes = generatorInputBytes;
}

TQueryOutput::TQueryOutput(NYT::TNode rowType, NYT::TNode::TListType rows)
    : RowType_(std::move(rowType))
    , Rows_(NYT::TNode::CreateList(std::move(rows)))
{
}

const NYT::TNode& TQueryOutput::GetRowType() const {
    return RowType_;
}

const NYT::TNode::TListType& TQueryOutput::GetRows() const {
    return Rows_.AsList();
}

TString ToYson(const TQueryOutput& result) {
    TStringStream output;
    NYson::TYsonWriter writer(&output, NYson::EYsonFormat::Pretty);
    NYT::TNodeVisitor visitor(&writer);
    writer.OnBeginMap();
    writer.OnKeyedItem("Type");
    visitor.Visit(result.GetRowType());
    writer.OnKeyedItem("Data");
    visitor.VisitList(result.GetRows());
    writer.OnEndMap();
    return output.Str();
}

THolder<IBenchmarkProgram> CreateBenchmarkProgram(const TBenchmarkProgramOptions& options) {
    if (options.BlockEngineSettings == "disable") {
        return MakeHolder<TBenchmarkProgram<TScalarBenchmarkInput>>(options);
    }
    return MakeHolder<TBenchmarkProgram<TBlockBenchmarkInput>>(options);
}

} // namespace NYql::NPureBench
