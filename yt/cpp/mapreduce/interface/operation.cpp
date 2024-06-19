#include "operation.h"

#include <yt/cpp/mapreduce/interface/helpers.h>

#include <util/generic/iterator_range.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
    i64 OutputTableCount = -1;
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TTaskName::TTaskName(TString taskName)
    : TaskName_(std::move(taskName))
{ }

TTaskName::TTaskName(const char* taskName)
    : TaskName_(taskName)
{ }

TTaskName::TTaskName(ETaskName taskName)
    : TaskName_(ToString(taskName))
{ }

const TString& TTaskName::Get() const
{
    return TaskName_;
}

////////////////////////////////////////////////////////////////////////////////

TCommandRawJob::TCommandRawJob(TStringBuf command)
    : Command_(command)
{ }

const TString& TCommandRawJob::GetCommand() const
{
    return Command_;
}

void TCommandRawJob::Do(const TRawJobContext& /* jobContext */)
{
    Y_ABORT("TCommandRawJob::Do must not be called");
}

REGISTER_NAMED_RAW_JOB("NYT::TCommandRawJob", TCommandRawJob)

////////////////////////////////////////////////////////////////////////////////

TCommandVanillaJob::TCommandVanillaJob(TStringBuf command)
    : Command_(command)
{ }

const TString& TCommandVanillaJob::GetCommand() const
{
    return Command_;
}

void TCommandVanillaJob::Do()
{
    Y_ABORT("TCommandVanillaJob::Do must not be called");
}

REGISTER_NAMED_VANILLA_JOB("NYT::TCommandVanillaJob", TCommandVanillaJob);

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TUnspecifiedTableStructure&, const TUnspecifiedTableStructure&)
{
    return true;
}

bool operator==(const TProtobufTableStructure& lhs, const TProtobufTableStructure& rhs)
{
    return lhs.Descriptor == rhs.Descriptor;
}

////////////////////////////////////////////////////////////////////////////////

const TVector<TStructuredTablePath>& TOperationInputSpecBase::GetStructuredInputs() const
{
    return StructuredInputs_;
}

const TVector<TStructuredTablePath>& TOperationOutputSpecBase::GetStructuredOutputs() const
{
    return StructuredOutputs_;
}

void TOperationInputSpecBase::AddStructuredInput(TStructuredTablePath path)
{
    Inputs_.push_back(path.RichYPath);
    StructuredInputs_.push_back(std::move(path));
}

void TOperationOutputSpecBase::AddStructuredOutput(TStructuredTablePath path)
{
    Outputs_.push_back(path.RichYPath);
    StructuredOutputs_.push_back(std::move(path));
}

////////////////////////////////////////////////////////////////////////////////

TVanillaTask& TVanillaTask::AddStructuredOutput(TStructuredTablePath path)
{
    TOperationOutputSpecBase::AddStructuredOutput(std::move(path));
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TStructuredRowStreamDescription IVanillaJob<void>::GetInputRowStreamDescription() const
{
    return TVoidStructuredRowStream();
}

TStructuredRowStreamDescription IVanillaJob<void>::GetOutputRowStreamDescription() const
{
    return TVoidStructuredRowStream();
}

////////////////////////////////////////////////////////////////////////////////

TRawJobContext::TRawJobContext(size_t outputTableCount)
    : InputFile_(Duplicate(0))
{
    for (size_t i = 0; i != outputTableCount; ++i) {
        OutputFileList_.emplace_back(Duplicate(3 * i + GetJobFirstOutputTableFD()));
    }
}

const TFile& TRawJobContext::GetInputFile() const
{
    return InputFile_;
}

const TVector<TFile>& TRawJobContext::GetOutputFileList() const
{
    return OutputFileList_;
}

////////////////////////////////////////////////////////////////////////////////

TUserJobSpec& TUserJobSpec::AddLocalFile(
    const TLocalFilePath& path,
    const TAddLocalFileOptions& options)
{
    LocalFiles_.emplace_back(path, options);
    return *this;
}

TUserJobSpec& TUserJobSpec::JobBinaryLocalPath(TString path, TMaybe<TString> md5)
{
    JobBinary_ = TJobBinaryLocalPath{path, md5};
    return *this;
}

TUserJobSpec& TUserJobSpec::JobBinaryCypressPath(TString path, TMaybe<TTransactionId> transactionId)
{
    JobBinary_ = TJobBinaryCypressPath{path, transactionId};
    return *this;
}

const TJobBinaryConfig& TUserJobSpec::GetJobBinary() const
{
    return JobBinary_;
}

TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> TUserJobSpec::GetLocalFiles() const
{
    return LocalFiles_;
}

////////////////////////////////////////////////////////////////////////////////

TJobOperationPreparer::TInputGroup::TInputGroup(TJobOperationPreparer& preparer, TVector<int> indices)
    : Preparer_(preparer)
    , Indices_(std::move(indices))
{ }

TJobOperationPreparer::TInputGroup& TJobOperationPreparer::TInputGroup::ColumnRenaming(const THashMap<TString, TString>& renaming)
{
    for (auto i : Indices_) {
        Preparer_.InputColumnRenaming(i, renaming);
    }
    return *this;
}

TJobOperationPreparer::TInputGroup& TJobOperationPreparer::TInputGroup::ColumnFilter(const TVector<TString>& columns)
{
    for (auto i : Indices_) {
        Preparer_.InputColumnFilter(i, columns);
    }
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::TInputGroup::EndInputGroup()
{
    return Preparer_;
}

TJobOperationPreparer::TOutputGroup::TOutputGroup(TJobOperationPreparer& preparer, TVector<int> indices)
    : Preparer_(preparer)
    , Indices_(std::move(indices))
{ }

TJobOperationPreparer::TOutputGroup& TJobOperationPreparer::TOutputGroup::Schema(const TTableSchema &schema)
{
    for (auto i : Indices_) {
        Preparer_.OutputSchema(i, schema);
    }
    return *this;
}

TJobOperationPreparer::TOutputGroup& TJobOperationPreparer::TOutputGroup::NoSchema()
{
    for (auto i : Indices_) {
        Preparer_.NoOutputSchema(i);
    }
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::TOutputGroup::EndOutputGroup()
{
    return Preparer_;
}

////////////////////////////////////////////////////////////////////////////////

TJobOperationPreparer::TJobOperationPreparer(const IOperationPreparationContext& context)
    : Context_(context)
    , OutputSchemas_(context.GetOutputCount())
    , InputColumnRenamings_(context.GetInputCount())
    , InputColumnFilters_(context.GetInputCount())
    , InputTableDescriptions_(context.GetInputCount())
    , OutputTableDescriptions_(context.GetOutputCount())
{ }

TJobOperationPreparer::TInputGroup TJobOperationPreparer::BeginInputGroup(int begin, int end)
{
    Y_ENSURE_EX(begin <= end, TApiUsageError()
        << "BeginInputGroup(): begin must not exceed end, got " << begin << ", " << end);
    TVector<int> indices;
    for (int i = begin; i < end; ++i) {
        ValidateInputTableIndex(i, TStringBuf("BeginInputGroup()"));
        indices.push_back(i);
    }
    return TInputGroup(*this, std::move(indices));
}


TJobOperationPreparer::TOutputGroup TJobOperationPreparer::BeginOutputGroup(int begin, int end)
{
    Y_ENSURE_EX(begin <= end, TApiUsageError()
        << "BeginOutputGroup(): begin must not exceed end, got " << begin << ", " << end);
    TVector<int> indices;
    for (int i = begin; i < end; ++i) {
        ValidateOutputTableIndex(i, TStringBuf("BeginOutputGroup()"));
        indices.push_back(i);
    }
    return TOutputGroup(*this, std::move(indices));
}

TJobOperationPreparer& TJobOperationPreparer::NodeOutput(int tableIndex)
{
    ValidateMissingOutputDescription(tableIndex);
    OutputTableDescriptions_[tableIndex] = StructuredTableDescription<TNode>();
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::OutputSchema(int tableIndex, TTableSchema schema)
{
    ValidateMissingOutputSchema(tableIndex);
    OutputSchemas_[tableIndex] = std::move(schema);
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::NoOutputSchema(int tableIndex)
{
    ValidateMissingOutputSchema(tableIndex);
    OutputSchemas_[tableIndex] = EmptyNonstrictSchema();
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::InputColumnRenaming(
    int tableIndex,
    const THashMap<TString,TString>& renaming)
{
    ValidateInputTableIndex(tableIndex, TStringBuf("InputColumnRenaming()"));
    InputColumnRenamings_[tableIndex] = renaming;
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::InputColumnFilter(int tableIndex, const TVector<TString>& columns)
{
    ValidateInputTableIndex(tableIndex, TStringBuf("InputColumnFilter()"));
    InputColumnFilters_[tableIndex] = columns;
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::FormatHints(TUserJobFormatHints newFormatHints)
{
    FormatHints_ = newFormatHints;
    return *this;
}

void TJobOperationPreparer::Finish()
{
    FinallyValidate();
}

TVector<TTableSchema> TJobOperationPreparer::GetOutputSchemas()
{
    TVector<TTableSchema> result;
    result.reserve(OutputSchemas_.size());
    for (auto& schema : OutputSchemas_) {
        Y_ABORT_UNLESS(schema.Defined());
        result.push_back(std::move(*schema));
        schema.Clear();
    }
    return result;
}

void TJobOperationPreparer::FinallyValidate() const
{
    TVector<int> illegallyMissingSchemaIndices;
    for (int i = 0; i < static_cast<int>(OutputSchemas_.size()); ++i) {
        if (!OutputSchemas_[i]) {
            illegallyMissingSchemaIndices.push_back(i);
        }
    }
    if (illegallyMissingSchemaIndices.empty()) {
        return;
    }
    TApiUsageError error;
    error << "Output table schemas are missing: ";
    for (auto i : illegallyMissingSchemaIndices) {
        error << "no. " << i << " (" << Context_.GetOutputPath(i).GetOrElse("<unknown path>") << "); ";
    }
    ythrow std::move(error);
}

////////////////////////////////////////////////////////////////////////////////

void TJobOperationPreparer::ValidateInputTableIndex(int tableIndex, TStringBuf message) const
{
    Y_ENSURE_EX(
        0 <= tableIndex && tableIndex < static_cast<int>(Context_.GetInputCount()),
        TApiUsageError() <<
        message << ": input table index " << tableIndex << " us out of range [0;" <<
        OutputSchemas_.size() << ")");
}

void TJobOperationPreparer::ValidateOutputTableIndex(int tableIndex, TStringBuf message) const
{
    Y_ENSURE_EX(
        0 <= tableIndex && tableIndex < static_cast<int>(Context_.GetOutputCount()),
        TApiUsageError() <<
        message << ": output table index " << tableIndex << " us out of range [0;" <<
        OutputSchemas_.size() << ")");
}

void TJobOperationPreparer::ValidateMissingOutputSchema(int tableIndex) const
{
    ValidateOutputTableIndex(tableIndex, "ValidateMissingOutputSchema()");
    Y_ENSURE_EX(!OutputSchemas_[tableIndex],
        TApiUsageError() <<
        "Output table schema no. " << tableIndex << " " <<
        "(" << Context_.GetOutputPath(tableIndex).GetOrElse("<unknown path>") << ") " <<
        "is already set");
}

void TJobOperationPreparer::ValidateMissingInputDescription(int tableIndex) const
{
    ValidateInputTableIndex(tableIndex, "ValidateMissingInputDescription()");
    Y_ENSURE_EX(!InputTableDescriptions_[tableIndex],
        TApiUsageError() <<
        "Description for input no. " << tableIndex << " " <<
        "(" << Context_.GetOutputPath(tableIndex).GetOrElse("<unknown path>") << ") " <<
        "is already set");
}

void TJobOperationPreparer::ValidateMissingOutputDescription(int tableIndex) const
{
    ValidateOutputTableIndex(tableIndex, "ValidateMissingOutputDescription()");
    Y_ENSURE_EX(!OutputTableDescriptions_[tableIndex],
        TApiUsageError() <<
        "Description for output no. " << tableIndex << " " <<
        "(" << Context_.GetOutputPath(tableIndex).GetOrElse("<unknown path>") << ") " <<
        "is already set");
}

TTableSchema TJobOperationPreparer::EmptyNonstrictSchema() {
    return TTableSchema().Strict(false);
}

////////////////////////////////////////////////////////////////////////////////

const TVector<THashMap<TString, TString>>& TJobOperationPreparer::GetInputColumnRenamings() const
{
    return InputColumnRenamings_;
}

const TVector<TMaybe<TVector<TString>>>& TJobOperationPreparer::GetInputColumnFilters() const
{
    return InputColumnFilters_;
}

const TVector<TMaybe<TTableStructure>>& TJobOperationPreparer::GetInputDescriptions() const
{
    return InputTableDescriptions_;
}

const TVector<TMaybe<TTableStructure>>& TJobOperationPreparer::GetOutputDescriptions() const
{
    return OutputTableDescriptions_;
}

const TUserJobFormatHints& TJobOperationPreparer::GetFormatHints() const
{
    return FormatHints_;
}

TJobOperationPreparer& TJobOperationPreparer::InputFormatHints(TFormatHints hints)
{
    FormatHints_.InputFormatHints(hints);
    return *this;
}

TJobOperationPreparer& TJobOperationPreparer::OutputFormatHints(TFormatHints hints)
{
    FormatHints_.OutputFormatHints(hints);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void IJob::PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& resultBuilder) const
{
    for (int i = 0; i < context.GetOutputCount(); ++i) {
        resultBuilder.NoOutputSchema(i);
    }
}

////////////////////////////////////////////////////////////////////////////////

IOperationPtr IOperationClient::Map(
    const TMapOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    const TOperationOptions& options)
{
    Y_ABORT_UNLESS(mapper.Get());

    return DoMap(
        spec,
        std::move(mapper),
        options);
}

IOperationPtr IOperationClient::Map(
    ::TIntrusivePtr<IMapperBase> mapper,
    const TOneOrMany<TStructuredTablePath>& input,
    const TOneOrMany<TStructuredTablePath>& output,
    const TMapOperationSpec& spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TMapOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Outputs_.empty(),
        TApiUsageError() << "TMapOperationSpec::Outputs MUST be empty");

    auto mapSpec = spec;
    for (const auto& inputPath : input.Parts_) {
        mapSpec.AddStructuredInput(inputPath);
    }
    for (const auto& outputPath : output.Parts_) {
        mapSpec.AddStructuredOutput(outputPath);
    }
    return Map(mapSpec, std::move(mapper), options);
}

IOperationPtr IOperationClient::Reduce(
    const TReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_ABORT_UNLESS(reducer.Get());

    return DoReduce(
        spec,
        std::move(reducer),
        options);
}

IOperationPtr IOperationClient::Reduce(
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOneOrMany<TStructuredTablePath>& input,
    const TOneOrMany<TStructuredTablePath>& output,
    const TSortColumns& reduceBy,
    const TReduceOperationSpec& spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TReduceOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Outputs_.empty(),
        TApiUsageError() << "TReduceOperationSpec::Outputs MUST be empty");
    Y_ENSURE_EX(spec.ReduceBy_.Parts_.empty(),
        TApiUsageError() << "TReduceOperationSpec::ReduceBy MUST be empty");

    auto reduceSpec = spec;
    for (const auto& inputPath : input.Parts_) {
        reduceSpec.AddStructuredInput(inputPath);
    }
    for (const auto& outputPath : output.Parts_) {
        reduceSpec.AddStructuredOutput(outputPath);
    }
    reduceSpec.ReduceBy(reduceBy);
    return Reduce(reduceSpec, std::move(reducer), options);
}

IOperationPtr IOperationClient::JoinReduce(
    const TJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_ABORT_UNLESS(reducer.Get());

    return DoJoinReduce(
        spec,
        std::move(reducer),
        options);
}

IOperationPtr IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_ABORT_UNLESS(reducer.Get());

    return DoMapReduce(
        spec,
        std::move(mapper),
        nullptr,
        std::move(reducer),
        options);
}

IOperationPtr IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    ::TIntrusivePtr<IReducerBase> reduceCombiner,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_ABORT_UNLESS(reducer.Get());

    return DoMapReduce(
        spec,
        std::move(mapper),
        std::move(reduceCombiner),
        std::move(reducer),
        options);
}

IOperationPtr IOperationClient::MapReduce(
    ::TIntrusivePtr<IMapperBase> mapper,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOneOrMany<TStructuredTablePath>& input,
    const TOneOrMany<TStructuredTablePath>& output,
    const TSortColumns& reduceBy,
    TMapReduceOperationSpec spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TMapReduceOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Outputs_.empty(),
        TApiUsageError() << "TMapReduceOperationSpec::Outputs MUST be empty");
    Y_ENSURE_EX(spec.ReduceBy_.Parts_.empty(),
        TApiUsageError() << "TMapReduceOperationSpec::ReduceBy MUST be empty");

    for (const auto& inputPath : input.Parts_) {
        spec.AddStructuredInput(inputPath);
    }
    for (const auto& outputPath : output.Parts_) {
        spec.AddStructuredOutput(outputPath);
    }
    spec.ReduceBy(reduceBy);
    return MapReduce(spec, std::move(mapper), std::move(reducer), options);
}

IOperationPtr IOperationClient::MapReduce(
    ::TIntrusivePtr<IMapperBase> mapper,
    ::TIntrusivePtr<IReducerBase> reduceCombiner,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOneOrMany<TStructuredTablePath>& input,
    const TOneOrMany<TStructuredTablePath>& output,
    const TSortColumns& reduceBy,
    TMapReduceOperationSpec spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TMapReduceOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Outputs_.empty(),
        TApiUsageError() << "TMapReduceOperationSpec::Outputs MUST be empty");
    Y_ENSURE_EX(spec.ReduceBy_.Parts_.empty(),
        TApiUsageError() << "TMapReduceOperationSpec::ReduceBy MUST be empty");

    for (const auto& inputPath : input.Parts_) {
        spec.AddStructuredInput(inputPath);
    }
    for (const auto& outputPath : output.Parts_) {
        spec.AddStructuredOutput(outputPath);
    }
    spec.ReduceBy(reduceBy);
    return MapReduce(spec, std::move(mapper), std::move(reduceCombiner), std::move(reducer), options);
}

IOperationPtr IOperationClient::Sort(
    const TOneOrMany<TRichYPath>& input,
    const TRichYPath& output,
    const TSortColumns& sortBy,
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TSortOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Output_.Path_.empty(),
        TApiUsageError() << "TSortOperationSpec::Output MUST be empty");
    Y_ENSURE_EX(spec.SortBy_.Parts_.empty(),
        TApiUsageError() << "TSortOperationSpec::SortBy MUST be empty");

    auto sortSpec = spec;
    for (const auto& inputPath : input.Parts_) {
        sortSpec.AddInput(inputPath);
    }
    sortSpec.Output(output);
    sortSpec.SortBy(sortBy);
    return Sort(sortSpec, options);
}

////////////////////////////////////////////////////////////////////////////////

TRawTableReaderPtr IStructuredJob::CreateCustomRawJobReader(int) const
{
    return nullptr;
}

THolder<IProxyOutput> IStructuredJob::CreateCustomRawJobWriter(size_t) const
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
