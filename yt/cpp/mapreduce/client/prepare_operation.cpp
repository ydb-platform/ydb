#include "prepare_operation.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>
#include <yt/cpp/mapreduce/raw_client/raw_batch_request.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TOperationPreparationContext::TOperationPreparationContext(
    const TStructuredJobTableList& structuredInputs,
    const TStructuredJobTableList& structuredOutputs,
    const TClientContext& context,
    const IClientRetryPolicyPtr& retryPolicy,
    TTransactionId transactionId)
    : Context_(context)
    , RetryPolicy_(retryPolicy)
    , TransactionId_(transactionId)
    , InputSchemas_(structuredInputs.size())
    , InputSchemasLoaded_(structuredInputs.size(), false)
{
    Inputs_.reserve(structuredInputs.size());
    for (const auto& input : structuredInputs) {
        Inputs_.push_back(input.RichYPath);
    }
    Outputs_.reserve(structuredOutputs.size());
    for (const auto& output : structuredOutputs) {
        Outputs_.push_back(output.RichYPath);
    }
}

TOperationPreparationContext::TOperationPreparationContext(
    TVector<TRichYPath> inputs,
    TVector<TRichYPath> outputs,
    const TClientContext& context,
    const IClientRetryPolicyPtr& retryPolicy,
    TTransactionId transactionId)
    : Context_(context)
    , RetryPolicy_(retryPolicy)
    , TransactionId_(transactionId)
    , InputSchemas_(inputs.size())
    , InputSchemasLoaded_(inputs.size(), false)
{
    Inputs_.reserve(inputs.size());
    for (auto& input : inputs) {
        Inputs_.push_back(std::move(input));
    }
    Outputs_.reserve(outputs.size());
    for (const auto& output : outputs) {
        Outputs_.push_back(std::move(output));
    }
}

int TOperationPreparationContext::GetInputCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TOperationPreparationContext::GetOutputCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TVector<TTableSchema>& TOperationPreparationContext::GetInputSchemas() const
{
    TVector<::NThreading::TFuture<TNode>> schemaFutures;
    NRawClient::TRawBatchRequest batch(Context_.Config);
    for (int tableIndex = 0; tableIndex < static_cast<int>(InputSchemas_.size()); ++tableIndex) {
        if (InputSchemasLoaded_[tableIndex]) {
            schemaFutures.emplace_back();
            continue;
        }
        Y_ABORT_UNLESS(Inputs_[tableIndex]);
        schemaFutures.push_back(batch.Get(TransactionId_, Inputs_[tableIndex]->Path_ + "/@schema", TGetOptions{}));
    }

    NRawClient::ExecuteBatch(
        RetryPolicy_->CreatePolicyForGenericRequest(),
        Context_,
        batch);

    for (int tableIndex = 0; tableIndex < static_cast<int>(InputSchemas_.size()); ++tableIndex) {
        if (schemaFutures[tableIndex].Initialized()) {
            Deserialize(InputSchemas_[tableIndex], schemaFutures[tableIndex].ExtractValueSync());
        }
    }

    return InputSchemas_;
}

const TTableSchema& TOperationPreparationContext::GetInputSchema(int index) const
{
    auto& schema = InputSchemas_[index];
    if (!InputSchemasLoaded_[index]) {
        Y_ABORT_UNLESS(Inputs_[index]);
        auto schemaNode = NRawClient::Get(
            RetryPolicy_->CreatePolicyForGenericRequest(),
            Context_,
            TransactionId_,
            Inputs_[index]->Path_ + "/@schema");
        Deserialize(schema, schemaNode);
    }
    return schema;
}

TMaybe<TYPath> TOperationPreparationContext::GetInputPath(int index) const
{
    Y_ABORT_UNLESS(index < static_cast<int>(Inputs_.size()));
    if (Inputs_[index]) {
        return Inputs_[index]->Path_;
    }
    return Nothing();
}

TMaybe<TYPath> TOperationPreparationContext::GetOutputPath(int index) const
{
    Y_ABORT_UNLESS(index < static_cast<int>(Outputs_.size()));
    if (Outputs_[index]) {
        return Outputs_[index]->Path_;
    }
    return Nothing();
}

////////////////////////////////////////////////////////////////////////////////

TSpeculativeOperationPreparationContext::TSpeculativeOperationPreparationContext(
    const TVector<TTableSchema>& previousResult,
    TStructuredJobTableList inputs,
    TStructuredJobTableList outputs)
    : InputSchemas_(previousResult)
    , Inputs_(std::move(inputs))
    , Outputs_(std::move(outputs))
{
    Y_ABORT_UNLESS(Inputs_.size() == previousResult.size());
}

int TSpeculativeOperationPreparationContext::GetInputCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TSpeculativeOperationPreparationContext::GetOutputCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TVector<TTableSchema>& TSpeculativeOperationPreparationContext::GetInputSchemas() const
{
    return InputSchemas_;
}

const TTableSchema& TSpeculativeOperationPreparationContext::GetInputSchema(int index) const
{
    Y_ABORT_UNLESS(index < static_cast<int>(InputSchemas_.size()));
    return InputSchemas_[index];
}

TMaybe<TYPath> TSpeculativeOperationPreparationContext::GetInputPath(int index) const
{
    Y_ABORT_UNLESS(index < static_cast<int>(Inputs_.size()));
    if (Inputs_[index].RichYPath) {
        return Inputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

TMaybe<TYPath> TSpeculativeOperationPreparationContext::GetOutputPath(int index) const
{
    Y_ABORT_UNLESS(index < static_cast<int>(Outputs_.size()));
    if (Outputs_[index].RichYPath) {
        return Outputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

////////////////////////////////////////////////////////////////////////////////

static void FixInputTable(TRichYPath& table, int index, const TJobOperationPreparer& preparer)
{
    const auto& columnRenamings = preparer.GetInputColumnRenamings();
    const auto& columnFilters = preparer.GetInputColumnFilters();

    if (!columnRenamings[index].empty()) {
        table.RenameColumns(columnRenamings[index]);
    }
    if (columnFilters[index]) {
        table.Columns(*columnFilters[index]);
    }
}

static void FixInputTable(TStructuredJobTable& table, int index, const TJobOperationPreparer& preparer)
{
    const auto& inputDescriptions = preparer.GetInputDescriptions();

    if (inputDescriptions[index] && std::holds_alternative<TUnspecifiedTableStructure>(table.Description)) {
        table.Description = *inputDescriptions[index];
    }
    if (table.RichYPath) {
        FixInputTable(*table.RichYPath, index, preparer);
    }
}

static void FixOutputTable(TRichYPath& /* table */, int /* index */, const TJobOperationPreparer& /* preparer */)
{ }

static void FixOutputTable(TStructuredJobTable& table, int index, const TJobOperationPreparer& preparer)
{
    const auto& outputDescriptions = preparer.GetOutputDescriptions();

    if (outputDescriptions[index] && std::holds_alternative<TUnspecifiedTableStructure>(table.Description)) {
        table.Description = *outputDescriptions[index];
    }
    if (table.RichYPath) {
        FixOutputTable(*table.RichYPath, index, preparer);
    }
}

template <typename TTables>
TVector<TTableSchema> PrepareOperation(
    const IJob& job,
    const IOperationPreparationContext& context,
    TTables* inputsPtr,
    TTables* outputsPtr,
    TUserJobFormatHints& hints)
{
    TJobOperationPreparer preparer(context);
    job.PrepareOperation(context, preparer);
    preparer.Finish();

    if (inputsPtr) {
        auto& inputs = *inputsPtr;
        for (int i = 0; i < static_cast<int>(inputs.size()); ++i) {
            FixInputTable(inputs[i], i, preparer);
        }
    }

    if (outputsPtr) {
        auto& outputs = *outputsPtr;
        for (int i = 0; i < static_cast<int>(outputs.size()); ++i) {
            FixOutputTable(outputs[i], i, preparer);
        }
    }

    auto applyPatch = [](TMaybe<TFormatHints>& origin, const TMaybe<TFormatHints>& patch) {
        if (origin) {
            if (patch) {
                origin->Merge(*patch);
            }
        } else {
            origin = patch;
        }
    };

    auto preparerHints = preparer.GetFormatHints();
    applyPatch(preparerHints.InputFormatHints_, hints.InputFormatHints_);
    applyPatch(preparerHints.OutputFormatHints_, hints.OutputFormatHints_);
    hints = std::move(preparerHints);

    return preparer.GetOutputSchemas();
}

template
TVector<TTableSchema> PrepareOperation<TStructuredJobTableList>(
    const IJob& job,
    const IOperationPreparationContext& context,
    TStructuredJobTableList* inputsPtr,
    TStructuredJobTableList* outputsPtr,
    TUserJobFormatHints& hints);

template
TVector<TTableSchema> PrepareOperation<TVector<TRichYPath>>(
    const IJob& job,
    const IOperationPreparationContext& context,
    TVector<TRichYPath>* inputsPtr,
    TVector<TRichYPath>* outputsPtr,
    TUserJobFormatHints& hints);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
