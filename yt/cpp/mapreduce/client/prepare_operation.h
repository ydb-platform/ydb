#pragma once

#include "structured_table_formats.h"

#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TOperationPreparationContext
    : public IOperationPreparationContext
{
public:
    TOperationPreparationContext(
        const TStructuredJobTableList& structuredInputs,
        const TStructuredJobTableList& structuredOutputs,
        const TClientContext& context,
        const IClientRetryPolicyPtr& retryPolicy,
        TTransactionId transactionId);

    TOperationPreparationContext(
        TVector<TRichYPath> inputs,
        TVector<TRichYPath> outputs,
        const TClientContext& context,
        const IClientRetryPolicyPtr& retryPolicy,
        TTransactionId transactionId);

    int GetInputCount() const override;
    int GetOutputCount() const override;

    const TVector<TTableSchema>& GetInputSchemas() const override;
    const TTableSchema& GetInputSchema(int index) const override;

    TMaybe<TYPath> GetInputPath(int index) const override;
    TMaybe<TYPath> GetOutputPath(int index) const override;

private:
    TVector<TMaybe<TRichYPath>> Inputs_;
    TVector<TMaybe<TRichYPath>> Outputs_;
    const TClientContext& Context_;
    const IClientRetryPolicyPtr RetryPolicy_;
    TTransactionId TransactionId_;

    mutable TVector<TTableSchema> InputSchemas_;
    mutable TVector<bool> InputSchemasLoaded_;
};

////////////////////////////////////////////////////////////////////////////////

class TSpeculativeOperationPreparationContext
    : public IOperationPreparationContext
{
public:
    TSpeculativeOperationPreparationContext(
        const TVector<TTableSchema>& previousResult,
        TStructuredJobTableList inputs,
        TStructuredJobTableList outputs);

    int GetInputCount() const override;
    int GetOutputCount() const override;

    const TVector<TTableSchema>& GetInputSchemas() const override;
    const TTableSchema& GetInputSchema(int index) const override;

    TMaybe<TYPath> GetInputPath(int index) const override;
    TMaybe<TYPath> GetOutputPath(int index) const override;

private:
    TVector<TTableSchema> InputSchemas_;
    TStructuredJobTableList Inputs_;
    TStructuredJobTableList Outputs_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TTables>
TVector<TTableSchema> PrepareOperation(
    const IJob& job,
    const IOperationPreparationContext& context,
    TTables* inputsPtr,
    TTables* outputsPtr,
    TUserJobFormatHints& hints);

////////////////////////////////////////////////////////////////////////////////

TJobOperationPreparer GetOperationPreparer(
    const IJob& job,
    const IOperationPreparationContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
