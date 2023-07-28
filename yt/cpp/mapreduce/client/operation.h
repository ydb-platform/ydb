#pragma once

#include "fwd.h"
#include "structured_table_formats.h"
#include "operation_preparer.h"

#include <yt/cpp/mapreduce/http/fwd.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/retry_policy.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public IOperation
{
public:
    class TOperationImpl;

public:
    explicit TOperation(TClientPtr client);
    TOperation(TOperationId id, TClientPtr client);
    virtual const TOperationId& GetId() const override;
    virtual TString GetWebInterfaceUrl() const override;

    void OnPrepared();
    void SetDelayedStartFunction(std::function<TOperationId()> start);
    virtual void Start() override;
    void OnPreparationException(std::exception_ptr e);
    virtual bool IsStarted() const override;

    virtual TString GetStatus() const override;
    void OnStatusUpdated(const TString& newStatus);

    virtual ::NThreading::TFuture<void> GetPreparedFuture() override;
    virtual ::NThreading::TFuture<void> GetStartedFuture() override;
    virtual ::NThreading::TFuture<void> Watch() override;

    virtual TVector<TFailedJobInfo> GetFailedJobInfo(const TGetFailedJobInfoOptions& options = TGetFailedJobInfoOptions()) override;
    virtual EOperationBriefState GetBriefState() override;
    virtual TMaybe<TYtError> GetError() override;
    virtual TJobStatistics GetJobStatistics() override;
    virtual TMaybe<TOperationBriefProgress> GetBriefProgress() override;
    virtual void AbortOperation() override;
    virtual void CompleteOperation() override;
    virtual void SuspendOperation(const TSuspendOperationOptions& options) override;
    virtual void ResumeOperation(const TResumeOperationOptions& options) override;
    virtual TOperationAttributes GetAttributes(const TGetOperationOptions& options) override;
    virtual void UpdateParameters(const TUpdateOperationParametersOptions& options) override;
    virtual TJobAttributes GetJob(const TJobId& jobId, const TGetJobOptions& options) override;
    virtual TListJobsResult ListJobs(const TListJobsOptions& options) override;

private:
    TClientPtr Client_;
    ::TIntrusivePtr<TOperationImpl> Impl_;
};

using TOperationPtr = ::TIntrusivePtr<TOperation>;

////////////////////////////////////////////////////////////////////////////////

struct TSimpleOperationIo
{
    TVector<TRichYPath> Inputs;
    TVector<TRichYPath> Outputs;

    TFormat InputFormat;
    TFormat OutputFormat;

    TVector<TSmallJobFile> JobFiles;
};

TSimpleOperationIo CreateSimpleOperationIoHelper(
    const IStructuredJob& structuredJob,
    const TOperationPreparer& preparer,
    const TOperationOptions& options,
    TStructuredJobTableList structuredInputs,
    TStructuredJobTableList structuredOutputs,
    TUserJobFormatHints hints,
    ENodeReaderFormat nodeReaderFormat,
    const THashSet<TString>& columnsUsedInOperations);

////////////////////////////////////////////////////////////////////////////////

void ExecuteMap(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TMapOperationSpec& spec,
    const ::TIntrusivePtr<IStructuredJob>& mapper,
    const TOperationOptions& options);

void ExecuteRawMap(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawMapOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& mapper,
    const TOperationOptions& options);

void ExecuteReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TReduceOperationSpec& spec,
    const ::TIntrusivePtr<IStructuredJob>& reducer,
    const TOperationOptions& options);

void ExecuteRawReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawReduceOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& reducer,
    const TOperationOptions& options);

void ExecuteJoinReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TJoinReduceOperationSpec& spec,
    const ::TIntrusivePtr<IStructuredJob>& reducer,
    const TOperationOptions& options);

void ExecuteRawJoinReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawJoinReduceOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& reducer,
    const TOperationOptions& options);

void ExecuteMapReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TMapReduceOperationSpec& spec,
    const ::TIntrusivePtr<IStructuredJob>& mapper,
    const ::TIntrusivePtr<IStructuredJob>& reduceCombiner,
    const ::TIntrusivePtr<IStructuredJob>& reducer,
    const TOperationOptions& options);

void ExecuteRawMapReduce(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRawMapReduceOperationSpec& spec,
    const ::TIntrusivePtr<IRawJob>& mapper,
    const ::TIntrusivePtr<IRawJob>& reduceCombiner,
    const ::TIntrusivePtr<IRawJob>& reducer,
    const TOperationOptions& options);

void ExecuteSort(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TSortOperationSpec& spec,
    const TOperationOptions& options);

void ExecuteMerge(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TMergeOperationSpec& spec,
    const TOperationOptions& options);

void ExecuteErase(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TEraseOperationSpec& spec,
    const TOperationOptions& options);

void ExecuteRemoteCopy(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TRemoteCopyOperationSpec& spec,
    const TOperationOptions& options);

void ExecuteVanilla(
    const TOperationPtr& operation,
    const TOperationPreparerPtr& preparer,
    const TVanillaOperationSpec& spec,
    const TOperationOptions& options);

EOperationBriefState CheckOperation(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TClientContext& context,
    const TOperationId& operationId);

void WaitForOperation(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TClientContext& context,
    const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<TOperation> ProcessOperation(
    NYT::NDetail::TClientPtr client,
    std::function<void()> prepare,
    ::TIntrusivePtr<TOperation> operation,
    const TOperationOptions& options);

void WaitIfRequired(const TOperationPtr& operation, const TClientPtr& client, const TOperationOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
