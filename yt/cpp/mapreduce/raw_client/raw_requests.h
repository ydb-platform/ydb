#pragma once

#include "raw_batch_request.h"

#include <yt/cpp/mapreduce/common/fwd.h>
#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRequestRetryPolicy;
struct TClientContext;
struct TExecuteBatchOptions;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail::NRawClient {

////////////////////////////////////////////////////////////////////////////////

TOperationAttributes ParseOperationAttributes(const TNode& node);

TCheckPermissionResponse ParseCheckPermissionResponse(const TNode& node);

////////////////////////////////////////////////////////////////////////////////

//
// marks `batchRequest' as executed
void ExecuteBatch(
    IRequestRetryPolicyPtr retryPolicy,
    const TClientContext& context,
    TRawBatchRequest& batchRequest,
    const TExecuteBatchOptions& options = TExecuteBatchOptions());

//
// Cypress
//

TNode Get(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options = TGetOptions());

TNode TryGet(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options);

void Set(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options = TSetOptions());

void MultisetAttributes(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode::TMapType& value,
    const TMultisetAttributesOptions& options = TMultisetAttributesOptions());

bool Exists(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TExistsOptions& options = TExistsOptions());

TNodeId Create(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options = TCreateOptions());

TNodeId CopyWithoutRetries(
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options = TCopyOptions());

TNodeId CopyInsideMasterCell(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options = TCopyOptions());

TNodeId MoveWithoutRetries(
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options = TMoveOptions());

TNodeId MoveInsideMasterCell(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options = TMoveOptions());

void Remove(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options = TRemoveOptions());

TNode::TListType List(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options = TListOptions());

TNodeId Link(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options = TLinkOptions());

TLockId Lock(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options = TLockOptions());

void Unlock(
    IRequestRetryPolicyPtr retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options = TUnlockOptions());

void Concatenate(
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options = TConcatenateOptions());

//
// Transactions
//

void PingTx(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId);

//
// Operations
//

TOperationAttributes GetOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TGetOperationOptions& options = TGetOperationOptions());

TOperationAttributes GetOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TString& operationId,
    const TGetOperationOptions& options = TGetOperationOptions());

void AbortOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId);

void CompleteOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId);

void SuspendOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TSuspendOperationOptions& options = TSuspendOperationOptions());

void ResumeOperation(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TResumeOperationOptions& options = TResumeOperationOptions());

TListOperationsResult ListOperations(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TListOperationsOptions& options = TListOperationsOptions());

void UpdateOperationParameters(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options = TUpdateOperationParametersOptions());

//
// Jobs
//

TJobAttributes GetJob(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options = TGetJobOptions());

TListJobsResult ListJobs(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TListJobsOptions& options = TListJobsOptions());

::TIntrusivePtr<IFileReader> GetJobInput(
    const TClientContext& context,
    const TJobId& jobId,
    const TGetJobInputOptions& options = TGetJobInputOptions());

::TIntrusivePtr<IFileReader> GetJobFailContext(
    const TClientContext& context,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& options = TGetJobFailContextOptions());

TString GetJobStderrWithRetries(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& /* options */ = TGetJobStderrOptions());

::TIntrusivePtr<IFileReader> GetJobStderr(
    const TClientContext& context,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& options = TGetJobStderrOptions());

//
// File cache
//

TMaybe<TYPath> GetFileFromCache(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options = TGetFileFromCacheOptions());

TYPath PutFileToCache(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options = TPutFileToCacheOptions());

//
// SkyShare
//

TNode::TListType SkyShareTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const std::vector<TYPath>& tablePaths,
    const TSkyShareTableOptions& options);

//
// Misc
//

TCheckPermissionResponse CheckPermission(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options = TCheckPermissionOptions());

TVector<TTabletInfo> GetTabletInfos(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TYPath& path,
    const TVector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options);

TVector<TTableColumnarStatistics> GetTableColumnarStatistics(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options);

TMultiTablePartitions GetTablePartitions(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options);

TRichYPath CanonizeYPath(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TRichYPath& path);

TVector<TRichYPath> CanonizeYPaths(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TVector<TRichYPath>& paths);

//
// Tables
//

void AlterTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options);

void AlterTableReplica(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options);

void DeleteRows(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options);

void FreezeTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TYPath& path,
    const TFreezeTableOptions& options);

void UnfreezeTable(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TYPath& path,
    const TUnfreezeTableOptions& options);


// Transactions
void AbortTransaction(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId);

void CommitTransaction(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId);

TTransactionId StartTransaction(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& parentId,
    const TStartTransactionOptions& options);

////////////////////////////////////////////////////////////////////////////////

template<typename TSrc, typename TBatchAdder>
auto BatchTransform(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TSrc& src,
    TBatchAdder batchAdder,
    const TExecuteBatchOptions& executeBatchOptions = {})
{
    TRawBatchRequest batch(context.Config);
    using TFuture = decltype(batchAdder(batch, *std::begin(src)));
    TVector<TFuture> futures;
    for (const auto& el : src) {
        futures.push_back(batchAdder(batch, el));
    }
    ExecuteBatch(retryPolicy, context, batch, executeBatchOptions);
    using TDst = decltype(futures[0].ExtractValueSync());
    TVector<TDst> result;
    result.reserve(std::size(src));
    for (auto& future : futures) {
        result.push_back(future.ExtractValueSync());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail::NRawClient
} // namespace NYT
