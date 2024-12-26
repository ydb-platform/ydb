#pragma once

#include "client.h"
#include "client_method_options.h"
#include "operation.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NHttpClient {
    class IHttpResponse;
    using IHttpResponsePtr = std::unique_ptr<IHttpResponse>;
}

////////////////////////////////////////////////////////////////////////////////

class IRawClient
    : public virtual TThrRefBase
{
public:
    // Cypress

    virtual TNode Get(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) = 0;

    virtual TNode TryGet(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) = 0;

    virtual void Set(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) = 0;

    virtual bool Exists(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TExistsOptions& options = {}) = 0;

    virtual void MultisetAttributes(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode::TMapType& value,
        const TMultisetAttributesOptions& options = {}) = 0;

    virtual TNodeId Create(
        TMutationId& mutatatonId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const ENodeType& type,
        const TCreateOptions& options = {}) = 0;

    virtual TNodeId CopyWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) = 0;

    virtual TNodeId CopyInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) = 0;

    virtual TNodeId MoveWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) = 0;

    virtual TNodeId MoveInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) = 0;

    virtual void Remove(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TRemoveOptions& options = {}) = 0;

    virtual TNode::TListType List(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TListOptions& options = {}) = 0;

    virtual TNodeId Link(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = {}) = 0;

    virtual TLockId Lock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = {}) = 0;

    virtual void Unlock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TUnlockOptions& options = {}) = 0;

    virtual void Concatenate(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& sourcePaths,
        const TRichYPath& destinationPath,
        const TConcatenateOptions& options = {}) = 0;

    // Transactions

    virtual TTransactionId StartTransaction(
        TMutationId& mutationId,
        const TTransactionId& parentId,
        const TStartTransactionOptions& options = {}) = 0;

    virtual void PingTransaction(const TTransactionId& transactionId) = 0;

    virtual void AbortTransaction(
        TMutationId& mutationId,
        const TTransactionId& transactionId) = 0;

    virtual void CommitTransaction(
        TMutationId& mutationId,
        const TTransactionId& transactionId) = 0;

    // Operations

    virtual TOperationId StartOperation(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        EOperationType type,
        const TNode& spec) = 0;

    virtual TOperationAttributes GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options = {}) = 0;

    virtual TOperationAttributes GetOperation(
        const TString& operationId,
        const TGetOperationOptions& options = {}) = 0;

    virtual void AbortOperation(
        TMutationId& mutationId,
        const TOperationId& operationId) = 0;

    virtual void CompleteOperation(
        TMutationId& mutationId,
        const TOperationId& operationId) = 0;

    virtual void SuspendOperation(
        TMutationId& mutationId,
        const TOperationId& operationId,
        const TSuspendOperationOptions& options = {}) = 0;

    virtual void ResumeOperation(
        TMutationId& mutationId,
        const TOperationId& operationId,
        const TResumeOperationOptions& options = {}) = 0;

    virtual TListOperationsResult ListOperations(const TListOperationsOptions& options = {}) = 0;

    virtual void UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options = {}) = 0;

    virtual NYson::TYsonString GetJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options = {}) = 0;

    virtual TListJobsResult ListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options = {}) = 0;

    virtual IFileReaderPtr GetJobInput(
        const TJobId& jobId,
        const TGetJobInputOptions& options = {}) = 0;

    virtual IFileReaderPtr GetJobFailContext(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobFailContextOptions& options = {}) = 0;

    virtual TString GetJobStderrWithRetries(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options = {}) = 0;

    virtual IFileReaderPtr GetJobStderr(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options = {}) = 0;

    virtual std::vector<TJobTraceEvent> GetJobTrace(
        const TOperationId& operationId,
        const TGetJobTraceOptions& options = {}) = 0;

    // SkyShare

    virtual NHttpClient::IHttpResponsePtr SkyShareTable(
        const std::vector<TYPath>& tablePaths,
        const TSkyShareTableOptions& options = {}) = 0;

    // Files
    virtual std::unique_ptr<IInputStream> ReadFile(
        const TTransactionId& transactionId,
        const TRichYPath& path,
        const TFileReaderOptions& options = {}) = 0;

    // File cache

    virtual TMaybe<TYPath> GetFileFromCache(
        const TTransactionId& transactionId,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options = {}) = 0;

    virtual TYPath PutFileToCache(
        const TTransactionId& transactionId,
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options = {}) = 0;

    // Tables

    virtual void MountTable(
        TMutationId& mutationId,
        const TYPath& path,
        const TMountTableOptions& options = {}) = 0;

    virtual void UnmountTable(
        TMutationId& mutationId,
        const TYPath& path,
        const TUnmountTableOptions& options = {}) = 0;

    virtual void RemountTable(
        TMutationId& mutationId,
        const TYPath& path,
        const TRemountTableOptions& options = {}) = 0;

    virtual void ReshardTableByPivotKeys(
        TMutationId& mutationId,
        const TYPath& path,
        const TVector<TKey>& keys,
        const TReshardTableOptions& options = {}) = 0;

    virtual void ReshardTableByTabletCount(
        TMutationId& mutationId,
        const TYPath& path,
        i64 tabletCount,
        const TReshardTableOptions& options = {}) = 0;

    virtual void InsertRows(
        const TYPath& path,
        const TNode::TListType& rows,
        const TInsertRowsOptions& options = {}) = 0;

    virtual void TrimRows(
        const TYPath& path,
        i64 tabletIndex,
        i64 rowCount,
        const TTrimRowsOptions& options = {}) = 0;

    virtual TNode::TListType LookupRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TLookupRowsOptions& options = {}) = 0;

    virtual TNode::TListType SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) = 0;

    virtual void AlterTable(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TAlterTableOptions& options = {}) = 0;

    virtual std::unique_ptr<IInputStream> ReadTable(
        const TTransactionId& transactionId,
        const TRichYPath& path,
        const TMaybe<TFormat>& format,
        const TTableReaderOptions& options = {}) = 0;

    virtual std::unique_ptr<IInputStream> ReadBlobTable(
        const TTransactionId& transactionId,
        const TRichYPath& path,
        const TKey& key,
        const TBlobTableReaderOptions& options = {}) = 0;

    virtual void AlterTableReplica(
        TMutationId& mutationId,
        const TReplicaId& replicaId,
        const TAlterTableReplicaOptions& options = {}) = 0;

    virtual void DeleteRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TDeleteRowsOptions& options = {}) = 0;

    virtual void FreezeTable(
        const TYPath& path,
        const TFreezeTableOptions& options = {}) = 0;

    virtual void UnfreezeTable(
        const TYPath& path,
        const TUnfreezeTableOptions& options = {}) = 0;

    // Misc

    virtual TCheckPermissionResponse CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options = {}) = 0;

    virtual TVector<TTabletInfo> GetTabletInfos(
        const TYPath& path,
        const TVector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options = {}) = 0;

    virtual TVector<TTableColumnarStatistics> GetTableColumnarStatistics(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options = {}) = 0;

    virtual TMultiTablePartitions GetTablePartitions(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& paths,
        const TGetTablePartitionsOptions& options = {}) = 0;

    virtual ui64 GenerateTimestamp() = 0;

    virtual TAuthorizationInfo WhoAmI() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
