#pragma once

#include <yt/cpp/mapreduce/http/context.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class THttpRawClient
    : public IRawClient
{
public:
    THttpRawClient(const TClientContext& context);

    // Cypress

    TNode Get(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) override;

    TNode TryGet(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options) override;

    void Set(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) override;

    bool Exists(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TExistsOptions& options = {}) override;

    void MultisetAttributes(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode::TMapType& value,
        const TMultisetAttributesOptions& options = {}) override;

    TNodeId Create(
        TMutationId& mutatatonId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const ENodeType& type,
        const TCreateOptions& options = {}) override;

    TNodeId CopyWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) override;

    TNodeId CopyInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) override;

    TNodeId MoveWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) override;

    TNodeId MoveInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) override;

    void Remove(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TRemoveOptions& options = {}) override;

    TNode::TListType List(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TListOptions& options = {}) override;

    TNodeId Link(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = {}) override;

    TLockId Lock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = {}) override;

    void Unlock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TUnlockOptions& options = {}) override;

    void Concatenate(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& sourcePaths,
        const TRichYPath& destinationPath,
        const TConcatenateOptions& options = {}) override;

    // Transactions

    TTransactionId StartTransaction(
        TMutationId& mutationId,
        const TTransactionId& parentId,
        const TStartTransactionOptions& options = {}) override;

    void PingTransaction(const TTransactionId& transactionId) override;

    void AbortTransaction(
        TMutationId& mutationId,
        const TTransactionId& transactionId) override;

    void CommitTransaction(
        TMutationId& mutationId,
        const TTransactionId& transactionId) override;

    // Operations

    TOperationId StartOperation(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        EOperationType type,
        const TNode& spec) override;

    TOperationAttributes GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options = {}) override;

    TOperationAttributes GetOperation(
        const TString& operationId,
        const TGetOperationOptions& options = {}) override;

    void AbortOperation(
        TMutationId& mutationId,
        const TOperationId& operationId) override;

    void CompleteOperation(
        TMutationId& mutationId,
        const TOperationId& operationId) override;

    void SuspendOperation(
        TMutationId& mutationId,
        const TOperationId& operationId,
        const TSuspendOperationOptions& options = {}) override;

    void ResumeOperation(
        TMutationId& mutationId,
        const TOperationId& operationId,
        const TResumeOperationOptions& options = {}) override;

    TListOperationsResult ListOperations(const TListOperationsOptions& options = {}) override;

    void UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options = {}) override;

    // Jobs

    NYson::TYsonString GetJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options = {}) override;

    TListJobsResult ListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options = {}) override;

    IFileReaderPtr GetJobInput(
        const TJobId& jobId,
        const TGetJobInputOptions& options = {}) override;

    IFileReaderPtr GetJobFailContext(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobFailContextOptions& options = {}) override;

    TString GetJobStderrWithRetries(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options = {}) override;

    IFileReaderPtr GetJobStderr(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options = {}) override;

    std::vector<TJobTraceEvent> GetJobTrace(
        const TOperationId& operationId,
        const TGetJobTraceOptions& options = {}) override;

    // SkyShare

    NHttpClient::IHttpResponsePtr SkyShareTable(
        const std::vector<TYPath>& tablePaths,
        const TSkyShareTableOptions& options = {}) override;

    // Files
    std::unique_ptr<IInputStream> ReadFile(
        const TTransactionId& transactionId,
        const TRichYPath& path,
        const TFileReaderOptions& options = {}) override;

    // File cache

    TMaybe<TYPath> GetFileFromCache(
        const TTransactionId& transactionId,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options = {}) override;

    TYPath PutFileToCache(
        const TTransactionId& transactionId,
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options = {}) override;

    // Tables

    void MountTable(
        TMutationId& mutationId,
        const TYPath& path,
        const TMountTableOptions& options = {}) override;

    void UnmountTable(
        TMutationId& mutationId,
        const TYPath& path,
        const TUnmountTableOptions& options = {}) override;

    void RemountTable(
        TMutationId& mutationId,
        const TYPath& path,
        const TRemountTableOptions& options = {}) override;

    void ReshardTableByPivotKeys(
        TMutationId& mutationId,
        const TYPath& path,
        const TVector<TKey>& keys,
        const TReshardTableOptions& options = {}) override;

    void ReshardTableByTabletCount(
        TMutationId& mutationId,
        const TYPath& path,
        i64 tabletCount,
        const TReshardTableOptions& options = {}) override;

    void InsertRows(
        const TYPath& path,
        const TNode::TListType& rows,
        const TInsertRowsOptions& options = {}) override;

    void TrimRows(
        const TYPath& path,
        i64 tabletIndex,
        i64 rowCount,
        const TTrimRowsOptions& options = {}) override;

    TNode::TListType LookupRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TLookupRowsOptions& options = {}) override;

    TNode::TListType SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) override;

    std::unique_ptr<IInputStream> ReadTable(
        const TTransactionId& transactionId,
        const TRichYPath& path,
        const TMaybe<TFormat>& format,
        const TTableReaderOptions& options = {}) override;

    std::unique_ptr<IInputStream> ReadBlobTable(
        const TTransactionId& transactionId,
        const TRichYPath& path,
        const TKey& key,
        const TBlobTableReaderOptions& options = {}) override;

    void AlterTable(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TAlterTableOptions& options = {}) override;

    void AlterTableReplica(
        TMutationId& mutationId,
        const TReplicaId& replicaId,
        const TAlterTableReplicaOptions& options = {}) override;

    void DeleteRows(
        const TYPath& path,
        const TNode::TListType& keys,
        const TDeleteRowsOptions& options = {}) override;

    void FreezeTable(
        const TYPath& path,
        const TFreezeTableOptions& options = {}) override;

    void UnfreezeTable(
        const TYPath& path,
        const TUnfreezeTableOptions& options = {}) override;

    // Misc

    TCheckPermissionResponse CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options = {}) override;

    TVector<TTabletInfo> GetTabletInfos(
        const TYPath& path,
        const TVector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options = {}) override;

    TVector<TTableColumnarStatistics> GetTableColumnarStatistics(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options = {}) override;

    TMultiTablePartitions GetTablePartitions(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& paths,
        const TGetTablePartitionsOptions& options = {}) override;

    ui64 GenerateTimestamp() override;

    TAuthorizationInfo WhoAmI() override;

private:
    const TClientContext Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
