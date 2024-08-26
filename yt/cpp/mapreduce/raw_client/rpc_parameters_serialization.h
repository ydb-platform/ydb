#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>

namespace NYT::NDetail::NRawClient {

////////////////////////////////////////////////////////////////////////////////

TNode SerializeParamsForCreate(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options);

TNode SerializeParamsForRemove(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TRemoveOptions& options);

TNode SerializeParamsForExists(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TExistsOptions& options);

TNode SerializeParamsForGet(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TGetOptions& options);

TNode SerializeParamsForSet(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TSetOptions& options);

TNode SerializeParamsForMultisetAttributes(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TMultisetAttributesOptions& options);

TNode SerializeParamsForList(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TListOptions& options);

TNode SerializeParamsForCopy(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options);

TNode SerializeParamsForMove(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options);

TNode SerializeParamsForLink(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options);

TNode SerializeParamsForLock(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options);

TNode SerializeParamsForUnlock(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TUnlockOptions& options);

TNode SerializeParamsForConcatenate(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options);

TNode SerializeParamsForPingTx(
    const TTransactionId& transactionId);

TNode SerializeParamsForGetOperation(const std::variant<TString, TOperationId>& aliasOrOperationId, const TGetOperationOptions& options);

TNode SerializeParamsForAbortOperation(
    const TOperationId& operationId);

TNode SerializeParamsForCompleteOperation(
    const TOperationId& operationId);

TNode SerializeParamsForSuspendOperation(
    const TOperationId& operationId,
    const TSuspendOperationOptions& options);

TNode SerializeParamsForResumeOperation(
    const TOperationId& operationId,
    const TResumeOperationOptions& options);

TNode SerializeParamsForListOperations(
    const TListOperationsOptions& options);

TNode SerializeParamsForUpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options);

TNode SerializeParamsForGetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options);

TNode SerializeParamsForListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options);

TNode SerializeParametersForInsertRows(
    const TString& pathPrefix,
    const TYPath& path,
    const TInsertRowsOptions& options);

TNode SerializeParametersForDeleteRows(
    const TString& pathPrefix,
    const TYPath& path,
    const TDeleteRowsOptions& options);

TNode SerializeParametersForTrimRows(
    const TString& pathPrefix,
    const TYPath& path,
    const TTrimRowsOptions& options);

TNode SerializeParamsForParseYPath(
    const TRichYPath& path);

TNode SerializeParamsForEnableTableReplica(
    const TReplicaId& replicaId);

TNode SerializeParamsForDisableTableReplica(
    const TReplicaId& replicaId);

TNode SerializeParamsForAlterTableReplica(
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options);

TNode SerializeParamsForFreezeTable(
    const TString& pathPrefix,
    const TYPath& path,
    const TFreezeTableOptions& options);

TNode SerializeParamsForUnfreezeTable(
    const TString& pathPrefix,
    const TYPath& path,
    const TUnfreezeTableOptions& options);

TNode SerializeParamsForAlterTable(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& path,
    const TAlterTableOptions& options);

TNode SerializeParamsForGetTableColumnarStatistics(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options);

TNode SerializeParamsForGetTablePartitions(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options);

TNode SerializeParamsForGetFileFromCache(
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions&);

TNode SerializeParamsForPutFileToCache(
    const TTransactionId& transactionId,
    const TString& pathPrefix,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options);

TNode SerializeParamsForSkyShareTable(
    const TString& serverName,
    const TString& pathPrefix,
    const std::vector<TYPath>& tablePaths,
    const TSkyShareTableOptions& options);

TNode SerializeParamsForCheckPermission(
    const TString& user,
    EPermission permission,
    const TString& pathPrefix,
    const TYPath& path,
    const TCheckPermissionOptions& options);

TNode SerializeParamsForGetTabletInfos(
    const TString& pathPrefix,
    const TYPath& path,
    const TVector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options);

TNode SerializeParamsForAbortTransaction(
    const TTransactionId& transactionId);

TNode SerializeParamsForCommitTransaction(
    const TTransactionId& transactionId);

TNode SerializeParamsForStartTransaction(
    const TTransactionId& parentTransactionId,
    TDuration txTimeout,
    const TStartTransactionOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail::NRawClient
