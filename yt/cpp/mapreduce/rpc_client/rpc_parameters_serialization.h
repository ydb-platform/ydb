#pragma once

#include <yt/cpp/mapreduce/interface/client_method_options.h>

#include <yt/yt/client/api/cypress_client.h>
#include <yt/yt/client/api/transaction.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

NYPath::TRichYPath ToApiRichPath(const TRichYPath& path);

TGuid YtGuidFromUtilGuid(TGUID guid);

TGUID UtilGuidFromYtGuid(TGuid guid);

NObjectClient::EObjectType ToApiObjectType(ENodeType type);

NCypressClient::ELockMode ToApiLockMode(ELockMode mode);

NYTree::EPermission ToApiPermission(EPermission permission);

NTransactionClient::EAtomicity ToApiAtomicity(EAtomicity atomicity);

////////////////////////////////////////////////////////////////////////////////

NApi::TGetNodeOptions SerializeOptionsForGet(
    const TTransactionId& transactionId,
    const TGetOptions& options);

NApi::TSetNodeOptions SerializeOptionsForSet(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TSetOptions& options);

NApi::TNodeExistsOptions SerializeOptionsForExists(
    const TTransactionId& transactionId,
    const TExistsOptions& options);

NApi::TMultisetAttributesNodeOptions SerializeOptionsForMultisetAttributes(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMultisetAttributesOptions& options);

NApi::TCreateObjectOptions SerializeOptionsForCreateObject(
    TMutationId& mutationId,
    const TCreateOptions& options);

NApi::TCreateNodeOptions SerializeOptionsForCreate(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCreateOptions& options);

NApi::TCopyNodeOptions SerializeOptionsForCopy(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCopyOptions& options);

NApi::TMoveNodeOptions SerializeOptionsForMove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMoveOptions& options);

NApi::TRemoveNodeOptions SerializeOptionsForRemove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TRemoveOptions& options);

NApi::TListNodeOptions SerializeOptionsForList(
    const TTransactionId& transactionId,
    const TListOptions& options);

NApi::TLinkNodeOptions SerializeOptionsForLink(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLinkOptions& options);

NApi::TLockNodeOptions SerializeOptionsForLock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLockOptions& options);

NApi::TUnlockNodeOptions SerializeOptionsForUnlock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TUnlockOptions& options);

NApi::TConcatenateNodesOptions SerializeOptionsForConcatenate(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TConcatenateOptions& options);

template <typename TDerived>
NApi::TTransactionStartOptions SerializeOptionsForStartTransaction(
    const TTabletTransactionOptions<TDerived>& options)
{
    NApi::TTransactionStartOptions result;
    if (options.Atomicity_) {
        result.Atomicity = ToApiAtomicity(*options.Atomicity_);
    }
    if (options.Durability_) {
        result.Durability = NTransactionClient::EDurability(*options.Durability_);
    }
    return result;
}

NApi::TTransactionStartOptions SerializeOptionsForStartTransaction(
    TMutationId& mutationId,
    const TTransactionId& parentId,
    TDuration timeout,
    const TStartTransactionOptions& options);

NApi::TTransactionAbortOptions SerializeOptionsForAbortTransaction(TMutationId& mutationId);

NApi::TTransactionCommitOptions SerializeOptionsForCommitTransaction(TMutationId& mutationId);

NApi::TStartOperationOptions SerializeOptionsForStartOperation(
    TMutationId& mutationId,
    const TTransactionId& transactionId);

NApi::TGetOperationOptions SerializeOptionsForGetOperation(const TGetOperationOptions& options, bool useAlias);

NApi::TSuspendOperationOptions SerializeOptionsForSuspendOperation(const TSuspendOperationOptions& options);

NApi::TListOperationsOptions SerializeOptionsForListOperations(const TListOperationsOptions& options);

NYson::TYsonString SerializeParametersForUpdateOperationParameters(const TUpdateOperationParametersOptions& options);

NApi::TGetJobOptions SerializeOptionsForGetJob(const TGetJobOptions& options);

NApi::TListJobsOptions SerializeOptionsForListJobs(const TListJobsOptions& options);

NApi::TGetJobTraceOptions SerializeOptionsForGetJobTrace(const TGetJobTraceOptions& options);

NApi::TFileReaderOptions SerializeOptionsForReadFile(
    const TTransactionId& transactionId,
    const TFileReaderOptions& options);

NApi::TFileWriterOptions SerializeOptionsForWriteFile(
    const TTransactionId& transactionId,
    const TFileWriterOptions& options);

NApi::TGetFileFromCacheOptions SerializeOptionsForGetFileFromCache(
    const TTransactionId& transactionId,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options);

NApi::TPutFileToCacheOptions SerializeOptionsForPutFileToCache(
    const TTransactionId& transactionId,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options);

NApi::TMountTableOptions SerializeOptionsForMountTable(
    TMutationId& mutationId,
    const TMountTableOptions& options);

NApi::TUnmountTableOptions SerializeOptionsForUnmountTable(
    TMutationId& mutationId,
    const TUnmountTableOptions& options);

NApi::TRemountTableOptions SerializeOptionsForRemountTable(
    TMutationId& mutationId,
    const TRemountTableOptions& options);

NApi::TReshardTableOptions SerializeOptionsForReshardTable(
    TMutationId& mutationId,
    const TReshardTableOptions& options);

NApi::TModifyRowsOptions SerializeOptionsForInsertRows(const TInsertRowsOptions& options);

NApi::TVersionedLookupRowsOptions SerializeOptionsForVersionedLookupRows(
    const NTableClient::TNameTablePtr& nameTable,
    const TLookupRowsOptions& options);

NApi::TLookupRowsOptions SerializeOptionsForLookupRows(
    const NTableClient::TNameTablePtr& nameTable,
    const TLookupRowsOptions& options);

NApi::TSelectRowsOptions SerializeOptionsForSelectRows(const TSelectRowsOptions& options);

NApi::TTableWriterOptions SerializeOptionsForWriteTable(
    const TTransactionId& transactionId,
    const TTableWriterOptions& options = {});

NApi::TTableReaderOptions SerializeOptionsForReadTable(
    const TTransactionId& transactionId,
    const TTableReaderOptions& options = {});

NApi::TReadTablePartitionOptions SerializeOptionsForReadTablePartition(
    const TTablePartitionReaderOptions& options = {});

NApi::TAlterTableOptions SerializeOptionsForAlterTable(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TAlterTableOptions& options);

NApi::TAlterTableReplicaOptions SerializeOptionsForAlterTableReplica(
    TMutationId& mutationId,
    const TAlterTableReplicaOptions& options);

NApi::TModifyRowsOptions SerializeOptionsForDeleteRows(const TDeleteRowsOptions& options);

NApi::TFreezeTableOptions SerializeOptionsForFreezeTable(const TFreezeTableOptions& options);

NApi::TUnfreezeTableOptions SerializeOptionsForUnfreezeTable(const TUnfreezeTableOptions& options);

NApi::TCheckPermissionOptions SerializeOptionsForCheckPermission(const TCheckPermissionOptions& options);

NApi::TGetColumnarStatisticsOptions SerializeOptionsForGetTableColumnarStatistics(
    const TTransactionId& transactionId,
    const TGetTableColumnarStatisticsOptions& options);

NApi::TPartitionTablesOptions SerializeOptionsForGetTablePartitions(
    const TTransactionId& transactionId,
    const TGetTablePartitionsOptions& options);

NApi::TDistributedWriteSessionStartOptions SerializeOptionsForStartDistributedTableSession(
    TMutationId& mutationId,
    i64 cookieCount,
    const TStartDistributedWriteTableOptions& options);

NApi::TDistributedWriteSessionFinishOptions SerializeOptionsForFinishDistributedTableSession(
    TMutationId& mutationId,
    const TFinishDistributedWriteTableOptions& options);

NApi::TDistributedWriteFileSessionStartOptions SerializeOptionsForStartDistributedFileSession(
    TMutationId& mutationId,
    i64 cookieCount,
    const TStartDistributedWriteFileOptions& options);

NApi::TDistributedWriteFileSessionFinishOptions SerializeOptionsForFinishDistributedFileSession(
    TMutationId& mutationId,
    const TFinishDistributedWriteFileOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
