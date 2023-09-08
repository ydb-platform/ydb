#include "delegating_client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TDelegatingClient::TDelegatingClient(IClientPtr underlying)
    : Underlying_(std::move(underlying))
{ }

IConnectionPtr TDelegatingClient::GetConnection()
{
    return Underlying_->GetConnection();
}

std::optional<TStringBuf> TDelegatingClient::GetClusterName(bool fetchIfNull)
{
    return Underlying_->GetClusterName(fetchIfNull);
}

TFuture<ITransactionPtr> TDelegatingClient::StartTransaction(
    NTransactionClient::ETransactionType type,
    const TTransactionStartOptions& options)
{
    return Underlying_->StartTransaction(type, options);
}

TFuture<IUnversionedRowsetPtr> TDelegatingClient::LookupRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TLookupRowsOptions& options)
{
    return Underlying_->LookupRows(path, std::move(nameTable), keys, options);
}

TFuture<IVersionedRowsetPtr> TDelegatingClient::VersionedLookupRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    return Underlying_->VersionedLookupRows(path, std::move(nameTable), keys, options);
}

TFuture<std::vector<IUnversionedRowsetPtr>> TDelegatingClient::MultiLookup(
    const std::vector<TMultiLookupSubrequest>& subrequests,
    const TMultiLookupOptions& options)
{
    return Underlying_->MultiLookup(subrequests, options);
}

TFuture<TSelectRowsResult> TDelegatingClient::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    return Underlying_->SelectRows(query, options);
}

TFuture<NYson::TYsonString> TDelegatingClient::ExplainQuery(
    const TString& query,
    const TExplainQueryOptions& options)
{
    return Underlying_->ExplainQuery(query, options);
}

TFuture<TPullRowsResult> TDelegatingClient::PullRows(
    const NYPath::TYPath& path,
    const TPullRowsOptions& options)
{
    return Underlying_->PullRows(path, options);
}

TFuture<ITableReaderPtr> TDelegatingClient::CreateTableReader(
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options)
{
    return Underlying_->CreateTableReader(path, options);
}

TFuture<ITableWriterPtr> TDelegatingClient::CreateTableWriter(
    const NYPath::TRichYPath& path,
    const TTableWriterOptions& options)
{
    return Underlying_->CreateTableWriter(path, options);
}

TFuture<NQueueClient::IQueueRowsetPtr> TDelegatingClient::PullQueue(
    const NYPath::TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueOptions& options)
{
    return Underlying_->PullQueue(queuePath, offset, partitionIndex, rowBatchReadOptions, options);
}

TFuture<NQueueClient::IQueueRowsetPtr> TDelegatingClient::PullConsumer(
    const NYPath::TRichYPath& consumerPath,
    const NYPath::TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullConsumerOptions& options)
{
    return Underlying_->PullConsumer(consumerPath, queuePath, offset, partitionIndex, rowBatchReadOptions, options);
}

TFuture<void> TDelegatingClient::RegisterQueueConsumer(
    const NYPath::TRichYPath& queuePath,
    const NYPath::TRichYPath& consumerPath,
    bool vital,
    const TRegisterQueueConsumerOptions& options)
{
    return Underlying_->RegisterQueueConsumer(queuePath, consumerPath, vital, options);
}

TFuture<void> TDelegatingClient::UnregisterQueueConsumer(
    const NYPath::TRichYPath& queuePath,
    const NYPath::TRichYPath& consumerPath,
    const TUnregisterQueueConsumerOptions& options)
{
    return Underlying_->UnregisterQueueConsumer(queuePath, consumerPath, options);
}

TFuture<std::vector<TListQueueConsumerRegistrationsResult>> TDelegatingClient::ListQueueConsumerRegistrations(
    const std::optional<NYPath::TRichYPath>& queuePath,
    const std::optional<NYPath::TRichYPath>& consumerPath,
    const TListQueueConsumerRegistrationsOptions& options)
{
    return Underlying_->ListQueueConsumerRegistrations(queuePath, consumerPath, options);
}

TFuture<NYson::TYsonString> TDelegatingClient::GetNode(
    const NYPath::TYPath& path,
    const TGetNodeOptions& options)
{
    return Underlying_->GetNode(path, options);
}

TFuture<void> TDelegatingClient::SetNode(
    const NYPath::TYPath& path,
    const NYson::TYsonString& value,
    const TSetNodeOptions& options)
{
    return Underlying_->SetNode(path, value, options);
}

TFuture<void> TDelegatingClient::MultisetAttributesNode(
    const NYPath::TYPath& path,
    const NYTree::IMapNodePtr& attributes,
    const TMultisetAttributesNodeOptions& options)
{
    return Underlying_->MultisetAttributesNode(path, attributes, options);
}

TFuture<void> TDelegatingClient::RemoveNode(
    const NYPath::TYPath& path,
    const TRemoveNodeOptions& options)
{
    return Underlying_->RemoveNode(path, options);
}

TFuture<NYson::TYsonString> TDelegatingClient::ListNode(
    const NYPath::TYPath& path,
    const TListNodeOptions& options)
{
    return Underlying_->ListNode(path, options);
}

TFuture<NCypressClient::TNodeId> TDelegatingClient::CreateNode(
    const NYPath::TYPath& path,
    NObjectClient::EObjectType type,
    const TCreateNodeOptions& options)
{
    return Underlying_->CreateNode(path, type, options);
}

TFuture<TLockNodeResult> TDelegatingClient::LockNode(
    const NYPath::TYPath& path,
    NCypressClient::ELockMode mode,
    const TLockNodeOptions& options)
{
    return Underlying_->LockNode(path, mode, options);
}

TFuture<void> TDelegatingClient::UnlockNode(
    const NYPath::TYPath& path,
    const TUnlockNodeOptions& options)
{
    return Underlying_->UnlockNode(path, options);
}

TFuture<NCypressClient::TNodeId> TDelegatingClient::CopyNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const TCopyNodeOptions& options)
{
    return Underlying_->CopyNode(srcPath, dstPath, options);
}

TFuture<NCypressClient::TNodeId> TDelegatingClient::MoveNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const TMoveNodeOptions& options)
{
    return Underlying_->MoveNode(srcPath, dstPath, options);
}

TFuture<NCypressClient::TNodeId> TDelegatingClient::LinkNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const TLinkNodeOptions& options)
{
    return Underlying_->LinkNode(srcPath, dstPath, options);
}

TFuture<void> TDelegatingClient::ConcatenateNodes(
    const std::vector<NYPath::TRichYPath>& srcPaths,
    const NYPath::TRichYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    return Underlying_->ConcatenateNodes(srcPaths, dstPath, options);
}

TFuture<bool> TDelegatingClient::NodeExists(
    const NYPath::TYPath& path,
    const TNodeExistsOptions& options)
{
    return Underlying_->NodeExists(path, options);
}

TFuture<void> TDelegatingClient::ExternalizeNode(
    const NYPath::TYPath& path,
    NObjectClient::TCellTag cellTag,
    const TExternalizeNodeOptions& options)
{
    return Underlying_->ExternalizeNode(path, cellTag, options);
}

TFuture<void> TDelegatingClient::InternalizeNode(
    const NYPath::TYPath& path,
    const TInternalizeNodeOptions& options)
{
    return Underlying_->InternalizeNode(path, options);
}

TFuture<NObjectClient::TObjectId> TDelegatingClient::CreateObject(
    NObjectClient::EObjectType type,
    const TCreateObjectOptions& options)
{
    return Underlying_->CreateObject(type, options);
}

TFuture<IFileReaderPtr> TDelegatingClient::CreateFileReader(
    const NYPath::TYPath& path,
    const TFileReaderOptions& options)
{
    return Underlying_->CreateFileReader(path, options);
}

IFileWriterPtr TDelegatingClient::CreateFileWriter(
    const NYPath::TRichYPath& path,
    const TFileWriterOptions& options)
{
    return Underlying_->CreateFileWriter(path, options);
}

IJournalReaderPtr TDelegatingClient::CreateJournalReader(
    const NYPath::TYPath& path,
    const TJournalReaderOptions& options)
{
    return Underlying_->CreateJournalReader(path, options);
}

IJournalWriterPtr TDelegatingClient::CreateJournalWriter(
    const NYPath::TYPath& path,
    const TJournalWriterOptions& options)
{
    return Underlying_->CreateJournalWriter(path, options);
}

void TDelegatingClient::Terminate()
{
    return Underlying_->Terminate();
}

const NTabletClient::ITableMountCachePtr& TDelegatingClient::GetTableMountCache()
{
    return Underlying_->GetTableMountCache();
}
const NChaosClient::IReplicationCardCachePtr& TDelegatingClient::GetReplicationCardCache()
{
    return Underlying_->GetReplicationCardCache();
}
const NTransactionClient::ITimestampProviderPtr& TDelegatingClient::GetTimestampProvider() {
    return Underlying_->GetTimestampProvider();
}

ITransactionPtr TDelegatingClient::AttachTransaction(
    NTransactionClient::TTransactionId transactionId,
    const TTransactionAttachOptions& options)
{
    return Underlying_->AttachTransaction(transactionId, options);
}

TFuture<void> TDelegatingClient::MountTable(
    const NYPath::TYPath& path,
    const TMountTableOptions& options)
{
    return Underlying_->MountTable(path, options);
}

TFuture<void> TDelegatingClient::UnmountTable(
    const NYPath::TYPath& path,
    const TUnmountTableOptions& options)
{
    return Underlying_->UnmountTable(path, options);
}

TFuture<void> TDelegatingClient::RemountTable(
    const NYPath::TYPath& path,
    const TRemountTableOptions& options)
{
    return Underlying_->RemountTable(path, options);
}

TFuture<void> TDelegatingClient::FreezeTable(
    const NYPath::TYPath& path,
    const TFreezeTableOptions& options)
{
    return Underlying_->FreezeTable(path, options);
}

TFuture<void> TDelegatingClient::UnfreezeTable(
    const NYPath::TYPath& path,
    const TUnfreezeTableOptions& options)
{
    return Underlying_->UnfreezeTable(path, options);
}

TFuture<void> TDelegatingClient::ReshardTable(
    const NYPath::TYPath& path,
    const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
    const TReshardTableOptions& options)
{
    return Underlying_->ReshardTable(path, pivotKeys, options);
}

TFuture<void> TDelegatingClient::ReshardTable(
    const NYPath::TYPath& path,
    int tabletCount,
    const TReshardTableOptions& options)
{
    return Underlying_->ReshardTable(path, tabletCount, options);
}

TFuture<std::vector<NTabletClient::TTabletActionId>> TDelegatingClient::ReshardTableAutomatic(
    const NYPath::TYPath& path,
    const TReshardTableAutomaticOptions& options)
{
    return Underlying_->ReshardTableAutomatic(path, options);
}

TFuture<void> TDelegatingClient::TrimTable(
    const NYPath::TYPath& path,
    int tabletIndex,
    i64 trimmedRowCount,
    const TTrimTableOptions& options)
{
    return Underlying_->TrimTable(path, tabletIndex, trimmedRowCount, options);
}

TFuture<void> TDelegatingClient::AlterTable(
    const NYPath::TYPath& path,
    const TAlterTableOptions& options)
{
    return Underlying_->AlterTable(path, options);
}

TFuture<void> TDelegatingClient::AlterTableReplica(
    NTabletClient::TTableReplicaId replicaId,
    const TAlterTableReplicaOptions& options)
{
    return Underlying_->AlterTableReplica(replicaId, options);
}

TFuture<void> TDelegatingClient::AlterReplicationCard(
    NChaosClient::TReplicationCardId replicationCardId,
    const TAlterReplicationCardOptions& options)
{
    return Underlying_->AlterReplicationCard(replicationCardId, options);
}

TFuture<NYson::TYsonString> TDelegatingClient::GetTablePivotKeys(
    const NYPath::TYPath& path,
    const TGetTablePivotKeysOptions& options)
{
    return Underlying_->GetTablePivotKeys(path, options);
}

TFuture<void> TDelegatingClient::CreateTableBackup(
    const TBackupManifestPtr& manifest,
    const TCreateTableBackupOptions& options)
{
    return Underlying_->CreateTableBackup(manifest, options);
}

TFuture<void> TDelegatingClient::RestoreTableBackup(
    const TBackupManifestPtr& manifest,
    const TRestoreTableBackupOptions& options)
{
    return Underlying_->RestoreTableBackup(manifest, options);
}

TFuture<std::vector<NTabletClient::TTableReplicaId>> TDelegatingClient::GetInSyncReplicas(
    const NYPath::TYPath& path,
    const NTableClient::TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TGetInSyncReplicasOptions& options)
{
    return Underlying_->GetInSyncReplicas(path, nameTable, keys, options);
}

TFuture<std::vector<NTabletClient::TTableReplicaId>> TDelegatingClient::GetInSyncReplicas(
    const NYPath::TYPath& path,
    const TGetInSyncReplicasOptions& options)
{
    return Underlying_->GetInSyncReplicas(path, options);
}

TFuture<std::vector<TTabletInfo>> TDelegatingClient::GetTabletInfos(
    const NYPath::TYPath& path,
    const std::vector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    return Underlying_->GetTabletInfos(path, tabletIndexes, options);
}

TFuture<TGetTabletErrorsResult> TDelegatingClient::GetTabletErrors(
    const NYPath::TYPath& path,
    const TGetTabletErrorsOptions& options)
{
    return Underlying_->GetTabletErrors(path, options);
}

TFuture<std::vector<NTabletClient::TTabletActionId>> TDelegatingClient::BalanceTabletCells(
    const TString& tabletCellBundle,
    const std::vector<NYPath::TYPath>& movableTables,
    const TBalanceTabletCellsOptions& options)
{
    return Underlying_->BalanceTabletCells(tabletCellBundle, movableTables, options);
}

TFuture<NChaosClient::TReplicationCardPtr> TDelegatingClient::GetReplicationCard(
    NChaosClient::TReplicationCardId replicationCardId,
    const TGetReplicationCardOptions& options)
{
    return Underlying_->GetReplicationCard(replicationCardId, options);
}

TFuture<void> TDelegatingClient::UpdateChaosTableReplicaProgress(
    NChaosClient::TReplicaId replicaId,
    const TUpdateChaosTableReplicaProgressOptions& options)
{
    return Underlying_->UpdateChaosTableReplicaProgress(replicaId, options);
}

TFuture<TSkynetSharePartsLocationsPtr> TDelegatingClient::LocateSkynetShare(
    const NYPath::TRichYPath& path,
    const TLocateSkynetShareOptions& options)
{
    return Underlying_->LocateSkynetShare(path, options);
}

TFuture<std::vector<NTableClient::TColumnarStatistics>> TDelegatingClient::GetColumnarStatistics(
    const std::vector<NYPath::TRichYPath>& path,
    const TGetColumnarStatisticsOptions& options)
{
    return Underlying_->GetColumnarStatistics(path, options);
}

TFuture<TMultiTablePartitions> TDelegatingClient::PartitionTables(
    const std::vector<NYPath::TRichYPath>& paths,
    const TPartitionTablesOptions& options)
{
    return Underlying_->PartitionTables(paths, options);
}

TFuture<void> TDelegatingClient::TruncateJournal(
    const NYPath::TYPath& path,
    i64 rowCount,
    const TTruncateJournalOptions& options)
{
    return Underlying_->TruncateJournal(path, rowCount, options);
}

TFuture<TGetFileFromCacheResult> TDelegatingClient::GetFileFromCache(
    const TString& md5,
    const TGetFileFromCacheOptions& options)
{
    return Underlying_->GetFileFromCache(md5, options);
}

TFuture<TPutFileToCacheResult> TDelegatingClient::PutFileToCache(
    const NYPath::TYPath& path,
    const TString& expectedMD5,
    const TPutFileToCacheOptions& options)
{
    return Underlying_->PutFileToCache(path, expectedMD5, options);
}

TFuture<void> TDelegatingClient::AddMember(
    const TString& group,
    const TString& member,
    const TAddMemberOptions& options)
{
    return Underlying_->AddMember(group, member, options);
}

TFuture<void> TDelegatingClient::RemoveMember(
    const TString& group,
    const TString& member,
    const TRemoveMemberOptions& options)
{
    return Underlying_->RemoveMember(group, member, options);
}

TFuture<TCheckPermissionResponse> TDelegatingClient::CheckPermission(
    const TString& user,
    const NYPath::TYPath& path,
    NYTree::EPermission permission,
    const TCheckPermissionOptions& options)
{
    return Underlying_->CheckPermission(user, path, permission, options);
}

TFuture<TCheckPermissionByAclResult> TDelegatingClient::CheckPermissionByAcl(
    const std::optional<TString>& user,
    NYTree::EPermission permission,
    NYTree::INodePtr acl,
    const TCheckPermissionByAclOptions& options)
{
    return Underlying_->CheckPermissionByAcl(user, permission, std::move(acl), options);
}

TFuture<void> TDelegatingClient::TransferAccountResources(
    const TString& srcAccount,
    const TString& dstAccount,
    NYTree::INodePtr resourceDelta,
    const TTransferAccountResourcesOptions& options)
{
    return Underlying_->TransferAccountResources(
        srcAccount, dstAccount, std::move(resourceDelta), options
    );
}

TFuture<void> TDelegatingClient::TransferPoolResources(
    const TString& srcPool,
    const TString& dstPool,
    const TString& poolTree,
    NYTree::INodePtr resourceDelta,
    const TTransferPoolResourcesOptions& options)
{
    return Underlying_->TransferPoolResources(
        srcPool, dstPool, poolTree, std::move(resourceDelta), options
    );
}

TFuture<NScheduler::TOperationId> TDelegatingClient::StartOperation(
    NScheduler::EOperationType type,
    const NYson::TYsonString& spec,
    const TStartOperationOptions& options)
{
    return Underlying_->StartOperation(type, spec, options);
}

TFuture<void> TDelegatingClient::AbortOperation(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const TAbortOperationOptions& options)
{
    return Underlying_->AbortOperation(operationIdOrAlias, options);
}

TFuture<void> TDelegatingClient::SuspendOperation(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const TSuspendOperationOptions& options)
{
    return Underlying_->SuspendOperation(operationIdOrAlias, options);
}

TFuture<void> TDelegatingClient::ResumeOperation(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const TResumeOperationOptions& options)
{
    return Underlying_->ResumeOperation(operationIdOrAlias, options);
}

TFuture<void> TDelegatingClient::CompleteOperation(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const TCompleteOperationOptions& options)
{
    return Underlying_->CompleteOperation(operationIdOrAlias, options);
}

TFuture<void> TDelegatingClient::UpdateOperationParameters(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const NYson::TYsonString& parameters,
    const TUpdateOperationParametersOptions& options)
{
    return Underlying_->UpdateOperationParameters(operationIdOrAlias, parameters, options);
}

TFuture<TOperation> TDelegatingClient::GetOperation(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const TGetOperationOptions& options)
{
    return Underlying_->GetOperation(operationIdOrAlias, options);
}

TFuture<void> TDelegatingClient::DumpJobContext(
    NJobTrackerClient::TJobId jobId,
    const NYPath::TYPath& path,
    const TDumpJobContextOptions& options)
{
    return Underlying_->DumpJobContext(jobId, path, options);
}

TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> TDelegatingClient::GetJobInput(
    NJobTrackerClient::TJobId jobId,
    const TGetJobInputOptions& options)
{
    return Underlying_->GetJobInput(jobId, options);
}

TFuture<NYson::TYsonString> TDelegatingClient::GetJobInputPaths(
    NJobTrackerClient::TJobId jobId,
    const TGetJobInputPathsOptions& options)
{
    return Underlying_->GetJobInputPaths(jobId, options);
}

TFuture<NYson::TYsonString> TDelegatingClient::GetJobSpec(
    NJobTrackerClient::TJobId jobId,
    const TGetJobSpecOptions& options)
{
    return Underlying_->GetJobSpec(jobId, options);
}

TFuture<TSharedRef> TDelegatingClient::GetJobStderr(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    NJobTrackerClient::TJobId jobId,
    const TGetJobStderrOptions& options)
{
    return Underlying_->GetJobStderr(operationIdOrAlias, jobId, options);
}

TFuture<TSharedRef> TDelegatingClient::GetJobFailContext(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    NJobTrackerClient::TJobId jobId,
    const TGetJobFailContextOptions& options)
{
    return Underlying_->GetJobFailContext(operationIdOrAlias, jobId, options);
}

TFuture<TListOperationsResult> TDelegatingClient::ListOperations(
    const TListOperationsOptions& options)
{
    return Underlying_->ListOperations(options);
}

TFuture<TListJobsResult> TDelegatingClient::ListJobs(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    const TListJobsOptions& options)
{
    return Underlying_->ListJobs(operationIdOrAlias, options);
}

TFuture<NYson::TYsonString> TDelegatingClient::GetJob(
    const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
    NJobTrackerClient::TJobId jobId,
    const TGetJobOptions& options)
{
    return Underlying_->GetJob(operationIdOrAlias, jobId, options);
}

TFuture<void> TDelegatingClient::AbandonJob(
    NJobTrackerClient::TJobId jobId,
    const TAbandonJobOptions& options)
{
    return Underlying_->AbandonJob(jobId, options);
}

TFuture<TPollJobShellResponse> TDelegatingClient::PollJobShell(
    NJobTrackerClient::TJobId jobId,
    const std::optional<TString>& shellName,
    const NYson::TYsonString& parameters,
    const TPollJobShellOptions& options)
{
    return Underlying_->PollJobShell(jobId, shellName, parameters, options);
}

TFuture<void> TDelegatingClient::AbortJob(
    NJobTrackerClient::TJobId jobId,
    const TAbortJobOptions& options)
{
    return Underlying_->AbortJob(jobId, options);
}

TFuture<TClusterMeta> TDelegatingClient::GetClusterMeta(
    const TGetClusterMetaOptions& options)
{
    return Underlying_->GetClusterMeta(options);
}

TFuture<void> TDelegatingClient::CheckClusterLiveness(
    const TCheckClusterLivenessOptions& options)
{
    return Underlying_->CheckClusterLiveness(options);
}

TFuture<int> TDelegatingClient::BuildSnapshot(
    const TBuildSnapshotOptions& options)
{
    return Underlying_->BuildSnapshot(options);
}

TFuture<TCellIdToSnapshotIdMap> TDelegatingClient::BuildMasterSnapshots(
    const TBuildMasterSnapshotsOptions& options)
{
    return Underlying_->BuildMasterSnapshots(options);
}

TFuture<void> TDelegatingClient::ExitReadOnly(
    NHydra::TCellId cellId,
    const TExitReadOnlyOptions& options)
{
    return Underlying_->ExitReadOnly(cellId, options);
}

TFuture<void> TDelegatingClient::MasterExitReadOnly(
    const TMasterExitReadOnlyOptions& options)
{
    return Underlying_->MasterExitReadOnly(options);
}

TFuture<void> TDelegatingClient::SwitchLeader(
    NHydra::TCellId cellId,
    const TString& newLeaderAddress,
    const TSwitchLeaderOptions& options)
{
    return Underlying_->SwitchLeader(cellId, newLeaderAddress, options);
}

TFuture<void> TDelegatingClient::ResetStateHash(
    NHydra::TCellId cellId,
    const TResetStateHashOptions& options)
{
    return Underlying_->ResetStateHash(cellId, options);
}

TFuture<void> TDelegatingClient::GCCollect(
    const TGCCollectOptions& options)
{
    return Underlying_->GCCollect(options);
}

TFuture<void> TDelegatingClient::KillProcess(
    const TString& address,
    const TKillProcessOptions& options)
{
    return Underlying_->KillProcess(address, options);
}

TFuture<TString> TDelegatingClient::WriteCoreDump(
    const TString& address,
    const TWriteCoreDumpOptions& options)
{
    return Underlying_->WriteCoreDump(address, options);
}

TFuture<TGuid> TDelegatingClient::WriteLogBarrier(
    const TString& address,
    const TWriteLogBarrierOptions& options)
{
    return Underlying_->WriteLogBarrier(address, options);
}

TFuture<TString> TDelegatingClient::WriteOperationControllerCoreDump(
    NJobTrackerClient::TOperationId operationId,
    const TWriteOperationControllerCoreDumpOptions& options)
{
    return Underlying_->WriteOperationControllerCoreDump(operationId, options);
}

TFuture<void> TDelegatingClient::HealExecNode(
    const TString& address,
    const THealExecNodeOptions& options)
{
    return Underlying_->HealExecNode(address, options);
}

TFuture<void> TDelegatingClient::SuspendCoordinator(
    NObjectClient::TCellId coordinatorCellId,
    const TSuspendCoordinatorOptions& options)
{
    return Underlying_->SuspendCoordinator(coordinatorCellId, options);
}

TFuture<void> TDelegatingClient::ResumeCoordinator(
    NObjectClient::TCellId coordinatorCellId,
    const TResumeCoordinatorOptions& options)
{
    return Underlying_->ResumeCoordinator(coordinatorCellId, options);
}

TFuture<void> TDelegatingClient::MigrateReplicationCards(
    NObjectClient::TCellId chaosCellId,
    const TMigrateReplicationCardsOptions& options)
{
    return Underlying_->MigrateReplicationCards(chaosCellId, options);
}

TFuture<void> TDelegatingClient::SuspendChaosCells(
    const std::vector<NObjectClient::TCellId>& cellIds,
    const TSuspendChaosCellsOptions& options)
{
    return Underlying_->SuspendChaosCells(cellIds, options);
}

TFuture<void> TDelegatingClient::ResumeChaosCells(
    const std::vector<NObjectClient::TCellId>& cellIds,
    const TResumeChaosCellsOptions& options)
{
    return Underlying_->ResumeChaosCells(cellIds, options);
}

TFuture<void> TDelegatingClient::SuspendTabletCells(
    const std::vector<NObjectClient::TCellId>& cellIds,
    const TSuspendTabletCellsOptions& options)
{
    return Underlying_->SuspendTabletCells(cellIds, options);
}

TFuture<void> TDelegatingClient::ResumeTabletCells(
    const std::vector<NObjectClient::TCellId>& cellIds,
    const TResumeTabletCellsOptions& options)
{
    return Underlying_->ResumeTabletCells(cellIds, options);
}

TFuture<TMaintenanceId> TDelegatingClient::AddMaintenance(
    EMaintenanceComponent component,
    const TString& address,
    EMaintenanceType type,
    const TString& comment,
    const TAddMaintenanceOptions& options)
{
    return Underlying_->AddMaintenance(component, address, type, comment, options);
}

TFuture<TMaintenanceCounts> TDelegatingClient::RemoveMaintenance(
    EMaintenanceComponent component,
    const TString& address,
    const TMaintenanceFilter& filter,
    const TRemoveMaintenanceOptions& options)
{
    return Underlying_->RemoveMaintenance(component, address, filter, options);
}

TFuture<TDisableChunkLocationsResult> TDelegatingClient::DisableChunkLocations(
    const TString& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TDisableChunkLocationsOptions& options)
{
    return Underlying_->DisableChunkLocations(nodeAddress, locationUuids, options);
}

TFuture<TDestroyChunkLocationsResult> TDelegatingClient::DestroyChunkLocations(
    const TString& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TDestroyChunkLocationsOptions& options)
{
    return Underlying_->DestroyChunkLocations(nodeAddress, locationUuids, options);
}

TFuture<TResurrectChunkLocationsResult> TDelegatingClient::ResurrectChunkLocations(
    const TString& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TResurrectChunkLocationsOptions& options)
{
    return Underlying_->ResurrectChunkLocations(nodeAddress, locationUuids, options);
}

TFuture<TRequestRebootResult> TDelegatingClient::RequestReboot(
    const TString& nodeAddress,
    const TRequestRebootOptions& options)
{
    return Underlying_->RequestReboot(nodeAddress, options);
}

TFuture<void> TDelegatingClient::SetUserPassword(
    const TString& user,
    const TString& currentPasswordSha256,
    const TString& newPasswordSha256,
    const TSetUserPasswordOptions& options)
{
    return Underlying_->SetUserPassword(
        user,
        currentPasswordSha256,
        newPasswordSha256,
        options);
}

TFuture<TIssueTokenResult> TDelegatingClient::IssueToken(
    const TString& user,
    const TString& passwordSha256,
    const TIssueTokenOptions& options)
{
    return Underlying_->IssueToken(
        user,
        passwordSha256,
        options);
}

TFuture<void> TDelegatingClient::RevokeToken(
    const TString& user,
    const TString& passwordSha256,
    const TString& tokenSha256,
    const TRevokeTokenOptions& options)
{
    return Underlying_->RevokeToken(
        user,
        passwordSha256,
        tokenSha256,
        options);
}

TFuture<TListUserTokensResult> TDelegatingClient::ListUserTokens(
    const TString& user,
    const TString& passwordSha256,
    const TListUserTokensOptions& options)
{
    return Underlying_->ListUserTokens(
        user,
        passwordSha256,
        options);
}

TFuture<NQueryTrackerClient::TQueryId> TDelegatingClient::StartQuery(
    NQueryTrackerClient::EQueryEngine engine,
    const TString& query,
    const TStartQueryOptions& options)
{
    return Underlying_->StartQuery(engine, query, options);
}

TFuture<void> TDelegatingClient::AbortQuery(
    NQueryTrackerClient::TQueryId queryId,
    const TAbortQueryOptions& options)
{
    return Underlying_->AbortQuery(queryId, options);
}

TFuture<TQueryResult> TDelegatingClient::GetQueryResult(
    NQueryTrackerClient::TQueryId queryId,
    i64 resultIndex,
    const TGetQueryResultOptions& options)
{
    return Underlying_->GetQueryResult(queryId, resultIndex, options);
}

TFuture<IUnversionedRowsetPtr> TDelegatingClient::ReadQueryResult(
    NQueryTrackerClient::TQueryId queryId,
    i64 resultIndex,
    const TReadQueryResultOptions& options)
{
    return Underlying_->ReadQueryResult(queryId, resultIndex, options);
}

TFuture<TQuery> TDelegatingClient::GetQuery(
    NQueryTrackerClient::TQueryId queryId,
    const TGetQueryOptions& options)
{
    return Underlying_->GetQuery(queryId, options);
}

TFuture<TListQueriesResult> TDelegatingClient::ListQueries(const TListQueriesOptions& options)
{
    return Underlying_->ListQueries(options);
}

TFuture<void> TDelegatingClient::AlterQuery(
    NQueryTrackerClient::TQueryId queryId,
    const TAlterQueryOptions& options)
{
    return Underlying_->AlterQuery(queryId, options);
}

////////////////////////////////////////////////////////////////////////////////

// Method below ensures that delegating client contains implementations for all
// methods of IClient. Tthis reduces the number of PR iterations you need to
// find that some out-of-yt/yt implementation of IClient does not compile.
void InstantiateDelegatingClient()
{
    auto delegatingClient = New<TDelegatingClient>(/*client*/ nullptr);
    Y_UNUSED(delegatingClient);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
