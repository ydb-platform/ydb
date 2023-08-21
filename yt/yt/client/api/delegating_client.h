#pragma once

#include "client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that implements IClient and delegates
//! all calls to an underlying instance.
class TDelegatingClient
    : public IClient
{
public:
    explicit TDelegatingClient(IClientPtr underlying);

    // IClientBase methods

    IConnectionPtr GetConnection() override;
    std::optional<TStringBuf> GetClusterName(bool fetchIfNull = true) override;

    // Transactions
    TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = {}) override;

    // Tables
    TFuture<IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) override;

    TFuture<IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options = {}) override;

    TFuture<std::vector<IUnversionedRowsetPtr>> MultiLookup(
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options = {}) override;

    TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) override;

    TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const TExplainQueryOptions& options = {}) override;

    TFuture<TPullRowsResult> PullRows(
        const NYPath::TYPath& path,
        const TPullRowsOptions& options = {}) override;

    TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options = {}) override;

    TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options = {}) override;

    // Queues
    TFuture<NQueueClient::IQueueRowsetPtr> PullQueue(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options = {}) override;

    TFuture<NQueueClient::IQueueRowsetPtr> PullConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullConsumerOptions& options = {}) override;

    TFuture<void> RegisterQueueConsumer(
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        bool vital,
        const TRegisterQueueConsumerOptions& options = {}) override;

    TFuture<void> UnregisterQueueConsumer(
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        const TUnregisterQueueConsumerOptions& options = {}) override;

    TFuture<std::vector<TListQueueConsumerRegistrationsResult>> ListQueueConsumerRegistrations(
        const std::optional<NYPath::TRichYPath>& queuePath,
        const std::optional<NYPath::TRichYPath>& consumerPath,
        const TListQueueConsumerRegistrationsOptions& options = {}) override;

    // Cypress
    TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = {}) override;

    TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options = {}) override;

    TFuture<void> MultisetAttributesNode(
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options = {}) override;

    TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = {}) override;

    TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options = {}) override;

    TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options = {}) override;

    TFuture<TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options = {}) override;

    TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options = {}) override;

    TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options = {}) override;

    TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options = {}) override;

    TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options = {}) override;

    TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options = {}) override;

    TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options = {}) override;

    TFuture<void> ExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options = {}) override;

    TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options = {}) override;

    // Objects
    TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options = {}) override;


    // Files
    TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options = {}) override;

    IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options = {}) override;

    // Journals
    IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options = {}) override;

    IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options = {}) override;

    // IClient methods

    void Terminate() override;

    const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;

    const NChaosClient::IReplicationCardCachePtr& GetReplicationCardCache() override;

    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;

    // Transactions
    ITransactionPtr AttachTransaction(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options = {}) override;

    // Tables
    TFuture<void> MountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options = {}) override;

    TFuture<void> UnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options = {}) override;

    TFuture<void> RemountTable(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options = {}) override;

    TFuture<void> FreezeTable(
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options = {}) override;

    TFuture<void> UnfreezeTable(
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options = {}) override;

    TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const TReshardTableOptions& options = {}) override;

    TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options = {}) override;

    TFuture<std::vector<NTabletClient::TTabletActionId>> ReshardTableAutomatic(
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options = {}) override;

    TFuture<void> TrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options = {}) override;

    TFuture<void> AlterTable(
        const NYPath::TYPath& path,
        const TAlterTableOptions& options = {}) override;

    TFuture<void> AlterTableReplica(
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options = {}) override;

    TFuture<void> AlterReplicationCard(
        NChaosClient::TReplicationCardId replicationCardId,
        const TAlterReplicationCardOptions& options = {}) override;

    TFuture<NYson::TYsonString> GetTablePivotKeys(
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options = {}) override;

    TFuture<void> CreateTableBackup(
        const TBackupManifestPtr& manifest,
        const TCreateTableBackupOptions& options = {}) override;

    TFuture<void> RestoreTableBackup(
        const TBackupManifestPtr& manifest,
        const TRestoreTableBackupOptions& options = {}) override;

    TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options = {}) override;

    TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        const TGetInSyncReplicasOptions& options = {}) override;

    TFuture<std::vector<TTabletInfo>> GetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options = {}) override;

    TFuture<TGetTabletErrorsResult> GetTabletErrors(
        const NYPath::TYPath& path,
        const TGetTabletErrorsOptions& options = {}) override;

    TFuture<std::vector<NTabletClient::TTabletActionId>> BalanceTabletCells(
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options = {}) override;

    TFuture<NChaosClient::TReplicationCardPtr> GetReplicationCard(
        NChaosClient::TReplicationCardId replicationCardId,
        const TGetReplicationCardOptions& options = {}) override;

    TFuture<void> UpdateChaosTableReplicaProgress(
        NChaosClient::TReplicaId replicaId,
        const TUpdateChaosTableReplicaProgressOptions& options = {}) override;

    TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options = {}) override;

    TFuture<std::vector<NTableClient::TColumnarStatistics>> GetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options = {}) override;

    TFuture<TMultiTablePartitions> PartitionTables(
        const std::vector<NYPath::TRichYPath>& paths,
        const TPartitionTablesOptions& options) override;

    // Journals
    TFuture<void> TruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options = {}) override;

    // Files
    TFuture<TGetFileFromCacheResult> GetFileFromCache(
        const TString& md5,
        const TGetFileFromCacheOptions& options = {}) override;

    TFuture<TPutFileToCacheResult> PutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options = {}) override;

    // Security
    TFuture<void> AddMember(
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options = {}) override;

    TFuture<void> RemoveMember(
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options = {}) override;

    TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {}) override;

    TFuture<TCheckPermissionByAclResult> CheckPermissionByAcl(
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options = {}) override;

    TFuture<void> TransferAccountResources(
        const TString& srcAccount,
        const TString& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options = {}) override;

    // Scheduler
    TFuture<void> TransferPoolResources(
        const TString& srcPool,
        const TString& dstPool,
        const TString& poolTree,
        NYTree::INodePtr resourceDelta,
        const TTransferPoolResourcesOptions& options = {}) override;

    TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options = {}) override;

    TFuture<void> AbortOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options = {}) override;

    TFuture<void> SuspendOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options = {}) override;

    TFuture<void> ResumeOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options = {}) override;

    TFuture<void> CompleteOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options = {}) override;

    TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options = {}) override;

    TFuture<TOperation> GetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options = {}) override;

    TFuture<void> DumpJobContext(
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options = {}) override;

    TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputOptions& options = {}) override;

    TFuture<NYson::TYsonString> GetJobInputPaths(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputPathsOptions& options = {}) override;

    TFuture<NYson::TYsonString> GetJobSpec(
        NJobTrackerClient::TJobId jobId,
        const TGetJobSpecOptions& options = {}) override;

    TFuture<TSharedRef> GetJobStderr(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobStderrOptions& options = {}) override;

    TFuture<TSharedRef> GetJobFailContext(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobFailContextOptions& options = {}) override;

    TFuture<TListOperationsResult> ListOperations(
        const TListOperationsOptions& options = {}) override;

    TFuture<TListJobsResult> ListJobs(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TListJobsOptions& options = {}
    ) override;

    TFuture<NYson::TYsonString> GetJob(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobOptions& options = {}) override;

    TFuture<void> AbandonJob(
        NJobTrackerClient::TJobId jobId,
        const TAbandonJobOptions& options = {}) override;

    TFuture<TPollJobShellResponse> PollJobShell(
        NJobTrackerClient::TJobId jobId,
        const std::optional<TString>& shellName,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options = {}) override;

    TFuture<void> AbortJob(
        NJobTrackerClient::TJobId jobId,
        const TAbortJobOptions& options = {}) override;

    // Metadata
    TFuture<TClusterMeta> GetClusterMeta(
        const TGetClusterMetaOptions& options = {}) override;

    TFuture<void> CheckClusterLiveness(
        const TCheckClusterLivenessOptions& options = {}) override;

    // Administration
    TFuture<int> BuildSnapshot(
        const TBuildSnapshotOptions& options = {}) override;

    TFuture<TCellIdToSnapshotIdMap> BuildMasterSnapshots(
        const TBuildMasterSnapshotsOptions& options = {}) override;

    TFuture<void> SwitchLeader(
        NHydra::TCellId cellId,
        const TString& newLeaderAddress,
        const TSwitchLeaderOptions& options = {}) override;

    TFuture<void> ResetStateHash(
        NHydra::TCellId cellId,
        const TResetStateHashOptions& options = {}) override;

    TFuture<void> GCCollect(
        const TGCCollectOptions& options = {}) override;

    TFuture<void> KillProcess(
        const TString& address,
        const TKillProcessOptions& options = {}) override;

    TFuture<TString> WriteCoreDump(
        const TString& address,
        const TWriteCoreDumpOptions& options = {}) override;

    TFuture<TGuid> WriteLogBarrier(
        const TString& address,
        const TWriteLogBarrierOptions& options = {}) override;

    TFuture<TString> WriteOperationControllerCoreDump(
        NJobTrackerClient::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options = {}) override;

    TFuture<void> HealExecNode(
        const TString& address,
        const THealExecNodeOptions& options = {}) override;

    TFuture<void> SuspendCoordinator(
        NObjectClient::TCellId coordinatorCellId,
        const TSuspendCoordinatorOptions& options = {}) override;

    TFuture<void> ResumeCoordinator(
        NObjectClient::TCellId coordinatorCellId,
        const TResumeCoordinatorOptions& options = {}) override;

    TFuture<void> MigrateReplicationCards(
        NObjectClient::TCellId chaosCellId,
        const TMigrateReplicationCardsOptions& options = {}) override;

    TFuture<void> SuspendChaosCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendChaosCellsOptions& options = {}) override;

    TFuture<void> ResumeChaosCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeChaosCellsOptions& options = {}) override;

    TFuture<void> SuspendTabletCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendTabletCellsOptions& options = {}) override;

    TFuture<void> ResumeTabletCells(
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeTabletCellsOptions& options = {}) override;

    TFuture<TMaintenanceId> AddMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        EMaintenanceType type,
        const TString& comment,
        const TAddMaintenanceOptions& options = {}) override;

    TFuture<TMaintenanceCounts> RemoveMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        const TMaintenanceFilter& filter,
        const TRemoveMaintenanceOptions& options = {}) override;

    TFuture<TDisableChunkLocationsResult> DisableChunkLocations(
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TDisableChunkLocationsOptions& options = {}) override;

    TFuture<TDestroyChunkLocationsResult> DestroyChunkLocations(
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TDestroyChunkLocationsOptions& options = {}) override;

    TFuture<TResurrectChunkLocationsResult> ResurrectChunkLocations(
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TResurrectChunkLocationsOptions& options = {}) override;

    TFuture<TRequestRebootResult> RequestReboot(
        const TString& nodeAddress,
        const TRequestRebootOptions& options = {}) override;

    TFuture<void> SetUserPassword(
        const TString& user,
        const TString& currentPasswordSha256,
        const TString& newPasswordSha256,
        const TSetUserPasswordOptions& options) override;

    TFuture<TIssueTokenResult> IssueToken(
        const TString& user,
        const TString& passwordSha256,
        const TIssueTokenOptions& options) override;

    TFuture<void> RevokeToken(
        const TString& user,
        const TString& passwordSha256,
        const TString& tokenSha256,
        const TRevokeTokenOptions& options) override;

    TFuture<TListUserTokensResult> ListUserTokens(
        const TString& user,
        const TString& passwordSha256,
        const TListUserTokensOptions& options) override;

    // Query tracker

    TFuture<NQueryTrackerClient::TQueryId> StartQuery(
        NQueryTrackerClient::EQueryEngine engine,
        const TString& query,
        const TStartQueryOptions& options) override;

    TFuture<void> AbortQuery(
        NQueryTrackerClient::TQueryId queryId,
        const TAbortQueryOptions& options) override;

    TFuture<TQueryResult> GetQueryResult(
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex,
        const TGetQueryResultOptions& options) override;

    TFuture<IUnversionedRowsetPtr> ReadQueryResult(
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex,
        const TReadQueryResultOptions& options) override;

    TFuture<TQuery> GetQuery(
        NQueryTrackerClient::TQueryId queryId,
        const TGetQueryOptions& options) override;

    TFuture<TListQueriesResult> ListQueries(const TListQueriesOptions& options) override;

    TFuture<void> AlterQuery(
        NQueryTrackerClient::TQueryId queryId,
        const TAlterQueryOptions& options) override;

protected:
    const IClientPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

