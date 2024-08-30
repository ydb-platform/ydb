#pragma once

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/distributed_table_sessions.h>
#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/ypath/rich.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TMockClient
    : public IClient
{
public:
    const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    void SetTableMountCache(NTabletClient::ITableMountCachePtr value);

    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;
    void SetTimestampProvider(NTransactionClient::ITimestampProviderPtr value);

    MOCK_METHOD(IConnectionPtr, GetConnection, (), (override));

    MOCK_METHOD(std::optional<TStringBuf>, GetClusterName, (bool fetchIfNull), (override));

    MOCK_METHOD(TFuture<ITransactionPtr>, StartTransaction, (
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options),
        (override));

    MOCK_METHOD(TFuture<TUnversionedLookupRowsResult>, LookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TVersionedLookupRowsResult>, VersionedLookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<TUnversionedLookupRowsResult>>, MultiLookupRows, (
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options),
        (override));

    MOCK_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options),
        (override));

    MOCK_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueue, (
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options),
        (override));

    MOCK_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueueConsumer, (
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        std::optional<i64> offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueConsumerOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, RegisterQueueConsumer, (
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        bool vital,
        const TRegisterQueueConsumerOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, UnregisterQueueConsumer, (
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        const TUnregisterQueueConsumerOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<TListQueueConsumerRegistrationsResult>>, ListQueueConsumerRegistrations, (
        const std::optional<NYPath::TRichYPath>& queuePath,
        const std::optional<NYPath::TRichYPath>& consumerPath,
        const TListQueueConsumerRegistrationsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TCreateQueueProducerSessionResult>, CreateQueueProducerSession, (
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TCreateQueueProducerSessionOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, RemoveQueueProducerSession, (
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TRemoveQueueProducerSessionOptions& options),
        (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, ExplainQuery, (
        const TString& query,
        const TExplainQueryOptions& options),
        (override));

    MOCK_METHOD(TFuture<TPullRowsResult>, PullRows, (
        const NYPath::TYPath& path,
        const TPullRowsOptions& options),
        (override));

    MOCK_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options),
        (override));

    MOCK_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options),
        (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, GetNode, (
        const NYPath::TYPath& path,
        const TGetNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, SetNode, (
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, MultisetAttributesNode, (
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, RemoveNode, (
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, ListNode, (
        const NYPath::TYPath& path,
        const TListNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<TLockNodeResult>, LockNode, (
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, UnlockNode, (
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ConcatenateNodes, (
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ExternalizeNode, (
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, InternalizeNode, (
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<bool>, NodeExists, (
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options),
        (override));

    MOCK_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options),
        (override));

    MOCK_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (
        const NYPath::TYPath& path,
        const TFileReaderOptions& options),
        (override));

    MOCK_METHOD(IFileWriterPtr, CreateFileWriter, (
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options),
        (override));

    MOCK_METHOD(IJournalReaderPtr, CreateJournalReader, (
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options),
        (override));

    MOCK_METHOD(IJournalWriterPtr, CreateJournalWriter, (
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options),
        (override));

    MOCK_METHOD(TFuture<int>, BuildSnapshot, (const TBuildSnapshotOptions& options),
        (override));

    MOCK_METHOD(TFuture<TCellIdToSnapshotIdMap>, BuildMasterSnapshots, (
        const TBuildMasterSnapshotsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TCellIdToConsistentStateMap>, GetMasterConsistentState, (
        const TGetMasterConsistentStateOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ExitReadOnly, (
        NHydra::TCellId cellId,
        const TExitReadOnlyOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, MasterExitReadOnly, (
        const TMasterExitReadOnlyOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, DiscombobulateNonvotingPeers, (
        NHydra::TCellId cellId,
        const TDiscombobulateNonvotingPeersOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, SwitchLeader, (
        NHydra::TCellId cellId,
        const std::string& newLeaderAddress,
        const TSwitchLeaderOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ResetStateHash, (
        NHydra::TCellId cellId,
        const TResetStateHashOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, GCCollect, (
        const TGCCollectOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, KillProcess, (
        const std::string& address,
        const TKillProcessOptions& options),
        (override));

    MOCK_METHOD(TFuture<TString>, WriteCoreDump, (
        const std::string& address,
        const TWriteCoreDumpOptions& options),
        (override));

    MOCK_METHOD(TFuture<TGuid>, WriteLogBarrier, (
        const std::string& address,
        const TWriteLogBarrierOptions& options),
        (override));

    MOCK_METHOD(TFuture<TString>, WriteOperationControllerCoreDump, (
        NJobTrackerClient::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, HealExecNode, (
        const std::string& address,
        const THealExecNodeOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, SuspendCoordinator, (
        NObjectClient::TCellId coordinatorCellid,
        const TSuspendCoordinatorOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ResumeCoordinator, (
        NObjectClient::TCellId coordinatorCellid,
        const TResumeCoordinatorOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, MigrateReplicationCards, (
        NObjectClient::TCellId chaosCellid,
        const TMigrateReplicationCardsOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, SuspendChaosCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendChaosCellsOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ResumeChaosCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeChaosCellsOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, SuspendTabletCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendTabletCellsOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ResumeTabletCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeTabletCellsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TMaintenanceIdPerTarget>, AddMaintenance, (
        EMaintenanceComponent component,
        const std::string& address,
        EMaintenanceType type,
        const TString& comment,
        const TAddMaintenanceOptions& options),
        (override));

    MOCK_METHOD(TFuture<TMaintenanceCountsPerTarget>, RemoveMaintenance, (
        EMaintenanceComponent component,
        const std::string& address,
        const TMaintenanceFilter& filter,
        const TRemoveMaintenanceOptions& options),
        (override));

    MOCK_METHOD(void, Terminate, (),
        (override));
    MOCK_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, (),
        (override));

    MOCK_METHOD(ITransactionPtr, AttachTransaction, (
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, MountTable, (
        const NYPath::TYPath& path,
        const TMountTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, UnmountTable, (
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, RemountTable, (
        const NYPath::TYPath& path,
        const TRemountTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, FreezeTable, (
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, UnfreezeTable, (
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const TReshardTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, ReshardTableAutomatic, (
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, TrimTable, (
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AlterTable, (
        const NYPath::TYPath& path,
        const TAlterTableOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AlterTableReplica, (
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options),
        (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, GetTablePivotKeys, (
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, CreateTableBackup,(
        const TBackupManifestPtr& manifest,
        const TCreateTableBackupOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, RestoreTableBackup, (
        const TBackupManifestPtr& manifest,
        const TRestoreTableBackupOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath& path,
        const TGetInSyncReplicasOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<TTabletInfo>>, GetTabletInfos, (
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options),
        (override));

    MOCK_METHOD(TFuture<TGetTabletErrorsResult>, GetTabletErrors, (
        const NYPath::TYPath& path,
        const TGetTabletErrorsOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, BalanceTabletCells, (
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options),
        (override));

    MOCK_METHOD(TFuture<NChaosClient::TReplicationCardPtr>, GetReplicationCard, (
        NChaosClient::TReplicationCardId replicationCardId,
        const TGetReplicationCardOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, UpdateChaosTableReplicaProgress, (
        NChaosClient::TReplicaId replicaId,
        const TUpdateChaosTableReplicaProgressOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AlterReplicationCard, (
        NChaosClient::TReplicationCardId replicationCardId,
        const TAlterReplicationCardOptions& options),
        (override));

    MOCK_METHOD(TFuture<TSkynetSharePartsLocationsPtr>, LocateSkynetShare, (
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options),
        (override));

    MOCK_METHOD(TFuture<std::vector<NTableClient::TColumnarStatistics>>, GetColumnarStatistics, (
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TMultiTablePartitions>, PartitionTables, (
        const std::vector<NYPath::TRichYPath>& paths,
        const TPartitionTablesOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, TruncateJournal, (
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options),
        (override));

    MOCK_METHOD(TFuture<TGetFileFromCacheResult>, GetFileFromCache, (
        const TString& md5,
        const TGetFileFromCacheOptions& options),
        (override));

    MOCK_METHOD(TFuture<TPutFileToCacheResult>, PutFileToCache, (
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AddMember, (
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, RemoveMember, (
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options),
        (override));

    MOCK_METHOD(TFuture<TCheckPermissionResponse>, CheckPermission, (
        const std::string& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options),
        (override));

    MOCK_METHOD(TFuture<TCheckPermissionByAclResult>, CheckPermissionByAcl, (
        const std::optional<std::string>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, TransferAccountResources, (
        const TString& srcAccount,
        const TString& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, TransferPoolResources, (
        const TString& srcPool,
        const TString& dstPool,
        const TString& poolTree,
        NYTree::INodePtr resourceDelta,
        const TTransferPoolResourcesOptions& options),
        (override));

    MOCK_METHOD(TFuture<NScheduler::TOperationId>, StartOperation, (
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AbortOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, SuspendOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, ResumeOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, CompleteOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, UpdateOperationParameters, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options),
        (override));

    MOCK_METHOD(TFuture<TOperation>, GetOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, DumpJobContext, (
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options),
        (override));

    MOCK_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobInput, (
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputOptions& options),
        (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, GetJobInputPaths, (
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputPathsOptions& options),
        (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, GetJobSpec, (
        NJobTrackerClient::TJobId jobId,
        const TGetJobSpecOptions& options),
        (override));

    MOCK_METHOD(TFuture<TSharedRef>, GetJobStderr, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobStderrOptions& options),
        (override));

    MOCK_METHOD(TFuture<TSharedRef>, GetJobFailContext, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobFailContextOptions& options),
        (override));

    MOCK_METHOD(TFuture<TListOperationsResult>, ListOperations, (
        const TListOperationsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TListJobsResult>, ListJobs, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TListJobsOptions& options),
        (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, GetJob, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AbandonJob, (
        NJobTrackerClient::TJobId jobId,
        const TAbandonJobOptions& options),
        (override));

    MOCK_METHOD(TFuture<TPollJobShellResponse>, PollJobShell, (
        NJobTrackerClient::TJobId jobId,
        const std::optional<TString>& shellName,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AbortJob, (
        NJobTrackerClient::TJobId jobId,
        const TAbortJobOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, DumpJobProxyLog, (
        NJobTrackerClient::TJobId jobId,
        NJobTrackerClient::TOperationId operationId,
        const NYPath::TYPath& path,
        const TDumpJobProxyLogOptions& options),
        (override));

    MOCK_METHOD(TFuture<TClusterMeta>, GetClusterMeta, (
        const TGetClusterMetaOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, CheckClusterLiveness, (
        const TCheckClusterLivenessOptions& options),
        (override));

    MOCK_METHOD(TFuture<TDisableChunkLocationsResult>, DisableChunkLocations, (
        const std::string& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TDisableChunkLocationsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TDestroyChunkLocationsResult>, DestroyChunkLocations, (
        const std::string& nodeAddress,
        bool recoverUnlinkedDisks,
        const std::vector<TGuid>& locationUuids,
        const TDestroyChunkLocationsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TResurrectChunkLocationsResult>, ResurrectChunkLocations, (
        const std::string& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TResurrectChunkLocationsOptions& options),
        (override));

    MOCK_METHOD(TFuture<TRequestRestartResult>, RequestRestart, (
        const std::string& nodeAddress,
        const TRequestRestartOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, SetUserPassword, (
        const std::string& user,
        const TString& currentPasswordSha256,
        const TString& newPasswordSha256,
        const TSetUserPasswordOptions& options),
        (override));

    MOCK_METHOD(TFuture<TIssueTokenResult>, IssueToken, (
        const std::string& user,
        const TString& passwordSha256,
        const TIssueTokenOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, RevokeToken, (
        const std::string& user,
        const TString& passwordSha256,
        const TString& tokenSha256,
        const TRevokeTokenOptions& options),
        (override));

    MOCK_METHOD(TFuture<TListUserTokensResult>, ListUserTokens, (
        const std::string& user,
        const TString& passwordSha256,
        const TListUserTokensOptions& options),
        (override));

    MOCK_METHOD(TFuture<NQueryTrackerClient::TQueryId>, StartQuery, (
        NQueryTrackerClient::EQueryEngine engine,
        const TString& query,
        const TStartQueryOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AbortQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TAbortQueryOptions& options),
        (override));

    MOCK_METHOD(TFuture<TQueryResult>, GetQueryResult, (
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex,
        const TGetQueryResultOptions& options),
        (override));

    MOCK_METHOD(TFuture<IUnversionedRowsetPtr>, ReadQueryResult, (
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex,
        const TReadQueryResultOptions& options),
        (override));

    MOCK_METHOD(TFuture<TQuery>, GetQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TGetQueryOptions& options),
        (override));

    MOCK_METHOD(TFuture<TListQueriesResult>, ListQueries, (
        const TListQueriesOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, AlterQuery, (
        NQueryTrackerClient::TQueryId queryId, const TAlterQueryOptions& options),
        (override));

    MOCK_METHOD(TFuture<TGetQueryTrackerInfoResult>, GetQueryTrackerInfo, (
        const TGetQueryTrackerInfoOptions& options),
        (override));

    MOCK_METHOD(TFuture<NBundleControllerClient::TBundleConfigDescriptorPtr>, GetBundleConfig, (
        const TString& bundleName,
        const NBundleControllerClient::TGetBundleConfigOptions& options), (override));

    MOCK_METHOD(TFuture<void>, SetBundleConfig, (
        const TString& bundleName,
        const NBundleControllerClient::TBundleTargetConfigPtr& bundleConfig,
        const NBundleControllerClient::TSetBundleConfigOptions& options), (override));

    MOCK_METHOD(TFuture<TGetPipelineSpecResult>, GetPipelineSpec, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineSpecOptions& options),
        (override));

    MOCK_METHOD(TFuture<TSetPipelineSpecResult>, SetPipelineSpec, (
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineSpecOptions& options),
        (override));

    MOCK_METHOD(TFuture<TGetPipelineDynamicSpecResult>, GetPipelineDynamicSpec, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineDynamicSpecOptions& options),
        (override));

    MOCK_METHOD(TFuture<TSetPipelineDynamicSpecResult>, SetPipelineDynamicSpec, (
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineDynamicSpecOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, StartPipeline, (
        const NYPath::TYPath& pipelinePath,
        const TStartPipelineOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, StopPipeline, (
        const NYPath::TYPath& pipelinePath,
        const TStopPipelineOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, PausePipeline, (
        const NYPath::TYPath& pipelinePath,
        const TPausePipelineOptions& options),
        (override));

    MOCK_METHOD(TFuture<TPipelineState>, GetPipelineState, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineStateOptions& options),
        (override));

    MOCK_METHOD(TFuture<TGetFlowViewResult>, GetFlowView, (
        const NYPath::TYPath& pipelinePath,
        const NYPath::TYPath& viewPath,
        const TGetFlowViewOptions& options),
        (override));

    MOCK_METHOD(TFuture<TDistributedWriteSessionPtr>, StartDistributedWriteSession, (
        const NYPath::TRichYPath& path,
        const TDistributedWriteSessionStartOptions& options),
        (override));

    MOCK_METHOD(TFuture<void>, FinishDistributedWriteSession, (
        TDistributedWriteSessionPtr session,
        const TDistributedWriteSessionFinishOptions& options),
        (override));

    MOCK_METHOD(TFuture<ITableWriterPtr>, CreateParticipantTableWriter, (
        const TDistributedWriteCookiePtr& cookie,
        const TParticipantTableWriterOptions& options),
        (override));

private:
    NTabletClient::ITableMountCachePtr TableMountCache_;
    NTransactionClient::ITimestampProviderPtr TimestampProvider_;
};

DEFINE_REFCOUNTED_TYPE(TMockClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
