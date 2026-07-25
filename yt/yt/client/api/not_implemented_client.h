#pragma once

#include "client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that implements IClient and throws
//! an exception ("Not implemented ...") on any method call
class TNotImplementedClient
    : public IClient
{
public:
    #define UNIMPLEMENTED_METHOD(returnType, method, signature) \
        returnType method signature override \
        { \
            throw TErrorException() <<= TError("Not implemented method: %v", #method); \
        }

    // IClientBase methods
    UNIMPLEMENTED_METHOD(IConnectionPtr, GetConnection, ())

    UNIMPLEMENTED_METHOD(TFuture<std::optional<std::string>>, GetClusterName,
        (bool))

    // IClient methods
    UNIMPLEMENTED_METHOD(void, Terminate, ())

    UNIMPLEMENTED_METHOD(const NTabletClient::ITableMountCachePtr&, GetTableMountCache, ())

    UNIMPLEMENTED_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, ())

    UNIMPLEMENTED_METHOD(const NTransactionClient::ITimestampProviderPtr&, GetTimestampProvider, ())
    UNIMPLEMENTED_METHOD(const TClientOptions&, GetOptions, ())

    // Transactions
    UNIMPLEMENTED_METHOD(TFuture<ITransactionPtr>, StartTransaction, (
        NTransactionClient::ETransactionType,
        const TTransactionStartOptions&))

    // Tables
    UNIMPLEMENTED_METHOD(TFuture<TUnversionedLookupRowsResult>, LookupRows, (
        const NYPath::TYPath&,
        NTableClient::TNameTablePtr,
        const TSharedRange<NTableClient::TLegacyKey>&,
        const TLookupRowsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TVersionedLookupRowsResult>, VersionedLookupRows, (
        const NYPath::TYPath&,
        NTableClient::TNameTablePtr,
        const TSharedRange<NTableClient::TLegacyKey>&,
        const TVersionedLookupRowsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TUnversionedLookupRowsResult>>, MultiLookupRows, (
        const std::vector<TMultiLookupSubrequest>&,
        const TMultiLookupOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const std::string&,
        const TSelectRowsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, ExplainQuery, (
        const std::string&,
        const TExplainQueryOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TPullRowsResult>, PullRows, (
        const NYPath::TYPath&,
        const TPullRowsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (
        const NYPath::TRichYPath&,
        const TTableReaderOptions&))

    UNIMPLEMENTED_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (
        const NYPath::TRichYPath&,
        const TTableWriterOptions&))

    // Queues
    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueue, (
        const NYPath::TRichYPath&,
        i64,
        int,
        const NQueueClient::TQueueRowBatchReadOptions&,
        const TPullQueueOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueueConsumer, (
        const NYPath::TRichYPath&,
        const NYPath::TRichYPath&,
        std::optional<i64>,
        int,
        const NQueueClient::TQueueRowBatchReadOptions&,
        const TPullQueueConsumerOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, RegisterQueueConsumer, (
        const NYPath::TRichYPath&,
        const NYPath::TRichYPath&,
        bool,
        const TRegisterQueueConsumerOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnregisterQueueConsumer, (
        const NYPath::TRichYPath&,
        const NYPath::TRichYPath&,
        const TUnregisterQueueConsumerOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TListQueueConsumerRegistrationsResult>>, ListQueueConsumerRegistrations, (
        const std::optional<NYPath::TRichYPath>&,
        const std::optional<NYPath::TRichYPath>&,
        const TListQueueConsumerRegistrationsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TCreateQueueProducerSessionResult>, CreateQueueProducerSession, (
        const NYPath::TRichYPath&,
        const NYPath::TRichYPath&,
        const NQueueClient::TQueueProducerSessionId&,
        const TCreateQueueProducerSessionOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveQueueProducerSession, (
        const NYPath::TRichYPath&,
        const NYPath::TRichYPath&,
        const NQueueClient::TQueueProducerSessionId&,
        const TRemoveQueueProducerSessionOptions&))

    // Cypress
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetNode, (
        const NYPath::TYPath&,
        const TGetNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SetNode, (
        const NYPath::TYPath&,
        const NYson::TYsonString&,
        const TSetNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, MultisetAttributesNode, (
        const NYPath::TYPath&,
        const NYTree::IMapNodePtr&,
        const TMultisetAttributesNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveNode, (
        const NYPath::TYPath&,
        const TRemoveNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, ListNode, (
        const NYPath::TYPath&,
        const TListNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (
        const NYPath::TYPath&,
        NObjectClient::EObjectType,
        const TCreateNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TLockNodeResult>, LockNode, (
        const NYPath::TYPath&,
        NCypressClient::ELockMode,
        const TLockNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnlockNode, (
        const NYPath::TYPath&,
        const TUnlockNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (
        const NYPath::TYPath&,
        const NYPath::TYPath&,
        const TCopyNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (
        const NYPath::TYPath&,
        const NYPath::TYPath&,
        const TMoveNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (
        const NYPath::TYPath&,
        const NYPath::TYPath&,
        const TLinkNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ConcatenateNodes, (
        const std::vector<NYPath::TRichYPath>&,
        const NYPath::TRichYPath&,
        const TConcatenateNodesOptions&))

    UNIMPLEMENTED_METHOD(TFuture<bool>, NodeExists, (
        const NYPath::TYPath&,
        const TNodeExistsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ExternalizeNode, (
        const NYPath::TYPath&,
        NObjectClient::TCellTag,
        const TExternalizeNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, InternalizeNode, (
        const NYPath::TYPath&,
        const TInternalizeNodeOptions&))

    // Objects
    UNIMPLEMENTED_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (
        NObjectClient::EObjectType,
        const TCreateObjectOptions&))

    // Files
    UNIMPLEMENTED_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (
        const NYPath::TYPath&,
        const TFileReaderOptions&))

    UNIMPLEMENTED_METHOD(IFileWriterPtr, CreateFileWriter, (
        const NYPath::TRichYPath&,
        const TFileWriterOptions&))

    UNIMPLEMENTED_METHOD(IJournalReaderPtr, CreateJournalReader, (
        const NYPath::TYPath&,
        const TJournalReaderOptions&))

    UNIMPLEMENTED_METHOD(IJournalWriterPtr, CreateJournalWriter, (
        const NYPath::TYPath&,
        const TJournalWriterOptions&))

    // Transactions
    UNIMPLEMENTED_METHOD(ITransactionPtr, AttachTransaction, (
        NTransactionClient::TTransactionId,
        const TTransactionAttachOptions&))

    UNIMPLEMENTED_METHOD(IPrerequisitePtr, AttachPrerequisite, (
        NPrerequisiteClient::TPrerequisiteId,
        const TPrerequisiteAttachOptions&))

    // Tables
    UNIMPLEMENTED_METHOD(TFuture<void>, MountTable, (
        const NYPath::TYPath&,
        const TMountTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnmountTable, (
        const NYPath::TYPath&,
        const TUnmountTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemountTable, (
        const NYPath::TYPath&,
        const TRemountTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, FreezeTable, (
        const NYPath::TYPath&,
        const TFreezeTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnfreezeTable, (
        const NYPath::TYPath&,
        const TUnfreezeTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, CancelTabletTransition, (
        NTabletClient::TTabletId,
        const TCancelTabletTransitionOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath&,
        const std::vector<NTableClient::TLegacyOwningKey>&,
        const TReshardTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath&,
        int,
        const TReshardTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, ReshardTableAutomatic, (
        const NYPath::TYPath&,
        const TReshardTableAutomaticOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, TrimTable, (
        const NYPath::TYPath&,
        int,
        i64,
        const TTrimTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTable, (
        const NYPath::TYPath&,
        const TAlterTableOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTableReplica, (
        NTabletClient::TTableReplicaId,
        const TAlterTableReplicaOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterReplicationCard, (
        NChaosClient::TReplicationCardId,
        const TAlterReplicationCardOptions&))

    UNIMPLEMENTED_METHOD(TFuture<IPrerequisitePtr>, StartChaosLease, (
        const TChaosLeaseStartOptions&));

    UNIMPLEMENTED_METHOD(TFuture<IPrerequisitePtr>, AttachChaosLease, (
        NChaosClient::TChaosLeaseId,
        const TChaosLeaseAttachOptions&));

    UNIMPLEMENTED_METHOD(TFuture<void>, SetUserBanned, (
        const std::string&,
        bool,
        const TSetUserBannedOptions& = {}))
    UNIMPLEMENTED_METHOD(TFuture<bool>, GetUserBanned, (
        const std::string&,
        const TGetUserBannedOptions& = {}))
    UNIMPLEMENTED_METHOD(TFuture<std::vector<std::string>>, ListBannedUsers, (
        const TListBannedUsersOptions& = {}))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetTablePivotKeys, (
        const NYPath::TYPath&,
        const TGetTablePivotKeysOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, CreateTableBackup, (
        const TBackupManifestPtr&,
        const TCreateTableBackupOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, RestoreTableBackup, (
        const TBackupManifestPtr&,
        const TRestoreTableBackupOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath&,
        const NTableClient::TNameTablePtr&,
        const TSharedRange<NTableClient::TLegacyKey>&,
        const TGetInSyncReplicasOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath&,
        const TGetInSyncReplicasOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TTabletInfo>>, GetTabletInfos, (
        const NYPath::TYPath&,
        const std::vector<int>&,
        const TGetTabletInfosOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGetTabletErrorsResult>, GetTabletErrors, (
        const NYPath::TYPath&,
        const TGetTabletErrorsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, BalanceTabletCells, (
        const std::string&,
        const std::vector<NYPath::TYPath>&,
        const TBalanceTabletCellsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NChaosClient::TReplicationCardPtr>, GetReplicationCard, (
        NChaosClient::TReplicationCardId,
        const TGetReplicationCardOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateChaosTableReplicaProgress, (
        NChaosClient::TReplicaId,
        const TUpdateChaosTableReplicaProgressOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TSkynetSharePartsLocationsPtr>, LocateSkynetShare, (
        const NYPath::TRichYPath&,
        const TLocateSkynetShareOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTableClient::TColumnarStatistics>>, GetColumnarStatistics, (
        const std::vector<NYPath::TRichYPath>&,
        const TGetColumnarStatisticsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TMultiTablePartitions>, PartitionTables, (
        const std::vector<NYPath::TRichYPath>&,
        const TPartitionTablesOptions&))

    UNIMPLEMENTED_METHOD(TFuture<ITablePartitionReaderPtr>, CreateTablePartitionReader, (
        const TTablePartitionCookiePtr&,
        const TReadTablePartitionOptions&))

    UNIMPLEMENTED_METHOD(TFuture<IFormattedTableReaderPtr>, CreateFormattedTableReader, (
        const NYPath::TRichYPath&,
        const NYson::TYsonString&,
        const TTableReaderOptions&))

    UNIMPLEMENTED_METHOD(TFuture<IFormattedTableReaderPtr>, CreateFormattedTablePartitionReader, (
        const TTablePartitionCookiePtr&,
        const NYson::TYsonString&,
        const TReadTablePartitionOptions&))

    // Journals
    UNIMPLEMENTED_METHOD(TFuture<void>, TruncateJournal, (
        const NYPath::TYPath&,
        i64,
        const TTruncateJournalOptions&))

    // Files
    UNIMPLEMENTED_METHOD(TFuture<TGetFileFromCacheResult>, GetFileFromCache, (
        const std::string&,
        const TGetFileFromCacheOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TPutFileToCacheResult>, PutFileToCache, (
        const NYPath::TYPath&,
        const std::string&,
        const TPutFileToCacheOptions&))

    // Security
    UNIMPLEMENTED_METHOD(TFuture<void>, AddMember, (
        const std::string&,
        const std::string&,
        const TAddMemberOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveMember, (
        const std::string&,
        const std::string&,
        const TRemoveMemberOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionResponse>, CheckPermission, (
        const std::string&,
        const NYPath::TYPath&,
        NYTree::EPermission,
        const TCheckPermissionOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionByAclResult>, CheckPermissionByAcl, (
        const std::optional<std::string>&,
        NYTree::EPermission,
        NYTree::INodePtr,
        const TCheckPermissionByAclOptions&))

    // Accounting
    UNIMPLEMENTED_METHOD(TFuture<void>, TransferAccountResources, (
        const std::string&,
        const std::string&,
        NYTree::INodePtr,
        const TTransferAccountResourcesOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, TransferPoolResources, (
        const std::string&,
        const std::string&,
        const std::string&,
        NYTree::INodePtr,
        const TTransferPoolResourcesOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, TransferBundleResources, (
        const std::string&,
        const std::string&,
        NYTree::INodePtr,
        const TTransferBundleResourcesOptions&))

    // Scheduler
    UNIMPLEMENTED_METHOD(TFuture<NScheduler::TOperationId>, StartOperation, (
        NScheduler::EOperationType,
        const NYson::TYsonString&,
        const TStartOperationOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbortOperation, (
        const NScheduler::TOperationIdOrAlias&,
        const TAbortOperationOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendOperation, (
        const NScheduler::TOperationIdOrAlias&,
        const TSuspendOperationOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeOperation, (
        const NScheduler::TOperationIdOrAlias&,
        const TResumeOperationOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, CompleteOperation, (
        const NScheduler::TOperationIdOrAlias&,
        const TCompleteOperationOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateOperationParameters, (
        const NScheduler::TOperationIdOrAlias&,
        const NYson::TYsonString&,
        const TUpdateOperationParametersOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, PatchOperationSpec, (
        const NScheduler::TOperationIdOrAlias&,
        const NScheduler::TSpecPatchList&,
        const TPatchOperationSpecOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TOperation>, GetOperation, (
        const NScheduler::TOperationIdOrAlias&,
        const TGetOperationOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, DumpJobContext, (
        NJobTrackerClient::TJobId,
        const NYPath::TYPath&,
        const TDumpJobContextOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobInput, (
        NJobTrackerClient::TJobId,
        const TGetJobInputOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobInputPaths, (
        NJobTrackerClient::TJobId,
        const TGetJobInputPathsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobSpec, (
        NJobTrackerClient::TJobId,
        const TGetJobSpecOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGetJobStderrResponse>, GetJobStderr, (
        const NScheduler::TOperationIdOrAlias&,
        NJobTrackerClient::TJobId,
        const TGetJobStderrOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobTrace, (
        const NScheduler::TOperationIdOrAlias&,
        NJobTrackerClient::TJobId,
        const TGetJobTraceOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TSharedRef>, GetJobFailContext, (
        const NScheduler::TOperationIdOrAlias&,
        NJobTrackerClient::TJobId,
        const TGetJobFailContextOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TOperationEvent>>, ListOperationEvents, (
        const NScheduler::TOperationIdOrAlias&,
        const TListOperationEventsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TListOperationsResult>, ListOperations, (
        const TListOperationsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TListJobsResult>, ListJobs, (
        const NScheduler::TOperationIdOrAlias&,
        const TListJobsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TJobTraceMeta>>, ListJobTraces, (
        const NScheduler::TOperationIdOrAlias&,
        NJobTrackerClient::TJobId,
        const TListJobTracesOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TCheckOperationPermissionResult>, CheckOperationPermission, (
        const std::string&,
        const NScheduler::TOperationIdOrAlias&,
        NYTree::EPermission,
        const TCheckOperationPermissionOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJob, (
        const NScheduler::TOperationIdOrAlias&,
        NJobTrackerClient::TJobId,
        const TGetJobOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbandonJob, (
        NJobTrackerClient::TJobId,
        const TAbandonJobOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TPollJobShellResponse>, PollJobShell, (
        NJobTrackerClient::TJobId,
        const std::optional<std::string>&,
        const NYson::TYsonString&,
        const TPollJobShellOptions&))

    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, RunJobShellCommand, (
        NJobTrackerClient::TJobId,
        const std::optional<std::string>&,
        const std::string&,
        const TRunJobShellCommandOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbortJob, (
        NJobTrackerClient::TJobId,
        const TAbortJobOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, DumpJobProxyLog, (
        NJobTrackerClient::TJobId,
        NJobTrackerClient::TOperationId,
        const NYPath::TYPath&,
        const TDumpJobProxyLogOptions&))

    // Metadata
    UNIMPLEMENTED_METHOD(TFuture<TClusterMeta>, GetClusterMeta, (
        const TGetClusterMetaOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, CheckClusterLiveness, (
        const TCheckClusterLivenessOptions&))

    // Administration
    UNIMPLEMENTED_METHOD(TFuture<int>, BuildSnapshot, (
        const TBuildSnapshotOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TCellIdToSnapshotIdMap>, BuildMasterSnapshots, (
        const TBuildMasterSnapshotsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TCellIdToConsistentStateMap>, GetMasterConsistentState, (
        const TGetMasterConsistentStateOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ExitReadOnly, (
        NHydra::TCellId,
        const TExitReadOnlyOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, MasterExitReadOnly, (
        const TMasterExitReadOnlyOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, FreezeHydraPeer, (
        NHydra::TCellId,
        const std::string&,
        const TFreezeHydraPeerOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, TruncateChangelog, (
        NHydra::TCellId,
        const std::string&,
        const TTruncateChangelogOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ScheduleRestart, (
        NHydra::TCellId,
        const std::string&,
        const TScheduleRestartOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResetDynamicallyPropagatedMasterCells, (
        const TResetDynamicallyPropagatedMasterCellsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, DiscombobulateNonvotingPeers, (
        NHydra::TCellId,
        const TDiscombobulateNonvotingPeersOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SwitchLeader, (
        NHydra::TCellId,
        const std::string&,
        const TSwitchLeaderOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResetStateHash, (
        NHydra::TCellId,
        const TResetStateHashOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, GCCollect, (
        const TGCCollectOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, KillProcess, (
        const std::string&,
        const TKillProcessOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::string>, WriteCoreDump, (
        const std::string&,
        const TWriteCoreDumpOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGuid>, WriteLogBarrier, (
        const std::string&,
        const TWriteLogBarrierOptions&))

    UNIMPLEMENTED_METHOD(TFuture<std::string>, WriteOperationControllerCoreDump, (
        NJobTrackerClient::TOperationId,
        const TWriteOperationControllerCoreDumpOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, HealExecNode, (
        const std::string&,
        const THealExecNodeOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendCoordinator, (
        NObjectClient::TCellId,
        const TSuspendCoordinatorOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeCoordinator, (
        NObjectClient::TCellId,
        const TResumeCoordinatorOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, MigrateReplicationCards, (
        NObjectClient::TCellId,
        const TMigrateReplicationCardsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendChaosCells, (
        const std::vector<NObjectClient::TCellId>&,
        const TSuspendChaosCellsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeChaosCells, (
        const std::vector<NObjectClient::TCellId>&,
        const TResumeChaosCellsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendTabletCells, (
        const std::vector<NObjectClient::TCellId>&,
        const TSuspendTabletCellsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeTabletCells, (
        const std::vector<NObjectClient::TCellId>&,
        const TResumeTabletCellsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceIdPerTarget>, AddMaintenance, (
        EMaintenanceComponent,
        const std::string&,
        EMaintenanceType,
        const std::string&,
        const TAddMaintenanceOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceCountsPerTarget>, RemoveMaintenance, (
        EMaintenanceComponent,
        const std::string&,
        const TMaintenanceFilter&,
        const TRemoveMaintenanceOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TDisableChunkLocationsResult>, DisableChunkLocations, (
        const std::string&,
        const std::vector<TGuid>&,
        const TDisableChunkLocationsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TDestroyChunkLocationsResult>, DestroyChunkLocations, (
        const std::string&,
        bool,
        const std::vector<TGuid>&,
        const TDestroyChunkLocationsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TResurrectChunkLocationsResult>, ResurrectChunkLocations, (
        const std::string&,
        const std::vector<TGuid>&,
        const TResurrectChunkLocationsOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TRequestRestartResult>, RequestRestart, (
        const std::string&,
        const TRequestRestartOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TCollectCoverageResult>, CollectCoverage, (
        const std::string&,
        const TCollectCoverageOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SetUserPassword, (
        const std::string&,
        const std::string&,
        const std::string&,
        const TSetUserPasswordOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TIssueTokenResult>, IssueToken, (
        const std::string&,
        const std::string&,
        const TIssueTokenOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, RevokeToken, (
        const std::string&,
        const std::string&,
        const std::string&,
        const TRevokeTokenOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TListUserTokensResult>, ListUserTokens, (
        const std::string&,
        const std::string&,
        const TListUserTokensOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGetCurrentUserResult>, GetCurrentUser, (
        const TGetCurrentUserOptions&))

    // Query tracker
    UNIMPLEMENTED_METHOD(TFuture<NQueryTrackerClient::TQueryId>, StartQuery, (
        NQueryTrackerClient::EQueryEngine,
        const std::string&,
        const TStartQueryOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbortQuery, (
        NQueryTrackerClient::TQueryId,
        const TAbortQueryOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TQueryResult>, GetQueryResult, (
        NQueryTrackerClient::TQueryId,
        i64,
        const TGetQueryResultOptions&))

    UNIMPLEMENTED_METHOD(TFuture<IUnversionedRowsetPtr>, ReadQueryResult, (
        NQueryTrackerClient::TQueryId,
        i64,
        const TReadQueryResultOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TQuery>, GetQuery, (
        NQueryTrackerClient::TQueryId,
        const TGetQueryOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TListQueriesResult>, ListQueries, (
        const TListQueriesOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterQuery, (
        NQueryTrackerClient::TQueryId,
        const TAlterQueryOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGetQueryTrackerInfoResult>, GetQueryTrackerInfo, (
        const TGetQueryTrackerInfoOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGetQueryDeclaredParametersInfoResult>, GetQueryDeclaredParametersInfo, (
        const TGetQueryDeclaredParametersInfoOptions&))

    // Bundle Controller
    UNIMPLEMENTED_METHOD(TFuture<NBundleControllerClient::TBundleConfigDescriptorPtr>, GetBundleConfig, (
        const std::string&,
        const NBundleControllerClient::TGetBundleConfigOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, SetBundleConfig, (
        const std::string&,
        const NBundleControllerClient::TBundleTargetConfigPtr&,
        const NBundleControllerClient::TSetBundleConfigOptions&))

    // Flow
    UNIMPLEMENTED_METHOD(TFuture<TGetPipelineSpecResult>, GetPipelineSpec, (
        const NYPath::TYPath&,
        const TGetPipelineSpecOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TSetPipelineSpecResult>, SetPipelineSpec, (
        const NYPath::TYPath&,
        const NYson::TYsonString&,
        const TSetPipelineSpecOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGetPipelineDynamicSpecResult>, GetPipelineDynamicSpec, (
        const NYPath::TYPath&,
        const TGetPipelineDynamicSpecOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TSetPipelineDynamicSpecResult>, SetPipelineDynamicSpec, (
        const NYPath::TYPath&,
        const NYson::TYsonString&,
        const TSetPipelineDynamicSpecOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, StartPipeline, (
        const NYPath::TYPath&,
        const TStartPipelineOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, StopPipeline, (
        const NYPath::TYPath&,
        const TStopPipelineOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, PausePipeline, (
        const NYPath::TYPath&,
        const TPausePipelineOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TPipelineState>, GetPipelineState, (
        const NYPath::TYPath&,
        const TGetPipelineStateOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TGetFlowViewResult>, GetFlowView, (
        const NYPath::TYPath&,
        const NYPath::TYPath&,
        const TGetFlowViewOptions&))

    UNIMPLEMENTED_METHOD(TFuture<TFlowExecuteResult>, FlowExecute, (
        const NYPath::TYPath&,
        const std::string&,
        const NYson::TYsonString&,
        const TFlowExecuteOptions& = {}))

    // Distributed client
    UNIMPLEMENTED_METHOD(TFuture<TDistributedWriteSessionWithCookies>, StartDistributedWriteSession, (
        const NYPath::TRichYPath&,
        const TDistributedWriteSessionStartOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, PingDistributedWriteSession, (
        TSignedDistributedWriteSessionPtr,
        const TDistributedWriteSessionPingOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, FinishDistributedWriteSession, (
        const TDistributedWriteSessionWithResults&,
        const TDistributedWriteSessionFinishOptions&))

    UNIMPLEMENTED_METHOD(TFuture<ITableFragmentWriterPtr>, CreateTableFragmentWriter, (
        const TSignedWriteFragmentCookiePtr&,
        const TTableFragmentWriterOptions&))

    // Distributed file client
    UNIMPLEMENTED_METHOD(TFuture<TDistributedWriteFileSessionWithCookies>, StartDistributedWriteFileSession, (
        const NYPath::TRichYPath&,
        const TDistributedWriteFileSessionStartOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, PingDistributedWriteFileSession, (
        const TSignedDistributedWriteFileSessionPtr&,
        const TDistributedWriteFileSessionPingOptions&))

    UNIMPLEMENTED_METHOD(TFuture<void>, FinishDistributedWriteFileSession, (
        const TDistributedWriteFileSessionWithResults&,
        const TDistributedWriteFileSessionFinishOptions&))

    UNIMPLEMENTED_METHOD(IFileFragmentWriterPtr, CreateFileFragmentWriter, (
        const TSignedWriteFileFragmentCookiePtr&,
        const TFileFragmentWriterOptions&))

    // Shuffle Service
    UNIMPLEMENTED_METHOD(TFuture<TSignedShuffleHandlePtr>, StartShuffle, (
        const std::string&,
        int,
        NObjectClient::TTransactionId,
        const TStartShuffleOptions&))

    UNIMPLEMENTED_METHOD(TFuture<IRowBatchReaderPtr>, CreateShuffleReader, (
        const TSignedShuffleHandlePtr&,
        int,
        std::optional<std::pair<int, int>>,
        const TShuffleReaderOptions&))

    UNIMPLEMENTED_METHOD(TFuture<IRowBatchWriterPtr>, CreateShuffleWriter, (
        const TSignedShuffleHandlePtr&,
        const std::string&,
        std::optional<int>,
        const TShuffleWriterOptions&))

    #undef UNIMPLEMENTED_METHOD
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
