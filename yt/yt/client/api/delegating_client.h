#pragma once

#include "client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that implements IClient and delegates
//! all calls to the underlying instance.
class TDelegatingClient
    : public IClient
{
public:
    explicit TDelegatingClient(IClientPtr underlying);

    #define DELEGATE_METHOD(returnType, method, signature, args) \
        returnType method signature override \
        { \
            return Underlying_->method args; \
        }

    // IClientBase methods
    DELEGATE_METHOD(IConnectionPtr, GetConnection, (), ())

    DELEGATE_METHOD(std::optional<TStringBuf>, GetClusterName,
        (bool fetchIfNull),
        (fetchIfNull))

    // IClient methods
    DELEGATE_METHOD(void, Terminate, (), ())

    DELEGATE_METHOD(const NTabletClient::ITableMountCachePtr&, GetTableMountCache, (), ())

    DELEGATE_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, (), ())

    DELEGATE_METHOD(const NTransactionClient::ITimestampProviderPtr&, GetTimestampProvider, (), ())

    // Transactions
    DELEGATE_METHOD(TFuture<ITransactionPtr>, StartTransaction, (
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options),
        (type, options))

    // Tables
    DELEGATE_METHOD(TFuture<TUnversionedLookupRowsResult>, LookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options),
        (path, nameTable, keys, options))

    DELEGATE_METHOD(TFuture<TVersionedLookupRowsResult>, VersionedLookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, nameTable, keys, options))

    DELEGATE_METHOD(TFuture<std::vector<TUnversionedLookupRowsResult>>, MultiLookupRows, (
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options),
        (subrequests, options))

    DELEGATE_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options),
        (query, options))

    DELEGATE_METHOD(TFuture<NYson::TYsonString>, ExplainQuery, (
        const TString& query,
        const TExplainQueryOptions& options),
        (query, options))

    DELEGATE_METHOD(TFuture<TPullRowsResult>, PullRows, (
        const NYPath::TYPath& path,
        const TPullRowsOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options),
        (path, options))

    // Queues
    DELEGATE_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueue, (
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options),
        (queuePath, offset, partitionIndex, rowBatchReadOptions, options))

    DELEGATE_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueueConsumer, (
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        std::optional<i64> offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueConsumerOptions& options),
        (consumerPath, queuePath, offset, partitionIndex, rowBatchReadOptions, options))

    DELEGATE_METHOD(TFuture<void>, RegisterQueueConsumer, (
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        bool vital,
        const TRegisterQueueConsumerOptions& options),
        (queuePath, consumerPath, vital, options))

    DELEGATE_METHOD(TFuture<void>, UnregisterQueueConsumer, (
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        const TUnregisterQueueConsumerOptions& options),
        (queuePath, consumerPath, options))

    DELEGATE_METHOD(TFuture<std::vector<TListQueueConsumerRegistrationsResult>>, ListQueueConsumerRegistrations, (
        const std::optional<NYPath::TRichYPath>& queuePath,
        const std::optional<NYPath::TRichYPath>& consumerPath,
        const TListQueueConsumerRegistrationsOptions& options),
        (queuePath, consumerPath, options))

    DELEGATE_METHOD(TFuture<TCreateQueueProducerSessionResult>, CreateQueueProducerSession, (
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TCreateQueueProducerSessionOptions& options),
        (producerPath, queuePath, sessionId, options))

    DELEGATE_METHOD(TFuture<void>, RemoveQueueProducerSession, (
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        const TRemoveQueueProducerSessionOptions& options),
        (producerPath, queuePath, sessionId, options))

    // Cypress
    DELEGATE_METHOD(TFuture<NYson::TYsonString>, GetNode, (
        const NYPath::TYPath& path,
        const TGetNodeOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, SetNode, (
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options),
        (path, value, options))

    DELEGATE_METHOD(TFuture<void>, MultisetAttributesNode, (
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options),
        (path, attributes, options))

    DELEGATE_METHOD(TFuture<void>, RemoveNode, (
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<NYson::TYsonString>, ListNode, (
        const NYPath::TYPath& path,
        const TListNodeOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))

    DELEGATE_METHOD(TFuture<TLockNodeResult>, LockNode, (
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options),
        (path, mode, options))

    DELEGATE_METHOD(TFuture<void>, UnlockNode, (
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options),
        (srcPath, dstPath, options))

    DELEGATE_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options),
        (srcPath, dstPath, options))

    DELEGATE_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options),
        (srcPath, dstPath, options))

    DELEGATE_METHOD(TFuture<void>, ConcatenateNodes, (
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options),
        (srcPaths, dstPath, options))

    DELEGATE_METHOD(TFuture<bool>, NodeExists, (
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, ExternalizeNode, (
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options),
        (path, cellTag, options))

    DELEGATE_METHOD(TFuture<void>, InternalizeNode, (
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options),
        (path, options))

    // Objects
    DELEGATE_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))

    // Files
    DELEGATE_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (
        const NYPath::TYPath& path,
        const TFileReaderOptions& options),
        (path, options))

    DELEGATE_METHOD(IFileWriterPtr, CreateFileWriter, (
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options),
        (path, options))

    DELEGATE_METHOD(IJournalReaderPtr, CreateJournalReader, (
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options),
        (path, options))

    DELEGATE_METHOD(IJournalWriterPtr, CreateJournalWriter, (
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options),
        (path, options))

    // Transactions
    DELEGATE_METHOD(ITransactionPtr, AttachTransaction, (
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options),
        (transactionId, options))

    // Tables
    DELEGATE_METHOD(TFuture<void>, MountTable, (
        const NYPath::TYPath& path,
        const TMountTableOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, UnmountTable, (
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, RemountTable, (
        const NYPath::TYPath& path,
        const TRemountTableOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, FreezeTable, (
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, UnfreezeTable, (
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const TReshardTableOptions& options),
        (path, pivotKeys, options))

    DELEGATE_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options),
        (path, tabletCount, options))

    DELEGATE_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, ReshardTableAutomatic, (
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, TrimTable, (
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options),
        (path, tabletIndex, trimmedRowCount, options))

    DELEGATE_METHOD(TFuture<void>, AlterTable, (
        const NYPath::TYPath& path,
        const TAlterTableOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, AlterTableReplica, (
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options),
        (replicaId, options))

    DELEGATE_METHOD(TFuture<void>, AlterReplicationCard, (
        NChaosClient::TReplicationCardId replicationCardId,
        const TAlterReplicationCardOptions& options),
        (replicationCardId, options))

    DELEGATE_METHOD(TFuture<NYson::TYsonString>, GetTablePivotKeys, (
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, CreateTableBackup, (
        const TBackupManifestPtr& manifest,
        const TCreateTableBackupOptions& options),
        (manifest, options))

    DELEGATE_METHOD(TFuture<void>, RestoreTableBackup, (
        const TBackupManifestPtr& manifest,
        const TRestoreTableBackupOptions& options),
        (manifest, options))

    DELEGATE_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options),
        (path, nameTable, keys, options))

    DELEGATE_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath& path,
        const TGetInSyncReplicasOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<std::vector<TTabletInfo>>, GetTabletInfos, (
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options),
        (path, tabletIndexes, options))

    DELEGATE_METHOD(TFuture<TGetTabletErrorsResult>, GetTabletErrors, (
        const NYPath::TYPath& path,
        const TGetTabletErrorsOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, BalanceTabletCells, (
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options),
        (tabletCellBundle, movableTables, options))

    DELEGATE_METHOD(TFuture<NChaosClient::TReplicationCardPtr>, GetReplicationCard, (
        NChaosClient::TReplicationCardId replicationCardId,
        const TGetReplicationCardOptions& options),
        (replicationCardId, options))

    DELEGATE_METHOD(TFuture<void>, UpdateChaosTableReplicaProgress, (
        NChaosClient::TReplicaId replicaId,
        const TUpdateChaosTableReplicaProgressOptions& options),
        (replicaId, options))

    DELEGATE_METHOD(TFuture<TSkynetSharePartsLocationsPtr>, LocateSkynetShare, (
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<std::vector<NTableClient::TColumnarStatistics>>, GetColumnarStatistics, (
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<TMultiTablePartitions>, PartitionTables, (
        const std::vector<NYPath::TRichYPath>& paths,
        const TPartitionTablesOptions& options),
        (paths, options))

    // Journals
    DELEGATE_METHOD(TFuture<void>, TruncateJournal, (
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options),
        (path, rowCount, options))

    // Files
    DELEGATE_METHOD(TFuture<TGetFileFromCacheResult>, GetFileFromCache, (
        const TString& md5,
        const TGetFileFromCacheOptions& options),
        (md5, options))

    DELEGATE_METHOD(TFuture<TPutFileToCacheResult>, PutFileToCache, (
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options),
        (path, expectedMD5, options))

    // Security
    DELEGATE_METHOD(TFuture<void>, AddMember, (
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options),
        (group, member, options))

    DELEGATE_METHOD(TFuture<void>, RemoveMember, (
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options),
        (group, member, options))

    DELEGATE_METHOD(TFuture<TCheckPermissionResponse>, CheckPermission, (
        const std::string& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options),
        (user, path, permission, options))

    DELEGATE_METHOD(TFuture<TCheckPermissionByAclResult>, CheckPermissionByAcl, (
        const std::optional<std::string>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options),
        (user, permission, acl, options))

    DELEGATE_METHOD(TFuture<void>, TransferAccountResources, (
        const TString& srcAccount,
        const TString& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options),
        (srcAccount, dstAccount, resourceDelta, options))

    // Scheduler
    DELEGATE_METHOD(TFuture<void>, TransferPoolResources, (
        const TString& srcPool,
        const TString& dstPool,
        const TString& poolTree,
        NYTree::INodePtr resourceDelta,
        const TTransferPoolResourcesOptions& options),
        (srcPool, dstPool, poolTree, resourceDelta, options))

    DELEGATE_METHOD(TFuture<NScheduler::TOperationId>, StartOperation, (
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options),
        (type, spec, options))

    DELEGATE_METHOD(TFuture<void>, AbortOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options),
        (operationIdOrAlias, options))

    DELEGATE_METHOD(TFuture<void>, SuspendOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options),
        (operationIdOrAlias, options))

    DELEGATE_METHOD(TFuture<void>, ResumeOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options),
        (operationIdOrAlias, options))

    DELEGATE_METHOD(TFuture<void>, CompleteOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options),
        (operationIdOrAlias, options))

    DELEGATE_METHOD(TFuture<void>, UpdateOperationParameters, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options),
        (operationIdOrAlias, parameters, options))

    DELEGATE_METHOD(TFuture<TOperation>, GetOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options),
        (operationIdOrAlias, options))

    DELEGATE_METHOD(TFuture<void>, DumpJobContext, (
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options),
        (jobId, path, options))

    DELEGATE_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobInput, (
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputOptions& options),
        (jobId, options))

    DELEGATE_METHOD(TFuture<NYson::TYsonString>, GetJobInputPaths, (
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputPathsOptions& options),
        (jobId, options))

    DELEGATE_METHOD(TFuture<NYson::TYsonString>, GetJobSpec, (
        NJobTrackerClient::TJobId jobId,
        const TGetJobSpecOptions& options),
        (jobId, options))

    DELEGATE_METHOD(TFuture<TSharedRef>, GetJobStderr, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobStderrOptions& options),
        (operationIdOrAlias, jobId, options))

    DELEGATE_METHOD(TFuture<TSharedRef>, GetJobFailContext, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobFailContextOptions& options),
        (operationIdOrAlias, jobId, options))

    DELEGATE_METHOD(TFuture<TListOperationsResult>, ListOperations, (
        const TListOperationsOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<TListJobsResult>, ListJobs, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TListJobsOptions& options),
        (operationIdOrAlias, options))

    DELEGATE_METHOD(TFuture<NYson::TYsonString>, GetJob, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NJobTrackerClient::TJobId jobId,
        const TGetJobOptions& options),
        (operationIdOrAlias, jobId, options))

    DELEGATE_METHOD(TFuture<void>, AbandonJob, (
        NJobTrackerClient::TJobId jobId,
        const TAbandonJobOptions& options),
        (jobId, options))

    DELEGATE_METHOD(TFuture<TPollJobShellResponse>, PollJobShell, (
        NJobTrackerClient::TJobId jobId,
        const std::optional<TString>& shellName,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options),
        (jobId, shellName, parameters, options))

    DELEGATE_METHOD(TFuture<void>, AbortJob, (
        NJobTrackerClient::TJobId jobId,
        const TAbortJobOptions& options),
        (jobId, options))

    DELEGATE_METHOD(TFuture<void>, DumpJobProxyLog, (
        NJobTrackerClient::TJobId jobId,
        NJobTrackerClient::TOperationId operationId,
        const NYPath::TYPath& path,
        const TDumpJobProxyLogOptions& options),
        (jobId, operationId, path, options))

    // Metadata
    DELEGATE_METHOD(TFuture<TClusterMeta>, GetClusterMeta, (
        const TGetClusterMetaOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<void>, CheckClusterLiveness, (
        const TCheckClusterLivenessOptions& options),
        (options))

    // Administration
    DELEGATE_METHOD(TFuture<int>, BuildSnapshot, (
        const TBuildSnapshotOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<TCellIdToSnapshotIdMap>, BuildMasterSnapshots, (
        const TBuildMasterSnapshotsOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<TCellIdToConsistentStateMap>, GetMasterConsistentState, (
        const TGetMasterConsistentStateOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<void>, ExitReadOnly, (
        NHydra::TCellId cellId,
        const TExitReadOnlyOptions& options),
        (cellId, options))

    DELEGATE_METHOD(TFuture<void>, MasterExitReadOnly, (
        const TMasterExitReadOnlyOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<void>, DiscombobulateNonvotingPeers, (
        NHydra::TCellId cellId,
        const TDiscombobulateNonvotingPeersOptions& options),
        (cellId, options))

    DELEGATE_METHOD(TFuture<void>, SwitchLeader, (
        NHydra::TCellId cellId,
        const std::string& newLeaderAddress,
        const TSwitchLeaderOptions& options),
        (cellId, newLeaderAddress, options))

    DELEGATE_METHOD(TFuture<void>, ResetStateHash, (
        NHydra::TCellId cellId,
        const TResetStateHashOptions& options),
        (cellId, options))

    DELEGATE_METHOD(TFuture<void>, GCCollect, (
        const TGCCollectOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<void>, KillProcess, (
        const std::string& address,
        const TKillProcessOptions& options),
        (address, options))

    DELEGATE_METHOD(TFuture<TString>, WriteCoreDump, (
        const std::string& address,
        const TWriteCoreDumpOptions& options),
        (address, options))

    DELEGATE_METHOD(TFuture<TGuid>, WriteLogBarrier, (
        const std::string& address,
        const TWriteLogBarrierOptions& options),
        (address, options))

    DELEGATE_METHOD(TFuture<TString>, WriteOperationControllerCoreDump, (
        NJobTrackerClient::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options),
        (operationId, options))

    DELEGATE_METHOD(TFuture<void>, HealExecNode, (
        const std::string& address,
        const THealExecNodeOptions& options),
        (address, options))

    DELEGATE_METHOD(TFuture<void>, SuspendCoordinator, (
        NObjectClient::TCellId coordinatorCellId,
        const TSuspendCoordinatorOptions& options),
        (coordinatorCellId, options))

    DELEGATE_METHOD(TFuture<void>, ResumeCoordinator, (
        NObjectClient::TCellId coordinatorCellId,
        const TResumeCoordinatorOptions& options),
        (coordinatorCellId, options))

    DELEGATE_METHOD(TFuture<void>, MigrateReplicationCards, (
        NObjectClient::TCellId chaosCellId,
        const TMigrateReplicationCardsOptions& options),
        (chaosCellId, options))

    DELEGATE_METHOD(TFuture<void>, SuspendChaosCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendChaosCellsOptions& options),
        (cellIds, options))

    DELEGATE_METHOD(TFuture<void>, ResumeChaosCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeChaosCellsOptions& options),
        (cellIds, options))

    DELEGATE_METHOD(TFuture<void>, SuspendTabletCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendTabletCellsOptions& options),
        (cellIds, options))

    DELEGATE_METHOD(TFuture<void>, ResumeTabletCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeTabletCellsOptions& options),
        (cellIds, options))

    DELEGATE_METHOD(TFuture<TMaintenanceIdPerTarget>, AddMaintenance, (
        EMaintenanceComponent component,
        const std::string& address,
        EMaintenanceType type,
        const TString& comment,
        const TAddMaintenanceOptions& options),
        (component, address, type, comment, options))

    DELEGATE_METHOD(TFuture<TMaintenanceCountsPerTarget>, RemoveMaintenance, (
        EMaintenanceComponent component,
        const std::string& address,
        const TMaintenanceFilter& filter,
        const TRemoveMaintenanceOptions& options),
        (component, address, filter, options))

    DELEGATE_METHOD(TFuture<TDisableChunkLocationsResult>, DisableChunkLocations, (
        const std::string& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TDisableChunkLocationsOptions& options),
        (nodeAddress, locationUuids, options))

    DELEGATE_METHOD(TFuture<TDestroyChunkLocationsResult>, DestroyChunkLocations, (
        const std::string& nodeAddress,
        bool recoverUnlinkedDisks,
        const std::vector<TGuid>& locationUuids,
        const TDestroyChunkLocationsOptions& options),
        (nodeAddress, recoverUnlinkedDisks, locationUuids, options))

    DELEGATE_METHOD(TFuture<TResurrectChunkLocationsResult>, ResurrectChunkLocations, (
        const std::string& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TResurrectChunkLocationsOptions& options),
        (nodeAddress, locationUuids, options))

    DELEGATE_METHOD(TFuture<TRequestRestartResult>, RequestRestart, (
        const std::string& nodeAddress,
        const TRequestRestartOptions& options),
        (nodeAddress, options))

    DELEGATE_METHOD(TFuture<void>, SetUserPassword, (
        const std::string& user,
        const TString& currentPasswordSha256,
        const TString& newPasswordSha256,
        const TSetUserPasswordOptions& options),
        (user, currentPasswordSha256, newPasswordSha256, options))

    DELEGATE_METHOD(TFuture<TIssueTokenResult>, IssueToken, (
        const std::string& user,
        const TString& passwordSha256,
        const TIssueTokenOptions& options),
        (user, passwordSha256, options))

    DELEGATE_METHOD(TFuture<void>, RevokeToken, (
        const std::string& user,
        const TString& passwordSha256,
        const TString& tokenSha256,
        const TRevokeTokenOptions& options),
        (user, passwordSha256, tokenSha256, options))

    DELEGATE_METHOD(TFuture<TListUserTokensResult>, ListUserTokens, (
        const std::string& user,
        const TString& passwordSha256,
        const TListUserTokensOptions& options),
        (user, passwordSha256, options))

    // Query tracker
    DELEGATE_METHOD(TFuture<NQueryTrackerClient::TQueryId>, StartQuery, (
        NQueryTrackerClient::EQueryEngine engine,
        const TString& query,
        const TStartQueryOptions& options),
        (engine, query, options))

    DELEGATE_METHOD(TFuture<void>, AbortQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TAbortQueryOptions& options),
        (queryId, options))

    DELEGATE_METHOD(TFuture<TQueryResult>, GetQueryResult, (
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex,
        const TGetQueryResultOptions& options),
        (queryId, resultIndex, options))

    DELEGATE_METHOD(TFuture<IUnversionedRowsetPtr>, ReadQueryResult, (
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex,
        const TReadQueryResultOptions& options),
        (queryId, resultIndex, options))

    DELEGATE_METHOD(TFuture<TQuery>, GetQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TGetQueryOptions& options),
        (queryId, options))

    DELEGATE_METHOD(TFuture<TListQueriesResult>, ListQueries, (
        const TListQueriesOptions& options),
        (options))

    DELEGATE_METHOD(TFuture<void>, AlterQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TAlterQueryOptions& options),
        (queryId, options))

    DELEGATE_METHOD(TFuture<TGetQueryTrackerInfoResult>, GetQueryTrackerInfo, (
        const TGetQueryTrackerInfoOptions& options),
        (options))

    // Bundle Controller
    DELEGATE_METHOD(TFuture<NBundleControllerClient::TBundleConfigDescriptorPtr>, GetBundleConfig, (
        const TString& bundleName,
        const NBundleControllerClient::TGetBundleConfigOptions& options),
        (bundleName, options))

    DELEGATE_METHOD(TFuture<void>, SetBundleConfig, (
        const TString& bundleName,
        const NBundleControllerClient::TBundleTargetConfigPtr& bundleConfig,
        const NBundleControllerClient::TSetBundleConfigOptions& options),
        (bundleName, bundleConfig, options))

    // Flow
    DELEGATE_METHOD(TFuture<TGetPipelineSpecResult>, GetPipelineSpec, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineSpecOptions& options),
        (pipelinePath, options))

    DELEGATE_METHOD(TFuture<TSetPipelineSpecResult>, SetPipelineSpec, (
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineSpecOptions& options),
        (pipelinePath, spec, options))

    DELEGATE_METHOD(TFuture<TGetPipelineDynamicSpecResult>, GetPipelineDynamicSpec, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineDynamicSpecOptions& options),
        (pipelinePath, options))

    DELEGATE_METHOD(TFuture<TSetPipelineDynamicSpecResult>, SetPipelineDynamicSpec, (
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineDynamicSpecOptions& options),
        (pipelinePath, spec, options))

    DELEGATE_METHOD(TFuture<void>, StartPipeline, (
        const NYPath::TYPath& pipelinePath,
        const TStartPipelineOptions& options),
        (pipelinePath, options))

    DELEGATE_METHOD(TFuture<void>, StopPipeline, (
        const NYPath::TYPath& pipelinePath,
        const TStopPipelineOptions& options),
        (pipelinePath, options))

    DELEGATE_METHOD(TFuture<void>, PausePipeline, (
        const NYPath::TYPath& pipelinePath,
        const TPausePipelineOptions& options),
        (pipelinePath, options))

    DELEGATE_METHOD(TFuture<TPipelineState>, GetPipelineState, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineStateOptions& options),
        (pipelinePath, options))

    DELEGATE_METHOD(TFuture<TGetFlowViewResult>, GetFlowView, (
        const NYPath::TYPath& pipelinePath,
        const NYPath::TYPath& viewPath,
        const TGetFlowViewOptions& options),
        (pipelinePath, viewPath, options))

    // Distributed client
    DELEGATE_METHOD(TFuture<TDistributedWriteSessionPtr>, StartDistributedWriteSession, (
        const NYPath::TRichYPath& path,
        const TDistributedWriteSessionStartOptions& options),
        (path, options))

    DELEGATE_METHOD(TFuture<void>, FinishDistributedWriteSession, (
        TDistributedWriteSessionPtr session,
        const TDistributedWriteSessionFinishOptions& options),
        (std::move(session), options))

    DELEGATE_METHOD(TFuture<ITableWriterPtr>, CreateParticipantTableWriter, (
        const TDistributedWriteCookiePtr& cookie,
        const TParticipantTableWriterOptions& options),
        (cookie, options))

    #undef DELEGATE_METHOD

protected:
    const IClientPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

