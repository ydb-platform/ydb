#pragma once

#include "client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that implements IClient and throws
//! an exception ("Not implemented ...") on any method call.
class TNotImplementedClient
    : public IClient
{
public:
    #define UNIMPLEMENTED_METHOD(returnType, method, signature) \
        returnType method signature override \
        { \
            THROW_ERROR_EXCEPTION("Not implemented method %v", #method); \
        }

    // IClientBase methods
    UNIMPLEMENTED_METHOD(IConnectionPtr, GetConnection, ())

    UNIMPLEMENTED_METHOD(TFuture<std::optional<std::string>>, GetClusterName,
        (bool /*fetchIfNull*/))

    // IClient methods
    UNIMPLEMENTED_METHOD(void, Terminate, ())

    UNIMPLEMENTED_METHOD(const NTabletClient::ITableMountCachePtr&, GetTableMountCache, ())

    UNIMPLEMENTED_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, ())

    UNIMPLEMENTED_METHOD(const NTransactionClient::ITimestampProviderPtr&, GetTimestampProvider, ())
    UNIMPLEMENTED_METHOD(const TClientOptions&, GetOptions, ())

    // Transactions
    UNIMPLEMENTED_METHOD(TFuture<ITransactionPtr>, StartTransaction, (
        NTransactionClient::ETransactionType /*type*/,
        const TTransactionStartOptions& /*options*/))

    // Tables
    UNIMPLEMENTED_METHOD(TFuture<TUnversionedLookupRowsResult>, LookupRows, (
        const NYPath::TYPath& /*path*/,
        NTableClient::TNameTablePtr /*nameTable*/,
        const TSharedRange<NTableClient::TLegacyKey>& /*keys*/,
        const TLookupRowsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TVersionedLookupRowsResult>, VersionedLookupRows, (
        const NYPath::TYPath& /*path*/,
        NTableClient::TNameTablePtr /*nameTable*/,
        const TSharedRange<NTableClient::TLegacyKey>& /*keys*/,
        const TVersionedLookupRowsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TUnversionedLookupRowsResult>>, MultiLookupRows, (
        const std::vector<TMultiLookupSubrequest>& /*subrequests*/,
        const TMultiLookupOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const std::string& /*query*/,
        const TSelectRowsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, ExplainQuery, (
        const std::string& /*query*/,
        const TExplainQueryOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TPullRowsResult>, PullRows, (
        const NYPath::TYPath& /*path*/,
        const TPullRowsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (
        const NYPath::TRichYPath& /*path*/,
        const TTableReaderOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (
        const NYPath::TRichYPath& /*path*/,
        const TTableWriterOptions& /*options*/))

    // Queues
    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueue, (
        const NYPath::TRichYPath& /*queuePath*/,
        i64 /*offset*/,
        int /*partitionIndex*/,
        const NQueueClient::TQueueRowBatchReadOptions& /*rowBatchReadOptions*/,
        const TPullQueueOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueueConsumer, (
        const NYPath::TRichYPath& /*consumerPath*/,
        const NYPath::TRichYPath& /*queuePath*/,
        std::optional<i64> /*offset*/,
        int /*partitionIndex*/,
        const NQueueClient::TQueueRowBatchReadOptions& /*rowBatchReadOptions*/,
        const TPullQueueConsumerOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, RegisterQueueConsumer, (
        const NYPath::TRichYPath& /*queuePath*/,
        const NYPath::TRichYPath& /*consumerPath*/,
        bool /*vital*/,
        const TRegisterQueueConsumerOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnregisterQueueConsumer, (
        const NYPath::TRichYPath& /*queuePath*/,
        const NYPath::TRichYPath& /*consumerPath*/,
        const TUnregisterQueueConsumerOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TListQueueConsumerRegistrationsResult>>, ListQueueConsumerRegistrations, (
        const std::optional<NYPath::TRichYPath>& /*queuePath*/,
        const std::optional<NYPath::TRichYPath>& /*consumerPath*/,
        const TListQueueConsumerRegistrationsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TCreateQueueProducerSessionResult>, CreateQueueProducerSession, (
        const NYPath::TRichYPath& /*producerPath*/,
        const NYPath::TRichYPath& /*queuePath*/,
        const NQueueClient::TQueueProducerSessionId& /*sessionId*/,
        const TCreateQueueProducerSessionOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveQueueProducerSession, (
        const NYPath::TRichYPath& /*producerPath*/,
        const NYPath::TRichYPath& /*queuePath*/,
        const NQueueClient::TQueueProducerSessionId& /*sessionId*/,
        const TRemoveQueueProducerSessionOptions& /*options*/))

    // Cypress
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetNode, (
        const NYPath::TYPath& /*path*/,
        const TGetNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SetNode, (
        const NYPath::TYPath& /*path*/,
        const NYson::TYsonString& /*value*/,
        const TSetNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, MultisetAttributesNode, (
        const NYPath::TYPath& /*path*/,
        const NYTree::IMapNodePtr& /*attributes*/,
        const TMultisetAttributesNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveNode, (
        const NYPath::TYPath& /*path*/,
        const TRemoveNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, ListNode, (
        const NYPath::TYPath& /*path*/,
        const TListNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (
        const NYPath::TYPath& /*path*/,
        NObjectClient::EObjectType /*type*/,
        const TCreateNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TLockNodeResult>, LockNode, (
        const NYPath::TYPath& /*path*/,
        NCypressClient::ELockMode /*mode*/,
        const TLockNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnlockNode, (
        const NYPath::TYPath& /*path*/,
        const TUnlockNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (
        const NYPath::TYPath& /*srcPath*/,
        const NYPath::TYPath& /*dstPath*/,
        const TCopyNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (
        const NYPath::TYPath& /*srcPath*/,
        const NYPath::TYPath& /*dstPath*/,
        const TMoveNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (
        const NYPath::TYPath& /*srcPath*/,
        const NYPath::TYPath& /*dstPath*/,
        const TLinkNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ConcatenateNodes, (
        const std::vector<NYPath::TRichYPath>& /*srcPaths*/,
        const NYPath::TRichYPath& /*dstPath*/,
        const TConcatenateNodesOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<bool>, NodeExists, (
        const NYPath::TYPath& /*path*/,
        const TNodeExistsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ExternalizeNode, (
        const NYPath::TYPath& /*path*/,
        NObjectClient::TCellTag /*cellTag*/,
        const TExternalizeNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, InternalizeNode, (
        const NYPath::TYPath& /*path*/,
        const TInternalizeNodeOptions& /*options*/))

    // Objects
    UNIMPLEMENTED_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (
        NObjectClient::EObjectType /*type*/,
        const TCreateObjectOptions& /*options*/))

    // Files
    UNIMPLEMENTED_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (
        const NYPath::TYPath& /*path*/,
        const TFileReaderOptions& /*options*/))

    UNIMPLEMENTED_METHOD(IFileWriterPtr, CreateFileWriter, (
        const NYPath::TRichYPath& /*path*/,
        const TFileWriterOptions& /*options*/))

    UNIMPLEMENTED_METHOD(IJournalReaderPtr, CreateJournalReader, (
        const NYPath::TYPath& /*path*/,
        const TJournalReaderOptions& /*options*/))

    UNIMPLEMENTED_METHOD(IJournalWriterPtr, CreateJournalWriter, (
        const NYPath::TYPath& /*path*/,
        const TJournalWriterOptions& /*options*/))

    // Transactions
    UNIMPLEMENTED_METHOD(ITransactionPtr, AttachTransaction, (
        NTransactionClient::TTransactionId /*transactionId*/,
        const TTransactionAttachOptions& /*options*/))

    UNIMPLEMENTED_METHOD(IPrerequisitePtr, AttachPrerequisite, (
        NPrerequisiteClient::TPrerequisiteId /*prerequisiteId*/,
        const TPrerequisiteAttachOptions& /*options*/))

    // Tables
    UNIMPLEMENTED_METHOD(TFuture<void>, MountTable, (
        const NYPath::TYPath& /*path*/,
        const TMountTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnmountTable, (
        const NYPath::TYPath& /*path*/,
        const TUnmountTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemountTable, (
        const NYPath::TYPath& /*path*/,
        const TRemountTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, FreezeTable, (
        const NYPath::TYPath& /*path*/,
        const TFreezeTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, UnfreezeTable, (
        const NYPath::TYPath& /*path*/,
        const TUnfreezeTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, CancelTabletTransition, (
        NTabletClient::TTabletId /*tabletId*/,
        const TCancelTabletTransitionOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath& /*path*/,
        const std::vector<NTableClient::TLegacyOwningKey>& /*pivotKeys*/,
        const TReshardTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (
        const NYPath::TYPath& /*path*/,
        int /*tabletCount*/,
        const TReshardTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, ReshardTableAutomatic, (
        const NYPath::TYPath& /*path*/,
        const TReshardTableAutomaticOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, TrimTable, (
        const NYPath::TYPath& /*path*/,
        int /*tabletIndex*/,
        i64 /*trimmedRowCount*/,
        const TTrimTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTable, (
        const NYPath::TYPath& /*path*/,
        const TAlterTableOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTableReplica, (
        NTabletClient::TTableReplicaId /*replicaId*/,
        const TAlterTableReplicaOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterReplicationCard, (
        NChaosClient::TReplicationCardId /*replicationCardId*/,
        const TAlterReplicationCardOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<IPrerequisitePtr>, StartChaosLease, (
        const TChaosLeaseStartOptions& /*options*/));

    UNIMPLEMENTED_METHOD(TFuture<IPrerequisitePtr>, AttachChaosLease, (
        NChaosClient::TChaosLeaseId /*chaosLeaseId*/,
        const TChaosLeaseAttachOptions& /*options*/));

    UNIMPLEMENTED_METHOD(TFuture<void>, PingChaosLease, (
        NChaosClient::TChaosLeaseId /*chaosLeaseId*/,
        const TChaosLeasePingOptions& /*options*/));

    UNIMPLEMENTED_METHOD(TFuture<void>, SetUserBanned, (
        const std::string& /*user*/,
        bool /*isBanned*/,
        const TSetUserBannedOptions& /*options*/ = {}))
    UNIMPLEMENTED_METHOD(TFuture<bool>, GetUserBanned, (
        const std::string& /*user*/,
        const TGetUserBannedOptions& /*options*/ = {}))
    UNIMPLEMENTED_METHOD(TFuture<std::vector<std::string>>, ListBannedUsers, (
        const TListBannedUsersOptions& /*options*/ = {}))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetTablePivotKeys, (
        const NYPath::TYPath& /*path*/,
        const TGetTablePivotKeysOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, CreateTableBackup, (
        const TBackupManifestPtr& /*manifest*/,
        const TCreateTableBackupOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, RestoreTableBackup, (
        const TBackupManifestPtr& /*manifest*/,
        const TRestoreTableBackupOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath& /*path*/,
        const NTableClient::TNameTablePtr& /*nameTable*/,
        const TSharedRange<NTableClient::TLegacyKey>& /*keys*/,
        const TGetInSyncReplicasOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath& /*path*/,
        const TGetInSyncReplicasOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TTabletInfo>>, GetTabletInfos, (
        const NYPath::TYPath& /*path*/,
        const std::vector<int>& /*tabletIndexes*/,
        const TGetTabletInfosOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGetTabletErrorsResult>, GetTabletErrors, (
        const NYPath::TYPath& /*path*/,
        const TGetTabletErrorsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, BalanceTabletCells, (
        const std::string& /*tabletCellBundle*/,
        const std::vector<NYPath::TYPath>& /*movableTables*/,
        const TBalanceTabletCellsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NChaosClient::TReplicationCardPtr>, GetReplicationCard, (
        NChaosClient::TReplicationCardId /*replicationCardId*/,
        const TGetReplicationCardOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateChaosTableReplicaProgress, (
        NChaosClient::TReplicaId /*replicaId*/,
        const TUpdateChaosTableReplicaProgressOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TSkynetSharePartsLocationsPtr>, LocateSkynetShare, (
        const NYPath::TRichYPath& /*path*/,
        const TLocateSkynetShareOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTableClient::TColumnarStatistics>>, GetColumnarStatistics, (
        const std::vector<NYPath::TRichYPath>& /*path*/,
        const TGetColumnarStatisticsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TMultiTablePartitions>, PartitionTables, (
        const std::vector<NYPath::TRichYPath>& /*paths*/,
        const TPartitionTablesOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<ITablePartitionReaderPtr>, CreateTablePartitionReader, (
        const TTablePartitionCookiePtr& /*descriptor*/,
        const TReadTablePartitionOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<IFormattedTableReaderPtr>, CreateFormattedTableReader, (
        const NYPath::TRichYPath& /*path*/,
        const NYson::TYsonString& /*format*/,
        const TTableReaderOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<IFormattedTableReaderPtr>, CreateFormattedTablePartitionReader, (
        const TTablePartitionCookiePtr& /*cookie*/,
        const NYson::TYsonString& /*format*/,
        const TReadTablePartitionOptions& /*options*/))

    // Journals
    UNIMPLEMENTED_METHOD(TFuture<void>, TruncateJournal, (
        const NYPath::TYPath& /*path*/,
        i64 /*rowCount*/,
        const TTruncateJournalOptions& /*options*/))

    // Files
    UNIMPLEMENTED_METHOD(TFuture<TGetFileFromCacheResult>, GetFileFromCache, (
        const std::string& /*md5*/,
        const TGetFileFromCacheOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TPutFileToCacheResult>, PutFileToCache, (
        const NYPath::TYPath& /*path*/,
        const std::string& /*expectedMD5*/,
        const TPutFileToCacheOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TFilePartitions>, PartitionFile, (
        const NYPath::TYPath& /*path*/,
        const std::vector<TFileReadRange>& /*ranges*/,
        const TPartitionFileOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<IFileReaderPtr>, CreateFilePartitionReader, (
        const TFilePartitionCookiePtr& /*cookie*/,
        const TReadFilePartitionOptions& /*options*/))

    // Security
    UNIMPLEMENTED_METHOD(TFuture<void>, AddMember, (
        const std::string& /*group*/,
        const std::string& /*member*/,
        const TAddMemberOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveMember, (
        const std::string& /*group*/,
        const std::string& /*member*/,
        const TRemoveMemberOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionResponse>, CheckPermission, (
        const std::string& /*user*/,
        const NYPath::TYPath& /*path*/,
        NYTree::EPermission /*permission*/,
        const TCheckPermissionOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionByAclResult>, CheckPermissionByAcl, (
        const std::optional<std::string>& /*user*/,
        NYTree::EPermission /*permission*/,
        NYTree::INodePtr /*acl*/,
        const TCheckPermissionByAclOptions& /*options*/))

    // Accounting
    UNIMPLEMENTED_METHOD(TFuture<void>, TransferAccountResources, (
        const std::string& /*srcAccount*/,
        const std::string& /*dstAccount*/,
        NYTree::INodePtr /*resourceDelta*/,
        const TTransferAccountResourcesOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, TransferPoolResources, (
        const std::string& /*srcPool*/,
        const std::string& /*dstPool*/,
        const std::string& /*poolTree*/,
        NYTree::INodePtr /*resourceDelta*/,
        const TTransferPoolResourcesOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, TransferBundleResources, (
        const std::string& /*srcBundle*/,
        const std::string& /*dstBundle*/,
        NYTree::INodePtr /*resourceDelta*/,
        const TTransferBundleResourcesOptions& /*options*/))

    // Scheduler
    UNIMPLEMENTED_METHOD(TFuture<NScheduler::TOperationId>, StartOperation, (
        NScheduler::EOperationType /*type*/,
        const NYson::TYsonString& /*spec*/,
        const TStartOperationOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbortOperation, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const TAbortOperationOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendOperation, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const TSuspendOperationOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeOperation, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const TResumeOperationOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, CompleteOperation, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const TCompleteOperationOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateOperationParameters, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const NYson::TYsonString& /*parameters*/,
        const TUpdateOperationParametersOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, PatchOperationSpec, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const NScheduler::TSpecPatchList& /*patches*/,
        const TPatchOperationSpecOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TOperation>, GetOperation, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const TGetOperationOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, DumpJobContext, (
        NJobTrackerClient::TJobId /*jobId*/,
        const NYPath::TYPath& /*path*/,
        const TDumpJobContextOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobInput, (
        NJobTrackerClient::TJobId /*jobId*/,
        const TGetJobInputOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobInputPaths, (
        NJobTrackerClient::TJobId /*jobId*/,
        const TGetJobInputPathsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobSpec, (
        NJobTrackerClient::TJobId /*jobId*/,
        const TGetJobSpecOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGetJobStderrResponse>, GetJobStderr, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        NJobTrackerClient::TJobId /*jobId*/,
        const TGetJobStderrOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobTrace, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        NJobTrackerClient::TJobId /*jobId*/,
        const TGetJobTraceOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TSharedRef>, GetJobFailContext, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        NJobTrackerClient::TJobId /*jobId*/,
        const TGetJobFailContextOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TOperationEvent>>, ListOperationEvents, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const TListOperationEventsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TListOperationsResult>, ListOperations, (
        const TListOperationsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TListJobsResult>, ListJobs, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        const TListJobsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::vector<TJobTraceMeta>>, ListJobTraces, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        NJobTrackerClient::TJobId /*jobId*/,
        const TListJobTracesOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TCheckOperationPermissionResult>, CheckOperationPermission, (
        const std::string& /*user*/,
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        NYTree::EPermission /*permission*/,
        const TCheckOperationPermissionOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJob, (
        const NScheduler::TOperationIdOrAlias& /*operationIdOrAlias*/,
        NJobTrackerClient::TJobId /*jobId*/,
        const TGetJobOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbandonJob, (
        NJobTrackerClient::TJobId /*jobId*/,
        const TAbandonJobOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TPollJobShellResponse>, PollJobShell, (
        NJobTrackerClient::TJobId /*jobId*/,
        const std::optional<std::string>& /*shellName*/,
        const NYson::TYsonString& /*parameters*/,
        const TPollJobShellOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, RunJobShellCommand, (
        NJobTrackerClient::TJobId /*jobId*/,
        const std::optional<std::string>& /*shellName*/,
        const std::string& /*command*/,
        const TRunJobShellCommandOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbortJob, (
        NJobTrackerClient::TJobId /*jobId*/,
        const TAbortJobOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, DumpJobProxyLog, (
        NJobTrackerClient::TJobId /*jobId*/,
        NJobTrackerClient::TOperationId /*operationId*/,
        const NYPath::TYPath& /*path*/,
        const TDumpJobProxyLogOptions& /*options*/))

    // Metadata
    UNIMPLEMENTED_METHOD(TFuture<TClusterMeta>, GetClusterMeta, (
        const TGetClusterMetaOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, CheckClusterLiveness, (
        const TCheckClusterLivenessOptions& /*options*/))

    // Administration
    UNIMPLEMENTED_METHOD(TFuture<int>, BuildSnapshot, (
        const TBuildSnapshotOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TCellIdToSnapshotIdMap>, BuildMasterSnapshots, (
        const TBuildMasterSnapshotsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TCellIdToConsistentStateMap>, GetMasterConsistentState, (
        const TGetMasterConsistentStateOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ExitReadOnly, (
        NHydra::TCellId /*cellId*/,
        const TExitReadOnlyOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, MasterExitReadOnly, (
        const TMasterExitReadOnlyOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, FreezeHydraPeer, (
        NHydra::TCellId /*cellId*/,
        const std::string& /*address*/,
        const TFreezeHydraPeerOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, TruncateChangelog, (
        NHydra::TCellId /*cellId*/,
        const std::string& /*address*/,
        const TTruncateChangelogOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ScheduleRestart, (
        NHydra::TCellId /*cellId*/,
        const std::string& /*address*/,
        const TScheduleRestartOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResetDynamicallyPropagatedMasterCells, (
        const TResetDynamicallyPropagatedMasterCellsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, DiscombobulateNonvotingPeers, (
        NHydra::TCellId /*cellId*/,
        const TDiscombobulateNonvotingPeersOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SwitchLeader, (
        NHydra::TCellId /*cellId*/,
        const std::string& /*newLeaderAddress*/,
        const TSwitchLeaderOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResetStateHash, (
        NHydra::TCellId /*cellId*/,
        const TResetStateHashOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, GCCollect, (
        const TGCCollectOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, KillProcess, (
        const std::string& /*address*/,
        const TKillProcessOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::string>, WriteCoreDump, (
        const std::string& /*address*/,
        const TWriteCoreDumpOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGuid>, WriteLogBarrier, (
        const std::string& /*address*/,
        const TWriteLogBarrierOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<std::string>, WriteOperationControllerCoreDump, (
        NJobTrackerClient::TOperationId /*operationId*/,
        const TWriteOperationControllerCoreDumpOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, HealExecNode, (
        const std::string& /*address*/,
        const THealExecNodeOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendCoordinator, (
        NObjectClient::TCellId /*coordinatorCellId*/,
        const TSuspendCoordinatorOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeCoordinator, (
        NObjectClient::TCellId /*coordinatorCellId*/,
        const TResumeCoordinatorOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, MigrateReplicationCards, (
        NObjectClient::TCellId /*chaosCellId*/,
        const TMigrateReplicationCardsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendChaosCells, (
        const std::vector<NObjectClient::TCellId>& /*cellIds*/,
        const TSuspendChaosCellsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeChaosCells, (
        const std::vector<NObjectClient::TCellId>& /*cellIds*/,
        const TResumeChaosCellsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendTabletCells, (
        const std::vector<NObjectClient::TCellId>& /*cellIds*/,
        const TSuspendTabletCellsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeTabletCells, (
        const std::vector<NObjectClient::TCellId>& /*cellIds*/,
        const TResumeTabletCellsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceIdPerTarget>, AddMaintenance, (
        EMaintenanceComponent /*component*/,
        const std::string& /*address*/,
        EMaintenanceType /*type*/,
        const std::string& /*comment*/,
        const TAddMaintenanceOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceCountsPerTarget>, RemoveMaintenance, (
        EMaintenanceComponent /*component*/,
        const std::string& /*address*/,
        const TMaintenanceFilter& /*filter*/,
        const TRemoveMaintenanceOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TDisableChunkLocationsResult>, DisableChunkLocations, (
        const std::string& /*nodeAddress*/,
        const std::vector<TGuid>& /*locationUuids*/,
        const TDisableChunkLocationsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TDestroyChunkLocationsResult>, DestroyChunkLocations, (
        const std::string& /*nodeAddress*/,
        bool /*recoverUnlinkedDisks*/,
        const std::vector<TGuid>& /*locationUuids*/,
        const TDestroyChunkLocationsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TResurrectChunkLocationsResult>, ResurrectChunkLocations, (
        const std::string& /*nodeAddress*/,
        const std::vector<TGuid>& /*locationUuids*/,
        const TResurrectChunkLocationsOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TRequestRestartResult>, RequestRestart, (
        const std::string& /*nodeAddress*/,
        const TRequestRestartOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TCollectCoverageResult>, CollectCoverage, (
        const std::string& /*address*/,
        const TCollectCoverageOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SetUserPassword, (
        const std::string& /*user*/,
        const std::string& /*currentPasswordSha256*/,
        const std::string& /*newPasswordSha256*/,
        const TSetUserPasswordOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TIssueTokenResult>, IssueToken, (
        const std::string& /*user*/,
        const std::string& /*passwordSha256*/,
        const TIssueTokenOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, RevokeToken, (
        const std::string& /*user*/,
        const std::string& /*passwordSha256*/,
        const std::string& /*tokenSha256*/,
        const TRevokeTokenOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TListUserTokensResult>, ListUserTokens, (
        const std::string& /*user*/,
        const std::string& /*passwordSha256*/,
        const TListUserTokensOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGetCurrentUserResult>, GetCurrentUser, (
        const TGetCurrentUserOptions& /*options*/))

    // Query tracker
    UNIMPLEMENTED_METHOD(TFuture<NQueryTrackerClient::TQueryId>, StartQuery, (
        NQueryTrackerClient::EQueryEngine /*engine*/,
        const std::string& /*query*/,
        const TStartQueryOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AbortQuery, (
        NQueryTrackerClient::TQueryId /*queryId*/,
        const TAbortQueryOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TQueryResult>, GetQueryResult, (
        NQueryTrackerClient::TQueryId /*queryId*/,
        i64 /*resultIndex*/,
        const TGetQueryResultOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<IUnversionedRowsetPtr>, ReadQueryResult, (
        NQueryTrackerClient::TQueryId /*queryId*/,
        i64 /*resultIndex*/,
        const TReadQueryResultOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TQuery>, GetQuery, (
        NQueryTrackerClient::TQueryId /*queryId*/,
        const TGetQueryOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TListQueriesResult>, ListQueries, (
        const TListQueriesOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, AlterQuery, (
        NQueryTrackerClient::TQueryId /*queryId*/,
        const TAlterQueryOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGetQueryTrackerInfoResult>, GetQueryTrackerInfo, (
        const TGetQueryTrackerInfoOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGetQueryDeclaredParametersInfoResult>, GetQueryDeclaredParametersInfo, (
        const TGetQueryDeclaredParametersInfoOptions& /*options*/))

    // Bundle Controller
    UNIMPLEMENTED_METHOD(TFuture<NBundleControllerClient::TBundleConfigDescriptorPtr>, GetBundleConfig, (
        const std::string& /*bundleName*/,
        const NBundleControllerClient::TGetBundleConfigOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, SetBundleConfig, (
        const std::string& /*bundleName*/,
        const NBundleControllerClient::TBundleTargetConfigPtr& /*bundleConfig*/,
        const NBundleControllerClient::TSetBundleConfigOptions& /*options*/))

    // Flow
    UNIMPLEMENTED_METHOD(TFuture<TGetPipelineSpecResult>, GetPipelineSpec, (
        const NYPath::TYPath& /*pipelinePath*/,
        const TGetPipelineSpecOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TSetPipelineSpecResult>, SetPipelineSpec, (
        const NYPath::TYPath& /*pipelinePath*/,
        const NYson::TYsonString& /*spec*/,
        const TSetPipelineSpecOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGetPipelineDynamicSpecResult>, GetPipelineDynamicSpec, (
        const NYPath::TYPath& /*pipelinePath*/,
        const TGetPipelineDynamicSpecOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TSetPipelineDynamicSpecResult>, SetPipelineDynamicSpec, (
        const NYPath::TYPath& /*pipelinePath*/,
        const NYson::TYsonString& /*spec*/,
        const TSetPipelineDynamicSpecOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, StartPipeline, (
        const NYPath::TYPath& /*pipelinePath*/,
        const TStartPipelineOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, StopPipeline, (
        const NYPath::TYPath& /*pipelinePath*/,
        const TStopPipelineOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, PausePipeline, (
        const NYPath::TYPath& /*pipelinePath*/,
        const TPausePipelineOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TPipelineState>, GetPipelineState, (
        const NYPath::TYPath& /*pipelinePath*/,
        const TGetPipelineStateOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TGetFlowViewResult>, GetFlowView, (
        const NYPath::TYPath& /*pipelinePath*/,
        const NYPath::TYPath& /*viewPath*/,
        const TGetFlowViewOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<TFlowExecuteResult>, FlowExecute, (
        const NYPath::TYPath& /*pipelinePath*/,
        const std::string& /*command*/,
        const NYson::TYsonString& /*argument*/,
        const TFlowExecuteOptions& /*options*/ = {}))

    // Distributed client
    UNIMPLEMENTED_METHOD(TFuture<TDistributedWriteSessionWithCookies>, StartDistributedWriteSession, (
        const NYPath::TRichYPath& /*path*/,
        const TDistributedWriteSessionStartOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, PingDistributedWriteSession, (
        TSignedDistributedWriteSessionPtr /*session*/,
        const TDistributedWriteSessionPingOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, FinishDistributedWriteSession, (
        const TDistributedWriteSessionWithResults& /*sessionWithResults*/,
        const TDistributedWriteSessionFinishOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<ITableFragmentWriterPtr>, CreateTableFragmentWriter, (
        const TSignedWriteFragmentCookiePtr& /*cookie*/,
        const TTableFragmentWriterOptions& /*options*/))

    // Distributed file client
    UNIMPLEMENTED_METHOD(TFuture<TDistributedWriteFileSessionWithCookies>, StartDistributedWriteFileSession, (
        const NYPath::TRichYPath& /*path*/,
        const TDistributedWriteFileSessionStartOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, PingDistributedWriteFileSession, (
        const TSignedDistributedWriteFileSessionPtr& /*session*/,
        const TDistributedWriteFileSessionPingOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<void>, FinishDistributedWriteFileSession, (
        const TDistributedWriteFileSessionWithResults& /*sessionWithResults*/,
        const TDistributedWriteFileSessionFinishOptions& /*options*/))

    UNIMPLEMENTED_METHOD(IFileFragmentWriterPtr, CreateFileFragmentWriter, (
        const TSignedWriteFileFragmentCookiePtr& /*cookie*/,
        const TFileFragmentWriterOptions& /*options*/))

    // Shuffle Service
    UNIMPLEMENTED_METHOD(TFuture<TSignedShuffleHandlePtr>, StartShuffle, (
        const std::string& /*account*/,
        int /*partitionCount*/,
        NObjectClient::TTransactionId /*transactionId*/,
        const TStartShuffleOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<IRowBatchReaderPtr>, CreateShuffleReader, (
        const TSignedShuffleHandlePtr& /*shuffleHandle*/,
        int /*partitionIndex*/,
        std::optional<std::pair<int, int>> /*logicalWriterIndexRange*/,
        const TShuffleReaderOptions& /*options*/))

    UNIMPLEMENTED_METHOD(TFuture<IRowBatchWriterPtr>, CreateShuffleWriter, (
        const TSignedShuffleHandlePtr& /*shuffleHandle*/,
        const std::string& /*partitionColumn*/,
        std::optional<int> /*logicalWriterIndex*/,
        const TShuffleWriterOptions& /*options*/))

    #undef UNIMPLEMENTED_METHOD
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
