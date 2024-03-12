#include "client.h"

#include "config.h"
#include "private.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/dynamic_table_transaction_mixin.h>
#include <yt/yt/client/api/queue_transaction_mixin.h>

#include <yt/yt/client/misc/method_helpers.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/helpers.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NClient::NFederated {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = FederatedClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClient)

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> GetDataCenterByClient(const IClientPtr& client)
{
    TListNodeOptions options;
    options.MaxSize = 1;

    auto items = NConcurrency::WaitFor(client->ListNode(RpcProxiesPath, options))
        .ValueOrThrow();
    auto itemsList = NYTree::ConvertTo<NYTree::IListNodePtr>(items);
    if (!itemsList->GetChildCount()) {
        return std::nullopt;
    }
    auto host = itemsList->GetChildren()[0];
    return NNet::InferYPClusterFromHostName(host->GetValue<TString>());
}

class TTransaction
    : public virtual ITransaction
    , public TDynamicTableTransactionMixin
    , public TQueueTransactionMixin
{
public:
    TTransaction(TClientPtr client, int clientIndex, ITransactionPtr underlying);

    TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = {}) override;

    TFuture<TUnversionedLookupRowsResult> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) override;

    TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) override;

    void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options) override;

    using TQueueTransactionMixin::AdvanceConsumer;
    TFuture<void> AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset,
        const TAdvanceConsumerOptions& options) override;

    TFuture<TTransactionFlushResult> Flush() override;

    TFuture<void> Ping(const NApi::TTransactionPingOptions& options = {}) override;

    TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options = TTransactionCommitOptions()) override;

    TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions()) override;

    TFuture<TVersionedLookupRowsResult> VersionedLookupRows(
        const NYPath::TYPath&, NTableClient::TNameTablePtr,
        const TSharedRange<NTableClient::TUnversionedRow>&,
        const TVersionedLookupRowsOptions&) override;

    TFuture<std::vector<TUnversionedLookupRowsResult>> MultiLookupRows(
        const std::vector<TMultiLookupSubrequest>&,
        const TMultiLookupOptions&) override;

    TFuture<NYson::TYsonString> ExplainQuery(const TString&, const TExplainQueryOptions&) override;

    TFuture<NYson::TYsonString> GetNode(const NYPath::TYPath&, const TGetNodeOptions&) override;

    TFuture<NYson::TYsonString> ListNode(const NYPath::TYPath&, const TListNodeOptions&) override;

    TFuture<bool> NodeExists(const NYPath::TYPath&, const TNodeExistsOptions&) override;

    TFuture<TPullRowsResult> PullRows(const NYPath::TYPath&, const TPullRowsOptions&) override;

    IClientPtr GetClient() const override
    {
        return Underlying_->GetClient();
    }

    NTransactionClient::ETransactionType GetType() const override
    {
        return Underlying_->GetType();
    }

    NTransactionClient::TTransactionId GetId() const override
    {
        return Underlying_->GetId();
    }

    NTransactionClient::TTimestamp GetStartTimestamp() const override
    {
        return Underlying_->GetStartTimestamp();
    }

    virtual NTransactionClient::EAtomicity GetAtomicity() const override
    {
        return Underlying_->GetAtomicity();
    }

    virtual NTransactionClient::EDurability GetDurability() const override
    {
        return Underlying_->GetDurability();
    }

    virtual TDuration GetTimeout() const override
    {
        return Underlying_->GetTimeout();
    }

    void Detach() override
    {
        return Underlying_->Detach();
    }

    void RegisterAlienTransaction(const ITransactionPtr& transaction) override
    {
        return Underlying_->RegisterAlienTransaction(transaction);
    }

    IConnectionPtr GetConnection() override
    {
        return Underlying_->GetConnection();
    }

    void SubscribeCommitted(const TCommittedHandler& handler) override
    {
        Underlying_->SubscribeCommitted(handler);
    }

    void UnsubscribeCommitted(const TCommittedHandler& handler) override
    {
        Underlying_->UnsubscribeCommitted(handler);
    }

    void SubscribeAborted(const TAbortedHandler& handler) override
    {
        Underlying_->SubscribeAborted(handler);
    }

    void UnsubscribeAborted(const TAbortedHandler& handler) override
    {
        Underlying_->UnsubscribeAborted(handler);
    }

    UNIMPLEMENTED_METHOD(TFuture<void>, SetNode, (const NYPath::TYPath&, const NYson::TYsonString&, const TSetNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MultisetAttributesNode, (const NYPath::TYPath&, const NYTree::IMapNodePtr&, const TMultisetAttributesNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveNode, (const NYPath::TYPath&, const TRemoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (const NYPath::TYPath&, NObjectClient::EObjectType, const TCreateNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TLockNodeResult>, LockNode, (const NYPath::TYPath&, NCypressClient::ELockMode, const TLockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnlockNode, (const NYPath::TYPath&, const TUnlockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TCopyNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TMoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TLinkNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ConcatenateNodes, (const std::vector<NYPath::TRichYPath>&, const NYPath::TRichYPath&, const TConcatenateNodesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ExternalizeNode, (const NYPath::TYPath&, NObjectClient::TCellTag, const TExternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, InternalizeNode, (const NYPath::TYPath&, const TInternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (NObjectClient::EObjectType, const TCreateObjectOptions&));
    UNIMPLEMENTED_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (const NYPath::TRichYPath&, const TTableReaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (const NYPath::TYPath&, const TFileReaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (const NYPath::TRichYPath&, const TTableWriterOptions&));
    UNIMPLEMENTED_METHOD(IFileWriterPtr, CreateFileWriter, (const NYPath::TRichYPath&, const TFileWriterOptions&));
    UNIMPLEMENTED_METHOD(IJournalReaderPtr, CreateJournalReader, (const NYPath::TYPath&, const TJournalReaderOptions&));
    UNIMPLEMENTED_METHOD(IJournalWriterPtr, CreateJournalWriter, (const NYPath::TYPath&, const TJournalWriterOptions&));

private:
    const TClientPtr Client_;
    const int ClientIndex_;
    const ITransactionPtr Underlying_;

    void OnResult(const TErrorOr<void>& error);
};

DECLARE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TClientDescription)

struct TClientDescription final
{
    TClientDescription(IClientPtr client, int priority)
        : Client(std::move(client))
        , Priority(priority)
    { }

    IClientPtr Client;
    int Priority;
    std::atomic<bool> HasErrors{false};
};

DEFINE_REFCOUNTED_TYPE(TClientDescription)

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        const std::vector<IClientPtr>& underlyingClients,
        TFederationConfigPtr config);

    TFuture<TUnversionedLookupRowsResult> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) override;
    TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) override;
    TFuture<std::vector<TUnversionedLookupRowsResult>> MultiLookupRows(
        const std::vector<TMultiLookupSubrequest>&,
        const TMultiLookupOptions&) override;
    TFuture<TVersionedLookupRowsResult> VersionedLookupRows(
        const NYPath::TYPath&, NTableClient::TNameTablePtr,
        const TSharedRange<NTableClient::TUnversionedRow>&,
        const TVersionedLookupRowsOptions&) override;
    TFuture<TPullRowsResult> PullRows(const NYPath::TYPath&, const TPullRowsOptions&) override;

    TFuture<NQueueClient::IQueueRowsetPtr> PullQueue(
        const NYPath::TRichYPath&,
        i64,
        int,
        const NQueueClient::TQueueRowBatchReadOptions&,
        const TPullQueueOptions&) override;
    TFuture<NQueueClient::IQueueRowsetPtr> PullConsumer(
        const NYPath::TRichYPath&,
        const NYPath::TRichYPath&,
        std::optional<i64>,
        int,
        const NQueueClient::TQueueRowBatchReadOptions&,
        const TPullConsumerOptions&) override;

    TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override;

    TFuture<NYson::TYsonString> ExplainQuery(const TString&, const TExplainQueryOptions&) override;

    TFuture<NYson::TYsonString> GetNode(const NYPath::TYPath&, const TGetNodeOptions&) override;
    TFuture<NYson::TYsonString> ListNode(const NYPath::TYPath&, const TListNodeOptions&) override;
    TFuture<bool> NodeExists(const NYPath::TYPath&, const TNodeExistsOptions&) override;

    TFuture<std::vector<TListQueueConsumerRegistrationsResult>> ListQueueConsumerRegistrations(const std::optional<NYPath::TRichYPath>&, const std::optional<NYPath::TRichYPath>&, const TListQueueConsumerRegistrationsOptions&) override;

    const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    TFuture<std::vector<TTabletInfo>> GetTabletInfos(const NYPath::TYPath&, const std::vector<int>&, const TGetTabletInfosOptions&) override;

    TFuture<NChaosClient::TReplicationCardPtr> GetReplicationCard(NChaosClient::TReplicationCardId, const TGetReplicationCardOptions&) override;

    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;

    ITransactionPtr AttachTransaction(NTransactionClient::TTransactionId, const TTransactionAttachOptions&) override;

    IConnectionPtr GetConnection() override
    {
        auto [client, _] = GetActiveClient();
        return client->GetConnection();
    }

    std::optional<TStringBuf> GetClusterName(bool fetchIfNull) override
    {
        auto [client, _] = GetActiveClient();
        return client->GetClusterName(fetchIfNull);
    }

    void Terminate() override
    { }

    // IClientBase unsupported methods.
    UNIMPLEMENTED_METHOD(TFuture<void>, SetNode, (const NYPath::TYPath&, const NYson::TYsonString&, const TSetNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MultisetAttributesNode, (const NYPath::TYPath&, const NYTree::IMapNodePtr&, const TMultisetAttributesNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveNode, (const NYPath::TYPath&, const TRemoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (const NYPath::TYPath&, NObjectClient::EObjectType, const TCreateNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TLockNodeResult>, LockNode, (const NYPath::TYPath&, NCypressClient::ELockMode, const TLockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnlockNode, (const NYPath::TYPath&, const TUnlockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TCopyNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TMoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TLinkNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ConcatenateNodes, (const std::vector<NYPath::TRichYPath>&, const NYPath::TRichYPath&, const TConcatenateNodesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ExternalizeNode, (const NYPath::TYPath&, NObjectClient::TCellTag, const TExternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, InternalizeNode, (const NYPath::TYPath&, const TInternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (NObjectClient::EObjectType, const TCreateObjectOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TQueryResult>, GetQueryResult, (NQueryTrackerClient::TQueryId, i64, const TGetQueryResultOptions&));

    // IClient unsupported methods.
    UNIMPLEMENTED_METHOD(TFuture<void>, RegisterQueueConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, bool, const TRegisterQueueConsumerOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnregisterQueueConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, const TUnregisterQueueConsumerOptions&));
    UNIMPLEMENTED_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, ());
    UNIMPLEMENTED_METHOD(TFuture<void>, MountTable, (const NYPath::TYPath&, const TMountTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnmountTable, (const NYPath::TYPath&, const TUnmountTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemountTable, (const NYPath::TYPath&, const TRemountTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, FreezeTable, (const NYPath::TYPath&, const TFreezeTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnfreezeTable, (const NYPath::TYPath&, const TUnfreezeTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (const NYPath::TYPath&, const std::vector<NTableClient::TUnversionedOwningRow>&, const TReshardTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (const NYPath::TYPath&, int, const TReshardTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, ReshardTableAutomatic, (const NYPath::TYPath&, const TReshardTableAutomaticOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TrimTable, (const NYPath::TYPath&, int, i64, const TTrimTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTable, (const NYPath::TYPath&, const TAlterTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTableReplica, (NTabletClient::TTableReplicaId, const TAlterTableReplicaOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterReplicationCard, (NChaosClient::TReplicationCardId, const TAlterReplicationCardOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (const NYPath::TYPath&, const NTableClient::TNameTablePtr&, const TSharedRange<NTableClient::TUnversionedRow>&, const TGetInSyncReplicasOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (const NYPath::TYPath&, const TGetInSyncReplicasOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGetTabletErrorsResult>, GetTabletErrors, (const NYPath::TYPath&, const TGetTabletErrorsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, BalanceTabletCells, (const TString&, const std::vector<NYPath::TYPath>&, const TBalanceTabletCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSkynetSharePartsLocationsPtr>, LocateSkynetShare, (const NYPath::TRichYPath&, const TLocateSkynetShareOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTableClient::TColumnarStatistics>>, GetColumnarStatistics, (const std::vector<NYPath::TRichYPath>&, const TGetColumnarStatisticsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TMultiTablePartitions>, PartitionTables, (const std::vector<NYPath::TRichYPath>&, const TPartitionTablesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetTablePivotKeys, (const NYPath::TYPath&, const TGetTablePivotKeysOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, CreateTableBackup, (const TBackupManifestPtr&, const TCreateTableBackupOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RestoreTableBackup, (const TBackupManifestPtr&, const TRestoreTableBackupOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TruncateJournal, (const NYPath::TYPath&, i64, const TTruncateJournalOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGetFileFromCacheResult>, GetFileFromCache, (const TString&, const TGetFileFromCacheOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TPutFileToCacheResult>, PutFileToCache, (const NYPath::TYPath&, const TString&, const TPutFileToCacheOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AddMember, (const TString&, const TString&, const TAddMemberOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveMember, (const TString&, const TString&, const TRemoveMemberOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionResponse>, CheckPermission, (const TString&, const NYPath::TYPath&, NYTree::EPermission, const TCheckPermissionOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionByAclResult>, CheckPermissionByAcl, (const std::optional<TString>&, NYTree::EPermission, NYTree::INodePtr, const TCheckPermissionByAclOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TransferAccountResources, (const TString&, const TString&, NYTree::INodePtr, const TTransferAccountResourcesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TransferPoolResources, (const TString&, const TString&, const TString&, NYTree::INodePtr, const TTransferPoolResourcesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NScheduler::TOperationId>, StartOperation, (NScheduler::EOperationType, const NYson::TYsonString&, const TStartOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbortOperation, (const NScheduler::TOperationIdOrAlias&, const TAbortOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendOperation, (const NScheduler::TOperationIdOrAlias&, const TSuspendOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeOperation, (const NScheduler::TOperationIdOrAlias&, const TResumeOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, CompleteOperation, (const NScheduler::TOperationIdOrAlias&, const TCompleteOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateOperationParameters, (const NScheduler::TOperationIdOrAlias&, const NYson::TYsonString&, const TUpdateOperationParametersOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TOperation>, GetOperation, (const NScheduler::TOperationIdOrAlias&, const TGetOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, DumpJobContext, (NJobTrackerClient::TJobId, const NYPath::TYPath&, const TDumpJobContextOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobInput, (NJobTrackerClient::TJobId, const TGetJobInputOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobInputPaths, (NJobTrackerClient::TJobId, const TGetJobInputPathsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobSpec, (NJobTrackerClient::TJobId, const TGetJobSpecOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSharedRef>, GetJobStderr, (const NScheduler::TOperationIdOrAlias&, NJobTrackerClient::TJobId, const TGetJobStderrOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSharedRef>, GetJobFailContext, (const NScheduler::TOperationIdOrAlias&, NJobTrackerClient::TJobId, const TGetJobFailContextOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListOperationsResult>, ListOperations, (const TListOperationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListJobsResult>, ListJobs, (const NScheduler::TOperationIdOrAlias&, const TListJobsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJob, (const NScheduler::TOperationIdOrAlias&, NJobTrackerClient::TJobId, const TGetJobOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbandonJob, (NJobTrackerClient::TJobId, const TAbandonJobOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TPollJobShellResponse>, PollJobShell, (NJobTrackerClient::TJobId, const std::optional<TString>&, const NYson::TYsonString&, const TPollJobShellOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbortJob, (NJobTrackerClient::TJobId, const TAbortJobOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TClusterMeta>, GetClusterMeta, (const TGetClusterMetaOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, CheckClusterLiveness, (const TCheckClusterLivenessOptions&));
    UNIMPLEMENTED_METHOD(TFuture<int>, BuildSnapshot, (const TBuildSnapshotOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TCellIdToSnapshotIdMap>, BuildMasterSnapshots, (const TBuildMasterSnapshotsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ExitReadOnly, (NObjectClient::TCellId, const TExitReadOnlyOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MasterExitReadOnly, (const TMasterExitReadOnlyOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, DiscombobulateNonvotingPeers, (NObjectClient::TCellId, const TDiscombobulateNonvotingPeersOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SwitchLeader, (NObjectClient::TCellId, const TString&, const TSwitchLeaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResetStateHash, (NObjectClient::TCellId, const TResetStateHashOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, GCCollect, (const TGCCollectOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, KillProcess, (const TString&, const TKillProcessOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TString>, WriteCoreDump, (const TString&, const TWriteCoreDumpOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGuid>, WriteLogBarrier, (const TString&, const TWriteLogBarrierOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TString>, WriteOperationControllerCoreDump, (NJobTrackerClient::TOperationId, const TWriteOperationControllerCoreDumpOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, HealExecNode, (const TString&, const THealExecNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendCoordinator, (NObjectClient::TCellId, const TSuspendCoordinatorOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeCoordinator, (NObjectClient::TCellId, const TResumeCoordinatorOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MigrateReplicationCards, (NObjectClient::TCellId, const TMigrateReplicationCardsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendChaosCells, (const std::vector<NObjectClient::TCellId>&, const TSuspendChaosCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeChaosCells, (const std::vector<NObjectClient::TCellId>&, const TResumeChaosCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendTabletCells, (const std::vector<NObjectClient::TCellId>&, const TSuspendTabletCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeTabletCells, (const std::vector<NObjectClient::TCellId>&, const TResumeTabletCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateChaosTableReplicaProgress, (NChaosClient::TReplicaId, const TUpdateChaosTableReplicaProgressOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceIdPerTarget>, AddMaintenance, (EMaintenanceComponent, const TString&, EMaintenanceType, const TString&, const TAddMaintenanceOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceCountsPerTarget>, RemoveMaintenance, (EMaintenanceComponent, const TString&, const TMaintenanceFilter&, const TRemoveMaintenanceOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TDisableChunkLocationsResult>, DisableChunkLocations, (const TString&, const std::vector<TGuid>&, const TDisableChunkLocationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TDestroyChunkLocationsResult>, DestroyChunkLocations, (const TString&, bool, const std::vector<TGuid>&, const TDestroyChunkLocationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TResurrectChunkLocationsResult>, ResurrectChunkLocations, (const TString&, const std::vector<TGuid>&, const TResurrectChunkLocationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TRequestRestartResult>, RequestRestart, (const TString&, const TRequestRestartOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SetUserPassword, (const TString&, const TString&, const TString&, const TSetUserPasswordOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TIssueTokenResult>, IssueToken, (const TString&, const TString&, const TIssueTokenOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RevokeToken, (const TString&, const TString&, const TString&, const TRevokeTokenOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListUserTokensResult>, ListUserTokens, (const TString&, const TString&, const TListUserTokensOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NQueryTrackerClient::TQueryId>, StartQuery, (NQueryTrackerClient::EQueryEngine, const TString&, const TStartQueryOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbortQuery, (NQueryTrackerClient::TQueryId, const TAbortQueryOptions&));
    UNIMPLEMENTED_METHOD(TFuture<IUnversionedRowsetPtr>, ReadQueryResult, (NQueryTrackerClient::TQueryId, i64, const TReadQueryResultOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TQuery>, GetQuery, (NQueryTrackerClient::TQueryId, const TGetQueryOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListQueriesResult>, ListQueries, (const TListQueriesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterQuery, (NQueryTrackerClient::TQueryId, const TAlterQueryOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGetQueryTrackerInfoResult>, GetQueryTrackerInfo, (const TGetQueryTrackerInfoOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NBundleControllerClient::TBundleConfigDescriptorPtr>, GetBundleConfig, (const TString&, const NBundleControllerClient::TGetBundleConfigOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SetBundleConfig, (const TString&, const NBundleControllerClient::TBundleTargetConfigPtr&, const NBundleControllerClient::TSetBundleConfigOptions&));
    UNIMPLEMENTED_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (const NYPath::TRichYPath&, const TTableReaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (const NYPath::TRichYPath&, const TTableWriterOptions&));
    UNIMPLEMENTED_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (const NYPath::TYPath&, const TFileReaderOptions&));
    UNIMPLEMENTED_METHOD(IFileWriterPtr, CreateFileWriter, (const NYPath::TRichYPath&, const TFileWriterOptions&));
    UNIMPLEMENTED_METHOD(IJournalReaderPtr, CreateJournalReader, (const NYPath::TYPath&, const TJournalReaderOptions&));
    UNIMPLEMENTED_METHOD(IJournalWriterPtr, CreateJournalWriter, (const NYPath::TYPath&, const TJournalWriterOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGetPipelineSpecResult>, GetPipelineSpec, (const NYPath::TYPath&, const TGetPipelineSpecOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSetPipelineSpecResult>, SetPipelineSpec, (const NYPath::TYPath&, const NYson::TYsonString&, const TSetPipelineSpecOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGetPipelineDynamicSpecResult>, GetPipelineDynamicSpec, (const NYPath::TYPath&, const TGetPipelineDynamicSpecOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSetPipelineDynamicSpecResult>, SetPipelineDynamicSpec, (const NYPath::TYPath&, const NYson::TYsonString&, const TSetPipelineDynamicSpecOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, StartPipeline, (const NYPath::TYPath&, const TStartPipelineOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, StopPipeline, (const NYPath::TYPath&, const TStopPipelineOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, PausePipeline, (const NYPath::TYPath&, const TPausePipelineOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TPipelineStatus>, GetPipelineStatus, (const NYPath::TYPath&, const TGetPipelineStatusOptions&));

private:
    friend class TTransaction;

    struct TActiveClientInfo
    {
        IClientPtr Client;
        int ClientIndex;
    };

    template <class T>
    TFuture<T> DoCall(int retryAttemptCount, const TCallback<TFuture<T>(const IClientPtr&, int)>& callee);
    void HandleError(const TErrorOr<void>& error, int clientIndex);

    void UpdateActiveClient();
    TActiveClientInfo GetActiveClient();

    void CheckClustersHealth();

private:
    const TFederationConfigPtr Config_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    std::vector<TClientDescriptionPtr> UnderlyingClients_;
    IClientPtr ActiveClient_;
    std::atomic<int> ActiveClientIndex_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
};

DECLARE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TClientPtr client, int clientIndex, ITransactionPtr underlying)
    : Client_(std::move(client))
    , ClientIndex_(clientIndex)
    , Underlying_(std::move(underlying))
{ }

void TTransaction::OnResult(const TErrorOr<void>& error)
{
    if (!error.IsOK()) {
        Client_->HandleError(error, ClientIndex_);
    }
}

#define TRANSACTION_METHOD_IMPL(ResultType, MethodName, Args)                                                           \
TFuture<ResultType> TTransaction::MethodName(Y_METHOD_USED_ARGS_DECLARATION(Args))                                      \
{                                                                                                                       \
    auto future = Underlying_->MethodName(Y_PASS_METHOD_USED_ARGS(Args));                                               \
    future.Subscribe(BIND(&TTransaction::OnResult, MakeStrong(this)));                                                  \
    return future;                                                                                                      \
} Y_SEMICOLON_GUARD

TRANSACTION_METHOD_IMPL(TUnversionedLookupRowsResult, LookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TLookupRowsOptions&));
TRANSACTION_METHOD_IMPL(TSelectRowsResult, SelectRows, (const TString&, const TSelectRowsOptions&));
TRANSACTION_METHOD_IMPL(void, Ping, (const NApi::TTransactionPingOptions&));
TRANSACTION_METHOD_IMPL(TTransactionCommitResult, Commit, (const TTransactionCommitOptions&));
TRANSACTION_METHOD_IMPL(void, Abort, (const TTransactionAbortOptions&));
TRANSACTION_METHOD_IMPL(TVersionedLookupRowsResult, VersionedLookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TVersionedLookupRowsOptions&));
TRANSACTION_METHOD_IMPL(std::vector<TUnversionedLookupRowsResult>, MultiLookupRows, (const std::vector<TMultiLookupSubrequest>&, const TMultiLookupOptions&));
TRANSACTION_METHOD_IMPL(TPullRowsResult, PullRows, (const NYPath::TYPath&, const TPullRowsOptions&));
TRANSACTION_METHOD_IMPL(void, AdvanceConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, int, std::optional<i64>, i64, const TAdvanceConsumerOptions&));
TRANSACTION_METHOD_IMPL(NYson::TYsonString, ExplainQuery, (const TString&, const TExplainQueryOptions&));
TRANSACTION_METHOD_IMPL(NYson::TYsonString, GetNode, (const NYPath::TYPath&, const TGetNodeOptions&));
TRANSACTION_METHOD_IMPL(NYson::TYsonString, ListNode, (const NYPath::TYPath&, const TListNodeOptions&));
TRANSACTION_METHOD_IMPL(bool, NodeExists, (const NYPath::TYPath&, const TNodeExistsOptions&));

void TTransaction::ModifyRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options)
{
    Underlying_->ModifyRows(path, nameTable, modifications, options);
}

TFuture<TTransactionFlushResult> TTransaction::Flush()
{
    auto future = Underlying_->Flush();
    future.Subscribe(BIND(&TTransaction::OnResult, MakeStrong(this)));
    return future;
}

TFuture<ITransactionPtr> TTransaction::StartTransaction(
    NTransactionClient::ETransactionType type,
    const TTransactionStartOptions& options)
{
    return Underlying_->StartTransaction(type, options).ApplyUnique(BIND(
        [this, this_ = MakeStrong(this)] (TErrorOr<ITransactionPtr>&& result) -> TErrorOr<ITransactionPtr> {
            if (!result.IsOK()) {
                Client_->HandleError(result, ClientIndex_);
                return result;
            } else {
                return {New<TTransaction>(Client_, ClientIndex_, result.Value())};
            }
        }
    ));
}

DEFINE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(const std::vector<IClientPtr>& underlyingClients, TFederationConfigPtr config)
    : Config_(std::move(config))
    , Executor_(New<NConcurrency::TPeriodicExecutor>(
        NRpc::TDispatcher::Get()->GetLightInvoker(),
        BIND(&TClient::CheckClustersHealth, MakeWeak(this)),
        Config_->ClusterHealthCheckPeriod))
{
    YT_VERIFY(!underlyingClients.empty());

    UnderlyingClients_.reserve(underlyingClients.size());
    const auto& localDatacenter = NNet::GetLocalYPCluster();
    for (const auto& client : underlyingClients) {
        int priority = GetDataCenterByClient(client) == localDatacenter ? 1 : 0;
        UnderlyingClients_.push_back(New<TClientDescription>(client, priority));
    }
    std::stable_sort(UnderlyingClients_.begin(), UnderlyingClients_.end(), [](const auto& lhs, const auto& rhs) {
        return lhs->Priority > rhs->Priority;
    });

    ActiveClient_ = UnderlyingClients_[0]->Client;
    ActiveClientIndex_ = 0;

    Executor_->Start();
}

void TClient::CheckClustersHealth()
{
    TCheckClusterLivenessOptions options;
    options.CheckCypressRoot = true;
    options.CheckTabletCellBundle = Config_->BundleName;

    int activeClientIndex = ActiveClientIndex_.load();
    std::optional<int> betterClientIndex;

    std::vector<TFuture<void>> checks;
    checks.reserve(UnderlyingClients_.size());

    for (const auto& clientDescription : UnderlyingClients_) {
        checks.emplace_back(clientDescription->Client->CheckClusterLiveness(options));
    }

    for (int index = 0; index < std::ssize(checks); ++index) {
        const auto& check = checks[index];
        bool hasErrors = !NConcurrency::WaitFor(check).IsOK();
        UnderlyingClients_[index]->HasErrors = hasErrors;
        if (!betterClientIndex && !hasErrors && index < activeClientIndex) {
            betterClientIndex = index;
        }
    }

    if (betterClientIndex && ActiveClientIndex_ == activeClientIndex) {
        int newClientIndex = *betterClientIndex;
        auto guard = NThreading::WriterGuard(Lock_);
        ActiveClient_ = UnderlyingClients_[newClientIndex]->Client;
        ActiveClientIndex_ = newClientIndex;
        return;
    }

    // If active cluster is not healthy, try changing it.
    if (UnderlyingClients_[activeClientIndex]->HasErrors) {
        auto guard = NThreading::WriterGuard(Lock_);
        // Check that active client wasn't changed.
        if (ActiveClientIndex_ == activeClientIndex && UnderlyingClients_[activeClientIndex]->HasErrors) {
            UpdateActiveClient();
        }
    }
}

template <class T>
TFuture<T> TClient::DoCall(int retryAttemptCount, const TCallback<TFuture<T>(const IClientPtr&, int)>& callee)
{
    auto [client, clientIndex] = GetActiveClient();
    return callee(client, clientIndex).ApplyUnique(BIND(
        [
            this,
            this_ = MakeStrong(this),
            retryAttemptCount,
            callee,
            clientIndex = clientIndex
        ] (TErrorOr<T>&& result) {
            if (!result.IsOK()) {
                HandleError(result, clientIndex);
                if (retryAttemptCount > 1) {
                    return DoCall<T>(retryAttemptCount - 1, callee);
                }
            }
            return MakeFuture(std::move(result));
        }));
}

TFuture<ITransactionPtr> TClient::StartTransaction(
    NTransactionClient::ETransactionType type,
    const NApi::TTransactionStartOptions& options)
{
    auto callee = BIND([this_ = MakeStrong(this), type, options] (const IClientPtr& client, int clientIndex) {
        return client->StartTransaction(type, options).ApplyUnique(BIND(
            [this_, clientIndex] (ITransactionPtr&& transaction) -> ITransactionPtr {
                return New<TTransaction>(std::move(this_), clientIndex, std::move(transaction));
            }));
    });

    return DoCall<ITransactionPtr>(Config_->ClusterRetryAttempts, callee);
}

#define CLIENT_METHOD_IMPL(ResultType, MethodName, Args)                                                                \
TFuture<ResultType> TClient::MethodName(Y_METHOD_USED_ARGS_DECLARATION(Args))                                           \
{                                                                                                                       \
    auto callee = BIND([Y_PASS_METHOD_USED_ARGS(Args)] (const IClientPtr& client, int /*clientIndex*/) {                \
        return client->MethodName(Y_PASS_METHOD_USED_ARGS(Args));                                                       \
    });                                                                                                                  \
    return DoCall<ResultType>(Config_->ClusterRetryAttempts, callee);                                                   \
} Y_SEMICOLON_GUARD

CLIENT_METHOD_IMPL(TUnversionedLookupRowsResult, LookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TLegacyKey>&, const TLookupRowsOptions&));
CLIENT_METHOD_IMPL(TSelectRowsResult, SelectRows, (const TString&, const TSelectRowsOptions&));
CLIENT_METHOD_IMPL(std::vector<TUnversionedLookupRowsResult>, MultiLookupRows, (const std::vector<TMultiLookupSubrequest>&, const TMultiLookupOptions&));
CLIENT_METHOD_IMPL(TVersionedLookupRowsResult, VersionedLookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TVersionedLookupRowsOptions&));
CLIENT_METHOD_IMPL(TPullRowsResult, PullRows, (const NYPath::TYPath&, const TPullRowsOptions&));
CLIENT_METHOD_IMPL(NQueueClient::IQueueRowsetPtr, PullQueue, (const NYPath::TRichYPath&, i64, int, const NQueueClient::TQueueRowBatchReadOptions&, const TPullQueueOptions&));
CLIENT_METHOD_IMPL(NQueueClient::IQueueRowsetPtr, PullConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, std::optional<i64>, int, const NQueueClient::TQueueRowBatchReadOptions&, const TPullConsumerOptions&));
CLIENT_METHOD_IMPL(NYson::TYsonString, ExplainQuery, (const TString&, const TExplainQueryOptions&));
CLIENT_METHOD_IMPL(NYson::TYsonString, GetNode, (const NYPath::TYPath&, const TGetNodeOptions&));
CLIENT_METHOD_IMPL(NYson::TYsonString, ListNode, (const NYPath::TYPath&, const TListNodeOptions&));
CLIENT_METHOD_IMPL(bool, NodeExists, (const NYPath::TYPath&, const TNodeExistsOptions&));
CLIENT_METHOD_IMPL(std::vector<TTabletInfo>, GetTabletInfos, (const NYPath::TYPath&, const std::vector<int>&, const TGetTabletInfosOptions&));
CLIENT_METHOD_IMPL(NChaosClient::TReplicationCardPtr, GetReplicationCard, (NChaosClient::TReplicationCardId, const TGetReplicationCardOptions&));
CLIENT_METHOD_IMPL(std::vector<TListQueueConsumerRegistrationsResult>, ListQueueConsumerRegistrations, (const std::optional<NYPath::TRichYPath>&, const std::optional<NYPath::TRichYPath>&, const TListQueueConsumerRegistrationsOptions&));

const NTabletClient::ITableMountCachePtr& TClient::GetTableMountCache()
{
    auto [client, _] = GetActiveClient();
    return client->GetTableMountCache();
}

const NTransactionClient::ITimestampProviderPtr& TClient::GetTimestampProvider()
{
    auto [client, _] = GetActiveClient();
    return client->GetTimestampProvider();
}

ITransactionPtr TClient::AttachTransaction(
    NTransactionClient::TTransactionId transactionId,
    const TTransactionAttachOptions& options)
{
    auto transactionClusterTag = NObjectClient::CellTagFromId(transactionId);
    for (const auto& clientDescription : UnderlyingClients_) {
        const auto& client = clientDescription->Client;
        auto clientClusterTag = client->GetConnection()->GetClusterTag();
        if (clientClusterTag == transactionClusterTag) {
            return client->AttachTransaction(transactionId, options);
        }
    }
    THROW_ERROR_EXCEPTION("No client is known for transaction %v", transactionId);
}

void TClient::HandleError(const TErrorOr<void>& error, int clientIndex)
{
    if (!NRpc::IsChannelFailureError(error) && !Config_->RetryAnyError) {
        return;
    }

    UnderlyingClients_[clientIndex]->HasErrors = true;
    if (ActiveClientIndex_ != clientIndex) {
        return;
    }

    auto guard = WriterGuard(Lock_);
    if (ActiveClientIndex_ != clientIndex) {
        return;
    }

    UpdateActiveClient();
}

void TClient::UpdateActiveClient()
{
    VERIFY_WRITER_SPINLOCK_AFFINITY(Lock_);

    int activeClientIndex = ActiveClientIndex_.load();

    for (int index = 0; index < std::ssize(UnderlyingClients_); ++index) {
        const auto& clientDescription = UnderlyingClients_[index];
        if (!clientDescription->HasErrors) {
            if (activeClientIndex != index) {
                YT_LOG_DEBUG("Active client was changed (PreviousClientIndex: %v, NewClientIndex: %v)",
                    activeClientIndex,
                    index);
            }

            ActiveClient_ = clientDescription->Client;
            ActiveClientIndex_ = index;
            break;
        }
    }
}

TClient::TActiveClientInfo TClient::GetActiveClient()
{
    auto guard = ReaderGuard(Lock_);
    YT_LOG_TRACE("Request will be send to the active client (ClientIndex: %v)",
        ActiveClientIndex_.load());
    return {ActiveClient_, ActiveClientIndex_.load()};
}

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    std::vector<NApi::IClientPtr> clients,
    TFederationConfigPtr config)
{
    return New<TClient>(
        std::move(clients),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
