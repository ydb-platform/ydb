#include "client_impl.h"

#include "config.h"
#include "helpers.h"
#include "private.h"
#include "table_mount_cache.h"
#include "table_writer.h"
#include "timestamp_provider.h"
#include "transaction.h"

#include <yt/yt/client/api/helpers.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/dynamic_channel_pool.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/stream.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/cast.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using NYT::ToProto;
using NYT::FromProto;

using namespace NAuth;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueueClient;
using namespace NRpc;
using namespace NScheduler;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    TConnectionPtr connection,
    const TClientOptions& clientOptions)
    : Connection_(std::move(connection))
    , RetryingChannel_(CreateSequoiaAwareRetryingChannel(
        CreateCredentialsInjectingChannel(
            Connection_->CreateChannel(false),
            clientOptions),
        /*retryProxyBanned*/ true))
    , ClientOptions_(clientOptions)
    , TableMountCache_(BIND(
        &CreateTableMountCache,
        Connection_->GetConfig()->TableMountCache,
        RetryingChannel_,
        RpcProxyClientLogger(),
        Connection_->GetConfig()->RpcTimeout))
    , TimestampProvider_(BIND(&TClient::CreateTimestampProvider, Unretained(this)))
{ }

const ITableMountCachePtr& TClient::GetTableMountCache()
{
    return TableMountCache_.Value();
}

const IReplicationCardCachePtr& TClient::GetReplicationCardCache()
{
    YT_UNIMPLEMENTED();
}

const ITimestampProviderPtr& TClient::GetTimestampProvider()
{
    return TimestampProvider_.Value();
}

void TClient::Terminate()
{ }

////////////////////////////////////////////////////////////////////////////////

IChannelPtr TClient::CreateSequoiaAwareRetryingChannel(NRpc::IChannelPtr channel, bool retryProxyBanned) const
{
    const auto& config = Connection_->GetConfig();
    bool retrySequoiaErrorsOnly = !config->EnableRetries;
    // NB: even if client's retries are disabled Sequoia transient failures are
    // still retriable. See IsRetriableError().
    return NRpc::CreateRetryingChannel(
        config->RetryingChannel,
        std::move(channel),
        BIND([=] (const TError& error) {
            return IsRetriableError(error, retryProxyBanned, retrySequoiaErrorsOnly);
        }));
}

IChannelPtr TClient::CreateNonRetryingChannelByAddress(const std::string& address) const
{
    return CreateCredentialsInjectingChannel(
        Connection_->CreateChannelByAddress(address),
        ClientOptions_);
}

////////////////////////////////////////////////////////////////////////////////

TConnectionPtr TClient::GetRpcProxyConnection()
{
    return Connection_;
}

TClientPtr TClient::GetRpcProxyClient()
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

IChannelPtr TClient::GetRetryingChannel() const
{
    return RetryingChannel_;
}

IChannelPtr TClient::CreateNonRetryingStickyChannel() const
{
    return CreateCredentialsInjectingChannel(
        Connection_->CreateChannel(true),
        ClientOptions_);
}

IChannelPtr TClient::WrapStickyChannelIntoRetrying(IChannelPtr underlying) const
{
    return CreateSequoiaAwareRetryingChannel(
        std::move(underlying),
        /*retryProxyBanned*/ false);
}

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr TClient::CreateTimestampProvider() const
{
    return NRpcProxy::CreateTimestampProvider(
        RetryingChannel_,
        Connection_->GetConfig()->RpcTimeout,
        Connection_->GetConfig()->TimestampProviderLatestTimestampUpdatePeriod,
        Connection_->GetConfig()->ClockClusterTag);
}

ITransactionPtr TClient::AttachTransaction(
    TTransactionId transactionId,
    const TTransactionAttachOptions& options)
{
    auto connection = GetRpcProxyConnection();
    auto client = GetRpcProxyClient();

    auto channel = options.StickyAddress
        ? WrapStickyChannelIntoRetrying(CreateNonRetryingChannelByAddress(options.StickyAddress))
        : GetRetryingChannel();

    auto proxy = CreateApiServiceProxy(channel);

    auto req = proxy.AttachTransaction();
    ToProto(req->mutable_transaction_id(), transactionId);
    // COMPAT(kiselyovp): remove auto_abort from the protocol
    req->set_auto_abort(false);
    if (options.PingPeriod) {
        req->set_ping_period(options.PingPeriod->GetValue());
    }
    req->set_ping(options.Ping);
    req->set_ping_ancestors(options.PingAncestors);

    auto rsp = NConcurrency::WaitFor(req->Invoke())
        .ValueOrThrow();

    auto transactionType = static_cast<ETransactionType>(rsp->type());
    auto startTimestamp = static_cast<TTimestamp>(rsp->start_timestamp());
    auto atomicity = static_cast<EAtomicity>(rsp->atomicity());
    auto durability = static_cast<EDurability>(rsp->durability());
    auto timeout = TDuration::FromValue(NYT::FromProto<i64>(rsp->timeout()));

    if (options.StickyAddress && transactionType != ETransactionType::Tablet) {
        THROW_ERROR_EXCEPTION("Sticky address is supported for tablet transactions only");
    }

    std::optional<TStickyTransactionParameters> stickyParameters;
    if (options.StickyAddress || transactionType == ETransactionType::Tablet) {
        stickyParameters.emplace();
        if (options.StickyAddress) {
            stickyParameters->ProxyAddress = options.StickyAddress;
        } else {
            stickyParameters->ProxyAddress = rsp->GetAddress();
        }
    }

    return CreateTransaction(
        std::move(connection),
        std::move(client),
        std::move(channel),
        transactionId,
        startTimestamp,
        transactionType,
        atomicity,
        durability,
        timeout,
        options.PingAncestors,
        options.PingPeriod,
        std::move(stickyParameters),
        rsp->sequence_number_source_id(),
        "Transaction attached");
}

TFuture<void> TClient::MountTable(
    const TYPath& path,
    const TMountTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.MountTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    NYT::ToProto(req->mutable_cell_id(), options.CellId);
    if (!options.TargetCellIds.empty()) {
        NYT::ToProto(req->mutable_target_cell_ids(), options.TargetCellIds);
    }
    req->set_freeze(options.Freeze);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::UnmountTable(
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.UnmountTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    req->set_force(options.Force);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::RemountTable(
    const TYPath& path,
    const TRemountTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RemountTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::FreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.FreezeTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::UnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.UnfreezeTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::ReshardTable(
    const TYPath& path,
    const std::vector<TLegacyOwningKey>& pivotKeys,
    const TReshardTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ReshardTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    auto writer = CreateWireProtocolWriter();
    // XXX(sandello): This is ugly and inefficient.
    std::vector<TUnversionedRow> keys;
    keys.reserve(pivotKeys.size());
    for (const auto& pivotKey : pivotKeys) {
        keys.push_back(pivotKey);
    }
    writer->WriteRowset(TRange(keys));
    req->Attachments() = writer->Finish();

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::ReshardTable(
    const TYPath& path,
    int tabletCount,
    const TReshardTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ReshardTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_tablet_count(tabletCount);
    if (options.Uniform) {
        req->set_uniform(*options.Uniform);
    }
    if (options.EnableSlicing) {
        req->set_enable_slicing(*options.EnableSlicing);
    }
    if (options.SlicingAccuracy) {
        req->set_slicing_accuracy(*options.SlicingAccuracy);
    }

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<std::vector<TTabletActionId>> TClient::ReshardTableAutomatic(
    const TYPath& path,
    const TReshardTableAutomaticOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ReshardTableAutomatic();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_keep_actions(options.KeepActions);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspReshardTableAutomaticPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        return FromProto<std::vector<TTabletActionId>>(rsp->tablet_actions());
    }));
}

TFuture<void> TClient::TrimTable(
    const TYPath& path,
    int tabletIndex,
    i64 trimmedRowCount,
    const TTrimTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.TrimTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_tablet_index(tabletIndex);
    req->set_trimmed_row_count(trimmedRowCount);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::AlterTable(
    const TYPath& path,
    const TAlterTableOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AlterTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    if (options.Schema) {
        req->set_schema(ConvertToYsonString(*options.Schema).ToString());
    }
    if (options.SchemaId) {
        ToProto(req->mutable_schema_id(), *options.SchemaId);
    }
    if (options.Dynamic) {
        req->set_dynamic(*options.Dynamic);
    }
    if (options.UpstreamReplicaId) {
        ToProto(req->mutable_upstream_replica_id(), *options.UpstreamReplicaId);
    }
    if (options.SchemaModification) {
        req->set_schema_modification(static_cast<NProto::ETableSchemaModification>(*options.SchemaModification));
    }
    if (options.ReplicationProgress) {
        ToProto(req->mutable_replication_progress(), *options.ReplicationProgress);
    }

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_transactional_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::AlterTableReplica(
    TTableReplicaId replicaId,
    const TAlterTableReplicaOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AlterTableReplica();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_replica_id(), replicaId);

    if (options.Enabled) {
        req->set_enabled(*options.Enabled);
    }
    if (options.Mode) {
        req->set_mode(static_cast<NProto::ETableReplicaMode>(*options.Mode));
    }
    if (options.PreserveTimestamps) {
        req->set_preserve_timestamps(*options.PreserveTimestamps);
    }
    if (options.Atomicity) {
        req->set_atomicity(static_cast<NProto::EAtomicity>(*options.Atomicity));
    }
    if (options.EnableReplicatedTableTracker) {
        req->set_enable_replicated_table_tracker(*options.EnableReplicatedTableTracker);
    }

    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<TYsonString> TClient::GetTablePivotKeys(
    const TYPath& path,
    const TGetTablePivotKeysOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetTablePivotKeys();
    SetTimeoutOptions(*req, options);

    req->set_represent_key_as_list(options.RepresentKeyAsList);
    req->set_path(path);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetTablePivotKeysPtr& rsp) {
        return TYsonString(rsp->value());
    }));
}

TFuture<void> TClient::CreateTableBackup(
    const TBackupManifestPtr& manifest,
    const TCreateTableBackupOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CreateTableBackup();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_manifest(), *manifest);
    req->set_checkpoint_timestamp_delay(ToProto<i64>(options.CheckpointTimestampDelay));
    req->set_checkpoint_check_period(ToProto<i64>(options.CheckpointCheckPeriod));
    req->set_checkpoint_check_timeout(ToProto<i64>(options.CheckpointCheckTimeout));
    req->set_force(options.Force);
    req->set_preserve_account(options.PreserveAccount);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::RestoreTableBackup(
    const TBackupManifestPtr& manifest,
    const TRestoreTableBackupOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RestoreTableBackup();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_manifest(), *manifest);
    req->set_force(options.Force);
    req->set_mount(options.Mount);
    req->set_enable_replicas(options.EnableReplicas);
    req->set_preserve_account(options.PreserveAccount);

    return req->Invoke().As<void>();
}

TFuture<std::vector<TTableReplicaId>> TClient::GetInSyncReplicas(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<TLegacyKey>& keys,
    const TGetInSyncReplicasOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetInSyncReplicas();
    SetTimeoutOptions(*req, options);

    if (options.Timestamp) {
        req->set_timestamp(options.Timestamp);
    }

    if (options.CachedSyncReplicasTimeout) {
        req->set_cached_sync_replicas_timeout(NYT::ToProto<i64>(*options.CachedSyncReplicasTimeout));
    }

    req->set_path(path);
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetInSyncReplicasPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        return FromProto<std::vector<TTableReplicaId>>(rsp->replica_ids());
    }));
}

TFuture<std::vector<TTableReplicaId>> TClient::GetInSyncReplicas(
    const TYPath& path,
    const TGetInSyncReplicasOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetInSyncReplicas();
    SetTimeoutOptions(*req, options);

    if (options.Timestamp) {
        req->set_timestamp(options.Timestamp);
    }

    if (options.CachedSyncReplicasTimeout) {
        req->set_cached_sync_replicas_timeout(NYT::ToProto<i64>(*options.CachedSyncReplicasTimeout));
    }

    req->set_path(path);
    req->RequireServerFeature(ERpcProxyFeature::GetInSyncWithoutKeys);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetInSyncReplicasPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        return FromProto<std::vector<TTableReplicaId>>(rsp->replica_ids());
    }));
}

TFuture<std::vector<TTabletInfo>> TClient::GetTabletInfos(
    const TYPath& path,
    const std::vector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetTabletInfos();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    ToProto(req->mutable_tablet_indexes(), tabletIndexes);
    req->set_request_errors(options.RequestErrors);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetTabletInfosPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        std::vector<TTabletInfo> tabletInfos;
        tabletInfos.reserve(rsp->tablets_size());
        for (const auto& protoTabletInfo : rsp->tablets()) {
            auto& tabletInfo = tabletInfos.emplace_back();
            tabletInfo.TotalRowCount = protoTabletInfo.total_row_count();
            tabletInfo.TrimmedRowCount = protoTabletInfo.trimmed_row_count();
            tabletInfo.DelayedLocklessRowCount = protoTabletInfo.delayed_lockless_row_count();
            tabletInfo.BarrierTimestamp = protoTabletInfo.barrier_timestamp();
            tabletInfo.LastWriteTimestamp = protoTabletInfo.last_write_timestamp();
            tabletInfo.TableReplicaInfos = protoTabletInfo.replicas().empty()
                ? std::nullopt
                : std::make_optional(std::vector<TTabletInfo::TTableReplicaInfo>());
            FromProto(&tabletInfo.TabletErrors, protoTabletInfo.tablet_errors());

            for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                auto& currentReplica = tabletInfo.TableReplicaInfos->emplace_back();
                currentReplica.ReplicaId = FromProto<TGuid>(protoReplicaInfo.replica_id());
                currentReplica.LastReplicationTimestamp = protoReplicaInfo.last_replication_timestamp();
                currentReplica.Mode = CheckedEnumCast<ETableReplicaMode>(protoReplicaInfo.mode());
                currentReplica.CurrentReplicationRowIndex = protoReplicaInfo.current_replication_row_index();
                currentReplica.CommittedReplicationRowIndex = protoReplicaInfo.committed_replication_row_index();
                currentReplica.ReplicationError = FromProto<TError>(protoReplicaInfo.replication_error());
            }
        }
        return tabletInfos;
    }));
}

TFuture<TGetTabletErrorsResult> TClient::GetTabletErrors(
    const TYPath& path,
    const TGetTabletErrorsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetTabletErrors();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    if (options.Limit) {
        req->set_limit(*options.Limit);
    }

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetTabletErrorsPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        TGetTabletErrorsResult tabletErrors;
        if (rsp->has_incomplete() && rsp->incomplete()) {
            tabletErrors.Incomplete = rsp->incomplete();
        }

        for (i64 index = 0; index != rsp->tablet_ids_size(); ++index) {
            std::vector<TError> errors;
            errors.reserve(rsp->tablet_errors(index).errors().size());
            for (const auto& protoError : rsp->tablet_errors(index).errors()) {
                errors.push_back(FromProto<TError>(protoError));
            }
            tabletErrors.TabletErrors[FromProto<TTabletId>(rsp->tablet_ids(index))] = std::move(errors);
        }

        for (i64 index = 0; index != rsp->replica_ids_size(); ++index) {
            std::vector<TError> errors;
            errors.reserve(rsp->replication_errors(index).errors().size());
            for (const auto& protoError : rsp->replication_errors(index).errors()) {
                errors.push_back(FromProto<TError>(protoError));
            }
            tabletErrors.ReplicationErrors[FromProto<TTableReplicaId>(rsp->replica_ids(index))] = std::move(errors);
        }
        return tabletErrors;
    }));
}

TFuture<std::vector<TTabletActionId>> TClient::BalanceTabletCells(
    const TString& tabletCellBundle,
    const std::vector<TYPath>& movableTables,
    const TBalanceTabletCellsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.BalanceTabletCells();
    SetTimeoutOptions(*req, options);

    req->set_bundle(tabletCellBundle);
    req->set_keep_actions(options.KeepActions);
    ToProto(req->mutable_movable_tables(), movableTables);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspBalanceTabletCellsPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        return FromProto<std::vector<TTabletActionId>>(rsp->tablet_actions());
    }));
}

TFuture<NChaosClient::TReplicationCardPtr> TClient::GetReplicationCard(
    NChaosClient::TReplicationCardId /*replicationCardId*/,
    const TGetReplicationCardOptions& /*options*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<void> TClient::UpdateChaosTableReplicaProgress(
    NChaosClient::TReplicaId /*replicaId*/,
    const TUpdateChaosTableReplicaProgressOptions& /*options*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<void> TClient::AlterReplicationCard(
    NChaosClient::TReplicationCardId replicationCardId,
    const TAlterReplicationCardOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AlterReplicationCard();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_replication_card_id(), replicationCardId);

    if (options.ReplicatedTableOptions) {
        req->set_replicated_table_options(ConvertToYsonString(options.ReplicatedTableOptions).ToString());
    }
    if (options.EnableReplicatedTableTracker) {
        req->set_enable_replicated_table_tracker(*options.EnableReplicatedTableTracker);
    }
    if (options.ReplicationCardCollocationId) {
        ToProto(req->mutable_replication_card_collocation_id(), *options.ReplicationCardCollocationId);
    }
    if (options.CollocationOptions) {
        req->set_collocation_options(ConvertToYsonString(options.CollocationOptions).ToString());
    }

    return req->Invoke().As<void>();
}

TFuture<ITableWriterPtr> TClient::CreateParticipantTableWriter(
    const TDistributedWriteCookiePtr& cookie,
    const TParticipantTableWriterOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.ParticipantWriteTable();
    InitStreamingRequest(*req);

    FillRequest(req.Get(), cookie, options);

    auto schema = New<TTableSchema>();
    return NRpc::CreateRpcClientOutputStream(
        std::move(req),
        BIND ([=] (const TSharedRef& metaRef) {
            NApi::NRpcProxy::NProto::TWriteTableMeta meta;
            if (!TryDeserializeProto(&meta, metaRef)) {
                THROW_ERROR_EXCEPTION("Failed to deserialize schema for participant table writer");
            }

            FromProto(schema.Get(), meta.schema());
        }))
        .Apply(BIND([=] (IAsyncZeroCopyOutputStreamPtr outputStream) {
            return NRpcProxy::CreateTableWriter(std::move(outputStream), std::move(schema));
        })).As<ITableWriterPtr>();
}

TFuture<IQueueRowsetPtr> TClient::PullQueue(
    const TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.PullQueue();
    req->SetResponseHeavy(true);
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_queue_path(), queuePath);
    req->set_offset(offset);
    req->set_partition_index(partitionIndex);
    ToProto(req->mutable_row_batch_read_options(), rowBatchReadOptions);

    req->set_use_native_tablet_node_api(options.UseNativeTabletNodeApi);
    req->set_replica_consistency(static_cast<NProto::EReplicaConsistency>(options.ReplicaConsistency));

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspPullQueuePtr& rsp) -> IQueueRowsetPtr {
        auto rowset = DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
        return CreateQueueRowset(rowset, rsp->start_offset());
    }));
}

TFuture<IQueueRowsetPtr> TClient::PullQueueConsumer(
    const TRichYPath& consumerPath,
    const TRichYPath& queuePath,
    std::optional<i64> offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueConsumerOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    // COMPAT(nadya73): Use PullConsumer (not PullQueueConsumer) for compatibility with old clusters.
    auto req = proxy.PullConsumer();
    req->SetResponseHeavy(true);
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_consumer_path(), consumerPath);
    ToProto(req->mutable_queue_path(), queuePath);
    if (offset) {
        req->set_offset(*offset);
    }
    req->set_partition_index(partitionIndex);
    ToProto(req->mutable_row_batch_read_options(), rowBatchReadOptions);

    req->set_replica_consistency(static_cast<NProto::EReplicaConsistency>(options.ReplicaConsistency));

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspPullQueueConsumerPtr& rsp) -> IQueueRowsetPtr {
        auto rowset = DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
        return CreateQueueRowset(rowset, rsp->start_offset());
    }));
}

TFuture<void> TClient::RegisterQueueConsumer(
    const TRichYPath& queuePath,
    const TRichYPath& consumerPath,
    bool vital,
    const TRegisterQueueConsumerOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RegisterQueueConsumer();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_queue_path(), queuePath);
    ToProto(req->mutable_consumer_path(), consumerPath);
    req->set_vital(vital);
    if (options.Partitions) {
        ToProto(req->mutable_partitions()->mutable_items(), *options.Partitions);
    }

    return req->Invoke().AsVoid();
}

TFuture<void> TClient::UnregisterQueueConsumer(
    const TRichYPath& queuePath,
    const TRichYPath& consumerPath,
    const TUnregisterQueueConsumerOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.UnregisterQueueConsumer();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_queue_path(), queuePath);
    ToProto(req->mutable_consumer_path(), consumerPath);

    return req->Invoke().AsVoid();
}

TFuture<std::vector<TListQueueConsumerRegistrationsResult>> TClient::ListQueueConsumerRegistrations(
    const std::optional<NYPath::TRichYPath>& queuePath,
    const std::optional<NYPath::TRichYPath>& consumerPath,
    const TListQueueConsumerRegistrationsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ListQueueConsumerRegistrations();
    SetTimeoutOptions(*req, options);

    if (queuePath) {
        ToProto(req->mutable_queue_path(), *queuePath);
    }
    if (consumerPath) {
        ToProto(req->mutable_consumer_path(), *consumerPath);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspListQueueConsumerRegistrationsPtr& rsp) {
        std::vector<TListQueueConsumerRegistrationsResult> result;
        result.reserve(rsp->registrations().size());
        for (const auto& registration : rsp->registrations()) {
            std::optional<std::vector<int>> partitions;
            if (registration.has_partitions()) {
                partitions = FromProto<std::vector<int>>(registration.partitions().items());
            }
            result.push_back({
                .QueuePath = FromProto<TRichYPath>(registration.queue_path()),
                .ConsumerPath = FromProto<TRichYPath>(registration.consumer_path()),
                .Vital = registration.vital(),
                .Partitions = std::move(partitions),
            });
        }
        return result;
    }));
}

TFuture<TCreateQueueProducerSessionResult> TClient::CreateQueueProducerSession(
    const TRichYPath& producerPath,
    const TRichYPath& queuePath,
    const NQueueClient::TQueueProducerSessionId& sessionId,
    const TCreateQueueProducerSessionOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CreateQueueProducerSession();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_producer_path(), producerPath);
    ToProto(req->mutable_queue_path(), queuePath);
    ToProto(req->mutable_session_id(), sessionId);
    if (options.UserMeta) {
        ToProto(req->mutable_user_meta(), ConvertToYsonString(options.UserMeta).ToString());
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCreateQueueProducerSessionPtr& rsp) {
        INodePtr userMeta;
        if (rsp->has_user_meta()) {
            userMeta = ConvertTo<INodePtr>(TYsonString(FromProto<TString>(rsp->user_meta())));
        }

        return TCreateQueueProducerSessionResult{
            .SequenceNumber = FromProto<TQueueProducerSequenceNumber>(rsp->sequence_number()),
            .Epoch = FromProto<TQueueProducerEpoch>(rsp->epoch()),
            .UserMeta = std::move(userMeta),
        };
    }));
}

TFuture<void> TClient::RemoveQueueProducerSession(
    const NYPath::TRichYPath& producerPath,
    const NYPath::TRichYPath& queuePath,
    const NQueueClient::TQueueProducerSessionId& sessionId,
    const TRemoveQueueProducerSessionOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RemoveQueueProducerSession();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_producer_path(), producerPath);
    ToProto(req->mutable_queue_path(), queuePath);
    ToProto(req->mutable_session_id(), sessionId);

    return req->Invoke().AsVoid();
}

TFuture<void> TClient::AddMember(
    const TString& group,
    const TString& member,
    const TAddMemberOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AddMember();
    SetTimeoutOptions(*req, options);

    req->set_group(group);
    req->set_member(member);
    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::RemoveMember(
    const TString& group,
    const TString& member,
    const TRemoveMemberOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RemoveMember();
    SetTimeoutOptions(*req, options);

    req->set_group(group);
    req->set_member(member);
    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);

    return req->Invoke().As<void>();
}

TFuture<TCheckPermissionResponse> TClient::CheckPermission(
    const TString& user,
    const TYPath& path,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CheckPermission();
    SetTimeoutOptions(*req, options);

    req->set_user(user);
    req->set_path(path);
    req->set_permission(static_cast<int>(permission));
    if (options.Columns) {
        auto* protoColumns = req->mutable_columns();
        ToProto(protoColumns->mutable_items(), *options.Columns);
    }
    if (options.Vital) {
        req->set_vital(*options.Vital);
    }

    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCheckPermissionPtr& rsp) {
        TCheckPermissionResponse response;
        static_cast<TCheckPermissionResult&>(response) = FromProto<TCheckPermissionResult>(rsp->result());
        if (rsp->has_columns()) {
            response.Columns = FromProto<std::vector<TCheckPermissionResult>>(rsp->columns().items());
        }
        return response;
    }));
}

TFuture<TCheckPermissionByAclResult> TClient::CheckPermissionByAcl(
    const std::optional<TString>& user,
    EPermission permission,
    INodePtr acl,
    const TCheckPermissionByAclOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CheckPermissionByAcl();
    SetTimeoutOptions(*req, options);

    if (user) {
        req->set_user(*user);
    }
    req->set_permission(static_cast<int>(permission));
    req->set_acl(ConvertToYsonString(acl).ToString());
    req->set_ignore_missing_subjects(options.IgnoreMissingSubjects);

    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCheckPermissionByAclPtr& rsp) {
        return FromProto<TCheckPermissionByAclResult>(rsp->result());
    }));
}

TFuture<void> TClient::TransferAccountResources(
    const TString& srcAccount,
    const TString& dstAccount,
    NYTree::INodePtr resourceDelta,
    const TTransferAccountResourcesOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.TransferAccountResources();
    SetTimeoutOptions(*req, options);

    req->set_src_account(srcAccount);
    req->set_dst_account(dstAccount);
    req->set_resource_delta(ConvertToYsonString(resourceDelta).ToString());

    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::TransferPoolResources(
    const TString& srcPool,
    const TString& dstPool,
    const TString& poolTree,
    NYTree::INodePtr resourceDelta,
    const TTransferPoolResourcesOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.TransferPoolResources();
    SetTimeoutOptions(*req, options);

    req->set_src_pool(srcPool);
    req->set_dst_pool(dstPool);
    req->set_pool_tree(poolTree);
    req->set_resource_delta(ConvertToYsonString(resourceDelta).ToString());

    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<NScheduler::TOperationId> TClient::StartOperation(
    NScheduler::EOperationType type,
    const TYsonString& spec,
    const TStartOperationOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.StartOperation();
    SetTimeoutOptions(*req, options);

    req->set_type(NProto::ConvertOperationTypeToProto(type));
    req->set_spec(spec.ToString());

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_transactional_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspStartOperationPtr& rsp) {
        return FromProto<TOperationId>(rsp->operation_id());
    }));
}

TFuture<void> TClient::AbortOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TAbortOperationOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AbortOperation();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);

    if (options.AbortMessage) {
        req->set_abort_message(*options.AbortMessage);
    }

    return req->Invoke().As<void>();
}

TFuture<void> TClient::SuspendOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TSuspendOperationOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SuspendOperation();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);
    req->set_abort_running_jobs(options.AbortRunningJobs);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::ResumeOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TResumeOperationOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ResumeOperation();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::CompleteOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TCompleteOperationOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CompleteOperation();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::UpdateOperationParameters(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TYsonString& parameters,
    const TUpdateOperationParametersOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.UpdateOperationParameters();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);

    req->set_parameters(parameters.ToString());

    return req->Invoke().As<void>();
}

TFuture<TOperation> TClient::GetOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TGetOperationOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetOperation();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);

    ToProto(req->mutable_master_read_options(), options);
    // COMPAT(max42): after 22.3 is everywhere, drop legacy field.
    if (options.Attributes) {
        ToProto(req->mutable_legacy_attributes(), *options.Attributes);
        ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
    }

    req->set_include_runtime(options.IncludeRuntime);
    req->set_maximum_cypress_progress_age(ToProto<i64>(options.MaximumCypressProgressAge));

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetOperationPtr& rsp) {
        auto attributes = ConvertToAttributes(TYsonStringBuf(rsp->meta()));
        TOperation operation;
        Deserialize(operation, std::move(attributes), /*clone*/ false);
        return operation;
    }));
}

TFuture<void> TClient::DumpJobContext(
    NJobTrackerClient::TJobId jobId,
    const TYPath& path,
    const TDumpJobContextOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.DumpJobContext();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_job_id(), jobId);
    req->set_path(path);

    return req->Invoke().As<void>();
}

TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> TClient::GetJobInput(
    NJobTrackerClient::TJobId jobId,
    const TGetJobInputOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.GetJobInput();
    if (options.Timeout) {
        SetTimeoutOptions(*req, options);
    } else {
        InitStreamingRequest(*req);
    }

    ToProto(req->mutable_job_id(), jobId);
    req->set_job_spec_source(static_cast<NProto::EJobSpecSource>(options.JobSpecSource));

    return CreateRpcClientInputStream(std::move(req));
}

TFuture<TYsonString> TClient::GetJobInputPaths(
    NJobTrackerClient::TJobId jobId,
    const TGetJobInputPathsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetJobInputPaths();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_job_id(), jobId);
    req->set_job_spec_source(static_cast<NProto::EJobSpecSource>(options.JobSpecSource));

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetJobInputPathsPtr& rsp) {
        return TYsonString(rsp->paths());
    }));
}

TFuture<TYsonString> TClient::GetJobSpec(
    NJobTrackerClient::TJobId jobId,
    const TGetJobSpecOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetJobSpec();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_job_id(), jobId);
    req->set_omit_node_directory(options.OmitNodeDirectory);
    req->set_omit_input_table_specs(options.OmitInputTableSpecs);
    req->set_omit_output_table_specs(options.OmitOutputTableSpecs);
    req->set_job_spec_source(static_cast<NProto::EJobSpecSource>(options.JobSpecSource));

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetJobSpecPtr& rsp) {
        return TYsonString(rsp->job_spec());
    }));
}

TFuture<TSharedRef> TClient::GetJobStderr(
    const TOperationIdOrAlias& operationIdOrAlias,
    NJobTrackerClient::TJobId jobId,
    const TGetJobStderrOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetJobStderr();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);
    ToProto(req->mutable_job_id(), jobId);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetJobStderrPtr& rsp) {
        YT_VERIFY(rsp->Attachments().size() == 1);
        return rsp->Attachments().front();
    }));
}

TFuture<TSharedRef> TClient::GetJobFailContext(
    const TOperationIdOrAlias& operationIdOrAlias,
    NJobTrackerClient::TJobId jobId,
    const TGetJobFailContextOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetJobFailContext();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);
    ToProto(req->mutable_job_id(), jobId);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetJobFailContextPtr& rsp) {
        YT_VERIFY(rsp->Attachments().size() == 1);
        return rsp->Attachments().front();
    }));
}

TFuture<TListOperationsResult> TClient::ListOperations(
    const TListOperationsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ListOperations();
    SetTimeoutOptions(*req, options);

    if (options.FromTime) {
        req->set_from_time(NYT::ToProto<i64>(*options.FromTime));
    }
    if (options.ToTime) {
        req->set_to_time(NYT::ToProto<i64>(*options.ToTime));
    }
    if (options.CursorTime) {
        req->set_cursor_time(NYT::ToProto<i64>(*options.CursorTime));
    }
    req->set_cursor_direction(static_cast<NProto::EOperationSortDirection>(options.CursorDirection));
    if (options.UserFilter) {
        req->set_user_filter(*options.UserFilter);
    }

    if (options.AccessFilter) {
        req->set_access_filter(ConvertToYsonString(options.AccessFilter).ToString());
    }

    if (options.StateFilter) {
        req->set_state_filter(NProto::ConvertOperationStateToProto(*options.StateFilter));
    }
    if (options.TypeFilter) {
        req->set_type_filter(NProto::ConvertOperationTypeToProto(*options.TypeFilter));
    }
    if (options.SubstrFilter) {
        req->set_substr_filter(*options.SubstrFilter);
    }
    if (options.Pool) {
        req->set_pool(*options.Pool);
    }
    if (options.PoolTree) {
        req->set_pool_tree(*options.PoolTree);
    }
    if (options.WithFailedJobs) {
        req->set_with_failed_jobs(*options.WithFailedJobs);
    }
    if (options.ArchiveFetchingTimeout) {
        req->set_archive_fetching_timeout(NYT::ToProto<i64>(options.ArchiveFetchingTimeout));
    }
    req->set_include_archive(options.IncludeArchive);
    req->set_include_counters(options.IncludeCounters);
    req->set_limit(options.Limit);

    // COMPAT(max42): after 22.3 is everywhere, drop legacy field.
    if (options.Attributes) {
        ToProto(req->mutable_legacy_attributes()->mutable_keys(), *options.Attributes);
        ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
    } else {
        req->mutable_legacy_attributes()->set_all(true);
    }

    req->set_enable_ui_mode(options.EnableUIMode);

    ToProto(req->mutable_master_read_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspListOperationsPtr& rsp) {
        return FromProto<TListOperationsResult>(rsp->result());
    }));
}

TFuture<TListJobsResult> TClient::ListJobs(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TListJobsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ListJobs();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);

    if (options.Type) {
        req->set_type(NProto::ConvertJobTypeToProto(*options.Type));
    }
    if (options.State) {
        req->set_state(NProto::ConvertJobStateToProto(*options.State));
    }
    if (options.Address) {
        req->set_address(*options.Address);
    }
    if (options.WithStderr) {
        req->set_with_stderr(*options.WithStderr);
    }
    if (options.WithFailContext) {
        req->set_with_fail_context(*options.WithFailContext);
    }
    if (options.WithSpec) {
        req->set_with_spec(*options.WithSpec);
    }
    if (options.JobCompetitionId) {
        ToProto(req->mutable_job_competition_id(), options.JobCompetitionId);
    }
    if (options.WithCompetitors) {
        req->set_with_competitors(*options.WithCompetitors);
    }
    if (options.WithMonitoringDescriptor) {
        req->set_with_monitoring_descriptor(*options.WithMonitoringDescriptor);
    }
    if (options.TaskName) {
        req->set_task_name(*options.TaskName);
    }

    req->set_sort_field(static_cast<NProto::EJobSortField>(options.SortField));
    req->set_sort_order(static_cast<NProto::EJobSortDirection>(options.SortOrder));

    req->set_limit(options.Limit);
    req->set_offset(options.Offset);

    req->set_include_cypress(options.IncludeCypress);
    req->set_include_controller_agent(options.IncludeControllerAgent);
    req->set_include_archive(options.IncludeArchive);

    req->set_data_source(static_cast<NProto::EDataSource>(options.DataSource));
    req->set_running_jobs_lookbehind_period(NYT::ToProto<i64>(options.RunningJobsLookbehindPeriod));

    ToProto(req->mutable_master_read_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspListJobsPtr& rsp) {
        return FromProto<TListJobsResult>(rsp->result());
    }));
}

TFuture<TYsonString> TClient::GetJob(
    const TOperationIdOrAlias& operationIdOrAlias,
    NJobTrackerClient::TJobId jobId,
    const TGetJobOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetJob();
    SetTimeoutOptions(*req, options);

    NScheduler::ToProto(req, operationIdOrAlias);
    ToProto(req->mutable_job_id(), jobId);

    // COMPAT(max42): after 22.3 is everywhere, drop legacy field.
    if (options.Attributes) {
        ToProto(req->mutable_legacy_attributes()->mutable_keys(), *options.Attributes);
        ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
    } else {
        req->mutable_legacy_attributes()->set_all(true);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetJobPtr& rsp) {
        return TYsonString(rsp->info());
    }));
}

TFuture<void> TClient::AbandonJob(
    NJobTrackerClient::TJobId jobId,
    const TAbandonJobOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AbandonJob();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_job_id(), jobId);

    return req->Invoke().As<void>();
}

TFuture<TPollJobShellResponse> TClient::PollJobShell(
    NJobTrackerClient::TJobId jobId,
    const std::optional<TString>& shellName,
    const TYsonString& parameters,
    const TPollJobShellOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.PollJobShell();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_job_id(), jobId);
    req->set_parameters(parameters.ToString());
    if (shellName) {
        req->set_shell_name(*shellName);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspPollJobShellPtr& rsp) {
        return TPollJobShellResponse {
            .Result = TYsonString(rsp->result()),
        };
    }));
}

TFuture<void> TClient::AbortJob(
    NJobTrackerClient::TJobId jobId,
    const TAbortJobOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AbortJob();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_job_id(), jobId);
    if (options.InterruptTimeout) {
        req->set_interrupt_timeout(NYT::ToProto<i64>(*options.InterruptTimeout));
    }

    return req->Invoke().As<void>();
}

TFuture<void> TClient::DumpJobProxyLog(
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::TOperationId operationId,
    const NYPath::TYPath& path,
    const TDumpJobProxyLogOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.DumpJobProxyLog();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_operation_id(), operationId);
    ToProto(req->mutable_path(), path);

    return req->Invoke().As<void>();
}

TFuture<TGetFileFromCacheResult> TClient::GetFileFromCache(
    const TString& md5,
    const TGetFileFromCacheOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetFileFromCache();
    SetTimeoutOptions(*req, options);
    ToProto(req->mutable_transactional_options(), options);

    req->set_md5(md5);
    req->set_cache_path(options.CachePath);

    ToProto(req->mutable_master_read_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetFileFromCachePtr& rsp) {
        return FromProto<TGetFileFromCacheResult>(rsp->result());
    }));
}

TFuture<TPutFileToCacheResult> TClient::PutFileToCache(
    const TYPath& path,
    const TString& expectedMD5,
    const TPutFileToCacheOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.PutFileToCache();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_transactional_options(), options);
    req->set_path(path);
    req->set_md5(expectedMD5);
    req->set_cache_path(options.CachePath);
    req->set_preserve_expiration_timeout(options.PreserveExpirationTimeout);

    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspPutFileToCachePtr& rsp) {
        return FromProto<TPutFileToCacheResult>(rsp->result());
    }));
}

TFuture<TClusterMeta> TClient::GetClusterMeta(
    const TGetClusterMetaOptions& /*options*/)
{
    ThrowUnimplemented("GetClusterMeta");
}

TFuture<void> TClient::CheckClusterLiveness(
    const TCheckClusterLivenessOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CheckClusterLiveness();
    SetTimeoutOptions(*req, options);

    req->set_check_cypress_root(options.CheckCypressRoot);
    req->set_check_secondary_master_cells(options.CheckSecondaryMasterCells);
    if (auto bundleName = options.CheckTabletCellBundle) {
        req->set_check_tablet_cell_bundle(*bundleName);
    }

    return req->Invoke().As<void>();
}

TFuture<TSkynetSharePartsLocationsPtr> TClient::LocateSkynetShare(
    const TRichYPath&,
    const TLocateSkynetShareOptions& /*options*/)
{
    ThrowUnimplemented("LocateSkynetShare");
}

TFuture<std::vector<TColumnarStatistics>> TClient::GetColumnarStatistics(
    const std::vector<TRichYPath>& path,
    const TGetColumnarStatisticsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetColumnarStatistics();
    SetTimeoutOptions(*req, options);

    for (const auto& subPath: path) {
        req->add_paths(ConvertToYsonString(subPath).ToString());
    }

    req->set_fetcher_mode(static_cast<NProto::EColumnarStatisticsFetcherMode>(options.FetcherMode));

    ToProto(req->mutable_fetch_chunk_spec_config(), options.FetchChunkSpecConfig);

    ToProto(req->mutable_fetcher_config(), options.FetcherConfig);

    req->set_enable_early_finish(options.EnableEarlyFinish);

    ToProto(req->mutable_transactional_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetColumnarStatisticsPtr& rsp) {
        return NYT::FromProto<std::vector<TColumnarStatistics>>(rsp->statistics());
    }));
}

TFuture<NApi::TMultiTablePartitions> TClient::PartitionTables(
    const std::vector<TRichYPath>& paths,
    const TPartitionTablesOptions& options)
{
    if (options.PartitionMode == NTableClient::ETablePartitionMode::Sorted) {
        THROW_ERROR_EXCEPTION("Sorted partitioning is not supported yet");
    }

    auto proxy = CreateApiServiceProxy();

    auto req = proxy.PartitionTables();
    SetTimeoutOptions(*req, options);

    for (const auto& path : paths) {
        req->add_paths(ToString(path));
    }

    ToProto(req->mutable_fetch_chunk_spec_config(), options.FetchChunkSpecConfig);

    ToProto(req->mutable_fetcher_config(), options.FetcherConfig);

    req->mutable_chunk_slice_fetcher_config()->set_max_slices_per_fetch(
        ToProto<i64>(options.ChunkSliceFetcherConfig->MaxSlicesPerFetch));

    req->set_partition_mode(static_cast<NProto::EPartitionTablesMode>(options.PartitionMode));

    req->set_data_weight_per_partition(options.DataWeightPerPartition);

    if (options.MaxPartitionCount) {
        req->set_max_partition_count(*options.MaxPartitionCount);
    }

    req->set_adjust_data_weight_per_partition(options.AdjustDataWeightPerPartition);

    req->set_enable_key_guarantee(options.EnableKeyGuarantee);

    ToProto(req->mutable_transactional_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspPartitionTablesPtr& rsp) {
        return FromProto<TMultiTablePartitions>(*rsp);
    }));
}

TFuture<void> TClient::TruncateJournal(
    const TYPath& path,
    i64 rowCount,
    const TTruncateJournalOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.TruncateJournal();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_row_count(rowCount);
    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);

    return req->Invoke().As<void>();
}

TFuture<int> TClient::BuildSnapshot(const TBuildSnapshotOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.BuildSnapshot();
    if (options.CellId) {
        ToProto(req->mutable_cell_id(), options.CellId);
    }
    req->set_set_read_only(options.SetReadOnly);
    req->set_wait_for_snapshot_completion(options.WaitForSnapshotCompletion);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspBuildSnapshotPtr>& rspOrError) -> int {
        const auto& rsp = rspOrError.ValueOrThrow();
        return rsp->snapshot_id();
    }));
}

TFuture<TCellIdToSnapshotIdMap> TClient::BuildMasterSnapshots(const TBuildMasterSnapshotsOptions& /*options*/)
{
    ThrowUnimplemented("BuildMasterSnapshots");
}

TFuture<TCellIdToConsistentStateMap> TClient::GetMasterConsistentState(const TGetMasterConsistentStateOptions& /*options*/)
{
    ThrowUnimplemented("GetMasterConsistentState");
}

TFuture<void> TClient::ExitReadOnly(
    NHydra::TCellId cellId,
    const TExitReadOnlyOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ExitReadOnly();
    ToProto(req->mutable_cell_id(), cellId);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::MasterExitReadOnly(const TMasterExitReadOnlyOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.MasterExitReadOnly();
    req->set_retry(options.Retry);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::DiscombobulateNonvotingPeers(
    NHydra::TCellId cellId,
    const TDiscombobulateNonvotingPeersOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.DiscombobulateNonvotingPeers();
    ToProto(req->mutable_cell_id(), cellId);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::SwitchLeader(
    NHydra::TCellId /*cellId*/,
    const std::string& /*newLeaderAddress*/,
    const TSwitchLeaderOptions& /*options*/)
{
    ThrowUnimplemented("SwitchLeader");
}

TFuture<void> TClient::ResetStateHash(
    NHydra::TCellId /*cellId*/,
    const TResetStateHashOptions& /*options*/)
{
    ThrowUnimplemented("ResetStateHash");
}

TFuture<void> TClient::GCCollect(const TGCCollectOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GCCollect();
    if (options.CellId) {
        ToProto(req->mutable_cell_id(), options.CellId);
    }

    return req->Invoke().As<void>();
}

TFuture<void> TClient::KillProcess(
    const std::string& /*address*/,
    const TKillProcessOptions& /*options*/)
{
    ThrowUnimplemented("KillProcess");
}

TFuture<TString> TClient::WriteCoreDump(
    const std::string& /*address*/,
    const TWriteCoreDumpOptions& /*options*/)
{
    ThrowUnimplemented("WriteCoreDump");
}

TFuture<TGuid> TClient::WriteLogBarrier(
    const std::string& /*address*/,
    const TWriteLogBarrierOptions& /*options*/)
{
    ThrowUnimplemented("WriteLogBarrier");
}

TFuture<TString> TClient::WriteOperationControllerCoreDump(
    TOperationId /*operationId*/,
    const TWriteOperationControllerCoreDumpOptions& /*options*/)
{
    ThrowUnimplemented("WriteOperationControllerCoreDump");
}

TFuture<void> TClient::HealExecNode(
    const std::string& /*address*/,
    const THealExecNodeOptions& /*options*/)
{
    ThrowUnimplemented("HealExecNode");
}

TFuture<void> TClient::SuspendCoordinator(
    TCellId coordinatorCellId,
    const TSuspendCoordinatorOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SuspendCoordinator();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_coordinator_cell_id(), coordinatorCellId);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::ResumeCoordinator(
    TCellId coordinatorCellId,
    const TResumeCoordinatorOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ResumeCoordinator();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_coordinator_cell_id(), coordinatorCellId);

    return req->Invoke().As<void>();
}

TFuture<void> TClient::MigrateReplicationCards(
    TCellId chaosCellId,
    const TMigrateReplicationCardsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.MigrateReplicationCards();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_chaos_cell_id(), chaosCellId);
    ToProto(req->mutable_replication_card_ids(), options.ReplicationCardIds);
    if (options.DestinationCellId) {
        ToProto(req->mutable_destination_cell_id(), options.DestinationCellId);
    }

    return req->Invoke().As<void>();
}

namespace {

NProto::EMaintenanceComponent ConvertMaintenanceComponentToProto(EMaintenanceComponent component)
{
    switch (component) {
        case EMaintenanceComponent::ClusterNode:
            return NProto::EMaintenanceComponent::MC_CLUSTER_NODE;
        case EMaintenanceComponent::HttpProxy:
            return NProto::EMaintenanceComponent::MC_HTTP_PROXY;
        case EMaintenanceComponent::RpcProxy:
            return NProto::EMaintenanceComponent::MC_RPC_PROXY;
        case EMaintenanceComponent::Host:
            return NProto::EMaintenanceComponent::MC_HOST;
        default:
            THROW_ERROR_EXCEPTION("Invalid maintenance component %Qlv", component);
    }
}

NProto::EMaintenanceType ConvertMaintenanceTypeToProto(EMaintenanceType type)
{
    switch (type) {
        case EMaintenanceType::Ban:
            return NProto::EMaintenanceType::MT_BAN;
        case EMaintenanceType::Decommission:
            return NProto::EMaintenanceType::MT_DECOMMISSION;
        case EMaintenanceType::DisableSchedulerJobs:
            return NProto::EMaintenanceType::MT_DISABLE_SCHEDULER_JOBS;
        case EMaintenanceType::DisableWriteSessions:
            return NProto::EMaintenanceType::MT_DISABLE_WRITE_SESSIONS;
        case EMaintenanceType::DisableTabletCells:
            return NProto::EMaintenanceType::MT_DISABLE_TABLET_CELLS;
        case EMaintenanceType::PendingRestart:
            return NProto::EMaintenanceType::MT_PENDING_RESTART;
        default:
            THROW_ERROR_EXCEPTION("Invalid maintenance type %Qv", type);
    }
}

} // namespace

TFuture<TMaintenanceIdPerTarget> TClient::AddMaintenance(
    EMaintenanceComponent component,
    const std::string& address,
    EMaintenanceType type,
    const TString& comment,
    const TAddMaintenanceOptions& options)
{
    ValidateMaintenanceComment(comment);

    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AddMaintenance();
    SetTimeoutOptions(*req, options);

    req->set_component(ConvertMaintenanceComponentToProto(component));
    req->set_address(ToProto<TProtobufString>(address));
    req->set_type(ConvertMaintenanceTypeToProto(type));
    req->set_comment(comment);
    req->set_supports_per_target_response(true);

    return req->Invoke().Apply(BIND(
        [address] (const TErrorOr<TApiServiceProxy::TRspAddMaintenancePtr>& rsp) {
            const auto& value = rsp.ValueOrThrow();

            TMaintenanceIdPerTarget result;

            // COMPAT(kvk1920): Compatibility with pre-24.2 RPC proxies.
            if (value->has_id()) {
                result[address] = FromProto<TMaintenanceId>(value->id());
            } else {
                result.reserve(value->id_per_target_size());
                for (const auto& [target, id] : value->id_per_target()) {
                    result.insert({target, FromProto<TMaintenanceId>(id)});
                }
            }
            return result;
        }));
}

TFuture<TMaintenanceCountsPerTarget> TClient::RemoveMaintenance(
    EMaintenanceComponent component,
    const std::string& address,
    const TMaintenanceFilter& filter,
    const TRemoveMaintenanceOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RemoveMaintenance();
    SetTimeoutOptions(*req, options);

    req->set_component(ConvertMaintenanceComponentToProto(component));
    req->set_address(ToProto<TProtobufString>(address));

    ToProto(req->mutable_ids(), filter.Ids);

    if (filter.Type) {
        req->set_type(ConvertMaintenanceTypeToProto(*filter.Type));
    }

    using TByUser = TMaintenanceFilter::TByUser;
    Visit(filter.User,
        [] (TByUser::TAll) {},
        [&] (TByUser::TMine) {
            req->set_mine(true);
        },
        [&] (const TString& user) {
            req->set_user(user);
        });

    req->set_supports_per_target_response(true);

    return req->Invoke().Apply(BIND([address] (const TErrorOr<TApiServiceProxy::TRspRemoveMaintenancePtr>& rsp) {
        auto rspValue = rsp.ValueOrThrow();
        TMaintenanceCountsPerTarget result;

        // COMPAT(kvk1920): Compatibility with pre-24.2 RPC proxies.
        if (!rspValue->supports_per_target_response()) {
            auto& counts = result[address];
            // COMPAT(kvk1920): Compatibility with pre-23.2 RPC proxies.
            if (!rspValue->use_map_instead_of_fields()) {
                counts[EMaintenanceType::Ban] = rspValue->ban();
                counts[EMaintenanceType::Decommission] = rspValue->decommission();
                counts[EMaintenanceType::DisableSchedulerJobs] = rspValue->disable_scheduler_jobs();
                counts[EMaintenanceType::DisableWriteSessions] = rspValue->disable_write_sessions();
                counts[EMaintenanceType::DisableTabletCells] = rspValue->disable_tablet_cells();
                counts[EMaintenanceType::PendingRestart] = rspValue->pending_restart();
            } else {
                for (const auto& [type, count] : rspValue->removed_maintenance_counts()) {
                    counts[CheckedEnumCast<EMaintenanceType>(type)] = count;
                }
            }
        } else {
            result.reserve(rspValue->removed_maintenance_counts_per_target_size());
            for (const auto& [target, protoCounts] : rspValue->removed_maintenance_counts_per_target()) {
                auto& counts = result[target];
                for (const auto& [type, count] : protoCounts.counts()) {
                    counts[CheckedEnumCast<EMaintenanceType>(type)] = count;
                }
            }
        }

        return result;
    }));
}

TFuture<void> TClient::SuspendChaosCells(
    const std::vector<TCellId>& cellIds,
    const TSuspendChaosCellsOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SuspendChaosCells();
    ToProto(req->mutable_cell_ids(), cellIds);

    return req->Invoke().AsVoid();
}

TFuture<void> TClient::ResumeChaosCells(
    const std::vector<TCellId>& cellIds,
    const TResumeChaosCellsOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ResumeChaosCells();
    ToProto(req->mutable_cell_ids(), cellIds);

    return req->Invoke().AsVoid();
}

TFuture<void> TClient::SuspendTabletCells(
    const std::vector<TCellId>& cellIds,
    const TSuspendTabletCellsOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SuspendTabletCells();
    ToProto(req->mutable_cell_ids(), cellIds);

    return req->Invoke().AsVoid();
}

TFuture<void> TClient::ResumeTabletCells(
    const std::vector<TCellId>& cellIds,
    const TResumeTabletCellsOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ResumeTabletCells();
    ToProto(req->mutable_cell_ids(), cellIds);

    return req->Invoke().AsVoid();
}

TFuture<TDisableChunkLocationsResult> TClient::DisableChunkLocations(
    const std::string& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TDisableChunkLocationsOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.DisableChunkLocations();
    ToProto(req->mutable_node_address(), nodeAddress);
    ToProto(req->mutable_location_uuids(), locationUuids);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspDisableChunkLocationsPtr& rsp) {
        auto locationUuids = FromProto<std::vector<TGuid>>(rsp->location_uuids());
        return TDisableChunkLocationsResult{
            .LocationUuids = locationUuids
        };
    }));
}

TFuture<TDestroyChunkLocationsResult> TClient::DestroyChunkLocations(
    const std::string& nodeAddress,
    bool recoverUnlinkedDisks,
    const std::vector<TGuid>& locationUuids,
    const TDestroyChunkLocationsOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.DestroyChunkLocations();
    req->set_recover_unlinked_disks(recoverUnlinkedDisks);
    ToProto(req->mutable_node_address(), nodeAddress);
    ToProto(req->mutable_location_uuids(), locationUuids);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspDestroyChunkLocationsPtr& rsp) {
        auto locationUuids = FromProto<std::vector<TGuid>>(rsp->location_uuids());
        return TDestroyChunkLocationsResult{
            .LocationUuids = locationUuids
        };
    }));
}

TFuture<TResurrectChunkLocationsResult> TClient::ResurrectChunkLocations(
    const std::string& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TResurrectChunkLocationsOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ResurrectChunkLocations();
    ToProto(req->mutable_node_address(), nodeAddress);
    ToProto(req->mutable_location_uuids(), locationUuids);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspResurrectChunkLocationsPtr& rsp) {
        auto locationUuids = FromProto<std::vector<TGuid>>(rsp->location_uuids());
        return TResurrectChunkLocationsResult{
            .LocationUuids = locationUuids
        };
    }));
}

TFuture<TRequestRestartResult> TClient::RequestRestart(
    const std::string& nodeAddress,
    const TRequestRestartOptions& /*options*/)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RequestRestart();
    ToProto(req->mutable_node_address(), nodeAddress);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspRequestRestartPtr& /*rsp*/) {
        return TRequestRestartResult();
    }));
}

TFuture<NQueryTrackerClient::TQueryId> TClient::StartQuery(
    NQueryTrackerClient::EQueryEngine engine,
    const TString& query,
    const TStartQueryOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.StartQuery();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);
    req->set_engine(NProto::ConvertQueryEngineToProto(engine));
    req->set_query(query);
    req->set_draft(options.Draft);

    if (options.Settings) {
        req->set_settings(ConvertToYsonString(options.Settings).ToString());
    }
    if (options.Annotations) {
        req->set_annotations(ConvertToYsonString(options.Annotations).ToString());
    }
    if (options.AccessControlObject) {
        req->set_access_control_object(*options.AccessControlObject);
    }
    if (options.AccessControlObjects) {
        auto* protoAccessControlObjects = req->mutable_access_control_objects();
        for (const auto& aco : *options.AccessControlObjects) {
            protoAccessControlObjects->add_items(aco);
        }
    }

    for (const auto& file : options.Files) {
        auto* protoFile = req->add_files();
        protoFile->set_name(file->Name);
        protoFile->set_content(file->Content);
        protoFile->set_type(static_cast<NProto::EContentType>(file->Type));
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspStartQueryPtr& rsp) {
        return FromProto<NQueryTrackerClient::TQueryId>(rsp->query_id());
    }));
}

TFuture<void> TClient::AbortQuery(
    NQueryTrackerClient::TQueryId queryId,
    const TAbortQueryOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AbortQuery();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(req->mutable_query_id(), queryId);

    if (options.AbortMessage) {
        req->set_abort_message(*options.AbortMessage);
    }

    return req->Invoke().AsVoid();
}

TFuture<TQueryResult> TClient::GetQueryResult(
    NQueryTrackerClient::TQueryId queryId,
    i64 resultIndex,
    const TGetQueryResultOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetQueryResult();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(req->mutable_query_id(), queryId);
    req->set_result_index(resultIndex);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetQueryResultPtr& rsp) {
        return TQueryResult{
            .Id = FromProto<NQueryTrackerClient::TQueryId>(rsp->query_id()),
            .ResultIndex = rsp->result_index(),
            .Error = FromProto<TError>(rsp->error()),
            .Schema = rsp->has_schema() ? FromProto<NTableClient::TTableSchemaPtr>(rsp->schema()) : nullptr,
            .DataStatistics = FromProto<NChunkClient::NProto::TDataStatistics>(rsp->data_statistics()),
            .IsTruncated = rsp->is_truncated(),
        };
    }));
}

TFuture<IUnversionedRowsetPtr> TClient::ReadQueryResult(
    NQueryTrackerClient::TQueryId queryId,
    i64 resultIndex,
    const TReadQueryResultOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ReadQueryResult();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(req->mutable_query_id(), queryId);
    req->set_result_index(resultIndex);

    if (options.Columns) {
        auto* protoColumns = req->mutable_columns();
        for (const auto& column : *options.Columns) {
            protoColumns->add_items(column);
        }
    }
    if (options.LowerRowIndex) {
        req->set_lower_row_index(*options.LowerRowIndex);
    }
    if (options.UpperRowIndex) {
        req->set_upper_row_index(*options.UpperRowIndex);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspReadQueryResultPtr& rsp) {
        return DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
    }));
}

TFuture<TQuery> TClient::GetQuery(
    NQueryTrackerClient::TQueryId queryId,
    const TGetQueryOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetQuery();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(req->mutable_query_id(), queryId);

    if (options.Attributes) {
        ToProto(req->mutable_attributes(), options.Attributes);
    }
    if (options.Timestamp) {
        req->set_timestamp(ToProto<i64>(options.Timestamp));
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetQueryPtr& rsp) {
        return FromProto<TQuery>(rsp->query());
    }));
}

TFuture<TListQueriesResult> TClient::ListQueries(
    const TListQueriesOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ListQueries();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);

    if (options.FromTime) {
        req->set_from_time(NYT::ToProto<i64>(*options.FromTime));
    }
    if (options.ToTime) {
        req->set_to_time(NYT::ToProto<i64>(*options.ToTime));
    }
    if (options.CursorTime) {
        req->set_cursor_time(NYT::ToProto<i64>(*options.CursorTime));
    }
    req->set_cursor_direction(static_cast<NProto::EOperationSortDirection>(options.CursorDirection));

    if (options.UserFilter) {
        req->set_user_filter(*options.UserFilter);
    }
    if (options.StateFilter) {
        req->set_state_filter(NProto::ConvertQueryStateToProto(*options.StateFilter));
    }
    if (options.EngineFilter) {
        req->set_engine_filter(NProto::ConvertQueryEngineToProto(*options.EngineFilter));
    }
    if (options.SubstrFilter) {
        req->set_substr_filter(*options.SubstrFilter);
    }

    req->set_limit(options.Limit);

    if (options.Attributes) {
        ToProto(req->mutable_attributes(), options.Attributes);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspListQueriesPtr& rsp) {
        return TListQueriesResult{
            .Queries = FromProto<std::vector<TQuery>>(rsp->queries()),
            .Incomplete = rsp->incomplete(),
            .Timestamp = rsp->timestamp(),
        };
    }));
}

TFuture<void> TClient::AlterQuery(
    NQueryTrackerClient::TQueryId queryId,
    const TAlterQueryOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.AlterQuery();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(req->mutable_query_id(), queryId);

    if (options.Annotations) {
        req->set_annotations(ConvertToYsonString(options.Annotations).ToString());
    }
    if (options.AccessControlObject) {
        req->set_access_control_object(*options.AccessControlObject);
    }
    if (options.AccessControlObjects) {
        auto* protoAccessControlObjects = req->mutable_access_control_objects();
        for (const auto& aco : *options.AccessControlObjects) {
            protoAccessControlObjects->add_items(aco);
        }
    }

    return req->Invoke().AsVoid();
}

TFuture<TGetQueryTrackerInfoResult> TClient::GetQueryTrackerInfo(
    const TGetQueryTrackerInfoOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetQueryTrackerInfo();
    SetTimeoutOptions(*req, options);

    req->set_query_tracker_stage(options.QueryTrackerStage);

    if (options.Attributes) {
        ToProto(req->mutable_attributes(), options.Attributes);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetQueryTrackerInfoPtr& rsp) {
        return TGetQueryTrackerInfoResult{
            .ClusterName = rsp->cluster_name(),
            .SupportedFeatures = TYsonString(rsp->supported_features()),
            .AccessControlObjects = FromProto<std::vector<TString>>(rsp->access_control_objects()),
        };
    }));
}

TFuture<NBundleControllerClient::TBundleConfigDescriptorPtr> TClient::GetBundleConfig(
    const TString& /*bundleName*/,
    const NBundleControllerClient::TGetBundleConfigOptions& /*options*/)
{
    ThrowUnimplemented("GetBundleConfig");
}

TFuture<void> TClient::SetBundleConfig(
    const TString& /*bundleName*/,
    const NBundleControllerClient::TBundleTargetConfigPtr& /*bundleConfig*/,
    const NBundleControllerClient::TSetBundleConfigOptions& /*options*/)
{
    ThrowUnimplemented("SetBundleConfig");
}

TFuture<void> TClient::SetUserPassword(
    const TString& /*user*/,
    const TString& /*currentPasswordSha256*/,
    const TString& /*newPasswordSha256*/,
    const TSetUserPasswordOptions& /*options*/)
{
    ThrowUnimplemented("SetUserPassword");
}

TFuture<TIssueTokenResult> TClient::IssueToken(
    const TString& /*user*/,
    const TString& /*passwordSha256*/,
    const TIssueTokenOptions& /*options*/)
{
    ThrowUnimplemented("IssueToken");
}

TFuture<void> TClient::RevokeToken(
    const TString& /*user*/,
    const TString& /*passwordSha256*/,
    const TString& /*tokenSha256*/,
    const TRevokeTokenOptions& /*options*/)
{
    ThrowUnimplemented("RevokeToken");
}

TFuture<TListUserTokensResult> TClient::ListUserTokens(
    const TString& /*user*/,
    const TString& /*passwordSha256*/,
    const TListUserTokensOptions& /*options*/)
{
    ThrowUnimplemented("ListUserTokens");
}

TFuture<TGetPipelineSpecResult> TClient::GetPipelineSpec(
    const NYPath::TYPath& pipelinePath,
    const TGetPipelineSpecOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetPipelineSpec();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetPipelineSpecPtr& rsp) {
        return TGetPipelineSpecResult{
            .Version = FromProto<NFlow::TVersion>(rsp->version()),
            .Spec = TYsonString(rsp->spec()),
        };
    }));
}

TFuture<TSetPipelineSpecResult> TClient::SetPipelineSpec(
    const NYPath::TYPath& pipelinePath,
    const NYson::TYsonString& spec,
    const TSetPipelineSpecOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SetPipelineSpec();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);
    req->set_spec(spec.ToString());
    req->set_force(options.Force);
    if (options.ExpectedVersion) {
        req->set_expected_version(ToProto<i64>(*options.ExpectedVersion));
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspSetPipelineSpecPtr& rsp) {
        return TSetPipelineSpecResult{
            .Version = FromProto<NFlow::TVersion>(rsp->version()),
        };
    }));
}

TFuture<TGetPipelineDynamicSpecResult> TClient::GetPipelineDynamicSpec(
    const NYPath::TYPath& pipelinePath,
    const TGetPipelineDynamicSpecOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetPipelineDynamicSpec();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetPipelineDynamicSpecPtr& rsp) {
        return TGetPipelineDynamicSpecResult{
            .Version = FromProto<NFlow::TVersion>(rsp->version()),
            .Spec = TYsonString(rsp->spec()),
        };
    }));
}

TFuture<TSetPipelineDynamicSpecResult> TClient::SetPipelineDynamicSpec(
    const NYPath::TYPath& pipelinePath,
    const NYson::TYsonString& spec,
    const TSetPipelineDynamicSpecOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SetPipelineDynamicSpec();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);
    req->set_spec(spec.ToString());
    if (options.ExpectedVersion) {
        req->set_expected_version(ToProto<i64>(*options.ExpectedVersion));
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspSetPipelineDynamicSpecPtr& rsp) {
        return TSetPipelineDynamicSpecResult{
            .Version = FromProto<NFlow::TVersion>(rsp->version()),
        };
    }));
}

TFuture<void> TClient::StartPipeline(
    const NYPath::TYPath& pipelinePath,
    const TStartPipelineOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.StartPipeline();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);

    return req->Invoke().AsVoid();
}

TFuture<void> TClient::StopPipeline(
    const NYPath::TYPath& pipelinePath,
    const TStopPipelineOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.StopPipeline();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);

    return req->Invoke().AsVoid();
}

TFuture<void> TClient::PausePipeline(
    const NYPath::TYPath& pipelinePath,
    const TPausePipelineOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.PausePipeline();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);

    return req->Invoke().AsVoid();
}

TFuture<TPipelineState> TClient::GetPipelineState(
    const NYPath::TYPath& pipelinePath,
    const TGetPipelineStateOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetPipelineState();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetPipelineStatePtr& rsp) {
        return TPipelineState{
            .State = FromProto<NFlow::EPipelineState>(rsp->state()),
        };
    }));
}

TFuture<TGetFlowViewResult> TClient::GetFlowView(
    const NYPath::TYPath& pipelinePath,
    const NYPath::TYPath& viewPath,
    const TGetFlowViewOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetFlowView();
    SetTimeoutOptions(*req, options);

    req->set_pipeline_path(pipelinePath);
    req->set_view_path(viewPath);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetFlowViewPtr& rsp) {
        return TGetFlowViewResult{
            .FlowViewPart = TYsonString(rsp->flow_view_part()),
        };
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
