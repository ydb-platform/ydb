#include "client_base.h"

#include "api_service_proxy.h"
#include "config.h"
#include "file_reader.h"
#include "file_writer.h"
#include "helpers.h"
#include "journal_reader.h"
#include "journal_writer.h"
#include "private.h"
#include "table_reader.h"
#include "table_writer.h"
#include "transaction.h"

#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/attribute_filter.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxTracingTagLength = 1000;
static const TString DisabledSelectQueryTracingTag = "Tag is disabled, look for enable_select_query_tracing_tag parameter";

TString SanitizeTracingTag(const TString& originalTag)
{
    if (originalTag.size() <= MaxTracingTagLength) {
        return originalTag;
    }
    return Format("%v ... TRUNCATED", originalTag.substr(0, MaxTracingTagLength));
}

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr TClientBase::GetConnection()
{
    return GetRpcProxyConnection();
}

TApiServiceProxy TClientBase::CreateApiServiceProxy(NRpc::IChannelPtr channel)
{
    if (!channel) {
        channel = GetRetryingChannel();
    }
    TApiServiceProxy proxy(channel);
    const auto& config = GetRpcProxyConnection()->GetConfig();
    proxy.SetDefaultTimeout(config->RpcTimeout);
    proxy.SetDefaultRequestCodec(config->RequestCodec);
    proxy.SetDefaultResponseCodec(config->ResponseCodec);
    proxy.SetDefaultEnableLegacyRpcCodecs(config->EnableLegacyRpcCodecs);

    NRpc::TStreamingParameters streamingParameters;
    streamingParameters.ReadTimeout = config->DefaultStreamingStallTimeout;
    streamingParameters.WriteTimeout = config->DefaultStreamingStallTimeout;
    proxy.DefaultClientAttachmentsStreamingParameters() = streamingParameters;
    proxy.DefaultServerAttachmentsStreamingParameters() = streamingParameters;

    return proxy;
}

void TClientBase::InitStreamingRequest(NRpc::TClientRequest& request)
{
    auto connection = GetRpcProxyConnection();
    const auto& config = connection->GetConfig();
    request.SetTimeout(config->DefaultTotalStreamingTimeout);
}

TFuture<ITransactionPtr> TClientBase::StartTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    // Keep some stuff to reuse it in the transaction.
    auto connection = GetRpcProxyConnection();
    auto client = GetRpcProxyClient();
    bool sticky = (type == ETransactionType::Tablet) || options.Sticky;
    // Based on this flag the sticky channel will be wrapped into retrying after the first non-retrying call.
    bool wrapStickyChannelOnResponse = false;
    NRpc::IChannelPtr channel;
    if (sticky) {
        channel = CreateNonRetryingStickyChannel();
        if (options.Id == NullTransactionId) {
            channel = WrapStickyChannelIntoRetrying(std::move(channel));
        } else {
            wrapStickyChannelOnResponse = true;
        }
    } else {
        channel = GetRetryingChannel();
    }

    const auto& config = connection->GetConfig();
    auto timeout = options.Timeout.value_or(config->DefaultTransactionTimeout);
    auto pingPeriod = options.PingPeriod.value_or(config->DefaultPingPeriod);

    auto proxy = CreateApiServiceProxy(channel);

    auto req = proxy.StartTransaction();
    req->SetTimeout(config->RpcTimeout);

    req->set_type(static_cast<NProto::ETransactionType>(type));
    req->set_timeout(ToProto<i64>(timeout));
    if (options.Deadline) {
        req->set_deadline(ToProto<ui64>(*options.Deadline));
    }
    if (options.Id) {
        ToProto(req->mutable_id(), options.Id);
    }
    if (options.ParentId) {
        ToProto(req->mutable_parent_id(), options.ParentId);
    }
    ToProto(req->mutable_prerequisite_transaction_ids(), options.PrerequisiteTransactionIds);

    if (options.ReplicateToMasterCellTags) {
        ToProto(
            req->mutable_replicate_to_master_cell_tags()->mutable_cell_tags(),
            *options.ReplicateToMasterCellTags);
    }

    // XXX(sandello): Better? Remove these fields from the protocol at all?
    // COMPAT(kiselyovp): remove auto_abort from the protocol
    req->set_auto_abort(false);
    req->set_sticky(sticky);
    req->set_ping(options.Ping);
    req->set_ping_ancestors(options.PingAncestors);
    req->set_atomicity(static_cast<NProto::EAtomicity>(options.Atomicity));
    req->set_durability(static_cast<NProto::EDurability>(options.Durability));
    if (options.Attributes) {
        ToProto(req->mutable_attributes(), *options.Attributes);
    }
    if (options.StartTimestamp != NullTimestamp) {
        req->set_start_timestamp(options.StartTimestamp);
    }

    return req->Invoke().Apply(BIND(
        [
            =,
            this,
            this_ = MakeStrong(this),
            connection = std::move(connection),
            client = std::move(client),
            channel = std::move(channel)
        ]
        (const TApiServiceProxy::TRspStartTransactionPtr& rsp) mutable {
            std::optional<TStickyTransactionParameters> stickyParameters;
            if (sticky) {
                stickyParameters.emplace().ProxyAddress = rsp->GetAddress();
            }
            auto transactionId = FromProto<TTransactionId>(rsp->id());
            auto startTimestamp = FromProto<TTimestamp>(rsp->start_timestamp());
            if (wrapStickyChannelOnResponse) {
                // The first call within the sticky channel has been non-retrying,
                // so wrap the channel into retrying now for future use.
                channel = WrapStickyChannelIntoRetrying(std::move(channel));
            }
            return CreateTransaction(
                std::move(connection),
                std::move(client),
                std::move(channel),
                transactionId,
                startTimestamp,
                type,
                options.Atomicity,
                options.Durability,
                timeout,
                options.PingAncestors,
                pingPeriod,
                std::move(stickyParameters),
                rsp->sequence_number_source_id(),
                "Transaction started");
        }));
}

////////////////////////////////////////////////////////////////////////////////
// CYPRESS
////////////////////////////////////////////////////////////////////////////////

TFuture<bool> TClientBase::NodeExists(
    const TYPath& path,
    const TNodeExistsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ExistsNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspExistsNodePtr& rsp) {
        return rsp->exists();
    }));
}

TFuture<TYsonString> TClientBase::GetNode(
    const TYPath& path,
    const TGetNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.GetNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    // COMPAT(max42): after 22.3 is everywhere, drop legacy field.
    if (options.Attributes) {
        ToProto(req->mutable_legacy_attributes()->mutable_keys(), options.Attributes.Keys);
        ToProto(req->mutable_attributes(), options.Attributes);
    } else {
        req->mutable_legacy_attributes()->set_all(true);
    }

    if (options.MaxSize) {
        req->set_max_size(*options.MaxSize);
    }

    ToProto(req->mutable_complexity_limits(), options.ComplexityLimits);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);
    if (options.Options) {
        ToProto(req->mutable_options(), *options.Options);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetNodePtr& rsp) {
        return TYsonString(rsp->value());
    }));
}

TFuture<TYsonString> TClientBase::ListNode(
    const TYPath& path,
    const TListNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ListNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    // COMPAT(max42): after 22.3 is everywhere, drop legacy field.
    if (options.Attributes) {
        ToProto(req->mutable_legacy_attributes()->mutable_keys(), options.Attributes.Keys);
        ToProto(req->mutable_attributes(), options.Attributes);
    } else {
        req->mutable_legacy_attributes()->set_all(true);
    }

    if (options.MaxSize) {
        req->set_max_size(*options.MaxSize);
    }

    ToProto(req->mutable_complexity_limits(), options.ComplexityLimits);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspListNodePtr& rsp) {
        return TYsonString(rsp->value());
    }));
}

TFuture<NCypressClient::TNodeId> TClientBase::CreateNode(
    const TYPath& path,
    NObjectClient::EObjectType type,
    const TCreateNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CreateNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_type(ToProto<int>(type));

    if (options.Attributes) {
        ToProto(req->mutable_attributes(), *options.Attributes);
    }
    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
    req->set_ignore_type_mismatch(options.IgnoreTypeMismatch);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCreateNodePtr& rsp) {
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<void> TClientBase::RemoveNode(
    const TYPath& path,
    const TRemoveNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.RemoveNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClientBase::SetNode(
    const TYPath& path,
    const TYsonString& value,
    const TSetNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SetNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_value(value.ToString());
    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClientBase::MultisetAttributesNode(
    const TYPath& path,
    const IMapNodePtr& attributes,
    const TMultisetAttributesNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.MultisetAttributesNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_force(options.Force);

    auto children = attributes->GetChildren();
    std::sort(children.begin(), children.end());
    for (const auto& [attribute, value] : children) {
        auto* protoSubrequest = req->add_subrequests();
        protoSubrequest->set_attribute(attribute);
        protoSubrequest->set_value(ConvertToYsonString(value).ToString());
    }

    ToProto(req->mutable_suppressable_access_tracking_options(), options);
    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<TLockNodeResult> TClientBase::LockNode(
    const TYPath& path,
    NCypressClient::ELockMode mode,
    const TLockNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.LockNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_mode(ToProto<int>(mode));

    req->set_waitable(options.Waitable);
    if (options.ChildKey) {
        req->set_child_key(*options.ChildKey);
    }
    if (options.AttributeKey) {
        req->set_attribute_key(*options.AttributeKey);
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspLockNodePtr& rsp) {
        TLockNodeResult result;
        FromProto(&result.NodeId, rsp->node_id());
        FromProto(&result.LockId, rsp->lock_id());
        FromProto(&result.Revision, rsp->revision());
        return result;
    }));
}

TFuture<void> TClientBase::UnlockNode(
    const TYPath& path,
    const TUnlockNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.UnlockNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<NCypressClient::TNodeId> TClientBase::CopyNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TCopyNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.CopyNode();
    SetTimeoutOptions(*req, options);

    req->set_src_path(srcPath);
    req->set_dst_path(dstPath);

    req->set_recursive(options.Recursive);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);
    req->set_force(options.Force);
    req->set_preserve_account(options.PreserveAccount);
    req->set_preserve_creation_time(options.PreserveCreationTime);
    req->set_preserve_modification_time(options.PreserveModificationTime);
    req->set_preserve_expiration_time(options.PreserveExpirationTime);
    req->set_preserve_expiration_timeout(options.PreserveExpirationTimeout);
    req->set_preserve_owner(options.PreserveOwner);
    req->set_preserve_acl(options.PreserveAcl);
    req->set_pessimistic_quota_check(options.PessimisticQuotaCheck);
    req->set_enable_cross_cell_copying(options.EnableCrossCellCopying);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCopyNodePtr& rsp) {
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<NCypressClient::TNodeId> TClientBase::MoveNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TMoveNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.MoveNode();
    SetTimeoutOptions(*req, options);

    req->set_src_path(srcPath);
    req->set_dst_path(dstPath);

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_preserve_account(options.PreserveAccount);
    req->set_preserve_creation_time(options.PreserveCreationTime);
    req->set_preserve_modification_time(options.PreserveModificationTime);
    req->set_preserve_expiration_time(options.PreserveExpirationTime);
    req->set_preserve_expiration_timeout(options.PreserveExpirationTimeout);
    req->set_preserve_owner(options.PreserveOwner);
    req->set_pessimistic_quota_check(options.PessimisticQuotaCheck);
    req->set_enable_cross_cell_copying(options.EnableCrossCellCopying);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspMoveNodePtr& rsp) {
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<NCypressClient::TNodeId> TClientBase::LinkNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TLinkNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.LinkNode();
    SetTimeoutOptions(*req, options);

    req->set_src_path(srcPath);
    req->set_dst_path(dstPath);

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_lock_existing(options.LockExisting);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspLinkNodePtr& rsp) {
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<void> TClientBase::ConcatenateNodes(
    const std::vector<TRichYPath>& srcPaths,
    const TRichYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ConcatenateNodes();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_src_paths(), srcPaths);
    ToProto(req->mutable_dst_path(), dstPath);
    ToProto(req->mutable_transactional_options(), options);
    // TODO(babenko)
    // ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClientBase::ExternalizeNode(
    const TYPath& path,
    TCellTag cellTag,
    const TExternalizeNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ExternalizeNode();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_path(), path);
    req->set_cell_tag(ToProto<int>(cellTag));
    ToProto(req->mutable_transactional_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TClientBase::InternalizeNode(
    const TYPath& path,
    const TInternalizeNodeOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.InternalizeNode();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_path(), path);
    ToProto(req->mutable_transactional_options(), options);

    return req->Invoke().As<void>();
}

TFuture<NObjectClient::TObjectId> TClientBase::CreateObject(
    NObjectClient::EObjectType type,
    const TCreateObjectOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.CreateObject();

    req->set_type(ToProto<int>(type));
    req->set_ignore_existing(options.IgnoreExisting);
    req->set_sync(options.Sync);
    if (options.Attributes) {
        ToProto(req->mutable_attributes(), *options.Attributes);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCreateObjectPtr& rsp) {
        return FromProto<NObjectClient::TObjectId>(rsp->object_id());
    }));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileReaderPtr> TClientBase::CreateFileReader(
    const TYPath& path,
    const TFileReaderOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.ReadFile();
    InitStreamingRequest(*req);

    req->set_path(path);
    if (options.Offset) {
        req->set_offset(*options.Offset);
    }
    if (options.Length) {
        req->set_length(*options.Length);
    }
    if (options.Config) {
        req->set_config(ConvertToYsonString(*options.Config).ToString());
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return NRpcProxy::CreateFileReader(std::move(req));
}

IFileWriterPtr TClientBase::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.WriteFile();
    InitStreamingRequest(*req);

    ToProto(req->mutable_path(), path);

    req->set_compute_md5(options.ComputeMD5);
    if (options.Config) {
        req->set_config(ConvertToYsonString(*options.Config).ToString());
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);

    return NRpcProxy::CreateFileWriter(std::move(req));
}

/////////////////////////////////////////////////////////////////////////////

IJournalReaderPtr TClientBase::CreateJournalReader(
    const TYPath& path,
    const TJournalReaderOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.ReadJournal();
    InitStreamingRequest(*req);

    req->set_path(path);

    if (options.FirstRowIndex) {
        req->set_first_row_index(*options.FirstRowIndex);
    }
    if (options.RowCount) {
        req->set_row_count(*options.RowCount);
    }
    if (options.Config) {
        req->set_config(ConvertToYsonString(*options.Config).ToString());
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return NRpcProxy::CreateJournalReader(std::move(req));
}

IJournalWriterPtr TClientBase::CreateJournalWriter(
    const TYPath& path,
    const TJournalWriterOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.WriteJournal();
    InitStreamingRequest(*req);

    req->set_path(path);

    if (options.Config) {
        req->set_config(ConvertToYsonString(*options.Config).ToString());
    }

    req->set_enable_multiplexing(options.EnableMultiplexing);
    req->set_enable_chunk_preallocation(options.EnableChunkPreallocation);
    req->set_replica_lag_limit(options.ReplicaLagLimit);
    // TODO(kiselyovp) profiler is ignored

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);

    return NRpcProxy::CreateJournalWriter(std::move(req));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> TClientBase::CreateTableReader(
    const TRichYPath& path,
    const TTableReaderOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.ReadTable();
    InitStreamingRequest(*req);

    ToProto(req->mutable_path(), path);

    req->set_unordered(options.Unordered);
    req->set_omit_inaccessible_columns(options.OmitInaccessibleColumns);
    req->set_enable_table_index(options.EnableTableIndex);
    req->set_enable_row_index(options.EnableRowIndex);
    req->set_enable_range_index(options.EnableRangeIndex);
    if (options.Config) {
        req->set_config(ConvertToYsonString(*options.Config).ToString());
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return NRpc::CreateRpcClientInputStream(std::move(req))
        .Apply(BIND([=] (IAsyncZeroCopyInputStreamPtr inputStream) {
            return NRpcProxy::CreateTableReader(std::move(inputStream));
        }));
}

TFuture<ITableWriterPtr> TClientBase::CreateTableWriter(
    const TRichYPath& path,
    const TTableWriterOptions& options)
{
    auto proxy = CreateApiServiceProxy();
    auto req = proxy.WriteTable();
    InitStreamingRequest(*req);

    ToProto(req->mutable_path(), path);

    if (options.Config) {
        req->set_config(ConvertToYsonString(*options.Config).ToString());
    }

    ToProto(req->mutable_transactional_options(), options);

    auto schema = New<TTableSchema>();
    return NRpc::CreateRpcClientOutputStream(
        std::move(req),
        BIND ([=] (const TSharedRef& metaRef) {
            NApi::NRpcProxy::NProto::TWriteTableMeta meta;
            if (!TryDeserializeProto(&meta, metaRef)) {
                THROW_ERROR_EXCEPTION("Failed to deserialize schema for table writer");
            }

            FromProto(schema.Get(), meta.schema());
        }))
        .Apply(BIND([=] (IAsyncZeroCopyOutputStreamPtr outputStream) {
            return NRpcProxy::CreateTableWriter(std::move(outputStream), std::move(schema));
        })).As<ITableWriterPtr>();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TUnversionedLookupRowsResult> TClientBase::LookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TLegacyKey>& keys,
    const TLookupRowsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.LookupRows();
    req->SetResponseHeavy(true);
    req->SetTimeout(options.Timeout.value_or(GetRpcProxyConnection()->GetConfig()->DefaultLookupRowsTimeout));

    req->set_path(path);
    if (NTracing::IsCurrentTraceContextRecorded()) {
        req->TracingTags().emplace_back("yt.table_path", path);
    }
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());

    if (!options.ColumnFilter.IsUniversal()) {
        for (auto id : options.ColumnFilter.GetIndexes()) {
            req->add_columns(TString(nameTable->GetName(id)));
        }
    }
    req->set_timestamp(options.Timestamp);
    req->set_retention_timestamp(options.RetentionTimestamp);
    req->set_keep_missing_rows(options.KeepMissingRows);
    req->set_enable_partial_result(options.EnablePartialResult);
    req->set_replica_consistency(static_cast<NProto::EReplicaConsistency>(options.ReplicaConsistency));
    if (options.UseLookupCache) {
        req->set_use_lookup_cache(*options.UseLookupCache);
    }

    req->SetMultiplexingBand(options.MultiplexingBand);
    req->set_multiplexing_band(static_cast<NProto::EMultiplexingBand>(options.MultiplexingBand));

    ToProto(req->mutable_tablet_read_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspLookupRowsPtr& rsp) {
        auto rowset = DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
        return TUnversionedLookupRowsResult{
            .Rowset = std::move(rowset),
        };
    }));
}

TFuture<TVersionedLookupRowsResult> TClientBase::VersionedLookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TLegacyKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.VersionedLookupRows();
    req->SetResponseHeavy(true);
    req->SetTimeout(options.Timeout.value_or(GetRpcProxyConnection()->GetConfig()->DefaultLookupRowsTimeout));

    req->set_path(path);
    if (NTracing::IsCurrentTraceContextRecorded()) {
        req->TracingTags().emplace_back("yt.table_path", path);
    }
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());

    if (!options.ColumnFilter.IsUniversal()) {
        for (auto id : options.ColumnFilter.GetIndexes()) {
            req->add_columns(TString(nameTable->GetName(id)));
        }
    }
    req->set_timestamp(options.Timestamp);
    req->set_keep_missing_rows(options.KeepMissingRows);
    req->set_enable_partial_result(options.EnablePartialResult);
    req->set_replica_consistency(static_cast<NProto::EReplicaConsistency>(options.ReplicaConsistency));
    if (options.UseLookupCache) {
        req->set_use_lookup_cache(*options.UseLookupCache);
    }

    req->SetMultiplexingBand(options.MultiplexingBand);
    req->set_multiplexing_band(static_cast<NProto::EMultiplexingBand>(options.MultiplexingBand));
    if (options.RetentionConfig) {
        ToProto(req->mutable_retention_config(), *options.RetentionConfig);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspVersionedLookupRowsPtr& rsp) {
        auto rowset = DeserializeRowset<TVersionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
        return TVersionedLookupRowsResult{
            .Rowset = std::move(rowset),
            .UnavailableKeyIndexes = FromProto<std::vector<int>>(rsp->unavailable_key_indexes()),
        };
    }));
}

TFuture<std::vector<TUnversionedLookupRowsResult>> TClientBase::MultiLookupRows(
    const std::vector<TMultiLookupSubrequest>& subrequests,
    const TMultiLookupOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.MultiLookup();
    req->SetResponseHeavy(true);
    req->SetTimeout(options.Timeout.value_or(GetRpcProxyConnection()->GetConfig()->DefaultLookupRowsTimeout));
    req->SetMultiplexingBand(options.MultiplexingBand);

    for (const auto& subrequest : subrequests) {
        auto* protoSubrequest = req->add_subrequests();

        protoSubrequest->set_path(subrequest.Path);

        const auto& subrequestOptions = subrequest.Options;
        if (!subrequestOptions.ColumnFilter.IsUniversal()) {
            for (auto id : subrequestOptions.ColumnFilter.GetIndexes()) {
                protoSubrequest->add_columns(TString(subrequest.NameTable->GetName(id)));
            }
        }
        protoSubrequest->set_keep_missing_rows(subrequestOptions.KeepMissingRows);
        protoSubrequest->set_enable_partial_result(subrequestOptions.EnablePartialResult);
        if (subrequestOptions.UseLookupCache) {
            protoSubrequest->set_use_lookup_cache(*subrequestOptions.UseLookupCache);
        }

        auto rowset = SerializeRowset(
            subrequest.NameTable,
            subrequest.Keys,
            protoSubrequest->mutable_rowset_descriptor());
        protoSubrequest->set_attachment_count(rowset.size());
        req->Attachments().insert(req->Attachments().end(), rowset.begin(), rowset.end());
    }

    if (NTracing::IsCurrentTraceContextRecorded()) {
        std::vector<TString> paths;
        paths.reserve(subrequests.size());
        for (const auto& subrequest : subrequests) {
            paths.emplace_back(subrequest.Path);
        }
        req->TracingTags().emplace_back("yt.table_paths", SanitizeTracingTag(NYson::ConvertToYsonString(paths).ToString()));
    }

    req->set_replica_consistency(static_cast<NProto::EReplicaConsistency>(options.ReplicaConsistency));
    req->set_timestamp(options.Timestamp);
    req->set_retention_timestamp(options.RetentionTimestamp);
    req->set_multiplexing_band(static_cast<NProto::EMultiplexingBand>(options.MultiplexingBand));
    ToProto(req->mutable_tablet_read_options(), options);

    return req->Invoke().Apply(BIND([subrequestCount = std::ssize(subrequests)] (const TApiServiceProxy::TRspMultiLookupPtr& rsp) {
        YT_VERIFY(subrequestCount == rsp->subresponses_size());

        std::vector<TUnversionedLookupRowsResult> result;
        result.reserve(subrequestCount);

        int beginAttachmentIndex = 0;
        for (const auto& subresponse : rsp->subresponses()) {
            int endAttachmentIndex = beginAttachmentIndex + subresponse.attachment_count();
            YT_VERIFY(endAttachmentIndex <= std::ssize(rsp->Attachments()));

            std::vector<TSharedRef> subresponseAttachments(
                rsp->Attachments().begin() + beginAttachmentIndex,
                rsp->Attachments().begin() + endAttachmentIndex);
            auto rowset = DeserializeRowset<TUnversionedRow>(
                subresponse.rowset_descriptor(),
                MergeRefsToRef<TRpcProxyClientBufferTag>(std::move(subresponseAttachments)));
            result.push_back({
                .Rowset = std::move(rowset),
                .UnavailableKeyIndexes = FromProto<std::vector<int>>(subresponse.unavailable_key_indexes()),
            });

            beginAttachmentIndex = endAttachmentIndex;
        }
        YT_VERIFY(beginAttachmentIndex == std::ssize(rsp->Attachments()));

        return result;
    }));
}

template<class TRequest>
void FillRequestBySelectRowsOptionsBase(
    const TSelectRowsOptionsBase& options,
    const std::optional<NYPath::TYPath>& defaultUdfRegistryPath,
    TRequest request)
{
    request->set_timestamp(options.Timestamp);
    if (options.UdfRegistryPath) {
        request->set_udf_registry_path(*options.UdfRegistryPath);
    } else if (defaultUdfRegistryPath) {
        request->set_udf_registry_path(*defaultUdfRegistryPath);
    }
    request->set_syntax_version(options.SyntaxVersion);
}

TFuture<TSelectRowsResult> TClientBase::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.SelectRows();
    req->SetResponseHeavy(true);
    req->set_query(query);

    const auto& config = GetRpcProxyConnection()->GetConfig();

    if (NTracing::IsCurrentTraceContextRecorded()) {
        if (config->EnableSelectQueryTracingTag) {
            req->TracingTags().emplace_back("yt.query", SanitizeTracingTag(query));
        } else {
            req->TracingTags().emplace_back("yt.query", DisabledSelectQueryTracingTag);
        }
    }

    FillRequestBySelectRowsOptionsBase(options, config->UdfRegistryPath, req);
    // TODO(ifsmirnov): retention timestamp in explain_query.
    req->set_retention_timestamp(options.RetentionTimestamp);
    // TODO(lukyan): Move to FillRequestBySelectRowsOptionsBase
    req->SetTimeout(options.Timeout.value_or(config->DefaultSelectRowsTimeout));

    if (options.InputRowLimit) {
        req->set_input_row_limit(*options.InputRowLimit);
    }
    if (options.OutputRowLimit) {
        req->set_output_row_limit(*options.OutputRowLimit);
    }
    req->set_range_expansion_limit(options.RangeExpansionLimit);
    req->set_max_subqueries(options.MaxSubqueries);
    req->set_allow_full_scan(options.AllowFullScan);
    req->set_allow_join_without_index(options.AllowJoinWithoutIndex);

    if (options.ExecutionPool) {
        req->set_execution_pool(*options.ExecutionPool);
    }
    if (options.PlaceholderValues) {
        req->set_placeholder_values(options.PlaceholderValues.ToString());
    }
    req->set_fail_on_incomplete_result(options.FailOnIncompleteResult);
    req->set_verbose_logging(options.VerboseLogging);
    req->set_new_range_inference(options.NewRangeInference);
    if (options.ExecutionBackend) {
        req->set_execution_backend(static_cast<int>(*options.ExecutionBackend));
    }
    req->set_enable_code_cache(options.EnableCodeCache);
    req->set_memory_limit_per_node(options.MemoryLimitPerNode);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);
    req->set_replica_consistency(static_cast<NProto::EReplicaConsistency>(options.ReplicaConsistency));
    req->set_use_canonical_null_relations(options.UseCanonicalNullRelations);
    req->set_merge_versioned_rows(options.MergeVersionedRows);
    ToProto(req->mutable_versioned_read_options(), options.VersionedReadOptions);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspSelectRowsPtr& rsp) {
        TSelectRowsResult result;
        result.Rowset = DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
        FromProto(&result.Statistics, rsp->statistics());
        return result;
    }));
}

TFuture<TYsonString> TClientBase::ExplainQuery(
    const TString& query,
    const TExplainQueryOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.ExplainQuery();
    req->set_query(query);
    FillRequestBySelectRowsOptionsBase(options, GetRpcProxyConnection()->GetConfig()->UdfRegistryPath, req);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspExplainQueryPtr& rsp) {
        return TYsonString(rsp->value());
    }));
}

TFuture<TPullRowsResult> TClientBase::PullRows(
    const TYPath& path,
    const TPullRowsOptions& options)
{
    auto proxy = CreateApiServiceProxy();

    auto req = proxy.PullRows();
    req->SetResponseHeavy(true);
    req->set_path(path);
    ToProto(req->mutable_upstream_replica_id(), options.UpstreamReplicaId);
    req->set_order_rows_by_timestamp(options.OrderRowsByTimestamp);
    req->set_tablet_rows_per_read(options.TabletRowsPerRead);
    ToProto(req->mutable_replication_progress(), options.ReplicationProgress);
    if (options.UpperTimestamp != NullTimestamp) {
        req->set_upper_timestamp(options.UpperTimestamp);
    }
    for (auto [tabletId, rowIndex] : options.StartReplicationRowIndexes) {
        auto* protoReplicationRowIndex = req->add_start_replication_row_indexes();
        ToProto(protoReplicationRowIndex->mutable_tablet_id(), tabletId);
        protoReplicationRowIndex->set_row_index(rowIndex);
    }

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspPullRowsPtr& rsp) {
        TPullRowsResult result;
        result.RowCount = rsp->row_count();
        result.DataWeight = rsp->data_weight();
        result.Versioned = rsp->versioned();
        FromProto(&result.ReplicationProgress, rsp->replication_progress());

        for (auto protoReplicationRowIndex : rsp->end_replication_row_indexes()) {
            auto tabletId = FromProto<TTabletId>(protoReplicationRowIndex.tablet_id());
            int rowIndex = protoReplicationRowIndex.row_index();
            if (result.EndReplicationRowIndexes.contains(tabletId)) {
                THROW_ERROR_EXCEPTION("Duplicate tablet id in end replication row indexes")
                    << TErrorAttribute("tablet_id", tabletId);
            }
            InsertOrCrash(result.EndReplicationRowIndexes, std::pair(tabletId, rowIndex));
        }

        result.Rowset = DeserializeRowset(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()),
            rsp->versioned());

        return result;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
