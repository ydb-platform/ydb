#include "transaction_impl.h"
#include "client_impl.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NApi;
using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(
    TConnectionPtr connection,
    TClientPtr client,
    NRpc::IChannelPtr channel,
    TTransactionId id,
    TTimestamp startTimestamp,
    ETransactionType type,
    EAtomicity atomicity,
    EDurability durability,
    TDuration timeout,
    bool pingAncestors,
    std::optional<TDuration> pingPeriod,
    std::optional<TStickyTransactionParameters> stickyParameters,
    i64 sequenceNumberSourceId,
    TStringBuf capitalizedCreationReason)
    : Connection_(std::move(connection))
    , Client_(std::move(client))
    , Channel_(std::move(channel))
    , Id_(id)
    , StartTimestamp_(startTimestamp)
    , Type_(type)
    , Atomicity_(atomicity)
    , Durability_(durability)
    , Timeout_(timeout)
    , PingAncestors_(pingAncestors)
    , PingPeriod_(pingPeriod)
    , StickyProxyAddress_(stickyParameters ? std::move(stickyParameters->ProxyAddress) : TString())
    , SequenceNumberSourceId_(sequenceNumberSourceId)
    , Logger(RpcProxyClientLogger.WithTag("TransactionId: %v, %v",
        Id_,
        Connection_->GetLoggingTag()))
    , Proxy_(Channel_)
{
    const auto& config = Connection_->GetConfig();
    Proxy_.SetDefaultTimeout(config->RpcTimeout);
    Proxy_.SetDefaultRequestCodec(config->RequestCodec);
    Proxy_.SetDefaultResponseCodec(config->ResponseCodec);
    Proxy_.SetDefaultEnableLegacyRpcCodecs(config->EnableLegacyRpcCodecs);

    YT_LOG_DEBUG("%v (Type: %v, StartTimestamp: %v, Atomicity: %v, "
        "Durability: %v, Timeout: %v, PingAncestors: %v, PingPeriod: %v, Sticky: %v, StickyProxyAddress: %v)",
        capitalizedCreationReason,
        GetType(),
        GetStartTimestamp(),
        GetAtomicity(),
        GetDurability(),
        GetTimeout(),
        PingAncestors_,
        PingPeriod_,
        /*sticky*/ stickyParameters.has_value(),
        StickyProxyAddress_);

    // TODO(babenko): don't run periodic pings if client explicitly disables them in options
    RunPeriodicPings();
}

IConnectionPtr TTransaction::GetConnection()
{
    return Connection_;
}

IClientPtr TTransaction::GetClient() const
{
    return Client_;
}

TTransactionId TTransaction::GetId() const
{
    return Id_;
}

TTimestamp TTransaction::GetStartTimestamp() const
{
    return StartTimestamp_;
}

ETransactionType TTransaction::GetType() const
{
    return Type_;
}

EAtomicity TTransaction::GetAtomicity() const
{
    return Atomicity_;
}

EDurability TTransaction::GetDurability() const
{
    return Durability_;
}

TDuration TTransaction::GetTimeout() const
{
    return Timeout_;
}

void TTransaction::RegisterAlienTransaction(const ITransactionPtr& transaction)
{
    {
        auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::InvalidTransactionState,
                "Transaction %v is in %Qlv state",
                GetId(),
                State_);
        }

        if (GetType() != ETransactionType::Tablet) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::MalformedAlienTransaction,
                "Transaction %v is of type %Qlv and hence does not allow alien transactions",
                GetId(),
                GetType());
        }

        if (GetId() != transaction->GetId()) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::MalformedAlienTransaction,
                "Transaction id mismatch: native %v, alien %v",
                GetId(),
                transaction->GetId());
        }

        AlienTransactions_.push_back(transaction);
    }

    YT_LOG_DEBUG("Alien transaction registered (AlienConnection: {%v})",
        transaction->GetConnection()->GetLoggingTag());
}

TFuture<void> TTransaction::Ping(const NApi::TTransactionPingOptions& /*options*/)
{
    return SendPing();
}

void TTransaction::Detach()
{
    {
        auto guard = Guard(SpinLock_);

        if (State_ == ETransactionState::Detached) {
            return;
        }

        State_ = ETransactionState::Detached;
    }

    YT_LOG_DEBUG("Transaction detached");

    auto req = Proxy_.DetachTransaction();
    ToProto(req->mutable_transaction_id(), GetId());
    // Fire-and-forget.
    YT_UNUSED_FUTURE(req->Invoke());
}

void TTransaction::SubscribeCommitted(const TCommittedHandler& handler)
{
    Committed_.Subscribe(handler);
}

void TTransaction::UnsubscribeCommitted(const TCommittedHandler& handler)
{
    Committed_.Unsubscribe(handler);
}

void TTransaction::SubscribeAborted(const TAbortedHandler& handler)
{
    Aborted_.Subscribe(handler);
}

void TTransaction::UnsubscribeAborted(const TAbortedHandler& handler)
{
    Aborted_.Unsubscribe(handler);
}

TFuture<TTransactionFlushResult> TTransaction::Flush()
{
    std::vector<TFuture<void>> futures;
    {
        auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Active) {
            return MakeFuture<TTransactionFlushResult>(TError(
                NTransactionClient::EErrorCode::InvalidTransactionState,
                "Transaction %v is in %Qlv state",
                GetId(),
                State_));
        }

        if (!AlienTransactions_.empty()) {
            return MakeFuture<TTransactionFlushResult>(TError(
                NTransactionClient::EErrorCode::AlienTransactionsForbidden,
                "Cannot flush transaction %v since it has %v alien transaction(s)",
                GetId(),
                AlienTransactions_.size()));
        }

        State_ = ETransactionState::Flushing;
        futures = FlushModifyRowsRequests();
    }

    YT_LOG_DEBUG("Flushing transaction");

    return AllSucceeded(futures)
        .Apply(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto req = Proxy_.FlushTransaction();
                ToProto(req->mutable_transaction_id(), GetId());
                return req->Invoke();
            }))
        .Apply(
            BIND([=, this, this_ = MakeStrong(this)] (const TApiServiceProxy::TErrorOrRspFlushTransactionPtr& rspOrError) -> TErrorOr<TTransactionFlushResult> {
                {
                    auto guard = Guard(SpinLock_);
                    if (rspOrError.IsOK() && State_ == ETransactionState::Flushing) {
                        State_ = ETransactionState::Flushed;
                    } else if (!rspOrError.IsOK()) {
                        YT_LOG_DEBUG(rspOrError, "Error flushing transaction");
                        YT_UNUSED_FUTURE(DoAbort(&guard));
                        THROW_ERROR_EXCEPTION("Error flushing transaction %v",
                            GetId())
                            << rspOrError;
                    }
                }

                const auto& rsp = rspOrError.Value();
                TTransactionFlushResult result{
                    .ParticipantCellIds = FromProto<std::vector<TCellId>>(rsp->participant_cell_ids())
                };

                YT_LOG_DEBUG("Transaction flushed (ParticipantCellIds: %v)",
                    result.ParticipantCellIds);

                return result;
            }));
}

TFuture<TTransactionCommitResult> TTransaction::Commit(const TTransactionCommitOptions& options)
{
    std::vector<TFuture<void>> futures;
    std::vector<NApi::ITransactionPtr> alienTransactions;
    {
        auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Active) {
            return MakeFuture<TTransactionCommitResult>(TError(
                NTransactionClient::EErrorCode::InvalidTransactionState,
                "Transaction %v is in %Qlv state",
                GetId(),
                State_));
        }

        State_ = ETransactionState::Committing;
        futures = FlushModifyRowsRequests();
        alienTransactions = std::move(AlienTransactions_);
    }

    YT_LOG_DEBUG("Committing transaction (AlienTransactionCount: %v)",
        alienTransactions.size());

    for (const auto& transaction : alienTransactions) {
        futures.push_back(
            transaction->Flush().Apply(
                BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TTransactionFlushResult>& resultOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Error flushing alien transaction");

                    const auto& result = resultOrError.Value();

                    YT_LOG_DEBUG("Alien transaction flushed (ParticipantCellIds: %v, AlienConnection: {%v})",
                        result.ParticipantCellIds,
                        transaction->GetConnection()->GetLoggingTag());

                    for (auto cellId : result.ParticipantCellIds) {
                        AdditionalParticipantCellIds_.insert(cellId);
                    }
                })));
    }

    return AllSucceeded(std::move(futures))
        .Apply(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto req = Proxy_.CommitTransaction();
                ToProto(req->mutable_transaction_id(), GetId());
                ToProto(req->mutable_additional_participant_cell_ids(), AdditionalParticipantCellIds_);
                ToProto(req->mutable_prerequisite_options(), options);
                return req->Invoke();
            }))
        .Apply(
            BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TApiServiceProxy::TRspCommitTransactionPtr>& rspOrError) {
                {
                    auto guard = Guard(SpinLock_);
                    if (rspOrError.IsOK() && State_ == ETransactionState::Committing) {
                        State_ = ETransactionState::Committed;
                    } else if (!rspOrError.IsOK()) {
                        YT_UNUSED_FUTURE(DoAbort(&guard));
                        THROW_ERROR_EXCEPTION("Error committing transaction %v",
                            GetId())
                            << rspOrError;
                    }
                }

                for (const auto& transaction : alienTransactions) {
                    transaction->Detach();
                }

                const auto& rsp = rspOrError.Value();
                TTransactionCommitResult result{
                    .PrimaryCommitTimestamp = rsp->primary_commit_timestamp(),
                    .CommitTimestamps = FromProto<NHiveClient::TTimestampMap>(rsp->commit_timestamps())
                };

                YT_LOG_DEBUG("Transaction committed (CommitTimestamps: %v)",
                    result.CommitTimestamps);

                Committed_.Fire();

                return result;
            }));
}

TFuture<void> TTransaction::Abort(const TTransactionAbortOptions& options)
{
    auto guard = Guard(SpinLock_);

    if (State_ == ETransactionState::Committed || State_ == ETransactionState::Detached) {
        return MakeFuture<void>(TError(
            NTransactionClient::EErrorCode::InvalidTransactionState,
            "Cannot abort since transaction %v is in %Qlv state",
            GetId(),
            State_));
    }

    return DoAbort(&guard, options);
}

void TTransaction::ModifyRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options)
{
    ValidateTabletTransactionId(GetId());

    for (const auto& modification : modifications) {
        // TODO(sandello): handle versioned rows
        YT_VERIFY(
            modification.Type == ERowModificationType::Write ||
            modification.Type == ERowModificationType::Delete ||
            modification.Type == ERowModificationType::WriteAndLock);
    }

    auto reqSequenceNumber = ModifyRowsRequestSequenceCounter_.fetch_add(1);

    auto req = Proxy_.ModifyRows();
    req->set_sequence_number(reqSequenceNumber);
    req->set_sequence_number_source_id(SequenceNumberSourceId_);
    ToProto(req->mutable_transaction_id(), GetId());
    req->set_path(path);
    if (NTracing::IsCurrentTraceContextRecorded()) {
        req->TracingTags().emplace_back("yt.table_path", path);
    }
    req->set_require_sync_replica(options.RequireSyncReplica);
    ToProto(req->mutable_upstream_replica_id(), options.UpstreamReplicaId);
    req->set_allow_missing_key_columns(options.AllowMissingKeyColumns);

    std::vector<TUnversionedRow> rows;
    rows.reserve(modifications.Size());

    bool usedStrongLocks = false;
    for (const auto& modification : modifications) {
        auto mask = modification.Locks;
        for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
            if (mask.Get(index) > MaxOldLockType) {
                THROW_ERROR_EXCEPTION("New locks are not supported in RPC client yet")
                    << TErrorAttribute("lock_index", index)
                    << TErrorAttribute("lock_type", mask.Get(index));
            }
            usedStrongLocks |= mask.Get(index) == ELockType::SharedStrong;
        }
    }

    if (usedStrongLocks) {
        req->Header().set_protocol_version_minor(YTRpcModifyRowsStrongLocksVersion);
    }

    for (const auto& modification : modifications) {
        rows.emplace_back(modification.Row);
        req->add_row_modification_types(static_cast<NProto::ERowModificationType>(modification.Type));
        if (usedStrongLocks) {
            auto locks = modification.Locks;
            YT_VERIFY(!locks.HasNewLocks());
            req->add_row_locks(locks.ToLegacyMask().GetBitmap());
        } else {
            TLegacyLockBitmap bitmap = 0;
            for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
                if (modification.Locks.Get(index) == ELockType::SharedWeak) {
                    bitmap |= 1u << index;
                }
            }
            req->add_row_read_locks(bitmap);
        }
    }

    req->Attachments() = SerializeRowset(
        nameTable,
        MakeRange(rows),
        req->mutable_rowset_descriptor());

    TFuture<void> future;
    const auto& config = Connection_->GetConfig();
    if (config->ModifyRowsBatchCapacity == 0) {
        ValidateActive();
        future = req->Invoke().As<void>();
    } else {
        YT_LOG_DEBUG("Pushing a subrequest into a batch modify rows request (SubrequestAttachmentCount: 1+%v)",
            req->Attachments().size());

        auto reqBody = SerializeProtoToRef(*req);

        {
            auto guard = Guard(SpinLock_);

            DoValidateActive();

            if (!BatchModifyRowsRequest_) {
                BatchModifyRowsRequest_ = Proxy_.BatchModifyRows();
                ToProto(BatchModifyRowsRequest_->mutable_transaction_id(), GetId());
            }

            BatchModifyRowsRequest_->Attachments().push_back(reqBody);
            BatchModifyRowsRequest_->Attachments().insert(
                BatchModifyRowsRequest_->Attachments().end(),
                req->Attachments().begin(),
                req->Attachments().end());
            BatchModifyRowsRequest_->add_part_counts(req->Attachments().size());

            if (BatchModifyRowsRequest_->part_counts_size() == config->ModifyRowsBatchCapacity) {
                future = InvokeBatchModifyRowsRequest();
            }
        }
    }

    if (future) {
        future
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)](const TError& error) {
                if (!error.IsOK()) {
                    YT_LOG_DEBUG(error, "Error sending row modifications");
                    YT_UNUSED_FUTURE(Abort());
                }
            }));

        {
            auto guard = Guard(SpinLock_);
            BatchModifyRowsFutures_.push_back(std::move(future));
        }
    }
}

TFuture<void> TTransaction::AdvanceConsumer(
    const NYPath::TRichYPath& consumerPath,
    const NYPath::TRichYPath& queuePath,
    int partitionIndex,
    std::optional<i64> oldOffset,
    i64 newOffset,
    const TAdvanceConsumerOptions& options)
{
    ValidateTabletTransactionId(GetId());

    THROW_ERROR_EXCEPTION_IF(newOffset < 0, "Queue consumer offset %v cannot be negative", newOffset);

    auto req = Proxy_.AdvanceConsumer();
    SetTimeoutOptions(*req, options);

    if (NTracing::IsCurrentTraceContextRecorded()) {
        req->TracingTags().emplace_back("yt.consumer_path", ToString(consumerPath));
        req->TracingTags().emplace_back("yt.queue_path", ToString(queuePath));
    }

    ToProto(req->mutable_transaction_id(), GetId());

    ToProto(req->mutable_consumer_path(), consumerPath);
    ToProto(req->mutable_queue_path(), queuePath);
    req->set_partition_index(partitionIndex);
    if (oldOffset) {
        req->set_old_offset(*oldOffset);
    }
    req->set_new_offset(newOffset);

    return req->Invoke().As<void>();
}

TFuture<ITransactionPtr> TTransaction::StartTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    ValidateActive();
    return Client_->StartTransaction(
        type,
        PatchTransactionId(options));
}

TFuture<TUnversionedLookupRowsResult> TTransaction::LookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TLegacyKey>& keys,
    const TLookupRowsOptions& options)
{
    ValidateActive();
    return Client_->LookupRows(
        path,
        std::move(nameTable),
        keys,
        PatchTransactionTimestamp(options));
}

TFuture<TVersionedLookupRowsResult> TTransaction::VersionedLookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<TLegacyKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    ValidateActive();
    return Client_->VersionedLookupRows(
        path,
        std::move(nameTable),
        keys,
        PatchTransactionTimestamp(options));
}

TFuture<std::vector<TUnversionedLookupRowsResult>> TTransaction::MultiLookupRows(
    const std::vector<TMultiLookupSubrequest>& subrequests,
    const TMultiLookupOptions& options)
{
    ValidateActive();
    return Client_->MultiLookupRows(
        subrequests,
        PatchTransactionTimestamp(options));
}

TFuture<TSelectRowsResult> TTransaction::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    ValidateActive();
    return Client_->SelectRows(
        query,
        PatchTransactionTimestamp(options));
}

TFuture<NYson::TYsonString> TTransaction::ExplainQuery(
    const TString& query,
    const TExplainQueryOptions& options)
{
    ValidateActive();
    return Client_->ExplainQuery(
        query,
        PatchTransactionTimestamp(options));
}

TFuture<TPullRowsResult> TTransaction::PullRows(
    const TYPath& path,
    const TPullRowsOptions& options)
{
    ValidateActive();
    return Client_->PullRows(
        path,
        options);
}

TFuture<ITableReaderPtr> TTransaction::CreateTableReader(
    const TRichYPath& path,
    const NApi::TTableReaderOptions& options)
{
    ValidateActive();
    return Client_->CreateTableReader(
        path,
        PatchTransactionId(options));
}

TFuture<ITableWriterPtr> TTransaction::CreateTableWriter(
    const TRichYPath& path,
    const NApi::TTableWriterOptions& options)
{
    ValidateActive();
    return Client_->CreateTableWriter(
        path,
        PatchTransactionId(options));
}

TFuture<NYson::TYsonString> TTransaction::GetNode(
    const TYPath& path,
    const TGetNodeOptions& options)
{
    ValidateActive();
    return Client_->GetNode(
        path,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::SetNode(
    const TYPath& path,
    const NYson::TYsonString& value,
    const TSetNodeOptions& options)
{
    ValidateActive();
    return Client_->SetNode(
        path,
        value,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::MultisetAttributesNode(
    const TYPath& path,
    const IMapNodePtr& attributes,
    const TMultisetAttributesNodeOptions& options)
{
    ValidateActive();
    return Client_->MultisetAttributesNode(
        path,
        attributes,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::RemoveNode(
    const TYPath& path,
    const TRemoveNodeOptions& options)
{
    ValidateActive();
    return Client_->RemoveNode(
        path,
        PatchTransactionId(options));
}

TFuture<NYson::TYsonString> TTransaction::ListNode(
    const TYPath& path,
    const TListNodeOptions& options)
{
    ValidateActive();
    return Client_->ListNode(
        path,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::CreateNode(
    const TYPath& path,
    EObjectType type,
    const TCreateNodeOptions& options)
{
    ValidateActive();
    return Client_->CreateNode(
        path,
        type,
        PatchTransactionId(options));
}

TFuture<TLockNodeResult> TTransaction::LockNode(
    const TYPath& path,
    ELockMode mode,
    const TLockNodeOptions& options)
{
    ValidateActive();
    return Client_->LockNode(
        path,
        mode,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::UnlockNode(
    const NYPath::TYPath& path,
    const NApi::TUnlockNodeOptions& options)
{
    ValidateActive();
    return Client_->UnlockNode(
        path,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::CopyNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TCopyNodeOptions& options)
{
    ValidateActive();
    return Client_->CopyNode(
        srcPath,
        dstPath,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::MoveNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TMoveNodeOptions& options)
{
    ValidateActive();
    return Client_->MoveNode(
        srcPath,
        dstPath,
        PatchTransactionId(options));
}

TFuture<TNodeId> TTransaction::LinkNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TLinkNodeOptions& options)
{
    ValidateActive();
    return Client_->LinkNode(
        srcPath,
        dstPath,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::ConcatenateNodes(
    const std::vector<TRichYPath>& srcPaths,
    const TRichYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    ValidateActive();
    return Client_->ConcatenateNodes(
        srcPaths,
        dstPath,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::ExternalizeNode(
    const TYPath& path,
    TCellTag cellTag,
    const TExternalizeNodeOptions& options)
{
    ValidateActive();
    return Client_->ExternalizeNode(
        path,
        cellTag,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::InternalizeNode(
    const TYPath& path,
    const TInternalizeNodeOptions& options)
{
    ValidateActive();
    return Client_->InternalizeNode(
        path,
        PatchTransactionId(options));
}

TFuture<bool> TTransaction::NodeExists(
    const TYPath& path,
    const TNodeExistsOptions& options)
{
    ValidateActive();
    return Client_->NodeExists(
        path,
        PatchTransactionId(options));
}

TFuture<TObjectId> TTransaction::CreateObject(
    EObjectType type,
    const TCreateObjectOptions& options)
{
    ValidateActive();
    return Client_->CreateObject(type, options);
}

TFuture<IFileReaderPtr> TTransaction::CreateFileReader(
    const TYPath& path,
    const TFileReaderOptions& options)
{
    ValidateActive();
    return Client_->CreateFileReader(
        path,
        PatchTransactionId(options));
}

IFileWriterPtr TTransaction::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    ValidateActive();
    return Client_->CreateFileWriter(
        path,
        PatchTransactionId(options));
}

IJournalReaderPtr TTransaction::CreateJournalReader(
    const TYPath& path,
    const TJournalReaderOptions& options)
{
    ValidateActive();
    return Client_->CreateJournalReader(
        path,
        PatchTransactionId(options));
}

IJournalWriterPtr TTransaction::CreateJournalWriter(
    const TYPath& path,
    const TJournalWriterOptions& options)
{
    ValidateActive();
    return Client_->CreateJournalWriter(
        path,
        PatchTransactionId(options));
}

TFuture<void> TTransaction::DoAbort(
    TGuard<NThreading::TSpinLock>* guard,
    const TTransactionAbortOptions& /*options*/)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    if (State_ == ETransactionState::Aborting || State_ == ETransactionState::Aborted) {
        return AbortPromise_.ToFuture();
    }

    YT_LOG_DEBUG("Aborting transaction");

    State_ = ETransactionState::Aborting;

    auto alienTransactions = AlienTransactions_;

    guard->Release();

    auto req = Proxy_.AbortTransaction();
    ToProto(req->mutable_transaction_id(), GetId());

    AbortPromise_.TrySetFrom(req->Invoke().Apply(
        BIND([=, this, this_ = MakeStrong(this)] (const TApiServiceProxy::TErrorOrRspAbortTransactionPtr& rspOrError) {
            {
                auto guard = Guard(SpinLock_);

                if (State_ != ETransactionState::Aborting) {
                    YT_LOG_DEBUG(rspOrError, "Transaction is no longer aborting, abort response ignored");
                    return;
                }

                if (rspOrError.IsOK()) {
                    YT_LOG_DEBUG("Transaction aborted");
                } else if (rspOrError.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
                    YT_LOG_DEBUG("Transaction has expired or was already aborted, ignored");
                } else {
                    YT_LOG_DEBUG(rspOrError, "Error aborting transaction, considered detached");
                    State_ = ETransactionState::Detached;
                    THROW_ERROR_EXCEPTION("Error aborting transaction %v",
                        GetId())
                        << rspOrError;
                }

                State_ = ETransactionState::Aborted;
            }

            Aborted_.Fire(TError("Transaction aborted by user request"));
        })));

    for (const auto& transaction : alienTransactions) {
        YT_UNUSED_FUTURE(transaction->Abort());
    }

    return AbortPromise_.ToFuture();
}

TFuture<void> TTransaction::SendPing()
{
    YT_LOG_DEBUG("Pinging transaction");

    auto req = Proxy_.PingTransaction();
    ToProto(req->mutable_transaction_id(), GetId());
    req->set_ping_ancestors(PingAncestors_);

    return req->Invoke().Apply(
        BIND([=, this, this_ = MakeStrong(this)] (const TApiServiceProxy::TErrorOrRspPingTransactionPtr& rspOrError) {
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Transaction pinged");
            } else if (rspOrError.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
                // Hard error.
                YT_LOG_DEBUG("Transaction has expired or was aborted");

                bool fireAborted = false;
                {
                    auto guard = Guard(SpinLock_);
                    if (State_ != ETransactionState::Committed &&
                        State_ != ETransactionState::Flushed &&
                        State_ != ETransactionState::FlushedModifications &&
                        State_ != ETransactionState::Aborted &&
                        State_ != ETransactionState::Detached)
                    {
                        State_ = ETransactionState::Aborted;
                        fireAborted = true;
                    }
                }

                auto error = TError(
                    NTransactionClient::EErrorCode::NoSuchTransaction,
                    "Transaction %v has expired or was aborted",
                    GetId());

                if (fireAborted) {
                    AbortPromise_.TrySet();
                    Aborted_.Fire(error);
                }

                THROW_ERROR(error);
            } else {
                // Soft error.
                YT_LOG_DEBUG(rspOrError, "Error pinging transaction");
                THROW_ERROR_EXCEPTION("Error pinging transaction %v",
                    GetId())
                    << rspOrError;
            }
        }));
}

void TTransaction::RunPeriodicPings()
{
    if (!PingPeriod_) {
        return;
    }

    if (!IsPingableState()) {
        return;
    }

    SendPing().Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
        if (!IsPingableState()) {
            return;
        }

        if (error.FindMatching(NYT::EErrorCode::Timeout)) {
            RunPeriodicPings();
            return;
        }

        YT_LOG_DEBUG("Transaction ping scheduled");

        TDelayedExecutor::Submit(
            BIND(&TTransaction::RunPeriodicPings, MakeWeak(this)),
            *PingPeriod_);
    }));
}

bool TTransaction::IsPingableState()
{
    auto guard = Guard(SpinLock_);
    return
        State_ == ETransactionState::Active ||
        State_ == ETransactionState::Flushing ||
        State_ == ETransactionState::Flushed ||
        State_ == ETransactionState::FlushingModifications ||
        State_ == ETransactionState::FlushedModifications ||
        State_ == ETransactionState::Committing;
}

void TTransaction::ValidateActive()
{
    auto guard = Guard(SpinLock_);
    DoValidateActive();
}

void TTransaction::DoValidateActive()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);
    if (State_ != ETransactionState::Active) {
        THROW_ERROR_EXCEPTION(
            NTransactionClient::EErrorCode::InvalidTransactionState,
            "Transaction %v is not active",
            GetId());
    }
}

TApiServiceProxy::TReqBatchModifyRowsPtr TTransaction::CreateBatchModifyRowsRequest()
{
    auto req = Proxy_.BatchModifyRows();
    ToProto(req->mutable_transaction_id(), GetId());
    return req;
}

TFuture<void> TTransaction::InvokeBatchModifyRowsRequest()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);
    YT_VERIFY(BatchModifyRowsRequest_);

    TApiServiceProxy::TReqBatchModifyRowsPtr batchRequest;
    batchRequest.Swap(BatchModifyRowsRequest_);
    if (batchRequest->part_counts_size() == 0) {
        return VoidFuture;
    }

    YT_LOG_DEBUG("Invoking a batch modify rows request (Subrequests: %v)",
        batchRequest->part_counts_size());

    return batchRequest->Invoke().As<void>();
}

std::vector<TFuture<void>> TTransaction::FlushModifyRowsRequests()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto futures = std::move(BatchModifyRowsFutures_);
    if (BatchModifyRowsRequest_) {
        futures.push_back(InvokeBatchModifyRowsRequest());
    }
    return futures;
}

TTransactionStartOptions TTransaction::PatchTransactionId(const TTransactionStartOptions& options)
{
    auto copiedOptions = options;
    copiedOptions.ParentId = GetId();
    return copiedOptions;
}

////////////////////////////////////////////////////////////////////////////////

const TString& TTransaction::GetStickyProxyAddress() const
{
    return StickyProxyAddress_;
}

TFuture<void> TTransaction::FlushModifications()
{
    std::vector<TFuture<void>> futures;
    {
        auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::InvalidTransactionState,
                "Transaction %v is in %Qlv state",
                GetId(),
                State_);
        }

        if (!AlienTransactions_.empty()) {
            return MakeFuture<void>(TError(
                NTransactionClient::EErrorCode::AlienTransactionsForbidden,
                "Cannot flush transaction %v modifications since it has %v alien transaction(s)",
                GetId(),
                AlienTransactions_.size()));
        }

        State_ = ETransactionState::FlushingModifications;
        futures = FlushModifyRowsRequests();
    }

    YT_LOG_DEBUG("Flushing transaction modifications");

    return AllSucceeded(futures)
        .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& rspOrError) {
            {
                auto guard = Guard(SpinLock_);
                if (rspOrError.IsOK() && State_ == ETransactionState::FlushingModifications) {
                    State_ = ETransactionState::FlushedModifications;
                } else if (!rspOrError.IsOK()) {
                    YT_LOG_DEBUG(rspOrError, "Error flushing transaction modifications");
                    YT_UNUSED_FUTURE(DoAbort(&guard));
                    THROW_ERROR_EXCEPTION("Error flushing transaction %v modifications",
                        GetId())
                        << rspOrError;
                }
            }

            YT_LOG_DEBUG("Transaction modifications flushed");

            ModificationsFlushed_.Fire();

            return TError();
        }));
}

void TTransaction::SubscribeModificationsFlushed(const TModificationsFlushedHandler& handler)
{
    ModificationsFlushed_.Subscribe(handler);
}

void TTransaction::UnsubscribeModificationsFlushed(const TModificationsFlushedHandler& handler)
{
    ModificationsFlushed_.Unsubscribe(handler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
