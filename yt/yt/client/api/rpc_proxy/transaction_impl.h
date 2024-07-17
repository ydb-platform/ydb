#pragma once

#include "client_base.h"

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/dynamic_table_transaction_mixin.h>
#include <yt/yt/client/api/queue_transaction_mixin.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    (Active)
    (Committing)
    (Committed)
    (Flushing)
    (Flushed)
    (FlushingModifications)
    (FlushedModifications)
    (Aborting)
    (Aborted)
    (AbortFailed)
    (Detached)
);

class TTransaction
    : public virtual NApi::ITransaction
    , public NApi::TDynamicTableTransactionMixin
    , public NApi::TQueueTransactionMixin
{
public:
    TTransaction(
        TConnectionPtr connection,
        TClientPtr client,
        NRpc::IChannelPtr channel,
        NTransactionClient::TTransactionId id,
        NTransactionClient::TTimestamp startTimestamp,
        NTransactionClient::ETransactionType type,
        NTransactionClient::EAtomicity atomicity,
        NTransactionClient::EDurability durability,
        TDuration timeout,
        bool pingAncestors,
        std::optional<TDuration> pingPeriod,
        std::optional<TStickyTransactionParameters> stickyParameters,
        i64 sequenceNumberSourceId,
        TStringBuf capitalizedCreationReason);

    // ITransaction implementation.
    NApi::IConnectionPtr GetConnection() override;
    NApi::IClientPtr GetClient() const override;

    NTransactionClient::ETransactionType GetType() const override;
    NTransactionClient::TTransactionId GetId() const override;
    NTransactionClient::TTimestamp GetStartTimestamp() const override;
    NTransactionClient::EAtomicity GetAtomicity() const override;
    NTransactionClient::EDurability GetDurability() const override;
    TDuration GetTimeout() const override;

    TFuture<void> Ping(const NApi::TTransactionPingOptions& options = {}) override;
    TFuture<NApi::TTransactionFlushResult> Flush() override;
    TFuture<NApi::TTransactionCommitResult> Commit(const NApi::TTransactionCommitOptions&) override;
    TFuture<void> Abort(const NApi::TTransactionAbortOptions& options = {}) override;
    void Detach() override;
    void RegisterAlienTransaction(const ITransactionPtr& transaction) override;

    void SubscribeCommitted(const TCommittedHandler& handler) override;
    void UnsubscribeCommitted(const TCommittedHandler& handler) override;

    void SubscribeAborted(const TAbortedHandler& handler) override;
    void UnsubscribeAborted(const TAbortedHandler& handler) override;

    void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NApi::TRowModification> modifications,
        const NApi::TModifyRowsOptions& options) override;

    using TQueueTransactionMixin::AdvanceQueueConsumer;
    TFuture<void> AdvanceQueueConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset,
        const TAdvanceQueueConsumerOptions& options) override;

    TFuture<TPushQueueProducerResult> PushQueueProducer(
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        NQueueClient::TQueueProducerEpoch epoch,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TPushQueueProducerOptions& options) override;

    // IClientBase implementation.
    TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override;

    TFuture<TUnversionedLookupRowsResult> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const NApi::TLookupRowsOptions& options) override;

    TFuture<TVersionedLookupRowsResult> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const NApi::TVersionedLookupRowsOptions& options) override;

    TFuture<std::vector<TUnversionedLookupRowsResult>> MultiLookupRows(
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options) override;

    TFuture<NApi::TSelectRowsResult> SelectRows(
        const TString& query,
        const NApi::TSelectRowsOptions& options) override;

    TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const NApi::TExplainQueryOptions& options) override;

    TFuture<NApi::TPullRowsResult> PullRows(
        const NYPath::TYPath& path,
        const NApi::TPullRowsOptions& options) override;

    TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const NApi::TTableReaderOptions& options) override;

    TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override;

    TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const NApi::TGetNodeOptions& options) override;

    TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const NApi::TSetNodeOptions& options) override;

    TFuture<void> MultisetAttributesNode(
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const NApi::TMultisetAttributesNodeOptions& options) override;

    TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const NApi::TRemoveNodeOptions& options) override;

    TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const NApi::TListNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const NApi::TCreateNodeOptions& options) override;

    TFuture<NApi::TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const NApi::TLockNodeOptions& options) override;

    TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const NApi::TUnlockNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TCopyNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TMoveNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TLinkNodeOptions& options) override;

    TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const NApi::TConcatenateNodesOptions& options) override;

    TFuture<void> ExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options) override;

    TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options) override;

    TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const NApi::TNodeExistsOptions& options) override;

    TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const NApi::TCreateObjectOptions& options) override;

    TFuture<NApi::IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const NApi::TFileReaderOptions& options) override;

    NApi::IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const NApi::TFileWriterOptions& options) override;

    NApi::IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const NApi::TJournalReaderOptions& options) override;

    NApi::IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const NApi::TJournalWriterOptions& options) override;

    // Custom methods.

    //! Returns proxy address this transaction is sticking to.
    //! Empty for non-sticky transactions (e.g.: master) or
    //! if address resolution is not supported by the implementation.
    const TString& GetStickyProxyAddress() const;

    //! Flushes all modifications to RPC proxy.
    //!
    //! In contrast to #Flush does not send #FlushTransaction request to RPC proxy and does not change
    //! the state of the transaction within RPC proxy allowing to work with the transaction after the call
    //! from a different transaction client.
    //!
    //! Useful for sending modifications to the same transaction from several different sources.
    TFuture<void> FlushModifications();

    using TModificationsFlushedHandlerSignature = void();
    using TModificationsFlushedHandler = TCallback<TModificationsFlushedHandlerSignature>;
    void SubscribeModificationsFlushed(const TModificationsFlushedHandler& handler);
    void UnsubscribeModificationsFlushed(const TModificationsFlushedHandler& handler);

private:
    const TConnectionPtr Connection_;
    const TClientPtr Client_;
    const NRpc::IChannelPtr Channel_;
    const NTransactionClient::TTransactionId Id_;
    const NTransactionClient::TTimestamp StartTimestamp_;
    const NTransactionClient::ETransactionType Type_;
    const NTransactionClient::EAtomicity Atomicity_;
    const NTransactionClient::EDurability Durability_;
    const TDuration Timeout_;
    const bool PingAncestors_;
    const std::optional<TDuration> PingPeriod_;
    const TString StickyProxyAddress_;
    const i64 SequenceNumberSourceId_;

    const NLogging::TLogger Logger;

    TApiServiceProxy Proxy_;

    std::atomic<i64> ModifyRowsRequestSequenceCounter_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    ETransactionState State_ = ETransactionState::Active;
    TPromise<void> AbortPromise_;
    std::vector<NApi::ITransactionPtr> AlienTransactions_;

    THashSet<NObjectClient::TCellId> AdditionalParticipantCellIds_;

    TApiServiceProxy::TReqBatchModifyRowsPtr BatchModifyRowsRequest_;
    std::vector<TFuture<void>> BatchModifyRowsFutures_;

    TSingleShotCallbackList<TCommittedHandlerSignature> Committed_;
    TSingleShotCallbackList<TAbortedHandlerSignature> Aborted_;
    TSingleShotCallbackList<TModificationsFlushedHandlerSignature> ModificationsFlushed_;

    TFuture<void> SendPing();
    void RunPeriodicPings();
    bool IsPingableState();

    TFuture<void> DoAbort(
        TGuard<NThreading::TSpinLock>* guard,
        const TTransactionAbortOptions& options = {});

    void ValidateActive();
    void DoValidateActive();

    TApiServiceProxy::TReqBatchModifyRowsPtr CreateBatchModifyRowsRequest();
    TFuture<void> InvokeBatchModifyRowsRequest();
    std::vector<TFuture<void>> FlushModifyRowsRequests();

    template <class T>
    T PatchTransactionId(const T& options);
    NApi::TTransactionStartOptions PatchTransactionId(const NApi::TTransactionStartOptions& options);
    template <class T>
    T PatchTransactionTimestamp(const T& options);
};

DEFINE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

#define TRANSACTION_IMPL_INL_H_
#include "transaction_impl-inl.h"
#undef TRANSACTION_IMPL_INL_H_
