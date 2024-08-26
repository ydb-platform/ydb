#pragma once

#include "transaction.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! A simple base class that implements ITransaction and delegates
//! all calls to the underlying instance.
class TDelegatingTransaction
    : public virtual ITransaction
{
public:
    explicit TDelegatingTransaction(ITransactionPtr underlying);

    // IClientBase methods

    IConnectionPtr GetConnection() override;

    // Transactions
    TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options) override;

    // Tables
    TFuture<TUnversionedLookupRowsResult> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options) override;

    TFuture<TVersionedLookupRowsResult> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options) override;

    TFuture<std::vector<TUnversionedLookupRowsResult>> MultiLookupRows(
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options) override;

    TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options) override;

    TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const TExplainQueryOptions& options) override;

    TFuture<TPullRowsResult> PullRows(
        const NYPath::TYPath& path,
        const TPullRowsOptions& options) override;

    TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options) override;

    TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options) override;

    // Cypress
    TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options) override;

    TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options) override;

    TFuture<void> MultisetAttributesNode(
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options) override;

    TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options) override;

    TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options) override;

    TFuture<TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options) override;

    TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options) override;

    TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options) override;

    TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options) override;

    TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options) override;

    TFuture<void> ExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options) override;

    TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options) override;

    // Objects
    TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options) override;

    // Files
    TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options) override;

    IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options) override;

    // Journals
    IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options) override;

    IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options) override;

    // ITransaction methods
    IClientPtr GetClient() const override;
    NTransactionClient::ETransactionType GetType() const override;
    NTransactionClient::TTransactionId GetId() const override;
    NTransactionClient::TTimestamp GetStartTimestamp() const override;
    NTransactionClient::EAtomicity GetAtomicity() const override;
    NTransactionClient::EDurability GetDurability() const override;
    TDuration GetTimeout() const override;

    TFuture<void> Ping(
        const NApi::TTransactionPingOptions& options) override;

    TFuture<TTransactionCommitResult> Commit(
        const TTransactionCommitOptions& options = TTransactionCommitOptions()) override;

    TFuture<void> Abort(
        const TTransactionAbortOptions& options = TTransactionAbortOptions()) override;

    void Detach() override;
    TFuture<TTransactionFlushResult> Flush() override;
    void RegisterAlienTransaction(const ITransactionPtr& transaction) override;

    void SubscribeCommitted(const TCommittedHandler& handler) override;
    void UnsubscribeCommitted(const TCommittedHandler& handler) override;

    void SubscribeAborted(const TAbortedHandler& handler) override;
    void UnsubscribeAborted(const TAbortedHandler& handler) override;

    // Tables
    void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TModifyRowsOptions& options,
        NTableClient::ELockType lockType = NTableClient::ELockType::Exclusive) override;
    void WriteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const TModifyRowsOptions& options) override;

    void DeleteRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const TModifyRowsOptions& options) override;

    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::TLockMask lockMask) override;
    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        NTableClient::ELockType lockType) override;
    void LockRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const std::vector<TString>& locks,
        NTableClient::ELockType lockType) override;

    void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options) override;

    // Queues
    void AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) override;
    TFuture<void> AdvanceQueueConsumer(
        const NYT::NYPath::TRichYPath& consumer,
        const NYT::NYPath::TRichYPath& queue,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset,
        const NYT::NApi::TAdvanceQueueConsumerOptions& options) override;

    TFuture<TPushQueueProducerResult> PushQueueProducer(
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        NQueueClient::TQueueProducerEpoch epoch,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TPushQueueProducerOptions& options) override;

    TFuture<TPushQueueProducerResult> PushQueueProducer(
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        NQueueClient::TQueueProducerEpoch epoch,
        NTableClient::TNameTablePtr nameTable,
        const std::vector<TSharedRef>& serializedRows,
        const TPushQueueProducerOptions& options) override;

    // Distributed table client
    TFuture<TDistributedWriteSessionPtr> StartDistributedWriteSession(
        const NYPath::TRichYPath& path,
        const TDistributedWriteSessionStartOptions& options = {}) override;

    TFuture<void> FinishDistributedWriteSession(
        TDistributedWriteSessionPtr session,
        const TDistributedWriteSessionFinishOptions& options = {}) override;

protected:
    const ITransactionPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

