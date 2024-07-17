#include "delegating_transaction.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TDelegatingTransaction::TDelegatingTransaction(ITransactionPtr underlying)
    : Underlying_(std::move(underlying))
{ }

#define DELEGATE_METHOD(returnType, method, signature, args) \
    returnType TDelegatingTransaction::method signature \
    { \
        return Underlying_->method args; \
    }

DELEGATE_METHOD(IConnectionPtr, GetConnection, (), ())

DELEGATE_METHOD(TFuture<ITransactionPtr>, StartTransaction, (
    NTransactionClient::ETransactionType type,
    const TTransactionStartOptions& options),
    (type, options))

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
    (path, std::move(nameTable), keys, options))

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

DELEGATE_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (
    NObjectClient::EObjectType type,
    const TCreateObjectOptions& options),
    (type, options))

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

DELEGATE_METHOD(IClientPtr, GetClient, () const, ())

DELEGATE_METHOD(NTransactionClient::ETransactionType, GetType, () const, ())

DELEGATE_METHOD(NTransactionClient::TTransactionId, GetId, () const, ())

DELEGATE_METHOD(NTransactionClient::TTimestamp, GetStartTimestamp, () const, ())

DELEGATE_METHOD(NTransactionClient::EAtomicity, GetAtomicity, () const, ())

DELEGATE_METHOD(NTransactionClient::EDurability, GetDurability, () const, ())

DELEGATE_METHOD(TDuration, GetTimeout, () const, ())

DELEGATE_METHOD(TFuture<void>, Ping, (
    const NApi::TTransactionPingOptions& options),
    (options))

DELEGATE_METHOD(TFuture<TTransactionCommitResult>, Commit, (
    const TTransactionCommitOptions& options),
    (options))

DELEGATE_METHOD(TFuture<void>, Abort, (
    const TTransactionAbortOptions& options),
    (options))

DELEGATE_METHOD(void, Detach, (), ())

DELEGATE_METHOD(TFuture<TTransactionFlushResult>, Flush, (), ())

DELEGATE_METHOD(void, RegisterAlienTransaction, (
    const ITransactionPtr& transaction),
    (transaction))

DELEGATE_METHOD(void, SubscribeCommitted, (const
    TCommittedHandler& handler),
    (handler))

DELEGATE_METHOD(void, UnsubscribeCommitted, (
    const TCommittedHandler& handler),
    (handler))

DELEGATE_METHOD(void, SubscribeAborted, (
    const TAbortedHandler& handler),
    (handler))

DELEGATE_METHOD(void, UnsubscribeAborted, (
    const TAbortedHandler& handler),
    (handler))

DELEGATE_METHOD(void, WriteRows, (
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TUnversionedRow> rows,
    const TModifyRowsOptions& options,
    NTableClient::ELockType lockType),
    (path, nameTable, rows, options, lockType))

DELEGATE_METHOD(void, WriteRows, (
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TVersionedRow> rows,
    const TModifyRowsOptions& options),
    (path, nameTable, rows, options))

DELEGATE_METHOD(void, DeleteRows, (
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TLegacyKey> keys,
    const TModifyRowsOptions& options),
    (path, nameTable, keys, options))

DELEGATE_METHOD(void, LockRows, (
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TLegacyKey> keys,
    NTableClient::TLockMask lockMask),
    (path, nameTable, keys, lockMask))

DELEGATE_METHOD(void, LockRows, (
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TLegacyKey> keys,
    NTableClient::ELockType lockType),
    (path, nameTable, keys, lockType))

DELEGATE_METHOD(void, LockRows, (
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TLegacyKey> keys,
    const std::vector<TString>& locks,
    NTableClient::ELockType lockType),
    (path, nameTable, keys, locks, lockType))

DELEGATE_METHOD(void, ModifyRows, (
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options),
    (path, nameTable, modifications, options))

DELEGATE_METHOD(void, AdvanceConsumer, (
    const NYPath::TRichYPath& consumerPath,
    const NYPath::TRichYPath& queuePath,
    int partitionIndex,
    std::optional<i64> oldOffset,
    i64 newOffset),
    (consumerPath, queuePath, partitionIndex, oldOffset, newOffset))

DELEGATE_METHOD(TFuture<void>, AdvanceQueueConsumer, (
    const NYT::NYPath::TRichYPath& consumer,
    const NYT::NYPath::TRichYPath& queue,
    int partitionIndex,
    std::optional<i64> oldOffset,
    i64 newOffset,
    const NYT::NApi::TAdvanceQueueConsumerOptions& options),
    (consumer, queue, partitionIndex, oldOffset, newOffset, options))

DELEGATE_METHOD(TFuture<TPushQueueProducerResult>, PushQueueProducer, (
    const NYPath::TRichYPath& producerPath,
    const NYPath::TRichYPath& queuePath,
    const NQueueClient::TQueueProducerSessionId& sessionId,
    NQueueClient::TQueueProducerEpoch epoch,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TUnversionedRow> rows,
    const TPushQueueProducerOptions& options),
    (producerPath, queuePath, sessionId, epoch, nameTable, rows, options))

#undef DELEGATE_METHOD

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
