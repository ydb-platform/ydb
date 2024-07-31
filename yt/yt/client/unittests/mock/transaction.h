#pragma once

#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/dynamic_table_transaction_mixin.h>
#include <yt/yt/client/api/queue_transaction_mixin.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TMockTransaction
    : public virtual ITransaction
    , public TDynamicTableTransactionMixin
    , public TQueueTransactionMixin
{
public:
    MOCK_METHOD(IConnectionPtr, GetConnection, (), (override));

    MOCK_METHOD(TFuture<ITransactionPtr>, StartTransaction, (
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options), (override));

    MOCK_METHOD(TFuture<TUnversionedLookupRowsResult>, LookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options), (override));

    MOCK_METHOD(TFuture<TVersionedLookupRowsResult>, VersionedLookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options), (override));

    MOCK_METHOD(TFuture<std::vector<TUnversionedLookupRowsResult>>, MultiLookupRows, (
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options), (override));

    MOCK_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options), (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, ExplainQuery, (
        const TString& query,
        const TExplainQueryOptions& options), (override));

    MOCK_METHOD(TFuture<TPullRowsResult>, PullRows, (
        const NYPath::TYPath& path,
        const TPullRowsOptions& options), (override));

    MOCK_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options), (override));

    MOCK_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options), (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, GetNode, (
        const NYPath::TYPath& path,
        const TGetNodeOptions& options), (override));

    MOCK_METHOD(TFuture<void>, SetNode, (
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options), (override));

    MOCK_METHOD(TFuture<void>, MultisetAttributesNode, (
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options), (override));

    MOCK_METHOD(TFuture<void>, RemoveNode, (
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options), (override));

    MOCK_METHOD(TFuture<NYson::TYsonString>, ListNode, (
        const NYPath::TYPath& path,
        const TListNodeOptions& options), (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options), (override));

    MOCK_METHOD(TFuture<TLockNodeResult>, LockNode, (
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options), (override));

    MOCK_METHOD(TFuture<void>, UnlockNode, (
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options), (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options), (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options), (override));

    MOCK_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options), (override));

    MOCK_METHOD(TFuture<void>, ConcatenateNodes, (
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options), (override));

    MOCK_METHOD(TFuture<void>, ExternalizeNode, (
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options), (override));

    MOCK_METHOD(TFuture<void>, InternalizeNode, (
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options), (override));

    MOCK_METHOD(TFuture<bool>, NodeExists, (
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options), (override));

    MOCK_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options), (override));

    MOCK_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (
        const NYPath::TYPath& path,
        const TFileReaderOptions& options), (override));

    MOCK_METHOD(IFileWriterPtr, CreateFileWriter, (
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options), (override));

    MOCK_METHOD(IJournalReaderPtr, CreateJournalReader, (
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options), (override));

    MOCK_METHOD(IJournalWriterPtr, CreateJournalWriter, (
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options), (override));

    // ITransaction
    IClientPtr Client;
    NTransactionClient::ETransactionType Type;
    NTransactionClient::TTransactionId Id;
    NTransactionClient::TTimestamp StartTimestamp;
    NTransactionClient::EAtomicity Atomicity;
    NTransactionClient::EDurability Durability;
    TDuration Timeout;

    IClientPtr GetClient() const override
    {
        return Client;
    }
    NTransactionClient::ETransactionType GetType() const override
    {
        return Type;
    }
    MOCK_METHOD(NTransactionClient::TTransactionId, GetId, (), (const, override));

    NTransactionClient::TTimestamp GetStartTimestamp() const override
    {
        return StartTimestamp;
    }
    NTransactionClient::EAtomicity GetAtomicity() const override
    {
        return Atomicity;
    }
    NTransactionClient::EDurability GetDurability() const override
    {
        return Durability;
    }
    TDuration GetTimeout() const override
    {
        return Timeout;
    }

    MOCK_METHOD(TFuture<void>, Ping, (const NApi::TTransactionPingOptions& options), (override));
    MOCK_METHOD(TFuture<TTransactionCommitResult>, Commit, (const TTransactionCommitOptions& options), (override));
    MOCK_METHOD(TFuture<void>, Abort, (const TTransactionAbortOptions& options), (override));
    MOCK_METHOD(void, Detach, (), (override));
    MOCK_METHOD(TFuture<TTransactionFlushResult>, Flush, (), (override));

    MOCK_METHOD(void, RegisterAlienTransaction, (const NApi::ITransactionPtr& transaction), (override));

    MOCK_METHOD(void, SubscribeCommitted, (const TCommittedHandler& callback), (override));
    MOCK_METHOD(void, UnsubscribeCommitted, (const TCommittedHandler& callback), (override));
    MOCK_METHOD(void, SubscribeAborted, (const TAbortedHandler& callback), (override));
    MOCK_METHOD(void, UnsubscribeAborted, (const TAbortedHandler& callback), (override));

    using TQueueTransactionMixin::AdvanceQueueConsumer;
    MOCK_METHOD(TFuture<void>, AdvanceQueueConsumer, (
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset,
        const TAdvanceQueueConsumerOptions& options), (override));

    MOCK_METHOD(void, ModifyRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options), (override));

    MOCK_METHOD(TFuture<TPushQueueProducerResult>, PushQueueProducer, (
        const NYPath::TRichYPath& producerPath,
        const NYPath::TRichYPath& queuePath,
        const NQueueClient::TQueueProducerSessionId& sessionId,
        NQueueClient::TQueueProducerEpoch epoch,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const TPushQueueProducerOptions& options), (override));
};

DEFINE_REFCOUNTED_TYPE(TMockTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
