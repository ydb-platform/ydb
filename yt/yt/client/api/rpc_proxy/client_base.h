#pragma once

#include "private.h"
#include "helpers.h"
#include "connection_impl.h"
#include "api_service_proxy.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/driver/private.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct TRpcProxyClientBufferTag
{ };

class TClientBase
    : public virtual NApi::IClientBase
{
protected:
    // Returns a bound RPC proxy connection for this interface.
    virtual TConnectionPtr GetRpcProxyConnection() = 0;
    // Returns a bound RPC proxy client for this interface.
    virtual TClientPtr GetRpcProxyClient() = 0;
    // Returns an RPC channel to use for API calls.
    virtual NRpc::IChannelPtr GetRetryingChannel() const = 0;
    // Returns an RPC channel to use for API calls within single transaction.
    // The channel is non-retrying, so should be wrapped into retrying channel on demand.
    virtual NRpc::IChannelPtr CreateNonRetryingStickyChannel() const = 0;
    // Wraps the underlying sticky channel into retrying channel.
    // Stickiness affects retries policy.
    virtual NRpc::IChannelPtr WrapStickyChannelIntoRetrying(NRpc::IChannelPtr underlying) const = 0;

    friend class TTransaction;

public:
    NApi::IConnectionPtr GetConnection() override;

    // Helpers for accessing low-lepel API service proxy.
    // Prefer using interface methods unless you know exactly what you are doing.
    TApiServiceProxy CreateApiServiceProxy(NRpc::IChannelPtr channel = {});
    void InitStreamingRequest(NRpc::TClientRequest& request);

    // Transactions.
    TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override;

    // Tables.
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
        const TMultiLookupOptions& options = {}) override;

    TFuture<NApi::TSelectRowsResult> SelectRows(
        const TString& query,
        const NApi::TSelectRowsOptions& options) override;

    TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const NApi::TExplainQueryOptions& options) override;

    virtual TFuture<TPullRowsResult> PullRows(
        const NYPath::TYPath& path,
        const NApi::TPullRowsOptions& options) override;

    // TODO(babenko): batch read and batch write.

    // Cypress.
    TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const NApi::TNodeExistsOptions& options) override;

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
        const TExternalizeNodeOptions& options = {}) override;

    TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options = {}) override;

    // Objects.
    TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const NApi::TCreateObjectOptions& options) override;

    //! NB: Readers and writers returned by methods below are NOT thread-safe.
    // Files.
    TFuture<NApi::IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const NApi::TFileReaderOptions& options) override;

    NApi::IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const NApi::TFileWriterOptions& options) override;

    // Journals.
    NApi::IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const NApi::TJournalReaderOptions& options) override;

    NApi::IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const NApi::TJournalWriterOptions& options) override;

    // Tables.
    TFuture<NApi::ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const NApi::TTableReaderOptions& options) override;

    TFuture<NApi::ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override;

    // Distributed table client
    TFuture<TDistributedWriteSessionPtr> StartDistributedWriteSession(
        const NYPath::TRichYPath& path,
        const TDistributedWriteSessionStartOptions& options) override;

    TFuture<void> FinishDistributedWriteSession(
        TDistributedWriteSessionPtr session,
        const TDistributedWriteSessionFinishOptions& options) override;
};

DEFINE_REFCOUNTED_TYPE(TClientBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
