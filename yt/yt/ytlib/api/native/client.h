#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IClientBase
    : public virtual NApi::IClientBase
{
    virtual TFuture<ITransactionPtr> StartNativeTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TTransactionCounters
{
    TTransactionCounters() = default;
    explicit TTransactionCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter TransactionCounter;
    NProfiling::TCounter CommittedTransactionCounter;
    NProfiling::TCounter AbortedTransactionCounter;

    NProfiling::TCounter TabletSessionCommitCounter;
    NProfiling::TCounter SuccessfulTabletSessionCommitCounter;
    NProfiling::TCounter RetriedSuccessfulTabletSessionCommitCounter;
    NProfiling::TCounter FailedTabletSessionCommitCounter;
};

struct TOperationApiCounters
{
    TOperationApiCounters() = default;
    explicit TOperationApiCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter GetOperationFromArchiveTimeoutCounter;
    NProfiling::TCounter GetOperationFromArchiveSuccessCounter;
    NProfiling::TCounter GetOperationFromArchiveFailureCounter;
};

struct TClientCounters
{
    TClientCounters() = default;
    explicit TClientCounters(const NProfiling::TProfiler& profiler);

    TTransactionCounters TransactionCounters;
    TOperationApiCounters OperationApiCounters;
};

////////////////////////////////////////////////////////////////////////////////

struct TSyncAlienCellOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    bool FullSync = false;
};

struct TSyncAlienCellsResult
{
    std::vector<NChaosClient::TAlienCellDescriptor> AlienCellDescriptors;
    bool EnableMetadataCells;
};

struct IClient
    : public IClientBase
    , public virtual NApi::IClient
{
    virtual const TClientOptions& GetOptions() = 0;
    virtual const IConnectionPtr& GetNativeConnection() = 0;
    virtual const NTransactionClient::TTransactionManagerPtr& GetTransactionManager() = 0;

    virtual const TClientCounters& GetCounters() const = 0;

    virtual NQueryClient::IFunctionRegistryPtr GetFunctionRegistry() = 0;
    virtual NQueryClient::TFunctionImplCachePtr GetFunctionImplCache() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;
    virtual NRpc::IChannelPtr GetCellChannelOrThrow(
        NElection::TCellId cellId) = 0;

    //! Returns authenticated channel to work with Cypress.
    //! See IConnection::GetCypressChannelOrThrow.
    virtual NRpc::IChannelPtr GetCypressChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;

    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual const NNodeTrackerClient::INodeChannelFactoryPtr& GetChannelFactory() = 0;

    virtual ITransactionPtr AttachNativeTransaction(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options = TTransactionAttachOptions()) = 0;

    virtual TFuture<TSyncAlienCellsResult> SyncAlienCells(
        const std::vector<NChaosClient::TAlienCellDescriptorLite>& alienCellDescriptors,
        const TSyncAlienCellOptions& options = {}) = 0;

    virtual bool DoesOperationsArchiveExist() = 0;

    virtual TFuture<TIssueTokenResult> IssueTemporaryToken(
        const TString& user,
        const NYTree::IAttributeDictionaryPtr& attributes,
        const TIssueTemporaryTokenOptions& options) = 0;

    virtual TFuture<void> RefreshTemporaryToken(
        const TString& user,
        const TString& token,
        const TRefreshTemporaryTokenOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

