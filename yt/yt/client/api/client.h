#pragma once

#include "connection.h"
#include "accounting_client.h"
#include "admin_client.h"
#include "cypress_client.h"
#include "distributed_table_client.h"
#include "etc_client.h"
#include "file_client.h"
#include "journal_client.h"
#include "operation_client.h"
#include "security_client.h"
#include "transaction_client.h"
#include "table_client.h"
#include "queue_client.h"
#include "query_tracker_client.h"
#include "flow_client.h"

#include <yt/yt/client/bundle_controller_client/bundle_controller_client.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

//! Provides a basic set of functions that can be invoked
//! both standalone and inside transaction.
/*
 *  This interface contains methods shared by IClient and ITransaction.
 *
 *  Thread affinity: single
 */
struct IClientBase
    : public virtual TRefCounted
    , public ITransactionClientBase
    , public ITableClientBase
    , public ICypressClientBase
    , public IFileClientBase
    , public IJournalClientBase
    , public IQueueClientBase
    , public IEtcClientBase
    , public IDistributedTableClientBase
{
    virtual IConnectionPtr GetConnection() = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientBase)

////////////////////////////////////////////////////////////////////////////////

//! A central entry point for all interactions with the YT cluster.
/*!
 *  In contrast to IConnection, each IClient represents an authenticated entity.
 *  The needed username is passed to #IConnection::CreateClient via options.
 *  Note that YT API has no built-in authentication mechanisms so it must be wrapped
 *  with appropriate logic.
 *
 *  Most methods accept |TransactionId| as a part of their options.
 *  A similar effect can be achieved by issuing requests via ITransaction.
 */
struct IClient
    : public virtual IClientBase
    , public ITransactionClient
    , public ITableClient
    , public IQueueClient
    , public IJournalClient
    , public IFileClient
    , public ISecurityClient
    , public IAccountingClient
    , public IOperationClient
    , public IAdminClient
    , public IQueryTrackerClient
    , public IEtcClient
    , public NBundleControllerClient::IBundleControllerClient
    , public IFlowClient
    , public IDistributedTableClient
{
    //! Terminates all channels.
    //! Aborts all pending uncommitted transactions.
    virtual void Terminate() = 0;

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() = 0;
    virtual const NChaosClient::IReplicationCardCachePtr& GetReplicationCardCache() = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() = 0;

    virtual std::optional<TStringBuf> GetClusterName(bool fetchIfNull = true) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

class TClusterAwareClientBase
    : public virtual IClient
{
public:
    //! Returns and caches the cluster name corresponding to this client.
    //! If available, the name is taken from the client's connection configuration.
    //! Otherwise, if fetchIfNull is set to true, the first call to this method
    //! will fetch the cluster name from Cypress via master caches.
    //!
    //! NB: Descendants of this class should be able to perform GetNode calls,
    //! so this cannot be used directly in tablet transactions.
    //! Use the transaction's parent client instead.
    std::optional<TStringBuf> GetClusterName(bool fetchIfNull) override;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    std::optional<TString> ClusterName_;

    std::optional<TString> FetchClusterNameFromMasterCache();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
