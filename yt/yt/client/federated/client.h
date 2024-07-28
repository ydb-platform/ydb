#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <vector>

//! Federated client is a wrapper for several YT-clients with ability
//! to retry asynchronously the same request with different underlying-clients.
//! Each YT-client typically corresponds to a different YT-cluster.
//! In case of errors (for example, cluster unavailability) federated client tries
//! to retry request via another client (cluster).
//!
//! Client in the same datacenter is more prior than other.
//!
//! Federated client implements IClient interface, but does not support
//! the most of mutable methods (except modifications inside transactions).
namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

struct IFederatedClientTransactionMixin
{
    //! Try fetch sticky proxy address if underlying transaction supports it.
    virtual std::optional<TString> TryGetStickyProxyAddress() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates a federated client with given underlying clients.
NApi::IClientPtr CreateClient(
    std::vector<NApi::IClientPtr> clients,
    TFederationConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
