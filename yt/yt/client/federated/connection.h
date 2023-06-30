#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/connection.h>

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

//! Creates federated connection with given underlying |connections|.
//! Federated connection is a wrapper for several connections with ability
//! to create federated client with clients created by |connections| with the same |TAuthenticationOptions|.
NApi::IConnectionPtr CreateConnection(
    std::vector<NApi::IConnectionPtr> connections,
    TFederationConfigPtr config);

//! Creates federated connection with given |config| and |options|.
NApi::IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    NApi::NRpcProxy::TConnectionOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
