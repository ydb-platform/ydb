#pragma once

#include "public.h"
#include "viable_peer_registry.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IMapPeerPriorityProvider
    : public IPeerPriorityProvider
{
    virtual void SetAddressToClusterOverride(THashMap<std::string, std::string> addressToClusterOverride) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMapPeerPriorityProvider)

// Returns EPeerPriority::LocalDc for any address.
IPeerPriorityProviderPtr GetDummyPeerPriorityProvider();

// Uses peer address and local host name to infer YP cluster names.
// Returns EPeerPriority::LocalDc if YP clusters match, EPeerPriority::ForeignDc otherwise.
IPeerPriorityProviderPtr GetYPClusterMatchingPeerPriorityProvider();

// Allows to set map that overrides datacenter name for given peers,
// compares it to YP cluster name inferred from the local host name.
// Tries to infer YP cluster name from peer address if there's no override for the given peer.
// Returns EPeerPriority::LocalDc if YP clusters match, EPeerPriority::ForeignDc otherwise.
IMapPeerPriorityProviderPtr CreateYPClusterMatchingPeerPriorityProviderWithOverrides();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
