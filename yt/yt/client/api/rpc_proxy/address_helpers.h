#pragma once

#include "public.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

// Network -> host:port.
using TAddressMap = THashMap<std::string, std::string>;

// Address type (e.g. RPC, HTTP) -> network -> host:port.
using TProxyAddressMap = THashMap<EAddressType, TAddressMap>;

extern const EAddressType DefaultAddressType;
extern const std::string DefaultNetworkName;

// Network -> [host:port].
using TNetworkAddressesMap = THashMap<std::string, std::vector<std::string>>;

// Address type (e.g. RPC, HTTP) -> network -> [host:port].
using TAddressTypeAddressesMap = THashMap<EAddressType, TNetworkAddressesMap>;

// Role -> address type -> network -> host:port.
using TBalancersMap = THashMap<std::string, TAddressTypeAddressesMap>;

////////////////////////////////////////////////////////////////////////////////

TAddressMap GetLocalAddresses(
    const NNodeTrackerClient::TNetworkAddressList& addresses,
    int port);

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GetAddressOrNull(
    const TProxyAddressMap& addresses,
    EAddressType addressType,
    const std::string& network);

////////////////////////////////////////////////////////////////////////////////

std::optional<std::vector<std::string>> GetBalancersOrNull(
    const TBalancersMap& balancers,
    const std::string& role,
    EAddressType addressType,
    const std::string& network);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
