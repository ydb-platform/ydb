#pragma once

#include "public.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

// Network -> host:port.
using TAddressMap = THashMap<TString, TString>;

// Address type (e.g. RPC, HTTP) -> network -> host:port.
using TProxyAddressMap = THashMap<EAddressType, TAddressMap>;

extern const EAddressType DefaultAddressType;
extern const TString DefaultNetworkName;

// Network -> [host:port].
using TNetworkAddressesMap = THashMap<TString, std::vector<TString>>;

// Address type (e.g. RPC, HTTP) -> network -> [host:port].
using TAddressTypeAddressesMap = THashMap<EAddressType, TNetworkAddressesMap>;

// Role -> address type -> network -> host:port.
using TBalancersMap = THashMap<TString, TAddressTypeAddressesMap>;

////////////////////////////////////////////////////////////////////////////////

TAddressMap GetLocalAddresses(
    const NNodeTrackerClient::TNetworkAddressList& addresses,
    int port);

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> GetAddressOrNull(
    const TProxyAddressMap& addresses,
    EAddressType addressType,
    const TString& network);

////////////////////////////////////////////////////////////////////////////////

std::optional<std::vector<TString>> GetBalancersOrNull(
    const TBalancersMap& balancers,
    const TString& role,
    EAddressType addressType,
    const TString& network);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
