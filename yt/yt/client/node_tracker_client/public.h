#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNodeStatistics;
class TNodeResources;
class TNodeResourceLimitsOverrides;

class TDiskResources;
class TDiskLocationResources;

class TAddressMap;
class TNodeAddressMap;

class TNodeDescriptor;
class TNodeDirectory;

} // namespace NProto

YT_DEFINE_ERROR_ENUM(
    ((NoSuchNode)        (1600))
    ((InvalidState)      (1601))
    ((NoSuchNetwork)     (1602))
    ((NoSuchRack)        (1603))
    ((NoSuchDataCenter)  (1604))
);

DEFINE_ENUM(EAddressType,
    ((InternalRpc)    (0))
    ((SkynetHttp)     (1))
    ((MonitoringHttp) (2))
);

YT_DEFINE_STRONG_TYPEDEF(TNodeId, ui32);
constexpr TNodeId InvalidNodeId = TNodeId(0);
constexpr TNodeId MaxNodeId = TNodeId((1 << 24) - 1); // TNodeId must fit into 24 bits (see TChunkReplica)

using THostId = NObjectClient::TObjectId;
using TRackId = NObjectClient::TObjectId;
using TDataCenterId = NObjectClient::TObjectId;

// Only domain names, without port number.
using TNetworkAddressList = std::vector<std::pair<std::string, std::string>>;
using TNetworkPreferenceList = std::vector<std::string>;

// Network -> host:port.
using TAddressMap = THashMap<std::string, std::string>;

// Address type (e.g. RPC, HTTP) -> network -> host:port.
using TNodeAddressMap = THashMap<EAddressType, TAddressMap>;

DECLARE_REFCOUNTED_CLASS(TNodeDirectory)
class TNodeDescriptor;

extern const std::string DefaultNetworkName;
extern const TNetworkPreferenceList DefaultNetworkPreferences;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
