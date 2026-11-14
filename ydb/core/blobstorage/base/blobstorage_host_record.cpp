#include "blobstorage_host_record.h"
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr::NBsController {

THostRecord::THostRecord(const TEvInterconnect::TNodeInfo& nodeInfo)
    : NodeId(nodeInfo.NodeId)
    , Location(nodeInfo.Location)
{}

THostRecord::THostRecord(const NKikimrBlobStorage::TNodeIdentifier& node)
    : NodeId(node.GetNodeId())
    , Location(node.GetLocation())
{}

THostRecordMapImpl::THostRecordMapImpl(TEvInterconnect::TEvNodesInfo *msg) {
    for (const TEvInterconnect::TNodeInfo& nodeInfo : msg->Nodes) {
        const THostId hostId(nodeInfo.Host, nodeInfo.Port);
        NodeIdToHostId.emplace(nodeInfo.NodeId, hostId);
        HostIdToRecord.emplace(hostId, nodeInfo);
        FqdnToNodeId.emplace(nodeInfo.Host, nodeInfo.NodeId);
    }
}

THostRecordMapImpl::THostRecordMapImpl(const NKikimrBlobStorage::TStorageConfig& config) {
    for (const auto& item : config.GetAllNodes()) {
        const THostId hostId(item.GetHost(), item.GetPort());
        const TNodeId nodeId = item.GetNodeId();
        NodeIdToHostId.emplace(nodeId, hostId);
        HostIdToRecord.emplace(hostId, item);
        FqdnToNodeId.emplace(item.GetHost(), nodeId);
    }
}

const TNodeLocation& THostRecordMapImpl::GetLocation(TNodeId nodeId) const {
    if (auto it = NodeIdToHostId.find(nodeId); it != NodeIdToHostId.end()) {
        if (auto hostIt = HostIdToRecord.find(it->second); hostIt != HostIdToRecord.end()) {
            return hostIt->second.Location;
        }
    }
    Y_ABORT();
}

TMaybe<TNodeId> THostRecordMapImpl::ResolveNodeId(const THostId& hostId) const {
    if (const auto it = HostIdToRecord.find(hostId); it != HostIdToRecord.end()) {
        return it->second.NodeId;
    } else {
        return {};
    }
}

TMaybe<THostRecordMapImpl::THostId> THostRecordMapImpl::GetHostId(TNodeId nodeId) const {
    if (const auto it = NodeIdToHostId.find(nodeId); it != NodeIdToHostId.end()) {
        return it->second;
    } else {
        return {};
    }
}

} // namespace NKikimr::NBsController
