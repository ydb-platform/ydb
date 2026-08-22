#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <util/generic/hash_multi_map.h>

namespace NKikimr {
namespace NBsController {

    using TNodeId = ui32;

    struct THostRecord {
        TNodeId NodeId;
        TNodeLocation Location;

        THostRecord(const TEvInterconnect::TNodeInfo& nodeInfo)
            : NodeId(nodeInfo.NodeId)
            , Location(nodeInfo.Location)
        {}

        THostRecord(const NKikimrBlobStorage::TNodeIdentifier& node)
            : NodeId(node.GetNodeId())
            , Location(node.GetLocation())
        {}
    };

    class THostRecordMapImpl {
        using THostId = std::tuple<TString, i32>;

        THashMap<THostId, THostRecord> HostIdToRecord;
        THashMap<TNodeId, THostId> NodeIdToHostId;
        THashMultiMap<TString, TNodeId> FqdnToNodeId;

    public:
        THostRecordMapImpl() = default;

        THostRecordMapImpl(TEvInterconnect::TEvNodesInfo *msg) {
            for (const TEvInterconnect::TNodeInfo& nodeInfo : msg->Nodes) {
                const THostId hostId(nodeInfo.Host, nodeInfo.Port);
                NodeIdToHostId.emplace(nodeInfo.NodeId, hostId);
                HostIdToRecord.emplace(hostId, nodeInfo);
                FqdnToNodeId.emplace(nodeInfo.Host, nodeInfo.NodeId);
            }
        }

        THostRecordMapImpl(const NKikimrBlobStorage::TStorageConfig& config) {
            for (const auto& item : config.GetAllNodes()) {
                const THostId hostId(item.GetHost(), item.GetPort());
                const TNodeId nodeId = item.GetNodeId();
                NodeIdToHostId.emplace(nodeId, hostId);
                HostIdToRecord.emplace(hostId, item);
                FqdnToNodeId.emplace(item.GetHost(), nodeId);
            }
        }

        const TNodeLocation& GetLocation(TNodeId nodeId) const {
            if (auto it = NodeIdToHostId.find(nodeId); it != NodeIdToHostId.end()) {
                if (auto hostIt = HostIdToRecord.find(it->second); hostIt != HostIdToRecord.end()) {
                    return hostIt->second.Location;
                }
            }
            Y_ABORT();
        }

        TMaybe<TNodeId> ResolveNodeId(const THostId& hostId) const {
            if (const auto it = HostIdToRecord.find(hostId); it != HostIdToRecord.end()) {
                return it->second.NodeId;
            } else {
                return {};
            }
        }

        TMaybe<THostId> GetHostId(TNodeId nodeId) const {
            if (const auto it = NodeIdToHostId.find(nodeId); it != NodeIdToHostId.end()) {
                return it->second;
            } else {
                return {};
            }
        }

        auto ResolveNodeId(const TString& fqdn) const {
            return FqdnToNodeId.equal_range(fqdn);
        }

        auto begin() const {
            return HostIdToRecord.begin();
        }

        auto end() const {
            return HostIdToRecord.end();
        }
    };

    using THostRecordMap = std::shared_ptr<THostRecordMapImpl>;

} // NBsController
} // NKikimr
