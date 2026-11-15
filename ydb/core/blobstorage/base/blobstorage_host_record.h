#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/library/actors/core/interconnect.h>
#include <util/generic/hash_multi_map.h>

namespace NKikimrBlobStorage {
    class TNodeIdentifier;
    class TStorageConfig;
}

namespace NKikimr {
namespace NBsController {

    using TNodeId = ui32;

    struct THostRecord {
        TNodeId NodeId;
        TNodeLocation Location;

        THostRecord(const TEvInterconnect::TNodeInfo& nodeInfo);

        THostRecord(const NKikimrBlobStorage::TNodeIdentifier& node);
    };

    class THostRecordMapImpl {
        using THostId = std::tuple<TString, i32>;

        THashMap<THostId, THostRecord> HostIdToRecord;
        THashMap<TNodeId, THostId> NodeIdToHostId;
        THashMultiMap<TString, TNodeId> FqdnToNodeId;

    public:
        THostRecordMapImpl() = default;

        THostRecordMapImpl(TEvInterconnect::TEvNodesInfo *msg);

        THostRecordMapImpl(const NKikimrBlobStorage::TStorageConfig& config);

        const TNodeLocation& GetLocation(TNodeId nodeId) const;

        TMaybe<TNodeId> ResolveNodeId(const THostId& hostId) const;

        TMaybe<THostId> GetHostId(TNodeId nodeId) const;

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
