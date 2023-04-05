#pragma once

#include "defs.h"

namespace NKikimr::NCms {

struct TPDiskID {
    ui32 NodeId;
    ui32 DiskId;

    TPDiskID()
        : NodeId(0)
        , DiskId(0)
    {
    }

    TPDiskID(ui32 nodeId, ui32 diskId)
        : NodeId(nodeId)
        , DiskId(diskId)
    {
    }

    explicit operator bool() const {
        return NodeId;
    }

    bool operator==(const TPDiskID &other) const {
        return NodeId == other.NodeId && DiskId == other.DiskId;
    }

    bool operator!=(const TPDiskID &other) const {
        return !(*this == other);
    }

    bool operator<(const TPDiskID &other) const {
        if (NodeId != other.NodeId)
            return NodeId < other.NodeId;
        return DiskId < other.DiskId;
    }

    TString ToString() const {
        return Sprintf("%" PRIu32 ":%" PRIu32, NodeId, DiskId);
    }

    void Serialize(NKikimrCms::TPDiskID *rec) const {
        rec->SetNodeId(NodeId);
        rec->SetDiskId(DiskId);
    }
};

struct TPDiskIDHash {
    size_t operator()(const TPDiskID &id) const {
        return THash<intptr_t>()((ui64)id.NodeId + ((ui64)id.DiskId << 32));
    }
};

} // namespace NKikimr::NCms

Y_DECLARE_OUT_SPEC(inline, NKikimr::NCms::TPDiskID, o, x) {
    o << x.ToString();
}
