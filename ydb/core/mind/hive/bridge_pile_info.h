#include "hive.h"

namespace NKikimr::NHive {

struct TBridgePileInfo {
    TBridgePileId Id;
    NKikimrBridge::TClusterState::EPileState State = NKikimrBridge::TClusterState::DISCONNECTED;
    bool IsPrimary = false;
    bool IsPromoted = false;

    std::unordered_set<TNodeId> Nodes;

    TBridgePileInfo(const TBridgeInfo::TPile& wardenPileInfo)
        : Id(wardenPileInfo.BridgePileId)
        , State(wardenPileInfo.State)
        , IsPrimary(wardenPileInfo.IsPrimary)
        , IsPromoted(wardenPileInfo.IsBeingPromoted)
    {
    }

    TBridgePileInfo(TBridgePileId id) : Id(id) {}

    bool operator==(const TBridgePileInfo& other) {
        return Id == other.Id
            && State == other.State
            && IsPrimary == other.IsPrimary
            && IsPromoted == other.IsPromoted;
    }
};
} // namespace NKikimr::NHive
