#pragma once

#include "appdata_fwd.h"
#include "defs.h"
#include "blobstorage_common.h"

#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr {

    struct TBridgeInfo {
        struct TPile {
            TString Name;
            TBridgePileId BridgePileId; // essentially an index into TBridgeInfo::Piles array
            std::vector<ui32> StaticNodeIds; // a sorted set of static node ids belonging to this very pile
            NKikimrBridge::TClusterState::EPileState State = {}; // state of this pile
            bool IsPrimary = false; // is this pile selected as primary
            bool IsBeingPromoted = false; // is this pile being promoted right now (and not the primary, but in sync with one)
        };

        THashMap<ui32, const TPile*> StaticNodeIdToPile; // node to pile map for static nodes
        std::vector<TPile> Piles; // a vector of piles
        const TPile *SelfNodePile = nullptr; // a reference to pile this node belongs to
        const TPile *PrimaryPile = nullptr; // a reference to the primary pile
        const TPile *BeingPromotedPile = nullptr; // a reference to the pile being promoted, or nullptr if none are promoted

        using TPtr = std::shared_ptr<const TBridgeInfo>;

        TBridgeInfo() = default;
        TBridgeInfo(const TBridgeInfo&) = delete;
        TBridgeInfo(TBridgeInfo&&) = default;

        const TPile *GetPile(TBridgePileId bridgePileId) const {
            Y_ABORT_UNLESS(bridgePileId.GetRawId() < Piles.size());
            return &Piles[bridgePileId.GetRawId()];
        }

        const TPile *GetPileForNode(ui32 nodeId) const {
            const auto it = StaticNodeIdToPile.find(nodeId);
            return it != StaticNodeIdToPile.end() ? it->second : nullptr;
        }

        template<typename T>
        void ForEachPile(T&& callback) const {
            for (size_t i = 0; i < Piles.size(); ++i) {
                callback(TBridgePileId::FromValue(i));
            }
        }
    };

    bool IsBridgeMode(const TActorContext &ctx);

} // NKikimr
