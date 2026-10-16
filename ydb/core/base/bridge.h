#pragma once

#include "appdata_fwd.h"
#include "defs.h"
#include "blobstorage_common.h"

#include <ydb/core/protos/bridge.pb.h>

#include <optional>

namespace NKikimrBlobStorage {
    class TGroupInfo;
}

namespace NKikimr {

    struct TBridgeInfo {
        struct TPile {
            TBridgePileId BridgePileId; // essentially an index into TBridgeInfo::Piles array
            TString Name; // textual name as written in config
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
        bool IsImplicit = false; // the cluster is not converted to bridged one yet, this is synthesized single-pile state

        using TPtr = std::shared_ptr<const TBridgeInfo>;

        TBridgeInfo() = default;
        TBridgeInfo(const TBridgeInfo&) = delete;
        TBridgeInfo(TBridgeInfo&&) = default;

        const TPile *GetPile(TBridgePileId bridgePileId) const {
            Y_ABORT_UNLESS(bridgePileId.GetPileIndex() < Piles.size());
            return &Piles[bridgePileId.GetPileIndex()];
        }

        const TPile *GetPileForNode(ui32 nodeId) const {
            const auto it = StaticNodeIdToPile.find(nodeId);
            return it != StaticNodeIdToPile.end() ? it->second : nullptr;
        }

        template<typename T>
        void ForEachPile(T&& callback) const {
            for (size_t i = 0; i < Piles.size(); ++i) {
                callback(TBridgePileId::FromPileIndex(i));
            }
        }
    };

    bool IsBridgeMode(const TActorContext &ctx);

    namespace NBridge {

        struct TPileStateTraits {
            const bool RequiresDataQuorum;
            const bool RequiresConfigQuorum;
            const bool AllowsConnection;
        };

        static constexpr TPileStateTraits PileStateTraits(NKikimrBridge::TClusterState::EPileState state) {
            switch (state) {                                               //  DQ     CQ     AC
                case NKikimrBridge::TClusterState::SYNCHRONIZED:       return {true,  true,  true };
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1: return {false, false, true };
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2: return {true,  true,  true };
                case NKikimrBridge::TClusterState::DISCONNECTED:       return {false, false, false};
                case NKikimrBridge::TClusterState::SUSPENDED:          return {false, true,  true };

                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
                    return {};
            }
        }

        bool IsSameClusterState(const NKikimrBridge::TClusterState& x, const NKikimrBridge::TClusterState& y);

        // Pile implied for nodes without pile name and groups without pile id: single-pile cluster (this is the case of
        // the cluster converted from non-bridged one) has them in its only pile, multi-pile one has no implicit pile.
        inline std::optional<size_t> GetImplicitPileIndex(size_t numPiles) {
            return numPiles == 1 ? std::make_optional<size_t>(0) : std::nullopt;
        }

        inline TBridgePileId GetImplicitPileId(const TBridgeInfo *bridgeInfo) {
            const auto index = bridgeInfo ? GetImplicitPileIndex(bridgeInfo->Piles.size()) : std::nullopt;
            return index ? TBridgePileId::FromPileIndex(*index) : TBridgePileId();
        }

        // Resolve pile index of a node by its (optional) pile name against the list of configured pile names; node
        // without pile name gets the implicit pile. Returns nullopt when pile can't be resolved.
        std::optional<size_t> ResolvePileIndex(const NKikimrConfig::TBridgeConfig& bridgeConfig,
            const std::optional<TString>& pileName);

        // Obtain pile in which group with designated pile id must be placed. Groups without pile id (which are not
        // bridged ones) are placed in the implicit pile; they can't be placed anywhere in multi-pile cluster.
        inline TBridgePileId GetPlacementPileId(TBridgePileId groupPileId, const TBridgeInfo *bridgeInfo) {
            return groupPileId ? groupPileId : GetImplicitPileId(bridgeInfo);
        }

        // Inline pile is the group serving as the only pile of its own (this is what groups of the cluster converted
        // from non-bridged one become): pile 0 of the group references the group itself with its current generation.
        void MakeInlinePileGroupState(NKikimrBridge::TGroupState *state, TGroupId groupId, ui32 generation);
        TBridgePileId GetInlinePileId();
        // make the group (with its id and generation already filled in) an inline pile
        void MakeInlinePile(NKikimrBlobStorage::TGroupInfo *group);

        // Generate implicit cluster state for the cluster with single pile that has not been converted yet.
        NKikimrBridge::TClusterState MakeImplicitSinglePileClusterState();

    }

} // NKikimr
