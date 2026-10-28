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
            TString Name; // textual name as written in config, may be empty
            bool IsFreeSlot = false; // slot of pile registry not taken by any pile; such pile has no name and no nodes
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
        const TPile *ImplicitPile = nullptr; // pile of nodes without pile name and of groups without pile id, if any
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

        // find pile of a node by its (optional) pile name; node without pile name belongs to the implicit pile
        const TPile *ResolvePile(const std::optional<TString>& pileName) const;

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

        // Pile implied for nodes without pile name and groups without pile id by the pile registry implied by bridge
        // config: single-pile cluster (this is the case of the cluster converted from non-bridged one) has them in its
        // only pile, multi-pile one has no implicit pile.
        inline std::optional<size_t> GetImplicitPileIndex(size_t numPiles) {
            return numPiles == 1 ? std::make_optional<size_t>(0) : std::nullopt;
        }

        inline TBridgePileId GetImplicitPileId(const TBridgeInfo *bridgeInfo) {
            return bridgeInfo && bridgeInfo->ImplicitPile ? bridgeInfo->ImplicitPile->BridgePileId : TBridgePileId();
        }

        // Resolve pile index of a node by its (optional) pile name against the list of configured pile names; node
        // without pile name gets the implicit pile. Returns nullopt when pile can't be resolved. Pile indices obtained
        // this way match pile ids only while the pile registry is the one implied by bridge config; use it only when
        // there is no storage config (and thus no registry) available yet.
        std::optional<size_t> ResolvePileIndex(const NKikimrConfig::TBridgeConfig& bridgeConfig,
            const std::optional<TString>& pileName);

        // Pile registry implied by bridge config (initial one, and the one of the cluster bridged before the registry
        // was introduced): piles get their ids in config order, the only pile (if there is a single one) is implicit.
        NKikimrBridge::TPileRegistry MakeImpliedPileRegistry(const NKikimrConfig::TBridgeConfig& bridgeConfig);

        // Resolve pile id of a node by its (optional) pile name against pile registry; node without pile name gets
        // the implicit pile. Returns invalid pile id when pile can't be resolved.
        TBridgePileId ResolvePileId(const NKikimrBridge::TPileRegistry& registry, const std::optional<TString>& pileName);

        // Pile registry for the new bridge config: piles remaining in the config keep their ids, slots of the dropped
        // ones get free, new piles take free slots in ascending order before the registry grows, free slots at the end
        // are trimmed. Implicit pile setting is kept only while its pile remains in the config.
        NKikimrBridge::TPileRegistry AssignPileIds(const NKikimrBridge::TPileRegistry& registry,
            const NKikimrConfig::TBridgeConfig& bridgeConfig);

        // Validate pile registry against the cluster state it accompanies. Free slot (the one without name) has no
        // nodes, so it can't be the primary, promoted or implicit pile, and it must be in the state that takes no part
        // in quorums. Empty name is a valid one, as bridge config allows it.
        std::optional<TString> ValidatePileRegistry(const NKikimrBridge::TPileRegistry& registry,
            const NKikimrBridge::TClusterState& clusterState);

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

        // Generate initial cluster state: every pile is synchronized, the first one is the primary. Single-pile state is
        // also the implicit one of the cluster that has not been converted to bridged one yet.
        NKikimrBridge::TClusterState MakeInitialClusterState(size_t numPiles);

    }

} // NKikimr
