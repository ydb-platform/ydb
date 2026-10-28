#pragma once

#include "appdata_fwd.h"
#include "defs.h"
#include "blobstorage_common.h"

#include <ydb/core/protos/bridge.pb.h>

#include <optional>
#include <utility>

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

        // the same as GetPile, but returns nullptr for the pile this bridge info doesn't know of (the newer one)
        const TPile *FindPile(TBridgePileId bridgePileId) const {
            return bridgePileId && bridgePileId.GetPileIndex() < Piles.size()
                ? &Piles[bridgePileId.GetPileIndex()]
                : nullptr;
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
            const bool AllowsConnectionWithinPile; // static nodes of the same pile may connect to each other
        };

        static constexpr TPileStateTraits PileStateTraits(NKikimrBridge::TClusterState::EPileState state) {
            switch (state) {                                               //  DQ     CQ     AC     ACWP
                case NKikimrBridge::TClusterState::SYNCHRONIZED:       return {true,  true,  true,  true };
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1: return {false, false, true,  true };
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2: return {true,  true,  true,  true };
                case NKikimrBridge::TClusterState::DISCONNECTED:       return {false, false, false, true };
                case NKikimrBridge::TClusterState::SUSPENDED:          return {false, true,  true,  true };
                case NKikimrBridge::TClusterState::INACTIVE:           return {false, false, false, false};

                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
                    return {};
            }
        }

        bool IsSameClusterState(const NKikimrBridge::TClusterState& x, const NKikimrBridge::TClusterState& y);

        // State of the pile in the cluster state; the pile not listed there is INACTIVE.
        inline NKikimrBridge::TClusterState::EPileState GetPileState(const NKikimrBridge::TClusterState& clusterState,
                TBridgePileId bridgePileId) {
            return bridgePileId && bridgePileId.GetPileIndex() < clusterState.PerPileStateSize()
                ? clusterState.GetPerPileState(bridgePileId.GetPileIndex())
                : NKikimrBridge::TClusterState::INACTIVE;
        }

        // Find the part of the bridged group in the designated pile; returns nullptr when the group has no part there
        // (the pile is INACTIVE).
        inline const NKikimrBridge::TGroupState::TPile *GetGroupPile(const NKikimrBridge::TGroupState& state,
                TBridgePileId bridgePileId) {
            if (!bridgePileId || state.PileSize() <= bridgePileId.GetPileIndex()) {
                return nullptr;
            }
            const auto& pile = state.GetPile(bridgePileId.GetPileIndex());
            return pile.GetAbsent() ? nullptr : &pile;
        }

        inline NKikimrBridge::TGroupState::TPile *GetGroupPile(NKikimrBridge::TGroupState *state,
                TBridgePileId bridgePileId) {
            return const_cast<NKikimrBridge::TGroupState::TPile*>(GetGroupPile(std::as_const(*state), bridgePileId));
        }

        // Invoke callback(bridgePileId, pile) for every part of the bridged group, in order of pile ids.
        template<typename TCallback>
        void ForEachGroupPile(const NKikimrBridge::TGroupState& state, TCallback&& callback) {
            for (size_t i = 0; i < state.PileSize(); ++i) {
                if (const auto& pile = state.GetPile(i); !pile.GetAbsent()) {
                    callback(TBridgePileId::FromPileIndex(i), pile);
                }
            }
        }

        template<typename TCallback>
        void ForEachGroupPile(NKikimrBridge::TGroupState *state, TCallback&& callback) {
            ForEachGroupPile(std::as_const(*state), [&](TBridgePileId bridgePileId,
                    const NKikimrBridge::TGroupState::TPile& pile) {
                callback(bridgePileId, const_cast<NKikimrBridge::TGroupState::TPile&>(pile));
            });
        }

        // Find the part of the bridged group satisfying predicate(bridgePileId, pile); returns nullptr if none does.
        template<typename TPredicate>
        const NKikimrBridge::TGroupState::TPile *FindGroupPile(const NKikimrBridge::TGroupState& state,
                TPredicate&& predicate) {
            for (size_t i = 0; i < state.PileSize(); ++i) {
                if (const auto& pile = state.GetPile(i); !pile.GetAbsent() &&
                        predicate(TBridgePileId::FromPileIndex(i), pile)) {
                    return &pile;
                }
            }
            return nullptr;
        }

        // Find the part of the bridged group served by the designated group.
        inline const NKikimrBridge::TGroupState::TPile *FindGroupPileByGroupId(const NKikimrBridge::TGroupState& state,
                TGroupId groupId) {
            return FindGroupPile(state, [&](TBridgePileId, const NKikimrBridge::TGroupState::TPile& pile) {
                return TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == groupId;
            });
        }

        inline NKikimrBridge::TGroupState::TPile *FindGroupPileByGroupId(NKikimrBridge::TGroupState *state,
                TGroupId groupId) {
            return const_cast<NKikimrBridge::TGroupState::TPile*>(FindGroupPileByGroupId(std::as_const(*state),
                groupId));
        }

        // Pile whose part of the bridged group is the only one sure to hold everything written to the group: the one
        // designated in TGroupState.AuthorityPileId, or the one of the only part of the group. Invalid pile id means
        // that parts in synchronized piles hold everything, as it is the case for the groups following pile states.
        TBridgePileId GetAuthorityPileId(const NKikimrBridge::TGroupState& state);

        // Pile of the operation changing the set of piles which is pending, or invalid pile id if there is none.
        inline TBridgePileId GetPendingPileOperationPileId(const NKikimrBridge::TClusterStateDetails& details) {
            return details.HasPendingPileOperation()
                ? TBridgePileId::FromProto(&details.GetPendingPileOperation(),
                    &NKikimrBridge::TClusterStateDetails::TPendingPileOperation::GetBridgePileId)
                : TBridgePileId();
        }

        // Make an entry for the part of the bridged group in the designated pile, which the group has no part in yet
        // (filling the gap with absent entries); returns the entry to fill in.
        NKikimrBridge::TGroupState::TPile *AddGroupPile(NKikimrBridge::TGroupState *state, TBridgePileId bridgePileId);

        // Drop the part of the bridged group in the designated pile (the pile is being removed); absent entries at the end
        // are not stored.
        void RemoveGroupPile(NKikimrBridge::TGroupState *state, TBridgePileId bridgePileId);

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
        // are trimmed. Implicit pile setting is kept only while its pile remains in the config, other settings (like
        // PileListChanged and RequiredFeatureLevel) are kept as they are.
        NKikimrBridge::TPileRegistry AssignPileIds(const NKikimrBridge::TPileRegistry& registry,
            const NKikimrConfig::TBridgeConfig& bridgeConfig);

        // Validate pile registry against the cluster state it accompanies. Free slot (the one without name) has no
        // nodes, so it can't be the primary, promoted or implicit pile, and it is INACTIVE. Empty name is a valid one,
        // as bridge config allows it.
        std::optional<TString> ValidatePileRegistry(const NKikimrBridge::TPileRegistry& registry,
            const NKikimrBridge::TClusterState& clusterState);

        // Bring the states of piles in line with the new pile registry (obtained by AssignPileIds from the current one):
        // piles keeping their slots keep their states, new piles and free slots are INACTIVE. Only INACTIVE piles can be
        // dropped; returns error otherwise.
        std::optional<TString> ApplyPileRegistryChange(const NKikimrBridge::TPileRegistry& current,
            const NKikimrBridge::TPileRegistry& next, NKikimrBridge::TClusterState *clusterState);

        // Obtain pile in which group with designated pile id must be placed. Groups without pile id (which are not
        // bridged ones) are placed in the implicit pile; they can't be placed anywhere in multi-pile cluster.
        inline TBridgePileId GetPlacementPileId(TBridgePileId groupPileId, const TBridgeInfo *bridgeInfo) {
            return groupPileId ? groupPileId : GetImplicitPileId(bridgeInfo);
        }

        // Inline pile is the group serving as the only pile of its own (this is what groups of the cluster converted
        // from non-bridged one become): pile 0 of the group references the group itself with its current generation.
        // Ordinary group of the multi-pile cluster becomes inline pile in the pile it resides in.
        TBridgePileId GetInlinePileId();
        void MakeInlinePileGroupState(NKikimrBridge::TGroupState *state, TGroupId groupId, ui32 generation,
            TBridgePileId bridgePileId = GetInlinePileId());
        // make the group (with its id and generation already filled in) an inline pile
        void MakeInlinePile(NKikimrBlobStorage::TGroupInfo *group);

        // Generate initial cluster state: every pile is synchronized, the first one is the primary. Single-pile state is
        // also the implicit one of the cluster that has not been converted to bridged one yet.
        NKikimrBridge::TClusterState MakeInitialClusterState(size_t numPiles);

    }

} // NKikimr
