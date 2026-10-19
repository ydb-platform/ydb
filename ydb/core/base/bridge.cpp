#include "bridge.h"

#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NKikimr {

    bool IsBridgeMode(const TActorContext &ctx) {
        return AppData(ctx)->BridgeModeEnabled;
    }

    const TBridgeInfo::TPile *TBridgeInfo::ResolvePile(const std::optional<TString>& pileName) const {
        if (!pileName) {
            return ImplicitPile;
        }
        for (const TPile& pile : Piles) {
            if (!pile.IsFreeSlot && pile.Name == *pileName) {
                return &pile;
            }
        }
        return nullptr;
    }

    namespace NBridge {

        bool IsSameClusterState(const NKikimrBridge::TClusterState& x, const NKikimrBridge::TClusterState& y) {
            Y_DEBUG_ABORT_UNLESS(x.PerPileStateSize() == y.PerPileStateSize());
            return x.PerPileStateSize() == y.PerPileStateSize()
                && std::ranges::equal(x.GetPerPileState(), y.GetPerPileState())
                && x.GetPrimaryPile() == y.GetPrimaryPile()
                && x.GetPromotedPile() == y.GetPromotedPile()
                && x.GetGeneration() == y.GetGeneration()
                && x.GetGuid() == y.GetGuid();
        }

        std::optional<size_t> ResolvePileIndex(const NKikimrConfig::TBridgeConfig& bridgeConfig,
                const std::optional<TString>& pileName) {
            const auto bridgePileId = ResolvePileId(MakeImpliedPileRegistry(bridgeConfig), pileName);
            return bridgePileId ? std::make_optional(bridgePileId.GetPileIndex()) : std::nullopt;
        }

        NKikimrBridge::TPileRegistry MakeImpliedPileRegistry(const NKikimrConfig::TBridgeConfig& bridgeConfig) {
            NKikimrBridge::TPileRegistry registry;
            for (const auto& pile : bridgeConfig.GetPiles()) {
                registry.AddPiles()->SetName(pile.GetName());
            }
            if (const auto index = GetImplicitPileIndex(bridgeConfig.PilesSize())) {
                TBridgePileId::FromPileIndex(*index).CopyToProto(&registry,
                    &NKikimrBridge::TPileRegistry::SetImplicitPileId);
            }
            return registry;
        }

        TBridgePileId ResolvePileId(const NKikimrBridge::TPileRegistry& registry, const std::optional<TString>& pileName) {
            if (!pileName) {
                return TBridgePileId::FromProto(&registry, &NKikimrBridge::TPileRegistry::GetImplicitPileId);
            }
            for (size_t i = 0; i < registry.PilesSize(); ++i) {
                if (const auto& pile = registry.GetPiles(i); pile.HasName() && pile.GetName() == *pileName) {
                    return TBridgePileId::FromPileIndex(i);
                }
            }
            return TBridgePileId();
        }

        NKikimrBridge::TPileRegistry AssignPileIds(const NKikimrBridge::TPileRegistry& registry,
                const NKikimrConfig::TBridgeConfig& bridgeConfig) {
            THashSet<TString> configured;
            for (const auto& pile : bridgeConfig.GetPiles()) {
                configured.insert(pile.GetName());
            }

            NKikimrBridge::TPileRegistry res;
            THashSet<TString> placed;
            for (const auto& pile : registry.GetPiles()) {
                auto *slot = res.AddPiles();
                if (pile.HasName() && configured.contains(pile.GetName())) {
                    slot->SetName(pile.GetName());
                    placed.insert(pile.GetName());
                }
            }

            size_t freeIndex = 0;
            for (const auto& pile : bridgeConfig.GetPiles()) {
                if (!placed.insert(pile.GetName()).second) {
                    continue; // this pile already has its slot
                }
                while (freeIndex < res.PilesSize() && res.GetPiles(freeIndex).HasName()) {
                    ++freeIndex;
                }
                auto *slot = freeIndex < res.PilesSize() ? res.MutablePiles(freeIndex) : res.AddPiles();
                slot->SetName(pile.GetName());
            }

            while (res.PilesSize() && !res.GetPiles(res.PilesSize() - 1).HasName()) {
                res.MutablePiles()->RemoveLast();
            }

            // implicit pile keeps its slot as long as it remains in the config
            if (const auto implicitPileId = TBridgePileId::FromProto(&registry,
                    &NKikimrBridge::TPileRegistry::GetImplicitPileId);
                    implicitPileId && implicitPileId.GetPileIndex() < registry.PilesSize()) {
                const auto& pile = registry.GetPiles(implicitPileId.GetPileIndex());
                if (pile.HasName() && configured.contains(pile.GetName())) {
                    res.SetImplicitPileId(registry.GetImplicitPileId());
                }
            }

            return res;
        }

        std::optional<TString> ValidatePileRegistry(const NKikimrBridge::TPileRegistry& registry,
                const NKikimrBridge::TClusterState& clusterState) {
            if (registry.PilesSize() != clusterState.PerPileStateSize()) {
                return TStringBuilder() << "number of piles in PileRegistry (" << registry.PilesSize()
                    << ") doesn't match number of piles in ClusterState (" << clusterState.PerPileStateSize() << ')';
            }
            const auto implicitPileId = TBridgePileId::FromProto(&registry,
                &NKikimrBridge::TPileRegistry::GetImplicitPileId);
            if (implicitPileId && registry.PilesSize() <= implicitPileId.GetPileIndex()) {
                return "ImplicitPileId in PileRegistry is out of range";
            }
            const auto primary = TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPrimaryPile);
            const auto promoted = TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPromotedPile);
            THashSet<TString> names;
            for (size_t i = 0; i < registry.PilesSize(); ++i) {
                const auto& pile = registry.GetPiles(i);
                const auto pileId = TBridgePileId::FromPileIndex(i);
                const auto state = clusterState.GetPerPileState(i);
                const auto traits = PileStateTraits(state);
                if (pile.HasName()) {
                    if (!names.insert(pile.GetName()).second) {
                        return TStringBuilder() << "duplicate pile name in PileRegistry: '" << pile.GetName() << "'";
                    }
                } else if (i + 1 == registry.PilesSize()) {
                    return "PileRegistry ends with a free slot";
                } else if (pileId == primary || pileId == promoted || pileId == implicitPileId) {
                    return TStringBuilder() << "free slot of PileRegistry is the primary, promoted or implicit pile"
                        " PileIndex# " << i;
                } else if (traits.RequiresDataQuorum || traits.RequiresConfigQuorum || traits.AllowsConnection) {
                    return TStringBuilder() << "free slot of PileRegistry is in state "
                        << NKikimrBridge::TClusterState::EPileState_Name(state) << " PileIndex# " << i;
                }
            }
            return std::nullopt;
        }

        TBridgePileId GetInlinePileId() {
            return TBridgePileId::FromPileIndex(0);
        }

        void MakeInlinePileGroupState(NKikimrBridge::TGroupState *state, TGroupId groupId, ui32 generation) {
            state->Clear();
            auto *pile = state->AddPile(); // index of the pile in the state is its pile id, see GetInlinePileId()
            groupId.CopyToProto(pile, &NKikimrBridge::TGroupState::TPile::SetGroupId);
            pile->SetGroupGeneration(generation);
            pile->SetStage(NKikimrBridge::TGroupState::SYNCED);
        }

        void MakeInlinePile(NKikimrBlobStorage::TGroupInfo *group) {
            const auto groupId = TGroupId::FromProto(group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            MakeInlinePileGroupState(group->MutableBridgeGroupState(), groupId, group->GetGroupGeneration());
            groupId.CopyToProto(group, &NKikimrBlobStorage::TGroupInfo::SetBridgeProxyGroupId);
            GetInlinePileId().CopyToProto(group, &NKikimrBlobStorage::TGroupInfo::SetBridgePileId);
        }

        NKikimrBridge::TClusterState MakeInitialClusterState(size_t numPiles) {
            NKikimrBridge::TClusterState state;
            for (size_t i = 0; i < numPiles; ++i) {
                state.AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
            }
            TBridgePileId::FromPileIndex(0).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPrimaryPile);
            TBridgePileId::FromPileIndex(0).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPromotedPile);
            return state;
        }

    } // NBridge

} // NKikimr
