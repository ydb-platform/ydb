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
            // number of piles differs when the list of piles changes
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

            // settings of the registry other than the list of piles (like PileListChanged) are kept as they are
            NKikimrBridge::TPileRegistry res = registry;
            res.ClearPiles();
            res.ClearImplicitPileId();
            THashSet<TString> placed;
            for (const auto& pile : registry.GetPiles()) {
                auto *slot = res.AddPiles();
                if (pile.HasName() && configured.contains(pile.GetName())) {
                    slot->CopyFrom(pile); // the pile keeps its slot along with its settings
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
            } else if (implicitPileId && clusterState.GetPerPileState(implicitPileId.GetPileIndex()) ==
                    NKikimrBridge::TClusterState::INACTIVE) {
                return "implicit pile is INACTIVE"; // nodes of implicit pile must be able to connect
            }
            const auto primary = TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPrimaryPile);
            const auto promoted = TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPromotedPile);
            THashSet<TString> names;
            for (size_t i = 0; i < registry.PilesSize(); ++i) {
                const auto& pile = registry.GetPiles(i);
                const auto pileId = TBridgePileId::FromPileIndex(i);
                const auto state = clusterState.GetPerPileState(i);
                if (pile.HasName()) {
                    if (!names.insert(pile.GetName()).second) {
                        return TStringBuilder() << "duplicate pile name in PileRegistry: '" << pile.GetName() << "'";
                    }
                } else if (i + 1 == registry.PilesSize()) {
                    return "PileRegistry ends with a free slot";
                } else if (pileId == primary || pileId == promoted || pileId == implicitPileId) {
                    return TStringBuilder() << "free slot of PileRegistry is the primary, promoted or implicit pile"
                        " PileIndex# " << i;
                } else if (state != NKikimrBridge::TClusterState::INACTIVE) {
                    return TStringBuilder() << "free slot of PileRegistry is in state "
                        << NKikimrBridge::TClusterState::EPileState_Name(state) << " PileIndex# " << i;
                }
            }
            return std::nullopt;
        }

        std::optional<TString> ApplyPileRegistryChange(const NKikimrBridge::TPileRegistry& current,
                const NKikimrBridge::TPileRegistry& next, NKikimrBridge::TClusterState *clusterState) {
            auto *states = clusterState->MutablePerPileState();
            if (current.PilesSize() != static_cast<size_t>(states->size())) {
                return "number of piles in PileRegistry doesn't match number of piles in ClusterState";
            }
            for (size_t i = 0; i < current.PilesSize(); ++i) {
                const auto& pile = current.GetPiles(i);
                if (i < next.PilesSize() && next.GetPiles(i).HasName() &&
                        next.GetPiles(i).GetName() == pile.GetName()) {
                    continue; // the pile keeps its slot
                } else if (pile.HasName() && states->Get(i) != NKikimrBridge::TClusterState::INACTIVE) {
                    return TStringBuilder() << "pile '" << pile.GetName() << "' in state "
                        << NKikimrBridge::TClusterState::EPileState_Name(states->Get(i))
                        << " can't be removed from BridgeConfig, only INACTIVE piles can";
                }
                states->Set(i, NKikimrBridge::TClusterState::INACTIVE); // pile is new to this slot, or slot is free
            }
            states->Resize(next.PilesSize(), NKikimrBridge::TClusterState::INACTIVE);
            return std::nullopt;
        }

        TBridgePileId GetInlinePileId() {
            return TBridgePileId::FromPileIndex(0);
        }

        void MakeInlinePileGroupState(NKikimrBridge::TGroupState *state, TGroupId groupId, ui32 generation,
                TBridgePileId bridgePileId) {
            state->Clear();
            auto *pile = AddGroupPile(state, bridgePileId); // index of the pile in the state is its pile id
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

        TBridgePileId GetAuthorityPileId(const NKikimrBridge::TGroupState& state) {
            if (const auto pileId = TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId)) {
                return pileId;
            }
            TBridgePileId res;
            size_t numParts = 0;
            ForEachGroupPile(state, [&](TBridgePileId pileId, const NKikimrBridge::TGroupState::TPile&) {
                res = pileId;
                ++numParts;
            });
            return numParts == 1 ? res : TBridgePileId();
        }

        NKikimrBridge::TGroupState::TPile *AddGroupPile(NKikimrBridge::TGroupState *state, TBridgePileId bridgePileId) {
            Y_ABORT_UNLESS(bridgePileId && !GetGroupPile(*state, bridgePileId));
            while (state->PileSize() <= bridgePileId.GetPileIndex()) {
                state->AddPile()->SetAbsent(true);
            }
            auto *pile = state->MutablePile(bridgePileId.GetPileIndex());
            pile->Clear();
            return pile;
        }

        void RemoveGroupPile(NKikimrBridge::TGroupState *state, TBridgePileId bridgePileId) {
            auto *pile = GetGroupPile(state, bridgePileId);
            Y_ABORT_UNLESS(pile);
            pile->Clear();
            pile->SetAbsent(true);
            while (state->PileSize() && state->GetPile(state->PileSize() - 1).GetAbsent()) {
                state->MutablePile()->RemoveLast();
            }
        }

    } // NBridge

} // NKikimr
