#include "bridge.h"

#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr {

    bool IsBridgeMode(const TActorContext &ctx) {
        return AppData(ctx)->BridgeModeEnabled;
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
            if (!pileName) {
                return GetImplicitPileIndex(bridgeConfig.PilesSize());
            }
            for (size_t i = 0; i < bridgeConfig.PilesSize(); ++i) {
                if (bridgeConfig.GetPiles(i).GetName() == *pileName) {
                    return i;
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

        NKikimrBridge::TClusterState MakeImplicitSinglePileClusterState() {
            NKikimrBridge::TClusterState state;
            state.AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
            TBridgePileId::FromPileIndex(0).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPrimaryPile);
            TBridgePileId::FromPileIndex(0).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPromotedPile);
            return state;
        }

    } // NBridge

} // NKikimr
