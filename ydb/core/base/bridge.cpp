#include "bridge.h"

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
                return bridgeConfig.PilesSize() == 1 ? std::make_optional<size_t>(0) : std::nullopt;
            }
            for (size_t i = 0; i < bridgeConfig.PilesSize(); ++i) {
                if (bridgeConfig.GetPiles(i).GetName() == *pileName) {
                    return i;
                }
            }
            return std::nullopt;
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
