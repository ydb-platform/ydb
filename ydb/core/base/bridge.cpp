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
                && x.GetGeneration() == y.GetGeneration();
        }

    } // NBridge

} // NKikimr
