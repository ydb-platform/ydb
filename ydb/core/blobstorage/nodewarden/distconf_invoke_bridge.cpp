#include "distconf_invoke.h"

#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::SwitchBridgeClusterState(const NKikimrBridge::TClusterState& newClusterState) {
        if (!RunCommonChecks()) {
            return;
        } else if (!Self->Cfg->BridgeConfig) {
            return FinishWithError(TResult::ERROR, "Bridge mode is not enabled");
        }

        // check new config alone
        const ui32 numPiles = Self->Cfg->BridgeConfig->PilesSize();
        if (newClusterState.PerPileStateSize() != numPiles) {
            return FinishWithError(TResult::ERROR, "incorrect number of per-pile states in new config");
        } else if (newClusterState.GetPrimaryPile() >= numPiles) {
            return FinishWithError(TResult::ERROR, "incorrect primary pile");
        } else if (newClusterState.GetPromotedPile() >= numPiles) {
            return FinishWithError(TResult::ERROR, "incorrect promoted pile");
        } else if (newClusterState.GetPerPileState(newClusterState.GetPrimaryPile()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            return FinishWithError(TResult::ERROR, "incorrect primary pile state");
        } else if (newClusterState.GetPerPileState(newClusterState.GetPromotedPile()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            return FinishWithError(TResult::ERROR, "incorrect promoted pile state");
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        if (config.HasClusterState()) {
            const NKikimrBridge::TClusterState& current = config.GetClusterState();
            Y_ABORT_UNLESS(current.PerPileStateSize() == numPiles);
            ui32 numDifferent = 0;
            for (ui32 i = 0; i < numPiles; ++i) {
                numDifferent += current.GetPerPileState(i) != newClusterState.GetPerPileState(i);
            }
            if (numDifferent > 1) {
                return FinishWithError(TResult::ERROR, "too many state changes in new configuration");
            }
        }

        config.SetGeneration(config.GetGeneration() + 1);
        config.MutableClusterState()->CopyFrom(newClusterState);
        StartProposition(&config);
    }

} // NKikimr::NStorage
