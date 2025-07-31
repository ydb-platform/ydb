#include "distconf_invoke.h"

#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::NeedBridgeMode() {
        if (!Self->Cfg->BridgeConfig) {
            throw TExError() << "Bridge mode is not enabled";
        }
    }

    void TInvokeRequestHandlerActor::SwitchBridgeClusterState(const TQuery::TSwitchBridgeClusterState& cmd) {
        NeedBridgeMode();

        RunCommonChecks(/*requireScepter=*/ false);

        const auto& newClusterState = cmd.GetNewClusterState();

        // check new config alone
        const ui32 numPiles = Self->Cfg->BridgeConfig->PilesSize();
        if (newClusterState.PerPileStateSize() != numPiles) {
            throw TExError() << "Incorrect number of per-pile states in new config";
        } else if (newClusterState.GetPrimaryPile() >= numPiles) {
            throw TExError() << "Incorrect primary pile";
        } else if (newClusterState.GetPromotedPile() >= numPiles) {
            throw TExError() << "Incorrect promoted pile";
        } else if (newClusterState.GetPerPileState(newClusterState.GetPrimaryPile()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            throw TExError() << "Incorrect primary pile state";
        } else if (newClusterState.GetPerPileState(newClusterState.GetPromotedPile()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            throw TExError() << "Incorrect promoted pile state";
        }

        if (Self->StorageConfig->HasClusterState()) {
            const NKikimrBridge::TClusterState& current = Self->StorageConfig->GetClusterState();
            Y_ABORT_UNLESS(current.PerPileStateSize() == numPiles);
            ui32 numDifferent = 0;
            for (ui32 i = 0; i < numPiles; ++i) {
                const auto currentState = current.GetPerPileState(i);
                const auto newState = newClusterState.GetPerPileState(i);
                if (currentState != newState) {
                    ++numDifferent;
                    switch (newState) {
                        case NKikimrBridge::TClusterState::DISCONNECTED:
                            // valid transition from any state
                            break;

                        case NKikimrBridge::TClusterState::SYNCHRONIZED:
                            throw TExError() << "Can't switch to SYNCHRONIZED directly";

                        case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                            if (currentState == NKikimrBridge::TClusterState::SYNCHRONIZED) {
                                throw TExError() << "Invalid transition from SYNCHRONIZED to NOT_SYNCHRONIZED_1";
                            }
                            break;

                        case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                            throw TExError() << "Can't switch to NOT_SYNCHRONIZED_2 directly";

                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                            Y_ABORT();
                    }
                }
            }
            if (numDifferent > 1) {
                throw TExError() << "Too many state changes in new configuration";
            } else if (current.GetGeneration() + 1 != newClusterState.GetGeneration()) {
                throw TExError() << "New cluster state generation# " << newClusterState.GetGeneration()
                    << " expected# " << current.GetGeneration() + 1;
            }
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto *clusterState = config.MutableClusterState();
        size_t changedPileIndex = Max<size_t>();
        for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
            if (clusterState->GetPerPileState(i) != newClusterState.GetPerPileState(i)) {
                Y_ABORT_UNLESS(changedPileIndex == Max<size_t>());
                changedPileIndex = i;
            }
        }
        clusterState->CopyFrom(newClusterState);

        auto *details = config.MutableClusterStateDetails();
        auto *entry = details->AddUnsyncedHistory();
        entry->MutableClusterState()->CopyFrom(newClusterState);
        entry->SetOperationGuid(RandomNumber<ui64>());
        for (ui32 i = 0; i < Self->Cfg->BridgeConfig->PilesSize(); ++i) {
            entry->AddUnsyncedPiles(i); // all piles are unsynced by default
        }

        if (changedPileIndex != Max<size_t>()) {
            for (size_t i = 0; i < details->PileSyncStateSize(); ++i) {
                if (const auto& state = details->GetPileSyncState(i); state.GetBridgePileId() == changedPileIndex) {
                    details->MutablePileSyncState()->DeleteSubrange(i, 1);
                    break;
                }
            }

            switch (clusterState->GetPerPileState(changedPileIndex)) {
                case NKikimrBridge::TClusterState::DISCONNECTED:
                    // this pile is not disconnected, there is no reason to synchronize it anymore
                    for (size_t i = 0; i < details->PileSyncStateSize(); ++i) {
                        const auto& item = details->GetPileSyncState(i);
                        if (item.GetBridgePileId() != changedPileIndex) {
                            break;
                        }
                        details->MutablePileSyncState()->DeleteSubrange(i, 1);
                        break;
                    }
                    break;

                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                    break;

                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                    Y_ABORT("invalid transition");

                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
            }
        }

        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::NotifyBridgeSyncFinished(const TQuery::TNotifyBridgeSyncFinished& cmd) {
        RunCommonChecks();
        if (!Self->Cfg->BridgeConfig) {
            throw TExError() << "Bridge mode is not enabled";
        } else if (Self->Cfg->BridgeConfig->PilesSize() <= cmd.GetBridgePileId()) {
            throw TExError() << "BridgePileId out of bounds";
        } else if (Self->StorageConfig->GetClusterState().GetGeneration() != cmd.GetGeneration()) {
            throw TExError() << "Generation mismatch";
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        auto *clusterState = config.MutableClusterState();
        auto *details = config.MutableClusterStateDetails();

        size_t stateIndex;
        NKikimrBridge::TClusterStateDetails::TPileSyncState *state = nullptr;
        for (stateIndex = 0; stateIndex < details->PileSyncStateSize(); ++stateIndex) {
            state = details->MutablePileSyncState(stateIndex);
            if (state->GetBridgePileId() == cmd.GetBridgePileId()) {
                break;
            } else {
                state = nullptr;
            }
        }
        if (!state) {
            throw TExError() << "Unsynced pile not found";
        }

        switch (cmd.GetStatus()) {
            case TQuery::TNotifyBridgeSyncFinished::Success:
                if (cmd.HasBSC()) {
                    if (!cmd.GetBSC()) {
                        throw TExError() << "Incorrect request";
                    }
                    state->SetUnsyncedBSC(false);
                }
                if (cmd.HasGroupId()) {
                    auto *groups = state->MutableUnsyncedGroupIds();
                    for (int i = 0; i < groups->size(); ++i) {
                        if (groups->at(i) == cmd.GetGroupId()) {
                            if (i != groups->size() - 1) {
                                groups->SwapElements(i, groups->size() - 1);
                            }
                            groups->RemoveLast();
                            break;
                        }
                    }
                }
                if (cmd.UnsyncedGroupIdsToAddSize()) {
                    const auto& v = cmd.GetUnsyncedGroupIdsToAdd();
                    state->MutableUnsyncedGroupIds()->Add(v.begin(), v.end());
                    CheckSyncersAfterCommit = true;
                }
                if (state->UnsyncedGroupIdsSize()) {
                    auto *groups = state->MutableUnsyncedGroupIds();
                    std::ranges::sort(*groups);
                    const auto [first, last] = std::ranges::unique(*groups);
                    groups->erase(first, last);
                }
                if (!state->GetUnsyncedBSC() && !state->UnsyncedGroupIdsSize()) {
                    // fully synced, can switch to SYNCHRONIZED
                    details->MutablePileSyncState()->DeleteSubrange(stateIndex, 1);
                    clusterState->SetPerPileState(cmd.GetBridgePileId(), NKikimrBridge::TClusterState::SYNCHRONIZED);
                    clusterState->SetGeneration(clusterState->GetGeneration() + 1);
                    auto *entry = details->AddUnsyncedHistory();
                    entry->MutableClusterState()->CopyFrom(*clusterState);
                    entry->SetOperationGuid(RandomNumber<ui64>());
                    for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                        entry->AddUnsyncedPiles(i);
                    }
                }
                break;

            case TQuery::TNotifyBridgeSyncFinished::TransientError:
                break;

            case TQuery::TNotifyBridgeSyncFinished::PermanentError: {
                bool found = false;
                for (const auto& err : state->GetPermanentErrorReasons()) {
                    if (err == cmd.GetErrorReason()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    state->AddPermanentErrorReasons(cmd.GetErrorReason());
                }
                break;
            }

            case NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot_TNotifyBridgeSyncFinished_EStatus_TEvNodeConfigInvokeOnRoot_TNotifyBridgeSyncFinished_EStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot_TNotifyBridgeSyncFinished_EStatus_TEvNodeConfigInvokeOnRoot_TNotifyBridgeSyncFinished_EStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }

        StartProposition(&config);
    }

} // NKikimr::NStorage
