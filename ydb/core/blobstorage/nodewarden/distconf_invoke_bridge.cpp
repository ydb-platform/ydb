#include "distconf_invoke.h"

#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    std::optional<TString> TInvokeRequestHandlerActor::ValidateSwitchBridgeClusterState(const NKikimrBridge::TClusterState& newClusterState) {
        if (!Self->Cfg->BridgeConfig) {
            return "Bridge mode is not enabled";
        }

        // check new config alone
        const ui32 numPiles = Self->Cfg->BridgeConfig->PilesSize();
        if (newClusterState.PerPileStateSize() != numPiles) {
            return "incorrect number of per-pile states in new config";
        } else if (newClusterState.GetPrimaryPile() >= numPiles) {
            return "incorrect primary pile";
        } else if (newClusterState.GetPromotedPile() >= numPiles) {
            return "incorrect promoted pile";
        } else if (newClusterState.GetPerPileState(newClusterState.GetPrimaryPile()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            return "incorrect primary pile state";
        } else if (newClusterState.GetPerPileState(newClusterState.GetPromotedPile()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            return "incorrect promoted pile state";
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
                            // invalid transition from any state
                            return "can't switch to SYNCHRONIZED directly";

                        case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED:
                            if (currentState == NKikimrBridge::TClusterState::SYNCHRONIZED) {
                                return "invalid transition from SYNCHRONIZED to NOT_SYNCHRONIZED";
                            }
                            break;

                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                            Y_ABORT();
                    }
                }
            }
            if (numDifferent > 1) {
                return "too many state changes in new configuration";
            }
            if (current.GetGeneration() + 1 != newClusterState.GetGeneration()) {
                return TStringBuilder() << "new cluster state generation# "
                    << newClusterState.GetGeneration() << " expected# " << current.GetGeneration() + 1;
            }
        }

        return std::nullopt; // no error
    }

    void TInvokeRequestHandlerActor::SwitchBridgeClusterState() {
        if (RunCommonChecks()) {
            StartProposition(&SwitchBridgeNewConfig.value());
        }
    }

    NKikimrBlobStorage::TStorageConfig TInvokeRequestHandlerActor::GetSwitchBridgeNewConfig(
            const NKikimrBridge::TClusterState& newClusterState) {
        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        config.SetGeneration(config.GetGeneration() + 1);
        auto *clusterState = config.MutableClusterState();
        size_t changedPileIndex = Max<size_t>();
        for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
            if (clusterState->GetPerPileState(i) != newClusterState.GetPerPileState(i)) {
                Y_ABORT_UNLESS(changedPileIndex == Max<size_t>());
                changedPileIndex = i;
            }
        }
        clusterState->CopyFrom(newClusterState);

        auto *history = config.MutableClusterStateHistory();
        auto *entry = history->AddUnsyncedEntries();
        entry->MutableClusterState()->CopyFrom(newClusterState);
        entry->SetOperationGuid(RandomNumber<ui64>());
        for (ui32 i = 0; i < Self->Cfg->BridgeConfig->PilesSize(); ++i) {
            entry->AddUnsyncedPiles(i); // all piles are unsynced by default
        }

        if (changedPileIndex == Max<size_t>()) {
            return config;
        }

        switch (clusterState->GetPerPileState(changedPileIndex)) {
            case NKikimrBridge::TClusterState::DISCONNECTED:
                // this pile is not disconnected, there is no reason to synchronize it anymore
                for (size_t i = 0; i < history->PileSyncStateSize(); ++i) {
                    const auto& item = history->GetPileSyncState(i);
                    if (item.GetBridgePileId() != changedPileIndex) {
                        break;
                    }
                    history->MutablePileSyncState()->DeleteSubrange(i, 1);
                    break;
                }
                break;

            case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED: {
                // we are coming into NOT_SYNCHRONIZED state; we have to build a list of all of our entities
                NKikimrBridge::TClusterStateHistory::TPileSyncState *state = nullptr;
                for (size_t i = 0; i < history->PileSyncStateSize(); ++i) {
                    state = history->MutablePileSyncState(i);
                    if (state->GetBridgePileId() == changedPileIndex) {
                        state->ClearUnsyncedGroupIds();
                        state->ClearUnsyncedBSC();
                        break;
                    } else {
                        state = nullptr;
                    }
                }
                if (!state) {
                    state = history->AddPileSyncState();
                    state->SetBridgePileId(changedPileIndex);
                }
                state->SetUnsyncedBSC(true);
                if (config.HasBlobStorageConfig()) {
                    if (const auto& bsConfig = config.GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
                        const auto& ss = bsConfig.GetServiceSet();
                        for (const auto& group : ss.GetGroups()) {
                            if (group.BridgeGroupIdsSize()) {
                                state->AddUnsyncedGroupIds(group.GetGroupID());
                            }
                        }
                    }
                }
                CheckSyncersAfterCommit = true;
                break;
            }

            case NKikimrBridge::TClusterState::SYNCHRONIZED:
                Y_ABORT("invalid transition");

            case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }

        return config;
    }

    void TInvokeRequestHandlerActor::NotifyBridgeSyncFinished(const TQuery::TNotifyBridgeSyncFinished& cmd) {
        if (!RunCommonChecks()) {
            return;
        } else if (!Self->Cfg->BridgeConfig) {
            return FinishWithError(TResult::ERROR, "Bridge mode is not enabled");
        } else if (Self->Cfg->BridgeConfig->PilesSize() <= cmd.GetBridgePileId()) {
            return FinishWithError(TResult::ERROR, "BridgePileId out of bounds");
        } else if (Self->StorageConfig->GetClusterState().GetGeneration() != cmd.GetGeneration()) {
            return FinishWithError(TResult::ERROR, "generation mismatch");
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        config.SetGeneration(config.GetGeneration() + 1);

        auto *clusterState = config.MutableClusterState();
        auto *history = config.MutableClusterStateHistory();

        size_t stateIndex;
        NKikimrBridge::TClusterStateHistory::TPileSyncState *state = nullptr;
        for (stateIndex = 0; stateIndex < history->PileSyncStateSize(); ++stateIndex) {
            state = history->MutablePileSyncState(stateIndex);
            if (state->GetBridgePileId() == cmd.GetBridgePileId()) {
                break;
            } else {
                state = nullptr;
            }
        }
        if (!state) {
            return FinishWithError(TResult::ERROR, "unsynced pile not found");
        }

        switch (cmd.GetStatus()) {
            case TQuery::TNotifyBridgeSyncFinished::Success:
                if (cmd.HasBSC()) {
                    if (!cmd.GetBSC()) {
                        return FinishWithError(TResult::ERROR, "incorrect request");
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
                }
                if (state->UnsyncedGroupIdsSize()) {
                    auto *groups = state->MutableUnsyncedGroupIds();
                    std::ranges::sort(*groups);
                    const auto [first, last] = std::ranges::unique(*groups);
                    groups->erase(first, last);
                }
                if (!state->GetUnsyncedBSC() && !state->UnsyncedGroupIdsSize()) {
                    // fully synced, can switch to SYNCHRONIZED
                    history->MutablePileSyncState()->DeleteSubrange(stateIndex, 1);
                    clusterState->SetPerPileState(cmd.GetBridgePileId(), NKikimrBridge::TClusterState::SYNCHRONIZED);
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
