#include "distconf.h"
#include <contrib/libs/xxhash/xxhash.h>
#include <google/protobuf/util/message_differencer.h>

#include <array>

namespace NKikimr::NStorage {

    namespace {

        // apply the callback to every state storage, state storage board and scheme board config present in the storage
        // config; stop at the first error returned by the callback
        template<typename T>
        std::optional<TString> ForEachStateStorageConfig(NKikimrBlobStorage::TStorageConfig *config, T&& callback) {
            using TConfig = NKikimrBlobStorage::TStorageConfig;
            static constexpr std::array fields{
                std::make_pair(&TConfig::HasStateStorageConfig, &TConfig::MutableStateStorageConfig),
                std::make_pair(&TConfig::HasStateStorageBoardConfig, &TConfig::MutableStateStorageBoardConfig),
                std::make_pair(&TConfig::HasSchemeBoardConfig, &TConfig::MutableSchemeBoardConfig),
            };
            for (const auto& [has, mutableConfig] : fields) {
                if ((config->*has)()) {
                    if (std::optional<TString> error = callback((config->*mutableConfig)())) {
                        return error;
                    }
                }
            }
            return std::nullopt;
        }

    } // anonymous

    bool TDistributedConfigKeeper::UpdateBridgeConfig(NKikimrBlobStorage::TStorageConfig *config) {
        bool changes = false;

        if (config->HasClusterState()) {
            auto *clusterState = config->MutableClusterState();
            if (clusterState->GetPrimaryPile() != clusterState->GetPromotedPile()) {
                const auto promotedPileId = TBridgePileId::FromProto(clusterState, &NKikimrBridge::TClusterState::GetPromotedPile);
                if (promotedPileId.GetPileIndex() < clusterState->PerPileStateSize() &&
                        clusterState->GetPerPileState(promotedPileId.GetPileIndex()) == NKikimrBridge::TClusterState::SYNCHRONIZED) {
                    // TODO(alexvru): some external conditions? wait for external components?
                    promotedPileId.CopyToProto(clusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
                    changes = true;
                }
            }
        }

        if (config->HasClusterStateDetails()) {
            Y_DEBUG_ABORT_UNLESS(config->HasClusterState());
            auto *clusterState = config->MutableClusterState();

            // switch unsynced piles (NOT_SYNCHRONIZED_1) to NOT_SYNCHRONIZED_2 when connected
            for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                if (clusterState->GetPerPileState(i) == NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1 &&
                        ConnectedUnsyncedPiles.contains(TBridgePileId::FromPileIndex(i))) {
                    clusterState->SetPerPileState(i, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
                    changes = true;
                }
            }

            // persist pile registry of the cluster bridged before the registry was introduced; it is implied by
            // BridgeConfig, so pile ids stay the same
            if (auto *details = config->MutableClusterStateDetails(); !details->HasPileRegistry() && Cfg &&
                    Cfg->BridgeConfig) {
                auto registry = NBridge::MakeImpliedPileRegistry(*Cfg->BridgeConfig);
                if (!NBridge::ValidatePileRegistry(registry, *clusterState)) {
                    details->MutablePileRegistry()->Swap(&registry);
                    changes = true;
                }
            }
        }

        return changes;
    }

    std::optional<NKikimrBridge::TPileRegistry> GetPileRegistry(const NKikimrBlobStorage::TStorageConfig& config,
            const NKikimrConfig::TBridgeConfig *bridgeConfig) {
        if (!bridgeConfig) {
            return std::nullopt;
        } else if (const auto& details = config.GetClusterStateDetails(); details.HasPileRegistry()) {
            return details.GetPileRegistry();
        } else {
            return NBridge::MakeImpliedPileRegistry(*bridgeConfig);
        }
    }

    std::optional<TString> UpdatePileRegistry(const NKikimrConfig::TBridgeConfig& bridgeConfig,
            NKikimrBlobStorage::TStorageConfig *config, const NKikimrConfig::TBridgeConfig *localBridgeConfig) {
        if (!config->HasClusterState()) {
            return std::nullopt; // the cluster is not bridged one yet, pile ids are not persisted anywhere
        }
        const auto registry = GetPileRegistry(*config, localBridgeConfig);
        if (!registry) {
            return "this node runs without BridgeConfig while the cluster is bridged one";
        }
        auto makeError = [&](TStringBuf message) {
            TStringBuilder err;
            err << message << ", expected piles#";
            for (const auto& pile : registry->GetPiles()) {
                if (pile.HasName()) {
                    err << " '" << pile.GetName() << "'";
                }
            }
            return err;
        };
        auto next = NBridge::AssignPileIds(*registry, bridgeConfig);
        auto equals = google::protobuf::util::MessageDifferencer::Equals;
        if (!equals(next, *registry)) {
            return makeError("list of bridge piles can't be changed");
        } else if (!equals(NBridge::MakeImpliedPileRegistry(bridgeConfig), next)) {
            // nodes running older versions take pile ids from positions of piles in their BridgeConfig
            return makeError("order of bridge piles can't be changed");
        }
        config->MutableClusterStateDetails()->MutablePileRegistry()->Swap(&next);
        return std::nullopt;
    }

    void TDistributedConfigKeeper::GenerateBridgeInitialState(const TNodeWardenConfig& cfg,
            NKikimrBlobStorage::TStorageConfig *config) {
        if (!cfg.BridgeConfig) {
            return; // no bridge mode enabled at all
        } else if (config->HasClusterState()) {
            return; // some cluster state has been already defined
        }

        config->MutableClusterState()->CopyFrom(NBridge::MakeInitialClusterState(cfg.BridgeConfig->PilesSize()));
        *config->MutableClusterStateDetails()->MutablePileRegistry() = NBridge::MakeImpliedPileRegistry(*cfg.BridgeConfig);
    }

    std::optional<TString> TDistributedConfigKeeper::TransformConfigBeforeCommit(NKikimrBlobStorage::TStorageConfig *config) {
        if (auto error = CheckSynchronizedPiles(config)) { // can change cluster state
            return error;
        } else if (auto error = UpdateClusterStateGeneration(config)) { // transforms cluster state change into unsynced history
            return error;
        } else if (auto error = TrimHistoryEntries(config)) { // trims unsynced history entries
            return error;
        } else if (auto error = ExtractStateStorageConfig(config)) { // extract final state storage config
            return error;
        }
        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::CheckSynchronizedPiles(NKikimrBlobStorage::TStorageConfig *config) {
        if (config->HasClusterStateDetails()) {
            auto *pileSyncState = config->MutableClusterStateDetails()->MutablePileSyncState();
            for (int stateIndex = 0; stateIndex < pileSyncState->size(); ++stateIndex) {
                auto *state = pileSyncState->Mutable(stateIndex);
                const auto bridgePileId = TBridgePileId::FromProto(state, &std::decay_t<decltype(*state)>::GetBridgePileId);

                auto checkIfStaticGroupsSynced = [&] {
                    for (const auto& groupInfo : config->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                        if (!groupInfo.HasBridgeGroupState()) {
                            continue;
                        }
                        const auto& groupState = groupInfo.GetBridgeGroupState();
                        if (groupState.PileSize() <= bridgePileId.GetPileIndex()) {
                            Y_DEBUG_ABORT();
                            return false;
                        }
                        const auto& pile = groupState.GetPile(bridgePileId.GetPileIndex());
                        if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                            return false;
                        }
                    }
                    return true;
                };

                if (!state->GetUnsyncedBSC() && checkIfStaticGroupsSynced()) {
                    // fully synced, can switch to SYNCHRONIZED
                    pileSyncState->DeleteSubrange(stateIndex--, 1);
                    config->MutableClusterState()->SetPerPileState(bridgePileId.GetPileIndex(),
                        NKikimrBridge::TClusterState::SYNCHRONIZED);
                }
            }
        }

        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::UpdateClusterStateGeneration(NKikimrBlobStorage::TStorageConfig *config) {
        if (config->HasClusterState() && (!config->GetPrevConfig().GetGeneration() ||
                !config->GetPrevConfig().HasClusterState() || // conversion from non-bridged cluster
                !NBridge::IsSameClusterState(config->GetClusterState(), config->GetPrevConfig().GetClusterState()))) {
            // update cluster state generation if something has changed
            auto *clusterState = config->MutableClusterState();
            clusterState->SetGeneration(config->GetPrevConfig().GetClusterState().GetGeneration() + 1);
            UpdateClusterStateGuid(clusterState);

            auto *entry = config->MutableClusterStateDetails()->AddUnsyncedHistory();
            entry->MutableClusterState()->CopyFrom(*clusterState);
            for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                TBridgePileId::FromPileIndex(i).CopyToProto(entry, &std::decay_t<decltype(*entry)>::AddUnsyncedPiles);
            }
        }

        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::TrimHistoryEntries(NKikimrBlobStorage::TStorageConfig *config) {
        if (config->HasClusterStateDetails()) {
            auto *details = config->MutableClusterStateDetails();
            auto *history = details->MutableUnsyncedHistory();

            Y_DEBUG_ABORT_UNLESS(config->HasClusterState());
            const auto& clusterState = config->GetClusterState();

            // filter out unsynced piles from config (they become synced when they are the part of the quorum)
            int maxFullySyncedIndex = -1;
            for (int i = 0; i < history->size(); ++i) {
                auto *unsyncedPiles = history->Mutable(i)->MutableUnsyncedPiles();
                for (int j = 0; j < unsyncedPiles->size(); ++j) {
                    const auto unsyncedBridgePileId = TBridgePileId::FromProto(unsyncedPiles,
                        &std::decay_t<decltype(*unsyncedPiles)>::at, j);
                    if (clusterState.PerPileStateSize() <= unsyncedBridgePileId.GetPileIndex()) {
                        Y_DEBUG_ABORT(); // incorrect per pile state
                    } else if (NBridge::PileStateTraits(clusterState.GetPerPileState(
                            unsyncedBridgePileId.GetPileIndex())).RequiresConfigQuorum) {
                        // this pile is a part of quorum we expect to obtain, so it will be config-synced
                        unsyncedPiles->SwapElements(j--, unsyncedPiles->size() - 1);
                        unsyncedPiles->RemoveLast();
                    }
                }
                if (unsyncedPiles->empty()) {
                    maxFullySyncedIndex = i;
                }
            }
            if (maxFullySyncedIndex > 0) {
                history->DeleteSubrange(0, maxFullySyncedIndex);
            }
        }

        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::ExtractStateStorageConfig(NKikimrBlobStorage::TStorageConfig *config) {
        // copy bridge info into state storage configs for easier access in replica processors/proxies
        if (config->HasClusterState()) {
            return ForEachStateStorageConfig(config, [&](NKikimrConfig::TStateStorageConfig *pb) -> std::optional<TString> {
                auto& clusterState = config->GetClusterState();

                // copy cluster state generation
                pb->SetClusterStateGeneration(clusterState.GetGeneration());
                pb->SetClusterStateGuid(clusterState.GetGuid());

                if (!pb->RingGroupsSize() || pb->HasRing()) {
                    return "configuration has Ring field set or no RingGroups";
                }

                auto *groups = pb->MutableRingGroups();
                for (int i = 0, count = groups->size(); i < count; ++i) {
                    auto *group = groups->Mutable(i);
                    if (!group->HasBridgePileId()) {
                        return "bridge pile id is not set for a ring group";
                    } else if (const auto pileId = TBridgePileId::FromProto(group, &std::decay_t<decltype(*group)>::GetBridgePileId);
                            pileId.GetPileIndex() < clusterState.PerPileStateSize()) {
                        using T = NKikimrConfig::TStateStorageConfig;
                        std::optional<T::EPileState> state;
                        if (pileId == TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPrimaryPile)) {
                            state = pileId == TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPromotedPile)
                                ? T::PRIMARY
                                : T::DEMOTED;
                        } else if (pileId == TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPromotedPile)) {
                            state = T::PROMOTED;
                        } else {
                            switch (clusterState.GetPerPileState(pileId.GetPileIndex())) {
                                case NKikimrBridge::TClusterState::DISCONNECTED:
                                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                                    state = T::DISCONNECTED;
                                    break;

                                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                                    state = T::NOT_SYNCHRONIZED;
                                    break;

                                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                                case NKikimrBridge::TClusterState::SUSPENDED:
                                    state = T::SYNCHRONIZED;
                                    break;

                                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                                    Y_DEBUG_ABORT("unexpected value");
                            }
                        }
                        if (!state) {
                            return "can't determine correct pile state for ring group";
                        }
                        group->SetPileState(*state);
                    } else {
                        return "bridge pile id is out of bounds";
                    }
                }
                return std::nullopt;
            });
        }

        return std::nullopt;
    }

    std::optional<TString> ConvertStorageConfigToBridge(NKikimrBlobStorage::TStorageConfig *config,
            const NKikimrConfig::TBridgeConfig& bridgeConfig) {
        if (config->HasClusterState()) {
            return "cluster is already bridged one";
        } else if (bridgeConfig.PilesSize() != 1) {
            return "BridgeConfig must contain exactly one pile to convert cluster to bridged one";
        }

        // the cluster has the only pile which is the primary one; generation and history are filled in when committing;
        // the pile is the implicit one, as nodes and groups of the cluster have no pile settings yet
        config->MutableClusterState()->CopyFrom(NBridge::MakeInitialClusterState(1));
        auto *details = config->MutableClusterStateDetails();
        details->SetBscConversionPending(true);
        *details->MutablePileRegistry() = NBridge::MakeImpliedPileRegistry(bridgeConfig);

        // make every static group an inline pile of itself; this requires generation change to make proxies and VDisks
        // pick up new group info
        auto *ss = config->MutableBlobStorageConfig()->MutableServiceSet();
        THashMap<TGroupId, ui32> prevGenerations;
        for (auto& group : *ss->MutableGroups()) {
            if (group.HasBridgeGroupState() || group.HasBridgeProxyGroupId() || group.HasBridgePileId()) {
                return "static group already has bridge settings";
            }
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            prevGenerations.emplace(groupId, group.GetGroupGeneration());
            group.SetGroupGeneration(group.GetGroupGeneration() + 1);
            NBridge::MakeInlinePile(&group);
        }
        for (auto& vdisk : *ss->MutableVDisks()) {
            auto vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (const auto it = prevGenerations.find(vdiskId.GroupID); it != prevGenerations.end() &&
                    it->second == vdiskId.GroupGeneration) { // donors keep their generations
                ++vdiskId.GroupGeneration;
                VDiskIDFromVDiskID(vdiskId, vdisk.MutableVDiskID());
            }
        }

        // bridged cluster requires every ring group to be bound to some pile; legacy Ring is turned into the ring group
        // with the same replica actor ids
        return ForEachStateStorageConfig(config, [](NKikimrConfig::TStateStorageConfig *pb) -> std::optional<TString> {
            if (pb->HasRing()) {
                pb->AddRingGroups()->Swap(pb->MutableRing());
                pb->ClearRing();
            }
            for (auto& ringGroup : *pb->MutableRingGroups()) {
                TBridgePileId::FromPileIndex(0).CopyToProto(&ringGroup,
                    &NKikimrConfig::TStateStorageConfig::TRing::SetBridgePileId);
            }
            return std::nullopt;
        });
    }

    void UpdateClusterStateGuid(NKikimrBridge::TClusterState *clusterState) {
        TString buffer;
        const bool success = clusterState->SerializeToString(&buffer);
        Y_ABORT_UNLESS(success);
        clusterState->SetGuid(XXH3_64bits(buffer.data(), buffer.size()));
    }

} // NKikimr::NStorage
