#include "distconf.h"
#include "distconf_invoke.h"
#include <contrib/libs/xxhash/xxhash.h>
#include <google/protobuf/util/message_differencer.h>

#include <array>

namespace NKikimr::NStorage {

    void UpdateStaticVDiskGenerations(NKikimrBlobStorage::TNodeWardenServiceSet *ss,
            const THashMap<TGroupId, ui32>& newGenerations) {
        for (auto& vdisk : *ss->MutableVDisks()) {
            auto vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (const auto it = newGenerations.find(vdiskId.GroupID); it != newGenerations.end() &&
                    it->second == vdiskId.GroupGeneration + 1) {
                vdiskId.GroupGeneration = it->second;
                VDiskIDFromVDiskID(vdiskId, vdisk.MutableVDiskID());
            }
        }
    }

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

        // bump the generation of the static bridge proxy group along with its inline pile, if it has one; the new
        // generation of the inline pile is recorded to make its VDisks follow it
        void BumpBridgeProxyGroupGeneration(NKikimrBlobStorage::TGroupInfo *group, THashMap<TGroupId, ui32>& bumped) {
            const auto groupId = TGroupId::FromProto(group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            group->SetGroupGeneration(group->GetGroupGeneration() + 1);
            if (auto *inlinePile = NBridge::FindGroupPileByGroupId(group->MutableBridgeGroupState(), groupId)) {
                inlinePile->SetGroupGeneration(group->GetGroupGeneration());
                bumped.emplace(groupId, group->GetGroupGeneration());
            }
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

            // switch unsynced piles (NOT_SYNCHRONIZED_1) to NOT_SYNCHRONIZED_2 when connected; this is the only change of
            // pile state making the pile take part in data quorum, and writers which don't know it yet skip the pile, so
            // it gets synced from the cluster state it happens in, with groups of the pile having new generations: the
            // ones routed by older group state are refused by VDisks, and the newer group state requires cluster state
            // this new to be served with
            std::vector<TBridgePileId> switchedPileIds;
            for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                if (clusterState->GetPerPileState(i) == NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1 &&
                        ConnectedUnsyncedPiles.contains(TBridgePileId::FromPileIndex(i))) {
                    clusterState->SetPerPileState(i, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
                    switchedPileIds.push_back(TBridgePileId::FromPileIndex(i));
                    changes = true;
                }
            }
            if (!switchedPileIds.empty()) {
                // the generation the cluster state gets on commit
                const ui64 generation = clusterState->GetGeneration() + 1;
                for (auto& pss : *config->MutableClusterStateDetails()->MutablePileSyncState()) {
                    const auto pileId = TBridgePileId::FromProto(&pss,
                        &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId);
                    if (std::ranges::find(switchedPileIds, pileId) != switchedPileIds.end()) {
                        pss.SetBecameUnsyncedGeneration(generation); // BSC resets parts of its groups in the pile
                    }
                }
                ResetStaticGroupPileParts(config, switchedPileIds, generation);
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
            NKikimrBlobStorage::TStorageConfig *config, const NKikimrConfig::TBridgeConfig *localBridgeConfig,
            const NKikimrConfig::TBridgeConfig *currentBridgeConfig, bool *pileListChanged) {
        *pileListChanged = false;
        if (!config->HasClusterState()) {
            return std::nullopt; // the cluster is not bridged one yet, pile ids are not persisted anywhere
        }
        const auto registry = GetPileRegistry(*config, localBridgeConfig);
        if (!registry) {
            return "this node runs without BridgeConfig while the cluster is bridged one";
        } else if (!bridgeConfig.PilesSize()) {
            return "bridged cluster can't run without bridge config";
        }
        auto next = NBridge::AssignPileIds(*registry, bridgeConfig);
        auto equals = google::protobuf::util::MessageDifferencer::Equals;
        // nodes running older versions take pile ids from positions of piles in their BridgeConfig, so they can't run
        // with the list of piles changed or reordered; the change is the one of pile ids, or of positions of piles
        // (against the ones in the current config, or the ones matching pile ids when it is unknown)
        const auto implied = NBridge::MakeImpliedPileRegistry(bridgeConfig);
        auto positions = next; // flags of the registry and settings of piles are not a matter of positions
        positions.ClearPileListChanged();
        positions.ClearRequiredFeatureLevel();
        for (auto& pile : *positions.MutablePiles()) {
            pile.ClearAddedGeneration();
        }
        *pileListChanged = !equals(next, *registry) || !equals(implied, currentBridgeConfig
            ? NBridge::MakeImpliedPileRegistry(*currentBridgeConfig)
            : positions);
        if (*pileListChanged && config->GetClusterStateDetails().GetBscConversionPending()) {
            return "list of bridge piles can't be changed until BSC finishes conversion of the cluster";
        } else if (auto error = NBridge::ApplyPileRegistryChange(*registry, next, config->MutableClusterState())) {
            return error;
        }
        if (*pileListChanged) {
            next.SetPileListChanged(true); // nodes taking pile ids from BridgeConfig positions can't work anymore
        }
        auto *details = config->MutableClusterStateDetails();
        if (details->GetNonUniformStorageEnabled() && !bridgeConfig.GetEnableNonUniformStorage()) {
            return "non-uniform storage activation can't be disabled";
        }
        if (bridgeConfig.GetEnableNonUniformStorage()) {
            if (details->GetBscConversionPending()) {
                return "non-uniform storage requires BSC conversion to complete";
            }
            details->SetNonUniformStorageEnabled(true);
            next.SetRequiredFeatureLevel(Max<ui32>(next.GetRequiredFeatureLevel(), BridgeFeatureLevelNonUniformStorage));
        }
        if (config->GetBlobStorageConfig().StoragePoolPileOverridesSize()) {
            next.SetRequiredFeatureLevel(Max<ui32>(next.GetRequiredFeatureLevel(), BridgeFeatureLevelPileOverrides));
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
        if (config->GetBlobStorageConfig().StoragePoolPileOverridesSize()) {
            config->MutableClusterStateDetails()->MutablePileRegistry()->SetRequiredFeatureLevel(BridgeFeatureLevelPileOverrides);
        }
        if (cfg.BridgeConfig->GetEnableNonUniformStorage()) {
            auto *details = config->MutableClusterStateDetails();
            details->SetNonUniformStorageEnabled(true);
            details->MutablePileRegistry()->SetRequiredFeatureLevel(Max<ui32>(
                details->GetPileRegistry().GetRequiredFeatureLevel(), BridgeFeatureLevelNonUniformStorage));
        }
    }

    std::optional<TString> TDistributedConfigKeeper::TransformConfigBeforeCommit(NKikimrBlobStorage::TStorageConfig *config) {
        if (auto error = CheckSynchronizedPiles(config)) { // can change cluster state
            return error;
        } else if (auto error = UpdateClusterStateGeneration(config)) { // transforms cluster state change into unsynced history
            return error;
        } else if (auto error = TrimHistoryEntries(config)) { // trims unsynced history entries
            return error;
        } else if (auto error = UpdateBridgeGroupStateGenerations(config)) { // needs final cluster state generation
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
                        const auto *pile = NBridge::GetGroupPile(groupInfo.GetBridgeGroupState(), bridgePileId);
                        if (!pile) {
                            Y_DEBUG_ABORT(); // pile being synced has its part in every bridged group
                            return false;
                        } else if (pile->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                            return false;
                        }
                    }
                    return true;
                };

                if (!state->GetUnsyncedBSC() && config->GetClusterState().GetPerPileState(bridgePileId.GetPileIndex())
                        == NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2 && checkIfStaticGroupsSynced()) {
                    // fully synced, can switch to SYNCHRONIZED; only the pile going through its sync
                    // (NOT_SYNCHRONIZED_2) finishes it, the one waiting to start (NOT_SYNCHRONIZED_1) may have parts
                    // reported synced by BSC for their previous sync, which are going to be reset
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
                if (clusterState->GetPerPileState(i) != NKikimrBridge::TClusterState::INACTIVE) {
                    // INACTIVE pile has no state to sync, it gets the whole config when it becomes part of the cluster
                    TBridgePileId::FromPileIndex(i).CopyToProto(entry,
                        &std::decay_t<decltype(*entry)>::AddUnsyncedPiles);
                }
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
                    const auto state = NBridge::GetPileState(clusterState, unsyncedBridgePileId);
                    if (state == NKikimrBridge::TClusterState::INACTIVE ||
                            NBridge::PileStateTraits(state).RequiresConfigQuorum) {
                        // this pile is a part of quorum we expect to obtain, so it will be config-synced, or it is not
                        // a part of the cluster anymore and it won't ever need this history
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

    std::optional<TString> TDistributedConfigKeeper::UpdateBridgeGroupStateGenerations(
            NKikimrBlobStorage::TStorageConfig *config) {
        if (!config->HasClusterState()) {
            return std::nullopt;
        }
        THashMap<TGroupId, ui32> prevGenerations;
        for (const auto& group : config->GetPrevConfig().GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            prevGenerations.emplace(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID),
                group.GetGroupGeneration());
        }
        // the state of bridged static group that has been changed (along with its generation) is made for the cluster
        // state being committed; proxies won't serve the group with older one
        const ui64 generation = config->GetClusterState().GetGeneration();
        for (auto& group : *config->MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups()) {
            if (!group.HasBridgeGroupState()) {
                continue;
            }
            const auto it = prevGenerations.find(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID));
            if (it == prevGenerations.end() || it->second != group.GetGroupGeneration()) {
                auto *state = group.MutableBridgeGroupState();
                state->SetClusterStateGeneration(Max(state->GetClusterStateGeneration(), generation));
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

                                case NKikimrBridge::TClusterState::INACTIVE:
                                    break; // INACTIVE pile can't have ring groups

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
            const NKikimrConfig::TBridgeConfig& bridgeConfig, bool skipStoragePoolConversion) {
        if (config->HasClusterState()) {
            return "cluster is already bridged one";
        } else if (bridgeConfig.PilesSize() != 1) {
            return "BridgeConfig must contain exactly one pile to convert cluster to bridged one";
        } else if (skipStoragePoolConversion && !bridgeConfig.GetEnableNonUniformStorage()) {
            return "skipping storage pool conversion requires bridge_config.enable_non_uniform_storage";
        } else if (!skipStoragePoolConversion && bridgeConfig.GetEnableNonUniformStorage()) {
            return "enable non-uniform storage only after BSC conversion completes";
        }

        // the cluster has the only pile which is the primary one; generation and history are filled in when committing;
        // the pile is the implicit one, as nodes and groups of the cluster have no pile settings yet
        config->MutableClusterState()->CopyFrom(NBridge::MakeInitialClusterState(1));
        auto *details = config->MutableClusterStateDetails();
        if (skipStoragePoolConversion) {
            // BSC has nothing to convert: its pools and groups stay non-bridged in the cluster supporting both kinds
            details->SetNonUniformStorageEnabled(true);
        } else {
            details->SetBscConversionPending(true);
        }
        *details->MutablePileRegistry() = NBridge::MakeImpliedPileRegistry(bridgeConfig);
        // nodes of older versions don't handle inline piles (their VDisks abort, their bridge proxies loop), so they
        // must neither get the converted config nor connect to the ones having it
        details->MutablePileRegistry()->SetRequiredFeatureLevel(BridgeFeatureLevel);

        // make every static group an inline pile of itself; this requires generation change to make proxies and VDisks
        // pick up new group info
        auto *ss = config->MutableBlobStorageConfig()->MutableServiceSet();
        THashMap<TGroupId, ui32> newGenerations;
        for (auto& group : *ss->MutableGroups()) {
            if (group.HasBridgeGroupState() || group.HasBridgeProxyGroupId() || group.HasBridgePileId()) {
                return "static group already has bridge settings";
            }
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            group.SetGroupGeneration(group.GetGroupGeneration() + 1);
            newGenerations.emplace(groupId, group.GetGroupGeneration());
            NBridge::MakeInlinePile(&group);
        }
        UpdateStaticVDiskGenerations(ss, newGenerations);

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

    std::optional<TString> TDistributedConfigKeeper::AddBridgePileToConfig(NKikimrBlobStorage::TStorageConfig *config,
            TBridgePileId pileId, const NKikimrBlobStorage::TBaseConfig *baseConfig) {
        auto *clusterState = config->MutableClusterState();
        if (NBridge::GetPileState(*clusterState, pileId) != NKikimrBridge::TClusterState::INACTIVE) {
            return "pile is not INACTIVE";
        }
        clusterState->SetPerPileState(pileId.GetPileIndex(), NKikimrBridge::TClusterState::DISCONNECTED);
        // the generation the cluster state gets on commit, the pile becomes unsynced in it
        const ui64 generation = config->GetClusterState().GetGeneration() + 1;
        clusterState->SetGeneration(generation);

        auto *details = config->MutableClusterStateDetails();
        auto *pss = details->AddPileSyncState();
        pileId.CopyToProto(pss, &NKikimrBridge::TClusterStateDetails::TPileSyncState::SetBridgePileId);
        pss->SetBecameUnsyncedGeneration(generation);
        pss->SetUnsyncedBSC(true);

        auto *op = details->MutablePendingPileOperation();
        pileId.CopyToProto(op, &NKikimrBridge::TClusterStateDetails::TPendingPileOperation::SetBridgePileId);
        op->SetKind(NKikimrBridge::TClusterStateDetails::TPendingPileOperation::ADD);
        // nodes of older versions (the ones of the pile being added too, as they were not asked) can't work with pending
        // pile operations, and the list of piles may have been changed by the version not setting PileListChanged
        auto *pileRegistry = details->MutablePileRegistry();
        pileRegistry->SetPileListChanged(true);
        pileRegistry->SetRequiredFeatureLevel(Max<ui32>(pileRegistry->GetRequiredFeatureLevel(),
            BridgeFeatureLevelPileOps));
        // nodes of the pile having older cluster state are the ones of its previous incarnation, if it has been removed
        if (pileRegistry->PilesSize() <= pileId.GetPileIndex()) {
            return "pile registry doesn't list the pile";
        }
        pileRegistry->MutablePiles(pileId.GetPileIndex())->SetAddedGeneration(generation);
        op->SetOperationId(config->GetGeneration() + 1); // the generation of the config being proposed

        // allocate the part of every bridged static group in the pile; the part is unsynced, so it is synced from the
        // very beginning when the pile rejoins the cluster
        auto *ss = config->MutableBlobStorageConfig()->MutableServiceSet();
        std::vector<TGroupId> proxyGroupIds;
        THashMap<TGroupId, TBlobStorageGroupType> pileGroupTypes;
        TGroupId nextGroupId = TGroupId::Zero();
        for (const auto& group : ss->GetGroups()) {
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            nextGroupId = Max(nextGroupId, groupId);
            if (group.HasBridgeGroupState()) {
                proxyGroupIds.push_back(groupId);
            }
            pileGroupTypes.emplace(groupId, TBlobStorageGroupType(static_cast<TBlobStorageGroupType::EErasureSpecies>(
                group.GetErasureSpecies())));
        }
        if (const ui32 next = details->GetNextStaticGroupId()) {
            nextGroupId = Max(nextGroupId, TGroupId::FromValue(next - 1)); // ids of the removed groups are not reused
        }
        auto findGroup = [&](TGroupId groupId) -> NKikimrBlobStorage::TGroupInfo* {
            for (auto& group : *ss->MutableGroups()) {
                if (TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID) == groupId) {
                    return &group;
                }
            }
            return nullptr;
        };
        THashMap<TGroupId, ui32> bumpedGenerations; // inline piles which get their generation bumped along with proxy
        for (const TGroupId proxyGroupId : proxyGroupIds) {
            std::optional<TBlobStorageGroupType> groupType;
            NBridge::FindGroupPile(findGroup(proxyGroupId)->GetBridgeGroupState(), [&](TBridgePileId,
                    const NKikimrBridge::TGroupState::TPile& pile) {
                const auto it = pileGroupTypes.find(TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId));
                if (it != pileGroupTypes.end()) {
                    groupType = it->second;
                }
                return groupType.has_value();
            });
            if (!groupType) {
                return TStringBuilder() << "can't find erasure of bridged static group GroupId# " << proxyGroupId;
            }

            // New parts inherit the configured static base, not whichever heterogeneous part is found first.
            if (config->GetSelfManagementConfig().HasErasureSpecies()) {
                TBlobStorageGroupType::EErasureSpecies species;
                if (!TBlobStorageGroupType::ParseErasureName(species, config->GetSelfManagementConfig().GetErasureSpecies())) {
                    return "invalid static group ErasureSpecies";
                }
                groupType = TBlobStorageGroupType(species);
            }
            ++nextGroupId;
            try {
                AllocateStaticGroup({
                    .Config = config,
                    .GroupId = nextGroupId,
                    .GroupGeneration = 1,
                    .GroupType = *groupType,
                    .BaseConfig = baseConfig,
                    .BridgePileId = pileId,
                    .BridgeProxyGroupId = proxyGroupId,
                });
            } catch (const TExConfigError& ex) {
                return TStringBuilder() << "failed to allocate part of static group GroupId# " << proxyGroupId
                    << " in the pile: " << ex.what();
            }

            auto *proxyGroup = findGroup(proxyGroupId); // groups may have been reallocated by adding the new one
            auto *pile = NBridge::AddGroupPile(proxyGroup->MutableBridgeGroupState(), pileId);
            nextGroupId.CopyToProto(pile, &NKikimrBridge::TGroupState::TPile::SetGroupId);
            pile->SetGroupGeneration(1);
            pile->SetBecameUnsyncedGeneration(generation);

            // proxy group gets new generation to make its users pick up the new pile
            BumpBridgeProxyGroupGeneration(proxyGroup, bumpedGenerations);
        }
        UpdateStaticVDiskGenerations(ss, bumpedGenerations);

        // state storage replicas of the pile; DISCONNECTED ring group takes no part in quorums, so it is added at once
        std::unordered_set<ui32> pileNodes;
        const auto registry = GetConfigPileRegistry(*config);
        for (const auto& node : config->GetAllNodes()) {
            if (ResolveNodePileId(registry, TNodeLocation(node.GetLocation())) == pileId) {
                pileNodes.insert(node.GetNodeId());
            }
        }
        if (pileNodes.empty()) {
            return "pile has no nodes";
        }
        std::unordered_set<ui32> usedNodes;
        auto addRingGroup = [&](NKikimrConfig::TStateStorageConfig *pb, bool automaticManagement) -> std::optional<TString> {
            if (!automaticManagement) {
                return std::nullopt; // the operator manages this config
            }
            NKikimrConfig::TStateStorageConfig generated;
            GenerateStateStorageConfig(&generated, *config, usedNodes, pileNodes, pb);
            for (auto& ringGroup : *generated.MutableRingGroups()) {
                if (TBridgePileId::FromProto(&ringGroup, &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId) !=
                        pileId) {
                    return "state storage ring group generated for wrong pile";
                }
                pb->AddRingGroups()->Swap(&ringGroup);
            }
            return std::nullopt;
        };
        const auto *sm = Cfg->SelfManagementConfig.get();
#define ADD_RING_GROUP(NAME) \
        if (config->Has##NAME##Config()) { \
            if (auto error = addRingGroup(config->Mutable##NAME##Config(), !sm || sm->GetAutomatic##NAME##Management())) { \
                return error; \
            } \
        }
        ADD_RING_GROUP(StateStorage)
        ADD_RING_GROUP(StateStorageBoard)
        ADD_RING_GROUP(SchemeBoard)
#undef ADD_RING_GROUP

        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::RemoveBridgePileFromConfig(NKikimrBlobStorage::TStorageConfig *config,
            TBridgePileId pileId) {
        auto *clusterState = config->MutableClusterState();
        if (NBridge::GetPileState(*clusterState, pileId) != NKikimrBridge::TClusterState::DISCONNECTED) {
            return "pile is not DISCONNECTED";
        }
        clusterState->SetPerPileState(pileId.GetPileIndex(), NKikimrBridge::TClusterState::INACTIVE);

        auto *details = config->MutableClusterStateDetails();
        EraseIf(*details->MutablePileSyncState(), [&](const auto& pss) {
            return TBridgePileId::FromProto(&pss, &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId) ==
                pileId;
        });
        details->ClearPendingPileOperation();
        auto *registry = details->MutablePileRegistry();
        if (TBridgePileId::FromProto(registry, &NKikimrBridge::TPileRegistry::GetImplicitPileId) == pileId) {
            registry->ClearImplicitPileId(); // nodes and groups of the cluster all have their piles now
        }

        // drop the part in the pile of every bridged static group; inline pile keeps serving as bridge proxy group only
        auto *ss = config->MutableBlobStorageConfig()->MutableServiceSet();
        for (const auto& group : ss->GetGroups()) {
            details->SetNextStaticGroupId(Max(details->GetNextStaticGroupId(), group.GetGroupID() + 1));
        }
        THashSet<TGroupId> removedGroupIds;
        THashMap<TGroupId, ui32> bumpedGenerations; // inline piles which get their generation bumped along with proxy
        for (auto& group : *ss->MutableGroups()) {
            if (!group.HasBridgeGroupState()) {
                continue;
            }
            auto *state = group.MutableBridgeGroupState();
            const auto *pile = NBridge::GetGroupPile(*state, pileId);
            if (!pile) {
                continue;
            }
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            const auto pileGroupId = TGroupId::FromProto(pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            removedGroupIds.insert(pileGroupId);
            NBridge::RemoveGroupPile(state, pileId);
            if (pileGroupId == groupId) {
                // the group stops being an inline pile, it is the same as bridge proxy group of native cluster now
                group.ClearRings();
                group.ClearErasureSpecies();
                group.ClearBridgePileId();
                group.ClearBridgeProxyGroupId();
            }

            // proxy group gets new generation to make its users forget the removed pile
            BumpBridgeProxyGroupGeneration(&group, bumpedGenerations);
        }
        EraseIf(*ss->MutableGroups(), [&](const auto& group) {
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            return removedGroupIds.contains(groupId) && !group.HasBridgeGroupState();
        });
        for (auto& vdisk : *ss->MutableVDisks()) {
            if (removedGroupIds.contains(VDiskIDFromVDiskID(vdisk.GetVDiskID()).GroupID)) {
                // the VDisk is destroyed by its node when the node gets this config; its slot stays occupied meanwhile
                vdisk.ClearDonorMode();
                vdisk.ClearDonors();
                vdisk.SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
            }
        }
        UpdateStaticVDiskGenerations(ss, bumpedGenerations);

        // state storage replicas of the pile
        return ForEachStateStorageConfig(config, [&](NKikimrConfig::TStateStorageConfig *pb) -> std::optional<TString> {
            EraseIf(*pb->MutableRingGroups(), [&](const auto& ringGroup) {
                return TBridgePileId::FromProto(&ringGroup, &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId) ==
                    pileId;
            });
            return std::nullopt;
        });
    }

    void ResetStaticGroupPileParts(NKikimrBlobStorage::TStorageConfig *config, const std::vector<TBridgePileId>& pileIds,
            ui64 becameUnsyncedGeneration) {
        // bump each static group once per configuration, even when parts in several piles are reset
        auto *ss = config->MutableBlobStorageConfig()->MutableServiceSet();
        THashMap<TGroupId, NKikimrBlobStorage::TGroupInfo*> groupMap;
        for (auto& group : *ss->MutableGroups()) {
            groupMap.emplace(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID), &group);
        }
        THashMap<TGroupId, ui32> changedGroupIds;
        for (const auto& [groupId, group] : groupMap) {
            if (!group->HasBridgeGroupState()) {
                continue;
            }
            auto *state = group->MutableBridgeGroupState();
            for (const TBridgePileId pileId : pileIds) {
                auto *pile = NBridge::GetGroupPile(state, pileId);
                Y_ABORT_UNLESS(pile); // the pile is not INACTIVE, so the group has its part there
                pile->ClearStage();
                pile->SetBecameUnsyncedGeneration(becameUnsyncedGeneration);
            }
            NBridge::ForEachGroupPile(state, [&](TBridgePileId, NKikimrBridge::TGroupState::TPile& pile) {
                const auto refGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                auto *refGroup = groupMap[refGroupId];
                Y_ABORT_UNLESS(refGroup);
                Y_ABORT_UNLESS(refGroup->GetGroupGeneration() == pile.GetGroupGeneration());
                refGroup->SetGroupGeneration(refGroup->GetGroupGeneration() + 1);
                pile.SetGroupGeneration(refGroup->GetGroupGeneration());
                changedGroupIds.emplace(refGroupId, refGroup->GetGroupGeneration());
            });
            BumpBridgeProxyGeneration(group, [&](TGroupId id) { return changedGroupIds.contains(id); });
        }
        UpdateStaticVDiskGenerations(ss, changedGroupIds);
    }

    void UpdateClusterStateGuid(NKikimrBridge::TClusterState *clusterState) {
        TString buffer;
        const bool success = clusterState->SerializeToString(&buffer);
        Y_ABORT_UNLESS(success);
        clusterState->SetGuid(XXH3_64bits(buffer.data(), buffer.size()));
    }

} // NKikimr::NStorage
