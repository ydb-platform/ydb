#include "distconf_invoke.h"
#include "ydb/core/base/statestorage.h"
#include "distconf_selfheal.h"

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    bool TInvokeRequestHandlerActor::GetRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig) {
        const NKikimrBlobStorage::TStorageConfig &config = *Self->StorageConfig;
        bool result = true;
        std::unordered_set<ui32> usedNodes;
        result &= Self->GenerateStateStorageConfig(currentConfig->MutableStateStorageConfig(), config, usedNodes);
        result &= Self->GenerateStateStorageConfig(currentConfig->MutableStateStorageBoardConfig(), config, usedNodes);
        result &= Self->GenerateStateStorageConfig(currentConfig->MutableSchemeBoardConfig(), config, usedNodes);
        return result;
    }

    void TInvokeRequestHandlerActor::AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig) {
        const NKikimrBlobStorage::TStorageConfig &config = *Self->StorageConfig;
        auto testNewConfig = [](auto newSSInfo, auto oldSSInfo) {
            THashSet<TActorId> replicas;
            for (const auto& ssInfo : { newSSInfo, oldSSInfo }) {
                for (auto& ringGroup : ssInfo->RingGroups) {
                    for (auto& ring : ringGroup.Rings) {
                        for (auto& node : ring.Replicas) {
                            if (!replicas.insert(node).second) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        };
        auto process = [&](const char *name, auto getFunc, auto ssMutableFunc, auto buildFunc) {
            ui32 actorIdOffset = 0;
            auto *newMutableConfig = (currentConfig->*ssMutableFunc)();
            if (newMutableConfig->RingGroupsSize() == 0) {
                return;
            }
            TIntrusivePtr<TStateStorageInfo> newSSInfo;
            TIntrusivePtr<TStateStorageInfo> oldSSInfo;
            oldSSInfo = (*buildFunc)((config.*getFunc)());
            newSSInfo = (*buildFunc)(*newMutableConfig);
            while (!testNewConfig(newSSInfo, oldSSInfo)) {
                if (actorIdOffset > 16) {
                    throw TExError() << name << " can not adjust RingGroupActorIdOffset";
                }
                for (ui32 rg : xrange(newMutableConfig->RingGroupsSize())) {
                    newMutableConfig->MutableRingGroups(rg)->SetRingGroupActorIdOffset(++actorIdOffset);
                }
                newSSInfo = (*buildFunc)(*newMutableConfig);
            }
        };
        #define F(NAME) \
            process(#NAME, &NKikimrBlobStorage::TStorageConfig::Get##NAME##Config, \
                &NKikimrBlobStorage::TStateStorageConfig::Mutable##NAME##Config, &NKikimr::Build##NAME##Info);
        F(StateStorage)
        F(StateStorageBoard)
        F(SchemeBoard)
        #undef F
    }

    void TInvokeRequestHandlerActor::GetCurrentStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig) {
        const NKikimrBlobStorage::TStorageConfig &config = *Self->StorageConfig;
        currentConfig->MutableStateStorageConfig()->CopyFrom(config.GetStateStorageConfig());
        currentConfig->MutableStateStorageBoardConfig()->CopyFrom(config.GetStateStorageBoardConfig());
        currentConfig->MutableSchemeBoardConfig()->CopyFrom(config.GetSchemeBoardConfig());
    }

    void TInvokeRequestHandlerActor::GetStateStorageConfig(const TQuery::TGetStateStorageConfig& cmd) {
        RunCommonChecks();

        FinishWithSuccess([&](auto *record) {
            auto* currentConfig = record->MutableStateStorageConfig();

            if (cmd.GetRecommended()) {
                GetRecommendedStateStorageConfig(currentConfig);
                AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(currentConfig);
            } else {
                GetCurrentStateStorageConfig(currentConfig);
            }
        });
    }

    void TInvokeRequestHandlerActor::SelfHealNodesStateUpdate(const TQuery::TSelfHealNodesStateUpdate& cmd) {
        RunCommonChecks();
        Self->SelfHealNodesState.clear();
        for (auto node : cmd.GetNodesState()) {
            Self->SelfHealNodesState[node.GetNodeId()] = node.GetState();
        }
        if (cmd.GetEnableSelfHealStateStorage()) {
            SelfHealStateStorage(cmd.GetWaitForConfigStep(), true);
        }
    }

    void TInvokeRequestHandlerActor::SelfHealStateStorage(const TQuery::TSelfHealStateStorage& cmd) {
        SelfHealStateStorage(cmd.GetWaitForConfigStep(), cmd.GetForceHeal());
    }

    void TInvokeRequestHandlerActor::SelfHealStateStorage(ui32 waitForConfigStep, bool forceHeal) {
        RunCommonChecks();
        NKikimrBlobStorage::TStateStorageConfig targetConfig;
        if (!GetRecommendedStateStorageConfig(&targetConfig) && !forceHeal) {
            throw TExError() << " Recommended configuration has faulty nodes and can not be applyed";
        }

        NKikimrBlobStorage::TStateStorageConfig currentConfig;
        GetCurrentStateStorageConfig(&currentConfig);

        if (Self->StateStorageSelfHealActor) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, Self->StateStorageSelfHealActor.value(), Self->SelfId(), nullptr, 0));
            Self->StateStorageSelfHealActor.reset();
        }
        enum ReconfigType {
            NONE,
            ONE_NODE,
            FULL
        };
        std::unordered_map<ui32, ui32> nodesToReplace;
        auto needReconfig = [&](auto clearFunc, auto ssMutableFunc, auto buildFunc) {
            auto copyCurrentConfig = currentConfig;
            auto ss = *(copyCurrentConfig.*ssMutableFunc)();
            if (ss.RingGroupsSize() == 0) {
                ss.MutableRing()->ClearRingGroupActorIdOffset();
            } else {
                for (ui32 i : xrange(ss.RingGroupsSize())) {
                    ss.MutableRingGroups(i)->ClearRingGroupActorIdOffset();
                }
            }
            TIntrusivePtr<TStateStorageInfo> newSSInfo;
            TIntrusivePtr<TStateStorageInfo> oldSSInfo;
            oldSSInfo = (*buildFunc)(ss);
            newSSInfo = (*buildFunc)(*(targetConfig.*ssMutableFunc)());
            if (oldSSInfo->RingGroups == newSSInfo->RingGroups) {
                (targetConfig.*clearFunc)();
                return ReconfigType::NONE;
            }

            if (oldSSInfo->RingGroups.size() != newSSInfo->RingGroups.size()) {
                return ReconfigType::FULL;
            }

            bool hasBadNodes = false;
            for (ui32 ringGroupIdx : xrange(oldSSInfo->RingGroups.size())) {
                auto& oldRg = oldSSInfo->RingGroups[ringGroupIdx];
                auto& newRg = newSSInfo->RingGroups[ringGroupIdx];
                if (oldRg.NToSelect != newRg.NToSelect || oldRg.Rings.size() != newRg.Rings.size()) {
                    return ReconfigType::FULL;
                }
                for (ui32 j : xrange(oldRg.Rings.size())) {
                    auto& oldRing = oldRg.Rings[j];
                    auto& newRing = newRg.Rings[j];
                    if (oldRing.IsDisabled != newRing.IsDisabled
                        || oldRing.UseRingSpecificNodeSelection != newRing.UseRingSpecificNodeSelection
                        || oldRing.Replicas.size() != newRing.Replicas.size()) {
                        return ReconfigType::FULL;
                    }
                    for (auto& actorId : oldRing.Replicas) {
                        if (!Self->SelfHealNodesState.contains(actorId.NodeId()) || Self->SelfHealNodesState.at(actorId.NodeId()) > 0) {
                            hasBadNodes = true;
                        }
                    }
                }
            }
            if (!hasBadNodes) {
                return ReconfigType::NONE; // Current config is optimal and all nodes are good
            }

            // Check can be node replacement applyed
            for (ui32 ringGroupIdx : xrange(oldSSInfo->RingGroups.size())) {
                auto& oldRg = oldSSInfo->RingGroups[ringGroupIdx];
                auto& newRg = newSSInfo->RingGroups[ringGroupIdx];

                // Find not changed rings and place them on previous position
                auto equalRingsByNodes = [](auto& ring1, auto& ring2) {
                    if (ring1.Replicas.size() != ring2.Replicas.size()) {
                        return false;
                    }
                    for(ui32 replicaPos : xrange(ring1.Replicas.size())) {
                        if (ring1.Replicas[replicaPos].NodeId() != ring2.Replicas[replicaPos].NodeId()) {
                            return false;
                        }
                    }
                    return true;
                };
                for (ui32 oldRingIdx : xrange(oldRg.Rings.size())) {
                    for (ui32 newRingIdx : xrange(newRg.Rings.size())) {
                        if (newRingIdx != oldRingIdx && equalRingsByNodes(oldRg.Rings[oldRingIdx], newRg.Rings[newRingIdx])) {
                            std::swap(newRg.Rings[newRingIdx], newRg.Rings[oldRingIdx]);
                            break;
                        }
                    }
                }
                for (ui32 j : xrange(oldRg.Rings.size())) {
                    auto& oldRing = oldRg.Rings[j];
                    auto& newRing = newRg.Rings[j];
                    if (oldRing == newRing) {
                        continue;
                    }
                    // Place replicas in ring on previous position
                    for (ui32 oldReplicaPos : xrange(oldRing.Replicas.size())) {
                        for (ui32 newReplicaPos : xrange(newRing.Replicas.size())) {
                            if (newReplicaPos != oldReplicaPos && oldRing.Replicas[oldReplicaPos].NodeId() == newRing.Replicas[newReplicaPos].NodeId()) {
                                std::swap(newRing.Replicas[newReplicaPos], newRing.Replicas[oldReplicaPos]);
                                break;
                            }
                        }
                    }

                    for (ui32 k : xrange(oldRing.Replicas.size())) {
                        auto oldRep = oldRing.Replicas[k].NodeId();
                        auto newRep = newRing.Replicas[k].NodeId();
                        if (oldRep == newRep) {
                            continue;
                        }
                        if (auto it = nodesToReplace.find(oldRep); it != nodesToReplace.end() && it->second != newRep) {
                            return ReconfigType::FULL;
                        }
                        nodesToReplace[oldRep] = newRep;
                    }
                }
            }
            if (nodesToReplace.size() == 1) {
                return ReconfigType::ONE_NODE;
            }
            return nodesToReplace.empty() ? ReconfigType::NONE : ReconfigType::FULL;
        };
        #define NEED_RECONFIG(NAME) needReconfig(&NKikimrBlobStorage::TStateStorageConfig::Clear##NAME##Config, &NKikimrBlobStorage::TStateStorageConfig::Mutable##NAME##Config, &NKikimr::Build##NAME##Info)
        auto needReconfigSS = NEED_RECONFIG(StateStorage);
        auto needReconfigSSB = NEED_RECONFIG(StateStorageBoard);
        auto needReconfigSB = NEED_RECONFIG(SchemeBoard);
        #undef NEED_RECONFIG

        if (needReconfigSS == ReconfigType::NONE && needReconfigSSB == ReconfigType::NONE && needReconfigSB == ReconfigType::NONE) {
            throw TExError() << "Current configuration is recommended. Nothing to self-heal.";
        }
        if (nodesToReplace.size() == 1 && needReconfigSS != ReconfigType::FULL && needReconfigSSB != ReconfigType::FULL && needReconfigSB != ReconfigType::FULL) {
            STLOG(PRI_DEBUG, BS_NODE, NW100, "Need to reconfig one node " << nodesToReplace.begin()->first << " to " << nodesToReplace.begin()->second
                , (CurrentConfig, currentConfig), (TargetConfig, targetConfig));

            TQuery::TReassignStateStorageNode cmd;
            cmd.SetFrom(nodesToReplace.begin()->first);
            cmd.SetTo(nodesToReplace.begin()->second);
            cmd.SetStateStorage(needReconfigSS == ReconfigType::ONE_NODE);
            cmd.SetStateStorageBoard(needReconfigSSB == ReconfigType::ONE_NODE);
            cmd.SetSchemeBoard(needReconfigSB == ReconfigType::ONE_NODE);
            ReassignStateStorageNode(cmd);
            return;
        }

        AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(&targetConfig);

        STLOG(PRI_DEBUG, BS_NODE, NW101, "Need to reconfig, starting StateStorageSelfHealActor", (CurrentConfig, currentConfig), (TargetConfig, targetConfig));

        Self->StateStorageSelfHealActor = Register(new TStateStorageSelfhealActor(Sender, Cookie,
            TDuration::Seconds(waitForConfigStep), std::move(currentConfig), std::move(targetConfig)));
        auto ev = PrepareResult(TResult::OK, std::nullopt);
        FinishWithSuccess();
    }

    void TInvokeRequestHandlerActor::ReconfigStateStorage(const NKikimrBlobStorage::TStateStorageConfig& cmd) {
        RunCommonChecks();

        STLOG(PRI_DEBUG, BS_NODE, NW67, "TInvokeRequestHandlerActor::ReconfigStateStorage",
                (StateStorageConfig, cmd));

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        if (!cmd.HasStateStorageConfig() && !cmd.HasStateStorageBoardConfig() && !cmd.HasSchemeBoardConfig()) {
            throw TExError() << "New configuration is not defined";
        }
        auto process = [&](const char *name, auto buildInfo, auto hasFunc, auto func, auto configHasFunc, auto configMutableFunc) {
            if (!(cmd.*hasFunc)()) {
                return;
            }
            if (!(config.*configHasFunc)()) {
                throw TExError() << name << " configuration is not filled in";
            }
            const auto &newSSConfig = (cmd.*func)();

            if (newSSConfig.HasRing()) {
                throw TExError() << "New " << name << " configuration Ring option is not allowed, use RingGroups";
            }
            if (newSSConfig.RingGroupsSize() < 1) {
                throw TExError() << "New " << name << " configuration RingGroups is not filled in";
            }
            if (newSSConfig.GetRingGroups(0).GetWriteOnly()) {
                throw TExError() << "New " << name << " configuration first RingGroup is writeOnly";
            }
            for (auto& rg : newSSConfig.GetRingGroups()) {
                if (rg.RingSize() && rg.NodeSize()) {
                    throw TExError() << name << " Ring and Node are defined, use the one of them";
                }
                const size_t numItems = Max(rg.RingSize(), rg.NodeSize());
                if (!rg.HasNToSelect() || numItems < 1 || rg.GetNToSelect() < 1 || rg.GetNToSelect() > numItems) {
                    throw TExError() << name << " invalid ring group selection";
                }
                for (auto &ring : rg.GetRing()) {
                    if (ring.RingSize() > 0) {
                        throw TExError() << name << " too deep nested ring declaration";
                    }
                    if (ring.HasRingGroupActorIdOffset()) {
                        throw TExError() << name << " RingGroupActorIdOffset should be used in ring group level, not ring";
                    }
                    if (ring.NodeSize() < 1) {
                        throw TExError() << name << " empty ring";
                    }
                }
            }
            try {
                TIntrusivePtr<TStateStorageInfo> newSSInfo;
                TIntrusivePtr<TStateStorageInfo> oldSSInfo;
                newSSInfo = (*buildInfo)(newSSConfig);
                oldSSInfo = (*buildInfo)(*(config.*configMutableFunc)());
                THashSet<TActorId> replicas;
                for (auto& ringGroup : newSSInfo->RingGroups) {
                    for (auto& ring : ringGroup.Rings) {
                        for (auto& node : ring.Replicas) {
                            if (!replicas.insert(node).second) {
                                throw TExError() << name << " replicas ActorId intersection, specify"
                                    " RingGroupActorIdOffset if you run multiple replicas on one node";
                            }
                        }
                    }
                }

                Y_ABORT_UNLESS(newSSInfo->RingGroups.size() > 0 && oldSSInfo->RingGroups.size() > 0);

                for (auto& newGroup : newSSInfo->RingGroups) {
                    if (newGroup.WriteOnly) {
                        continue;
                    }
                    bool found = false;
                    for (auto& rg : oldSSInfo->RingGroups) {
                        if (newGroup.SameConfiguration(rg)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw TExError() << "New introduced ring group should be WriteOnly old: " << oldSSInfo->ToString()
                            << " new: " << newSSInfo->ToString();
                    }
                }
                for (auto& oldGroup : oldSSInfo->RingGroups) {
                    if (oldGroup.WriteOnly) {
                        continue;
                    }
                    bool found = false;
                    for (auto& rg : newSSInfo->RingGroups) {
                        if (oldGroup.SameConfiguration(rg)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw TExError() << "Can not delete not WriteOnly ring group. Make it WriteOnly before deletion old: "
                            << oldSSInfo->ToString() << " new: " << newSSInfo->ToString();
                    }
                }
            } catch (const TExError& e) {
                throw e;
            } catch (const std::exception& e) {
                throw TExError() << "Can not build " << name << " info from config. " << e.what();
            }
            auto* ssConfig = (config.*configMutableFunc)();
            if (newSSConfig.RingGroupsSize() == 1) {
                ssConfig->MutableRing()->CopyFrom(newSSConfig.GetRingGroups(0));
                ssConfig->ClearRingGroups();
            } else {
                ssConfig->CopyFrom(newSSConfig);
            }
        };

#define PROCESS(NAME) \
        process(#NAME, &NKikimr::Build##NAME##Info, \
                &NKikimrBlobStorage::TStateStorageConfig::Has##NAME##Config, \
                &NKikimrBlobStorage::TStateStorageConfig::Get##NAME##Config, \
                &NKikimrBlobStorage::TStorageConfig::Has##NAME##Config, \
                &NKikimrBlobStorage::TStorageConfig::Mutable##NAME##Config);
        PROCESS(StateStorage)
        PROCESS(StateStorageBoard)
        PROCESS(SchemeBoard)

        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::ReassignStateStorageNode(const TQuery::TReassignStateStorageNode& cmd) {
        RunCommonChecks();

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        auto process = [&](const char *name, auto hasFunc, auto mutableFunc) {
            if (!(config.*hasFunc)()) {
                throw TExError() << name << " configuration is not filled in";
            }

            bool found = false;

            auto *m = (config.*mutableFunc)();
            for (size_t i = 0; i < m->RingGroupsSize(); i++) {
                auto *ring = m->MutableRingGroups(i);
                if (ring->RingSize() && ring->NodeSize()) {
                    throw TExError() << name << " incorrect configuration: both Ring and Node fields are set";
                }

                const size_t numItems = Max(ring->RingSize(), ring->NodeSize());

                auto replace = [&](auto *ring, size_t i) {
                    if (ring->GetNode(i) == cmd.GetFrom()) {
                        if (found) {
                            throw TExError() << name << " ambiguous From node";
                        } else {
                            found = true;
                            ring->MutableNode()->Set(i, cmd.GetTo());
                        }
                    }
                };

                for (size_t i = 0; i < numItems; ++i) {
                    if (ring->RingSize()) {
                        const auto& r = ring->GetRing(i);
                        if (r.RingSize()) {
                            throw TExError() << name << " incorrect configuration: Ring is way too nested";
                        }
                        const size_t numNodes = r.NodeSize();
                        for (size_t k = 0; k < numNodes; ++k) {
                            if (r.GetNode(k) == cmd.GetFrom()) {
                                replace(ring->MutableRing(i), k);
                            }
                        }
                    } else {
                        if (ring->GetNode(i) == cmd.GetFrom()) {
                            replace(ring, i);
                        }
                    }
                }
            }
            if (!found) {
                throw TExError() << name << " From node not found";
            }
        };

#define F(NAME) \
        if (cmd.Get##NAME()) { \
            process(#NAME, &NKikimrBlobStorage::TStorageConfig::Has##NAME##Config, \
                &NKikimrBlobStorage::TStorageConfig::Mutable##NAME##Config); \
        }
        F(StateStorage)
        F(StateStorageBoard)
        F(SchemeBoard)
#undef F

        StartProposition(&config);
    }

} // NKikimr::NStorage
