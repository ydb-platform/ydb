#include "distconf_invoke.h"
#include "ydb/core/base/statestorage.h"
#include "distconf_selfheal.h"

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::GetRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig) {
        const NKikimrBlobStorage::TStorageConfig &config = *Self->StorageConfig;
        Self->GenerateStateStorageConfig(currentConfig->MutableStateStorageConfig(), config);
        Self->GenerateStateStorageConfig(currentConfig->MutableStateStorageBoardConfig(), config);
        Self->GenerateStateStorageConfig(currentConfig->MutableSchemeBoardConfig(), config);
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

    void TInvokeRequestHandlerActor::SelfHealBadNodesListUpdate(const TQuery::TSelfHealBadNodesListUpdate& cmd) {
        RunCommonChecks();
        Self->SelfHealBadNodes.clear();
        for(auto nodeId : cmd.GetBadNodes()) {
            Self->SelfHealBadNodes.insert(nodeId);
        }
        STLOG(PRI_DEBUG, BS_NODE, NW52, "SelfHealBadNodes: " << (Self->SelfHealBadNodes));
        if (cmd.GetEnableSelfHealStateStorage()) {
            SelfHealStateStorage(cmd.GetWaitForConfigStep());
        }
    }

    void TInvokeRequestHandlerActor::SelfHealStateStorage(bool waitForConfigStep) {
        RunCommonChecks();
        if (Self->StateStorageSelfHealActor) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, Self->StateStorageSelfHealActor.value(), Self->SelfId(), nullptr, 0));
            Self->StateStorageSelfHealActor.reset();
        }
        NKikimrBlobStorage::TStateStorageConfig currentConfig;
        GetCurrentStateStorageConfig(&currentConfig);

        NKikimrBlobStorage::TStateStorageConfig targetConfig;
        GetRecommendedStateStorageConfig(&targetConfig);

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
            STLOG(PRI_DEBUG, BS_NODE, NW52, "needReconfig " << (oldSSInfo->RingGroups != newSSInfo->RingGroups));
            if (oldSSInfo->RingGroups == newSSInfo->RingGroups) {
                (targetConfig.*clearFunc)();
                return false;
            }
            return true;
        };
        #define NEED_RECONFIG(NAME) needReconfig(&NKikimrBlobStorage::TStateStorageConfig::Clear##NAME##Config, &NKikimrBlobStorage::TStateStorageConfig::Mutable##NAME##Config, &NKikimr::Build##NAME##Info)
        auto needReconfigSS = NEED_RECONFIG(StateStorage);
        auto needReconfigSSB = NEED_RECONFIG(StateStorageBoard);
        auto needReconfigSB = NEED_RECONFIG(SchemeBoard);

        if (!needReconfigSS && !needReconfigSSB && !needReconfigSB) {
            throw TExError() << "Current configuration is recommended. Nothing to self-heal.";
        }
        #undef NEED_RECONFIG

        AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(&targetConfig);

        Self->StateStorageSelfHealActor = Register(new TStateStorageSelfhealActor(Sender, Cookie,
            TDuration::Seconds(waitForConfigStep), std::move(currentConfig), std::move(targetConfig)));
        auto ev = PrepareResult(TResult::OK, std::nullopt);
        FinishWithSuccess();
    }

    void TInvokeRequestHandlerActor::ReconfigStateStorage(const NKikimrBlobStorage::TStateStorageConfig& cmd) {
        RunCommonChecks();

        STLOG(PRI_DEBUG, BS_NODE, NW52, "TInvokeRequestHandlerActor::ReconfigStateStorage",
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
                    if(ring.HasRingGroupActorIdOffset()) {
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
                    for(auto& ring : ringGroup.Rings) {
                        for(auto& node : ring.Replicas) {
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

Y_DECLARE_OUT_SPEC(inline, std::unordered_set<ui32>, o, x) {
    o << '[';
    for (auto it = x.begin(); it != x.end(); ++it) {
        if (it != x.begin()) {
            o << ',';
        }
        o << *it;
    }
    o << ']';
}
