#include "distconf_invoke.h"
#include "ydb/core/base/statestorage.h"
#include "distconf_selfheal.h"

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::GetRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig) {
        const NKikimrBlobStorage::TStorageConfig &config = *Self->StorageConfig;
        GenerateStateStorageConfig(currentConfig->MutableStateStorageConfig(), config);
        GenerateStateStorageConfig(currentConfig->MutableStateStorageBoardConfig(), config);
        GenerateStateStorageConfig(currentConfig->MutableSchemeBoardConfig(), config);
    }

    bool TInvokeRequestHandlerActor::AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig) {
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
                return true;
            }
            TIntrusivePtr<TStateStorageInfo> newSSInfo;
            TIntrusivePtr<TStateStorageInfo> oldSSInfo;
            oldSSInfo = (*buildFunc)((config.*getFunc)());
            newSSInfo = (*buildFunc)(*newMutableConfig);
            while (!testNewConfig(newSSInfo, oldSSInfo)) {
                if (actorIdOffset > 16) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << name << " can not adjust RingGroupActorIdOffset");
                    return false;
                }
                for (ui32 rg : xrange(newMutableConfig->RingGroupsSize())) {
                    newMutableConfig->MutableRingGroups(rg)->SetRingGroupActorIdOffset(++actorIdOffset);
                }
                newSSInfo = (*buildFunc)(*newMutableConfig);
            }
            return true;
        };
        #define F(NAME) \
        if (!process(#NAME, &NKikimrBlobStorage::TStorageConfig::Get##NAME##Config, &NKikimrBlobStorage::TStateStorageConfig::Mutable##NAME##Config, &NKikimr::Build##NAME##Info)) { \
            return false; \
        }
        F(StateStorage)
        F(StateStorageBoard)
        F(SchemeBoard)
        #undef F
        return true;
    }

    void TInvokeRequestHandlerActor::GetCurrentStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig) {
        const NKikimrBlobStorage::TStorageConfig &config = *Self->StorageConfig;
        currentConfig->MutableStateStorageConfig()->CopyFrom(config.GetStateStorageConfig());
        currentConfig->MutableStateStorageBoardConfig()->CopyFrom(config.GetStateStorageBoardConfig());
        currentConfig->MutableSchemeBoardConfig()->CopyFrom(config.GetSchemeBoardConfig());
    }

    void TInvokeRequestHandlerActor::GetStateStorageConfig(const TQuery::TGetStateStorageConfig& cmd) {
        if (!RunCommonChecks()) {
            return;
        }
        auto ev = PrepareResult(TResult::OK, std::nullopt);
        auto* currentConfig = ev->Record.MutableStateStorageConfig();

        if (cmd.GetRecommended()) {
            GetRecommendedStateStorageConfig(currentConfig);
            if (!AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(currentConfig)) {
                return;
            }
        } else {
            GetCurrentStateStorageConfig(currentConfig);
        }
        Finish(Sender, SelfId(), ev.release(), 0, Cookie);
    }

    void TInvokeRequestHandlerActor::SelfHealStateStorage(const TQuery::TSelfHealStateStorage& cmd) {
        if (!RunCommonChecks()) {
            return;
        }
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
            STLOG(PRI_DEBUG, BS_NODE, NW52, "needReconfig " << (oldSSInfo->RingGroups == newSSInfo->RingGroups));
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
            FinishWithError(TResult::ERROR, TStringBuilder() << "Current configuration is recommended. Nothing to self-heal.");
            return;
        }
        #undef NEED_RECONFIG

        if (!AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(&targetConfig)) {
            return;
        }

        Self->StateStorageSelfHealActor = Register(new TStateStorageSelfhealActor(Sender, Cookie, TDuration::Seconds(cmd.GetWaitForConfigStep()), std::move(currentConfig), std::move(targetConfig)));
        auto ev = PrepareResult(TResult::OK, std::nullopt);
        Finish(Sender, SelfId(), ev.release(), 0, Cookie);
    }

    void TInvokeRequestHandlerActor::ReconfigStateStorage(const NKikimrBlobStorage::TStateStorageConfig& cmd) {
        if (!RunCommonChecks()) {
            return;
        }

        STLOG(PRI_DEBUG, BS_NODE, NW52, "TInvokeRequestHandlerActor::ReconfigStateStorage",
                (StateStorageConfig, cmd));

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        if (!cmd.HasStateStorageConfig() && !cmd.HasStateStorageBoardConfig() && !cmd.HasSchemeBoardConfig()) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "New configuration is not defined");
            return;
        }
        auto process = [&](const char *name, auto buildInfo, auto hasFunc, auto func, auto configHasFunc, auto configMutableFunc) {
            if (!(cmd.*hasFunc)()) {
                return true;
            }
            if (!(config.*configHasFunc)()) {
                FinishWithError(TResult::ERROR, TStringBuilder() << name << " configuration is not filled in");
                return false;
            }
            const auto &newSSConfig = (cmd.*func)();

            if (newSSConfig.HasRing()) {
                FinishWithError(TResult::ERROR, TStringBuilder() << "New " << name << " configuration Ring option is not allowed, use RingGroups");
                return false;
            }
            if (newSSConfig.RingGroupsSize() < 1) {
                FinishWithError(TResult::ERROR, TStringBuilder() << "New " << name << " configuration RingGroups is not filled in");
                return false;
            }
            if (newSSConfig.GetRingGroups(0).GetWriteOnly()) {
                FinishWithError(TResult::ERROR, TStringBuilder() << "New " << name << " configuration first RingGroup is writeOnly");
                return false;
            }
            for (auto& rg : newSSConfig.GetRingGroups()) {
                if (rg.RingSize() && rg.NodeSize()) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << name << " Ring and Node are defined, use the one of them");
                    return false;
                }
                const size_t numItems = Max(rg.RingSize(), rg.NodeSize());
                if (!rg.HasNToSelect() || numItems < 1 || rg.GetNToSelect() < 1 || rg.GetNToSelect() > numItems) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << name << " invalid ring group selection");
                    return false;
                }
                for (auto &ring : rg.GetRing()) {
                    if (ring.RingSize() > 0) {
                        FinishWithError(TResult::ERROR, TStringBuilder() << name << " too deep nested ring declaration");
                        return false;
                    }
                    if(ring.HasRingGroupActorIdOffset()) {
                        FinishWithError(TResult::ERROR, TStringBuilder() << name << " RingGroupActorIdOffset should be used in ring group level, not ring");
                        return false;
                    }
                    if (ring.NodeSize() < 1) {
                        FinishWithError(TResult::ERROR, TStringBuilder() << name << " empty ring");
                        return false;
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
                            if(!replicas.insert(node).second) {
                                FinishWithError(TResult::ERROR, TStringBuilder() << name << " replicas ActorId intersection, specify RingGroupActorIdOffset if you run multiple replicas on one node");
                                return false;
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
                        FinishWithError(TResult::ERROR, TStringBuilder() <<
                            "New introduced ring group should be WriteOnly old:" << oldSSInfo->ToString() <<" new: " << newSSInfo->ToString());
                        return false;
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
                        FinishWithError(TResult::ERROR, TStringBuilder() <<
                            "Can not delete not WriteOnly ring group. Make it WriteOnly before deletion old:" << oldSSInfo->ToString() <<" new: " << newSSInfo->ToString());
                        return false;
                    }
                }
            } catch(std::exception &e) {
                FinishWithError(TResult::ERROR, TStringBuilder() << "Can not build " << name << " info from config. " << e.what());
                return false;
            }
            auto* ssConfig = (config.*configMutableFunc)();
            if (newSSConfig.RingGroupsSize() == 1) {
                ssConfig->MutableRing()->CopyFrom(newSSConfig.GetRingGroups(0));
                ssConfig->ClearRingGroups();
            } else {
                ssConfig->CopyFrom(newSSConfig);
            }
            return true;
        };

#define PROCESS(NAME) \
        if (!process(#NAME, &NKikimr::Build##NAME##Info, \
                &NKikimrBlobStorage::TStateStorageConfig::Has##NAME##Config, \
                &NKikimrBlobStorage::TStateStorageConfig::Get##NAME##Config, \
                &NKikimrBlobStorage::TStorageConfig::Has##NAME##Config, \
                &NKikimrBlobStorage::TStorageConfig::Mutable##NAME##Config)) { \
            return; \
        }
        PROCESS(StateStorage)
        PROCESS(StateStorageBoard)
        PROCESS(SchemeBoard)
        config.SetGeneration(config.GetGeneration() + 1);
        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::ReassignStateStorageNode(const TQuery::TReassignStateStorageNode& cmd) {
        if (!RunCommonChecks()) {
            return;
        }
        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        auto process = [&](const char *name, auto hasFunc, auto mutableFunc) {
            if (!(config.*hasFunc)()) {
                FinishWithError(TResult::ERROR, TStringBuilder() << name << " configuration is not filled in");
                return false;
            }

            bool found = false;

            auto *m = (config.*mutableFunc)();
            for (size_t i = 0; i < m->RingGroupsSize(); i++) {
                auto *ring = m->MutableRingGroups(i);
                if (ring->RingSize() && ring->NodeSize()) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << name << " incorrect configuration:"
                        " both Ring and Node fields are set");
                    return false;
                }

                const size_t numItems = Max(ring->RingSize(), ring->NodeSize());

                auto replace = [&](auto *ring, size_t i) {
                    if (ring->GetNode(i) == cmd.GetFrom()) {
                        if (found) {
                            FinishWithError(TResult::ERROR, TStringBuilder() << name << " ambiguous From node");
                            return false;
                        } else {
                            found = true;
                            ring->MutableNode()->Set(i, cmd.GetTo());
                        }
                    }
                    return true;
                };

                for (size_t i = 0; i < numItems; ++i) {
                    if (ring->RingSize()) {
                        const auto& r = ring->GetRing(i);
                        if (r.RingSize()) {
                            FinishWithError(TResult::ERROR, TStringBuilder() << name << " incorrect configuration:"
                                " Ring is way too nested");
                            return false;
                        }
                        const size_t numNodes = r.NodeSize();
                        for (size_t k = 0; k < numNodes; ++k) {
                            if (r.GetNode(k) == cmd.GetFrom() && !replace(ring->MutableRing(i), k)) {
                                return false;
                            }
                        }
                    } else {
                        if (ring->GetNode(i) == cmd.GetFrom() && !replace(ring, i)) {
                            return false;
                        }
                    }
                }
            }
            if (!found) {
                FinishWithError(TResult::ERROR, TStringBuilder() << name << " From node not found");
                return false;
            }

            return true;
        };

#define F(NAME) \
        if (cmd.Get##NAME() && !process(#NAME, &NKikimrBlobStorage::TStorageConfig::Has##NAME##Config, \
                &NKikimrBlobStorage::TStorageConfig::Mutable##NAME##Config)) { \
            return; \
        }
        F(StateStorage)
        F(StateStorageBoard)
        F(SchemeBoard)
#undef F
        config.SetGeneration(config.GetGeneration() + 1);
        StartProposition(&config);
    }

} // NKikimr::NStorage
