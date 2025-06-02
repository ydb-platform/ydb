#include "distconf_invoke.h"
#include "ydb/core/base/statestorage.h"

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::GetStateStorageConfig() {
        if (!RunCommonChecks()) {
            return;
        }
        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto ev = PrepareResult(TResult::OK, std::nullopt);
        auto* currentConfig = ev->Record.MutableStateStorageConfig();
        currentConfig->MutableStateStorageConfig()->CopyFrom(config.GetStateStorageConfig());
        currentConfig->MutableStateStorageBoardConfig()->CopyFrom(config.GetStateStorageBoardConfig());
        currentConfig->MutableSchemeBoardConfig()->CopyFrom(config.GetSchemeBoardConfig());
        Finish(Sender, SelfId(), ev.release(), 0, Cookie);
    }

    void TInvokeRequestHandlerActor::ReconfigStateStorage(const NKikimrBlobStorage::TStateStorageConfig& cmd) {
        if (!RunCommonChecks()) {
            return;
        }

        STLOG(PRI_DEBUG, BS_NODE, NW52, "TInvokeRequestHandlerActor::ReconfigStateStorage",
                (StateStorageConfig, cmd));

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        if (cmd.HasSchemeBoardConfig()) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "Board and SchemeBoard are not supported");
            return;   
        }
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

                bool found = false;
                Y_ABORT_UNLESS(newSSInfo->RingGroups.size() > 0 && oldSSInfo->RingGroups.size() > 0);
                auto& firstGroup = newSSInfo->RingGroups[0];
                for (auto& rg : oldSSInfo->RingGroups) {
                    if (firstGroup == rg) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << 
                        "New " << name << " configuration first ring group should be equal to old config old:" << oldSSInfo->ToString() <<" new: " << newSSInfo->ToString());
                    return false;
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

        config.SetGeneration(config.GetGeneration() + 1);
        StartProposition(&config);
    }

} // NKikimr::NStorage
