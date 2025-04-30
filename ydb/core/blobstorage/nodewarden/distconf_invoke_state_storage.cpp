#include "distconf_invoke.h"

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::ReconfigStateStorage(const TQuery::TReconfigStateStorage& cmd) {
        if (!RunCommonChecks()) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "CommonChecks are not passed");
            return;
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        const auto &newSSConfig = cmd.GetNewStateStorageConfig();

        if (newSSConfig.HasRing()) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "New configuration Ring option is not allowed use RingGroups");
            return;
        }
        if (newSSConfig.RingGroupsSize() < 1) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "New configuration RingGroups is not filled in");
            return;
        }
        if (newSSConfig.GetRingGroups(0).GetWriteOnly()) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "New configuration first RingGroup is writeOnly");
            return;
        }
        if (!config.HasStateStorageConfig()) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "StateStorage configuration is not filled in");
            return;
        }
        //TODO: Validate new config at least 1 ringGroup in ReadWrite state should be not changed 

        config.MutableStateStorageConfig()->CopyFrom(newSSConfig);
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
                auto *ringGroup = m->MutableRingGroups(i);
                auto *ring = ringGroup->MutableRing();
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
