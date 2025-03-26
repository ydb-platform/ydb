#include "distconf_invoke.h"

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

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

            auto *m = (config.*mutableFunc)();
            auto *ring = m->MutableRing();
            if (ring->RingSize() && ring->NodeSize()) {
                FinishWithError(TResult::ERROR, TStringBuilder() << name << " incorrect configuration:"
                    " both Ring and Node fields are set");
                return false;
            }

            const size_t numItems = Max(ring->RingSize(), ring->NodeSize());
            bool found = false;

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
