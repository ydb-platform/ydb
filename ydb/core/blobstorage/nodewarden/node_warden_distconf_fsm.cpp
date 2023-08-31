#include "node_warden_distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
//        if (RootState == ERootState::INITIAL && !Binding && HasQuorum()) {
//            STLOG(PRI_DEBUG, BS_NODE, NWDC18, "Starting QUORUM_CHECK_TIMEOUT");
//            TActivationContext::Schedule(QuorumCheckTimeout, new IEventHandle(TEvPrivate::EvQuorumCheckTimeout, 0,
//                SelfId(), {}, nullptr, 0));
//            RootState = ERootState::QUORUM_CHECK_TIMEOUT;
//        }
    }

    void TDistributedConfigKeeper::HandleQuorumCheckTimeout() {
        if (HasQuorum()) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Quorum check timeout hit, quorum remains");

            RootState = ERootState::COLLECT_CONFIG;

            NKikimrBlobStorage::TEvNodeConfigScatter task;
            task.MutableCollectConfigs();
            IssueScatterTask(true, std::move(task));
        } else {
            STLOG(PRI_DEBUG, BS_NODE, NWDC20, "Quorum check timeout hit, quorum reset");
            RootState = ERootState::INITIAL; // fall back to waiting for quorum
            IssueNextBindRequest();
        }
    }

    void TDistributedConfigKeeper::ProcessGather(NKikimrBlobStorage::TEvNodeConfigGather *res) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC27, "ProcessGather", (RootState, RootState), (Res, *res));

        switch (RootState) {
            case ERootState::COLLECT_CONFIG:
                if (res->HasCollectConfigs()) {
                    ProcessCollectConfigs(std::move(res->MutableCollectConfigs()));
                } else {
                    // unexpected reply?
                }
                break;

            case ERootState::PROPOSE_NEW_STORAGE_CONFIG:

            default:
                break;
        }
    }

    bool TDistributedConfigKeeper::HasQuorum() const {
        // we have strict majority of all nodes (including this one)
        return AllBoundNodes.size() + 1 > (NodeIds.size() + 1) / 2;
    }

    void TDistributedConfigKeeper::ProcessCollectConfigs(NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs *res) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (Res, *res));

    }

} // NKikimr::NStorage
