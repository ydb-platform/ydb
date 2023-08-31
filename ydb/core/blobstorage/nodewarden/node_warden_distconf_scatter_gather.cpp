#include "node_warden_distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::IssueScatterTask(bool locallyGenerated, NKikimrBlobStorage::TEvNodeConfigScatter&& task) {
        const ui64 cookie = NextScatterCookie++;
        STLOG(PRI_DEBUG, BS_NODE, NWDC21, "IssueScatterTask", (Task, task), (Cookie, cookie));
        Y_VERIFY(locallyGenerated || Binding);
        const auto [it, inserted] = ScatterTasks.try_emplace(cookie, locallyGenerated ? std::nullopt : Binding,
            std::move(task));
        Y_VERIFY(inserted);
        TScatterTask& scatterTask = it->second;
        for (auto& [nodeId, info] : DirectBoundNodes) {
            IssueScatterTaskForNode(nodeId, info, cookie, scatterTask);
        }
        if (scatterTask.PendingNodes.empty()) {
            CompleteScatterTask(scatterTask);
            ScatterTasks.erase(it);
        }
    }

    void TDistributedConfigKeeper::IssueScatterTaskForNode(ui32 nodeId, TBoundNode& info, ui64 cookie, TScatterTask& scatterTask) {
        auto ev = std::make_unique<TEvNodeConfigScatter>();
        ev->Record.CopyFrom(scatterTask.Task);
        ev->Record.SetCookie(cookie);
        SendEvent(nodeId, info, std::move(ev));
        info.ScatterTasks.insert(cookie);
        scatterTask.PendingNodes.insert(nodeId);
    }

    void TDistributedConfigKeeper::CompleteScatterTask(TScatterTask& task) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC22, "CompleteScatterTask", (Task, task.Task));

        // some state checks
        if (task.Origin) {
            Y_VERIFY(Binding); // when binding is dropped, all scatter tasks must be dropped too
            Y_VERIFY(Binding == task.Origin); // binding must not change
        }

        NKikimrBlobStorage::TEvNodeConfigGather res;
        if (task.Task.HasCookie()) {
            res.SetCookie(task.Task.GetCookie());
        }

        switch (task.Task.GetRequestCase()) {
            case NKikimrBlobStorage::TEvNodeConfigScatter::kCollectConfigs:
                GenerateCollectConfigs(res.MutableCollectConfigs(), task);
                break;

            case NKikimrBlobStorage::TEvNodeConfigScatter::kProposeStorageConfig:
                break;

            case NKikimrBlobStorage::TEvNodeConfigScatter::kCommitStorageConfig:
                break;

            case NKikimrBlobStorage::TEvNodeConfigScatter::REQUEST_NOT_SET:
                // unexpected case
                break;
        }

        if (task.Origin) {
            auto reply = std::make_unique<TEvNodeConfigGather>();
            res.Swap(&reply->Record);
            SendEvent(*Binding, std::move(reply));
        } else {
            ProcessGather(&res);
        }
    }

    void TDistributedConfigKeeper::GenerateCollectConfigs(NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs *response, TScatterTask& task) {
        THashMap<std::tuple<ui64, TString>, NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs::TItem*> configs;

        auto addConfig = [&](const NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs::TItem& item) {
            const auto& config = item.GetConfig();
            const auto key = std::make_tuple(config.GetGeneration(), config.GetFingerprint());
            auto& ptr = configs[key];
            if (!ptr) {
                ptr = response->AddItems();
                ptr->MutableConfig()->CopyFrom(config);
            }
            for (const auto& node : item.GetNodes()) {
                ptr->AddNodes()->CopyFrom(node);
            }
        };

        NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs::TItem s;
        auto *node = s.AddNodes();
        node->SetHost(SelfHost);
        node->SetPort(SelfPort);
        node->SetNodeId(SelfId().NodeId());
        auto *cfg = s.MutableConfig();
        cfg->CopyFrom(StorageConfig);
        addConfig(s);

        for (const auto& reply : task.CollectedReplies) {
            if (reply.HasCollectConfigs()) {
                for (const auto& item : reply.GetCollectConfigs().GetItems()) {
                    addConfig(item);
                }
            }
        }
    }

    void TDistributedConfigKeeper::AbortScatterTask(ui64 cookie, ui32 nodeId) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC23, "AbortScatterTask", (Cookie, cookie), (NodeId, nodeId));

        const auto it = ScatterTasks.find(cookie);
        Y_VERIFY(it != ScatterTasks.end());
        TScatterTask& task = it->second;

        const size_t n = task.PendingNodes.erase(nodeId);
        Y_VERIFY(n == 1);
        if (task.PendingNodes.empty()) {
            CompleteScatterTask(task);
            ScatterTasks.erase(it);
        }
    }

    void TDistributedConfigKeeper::AbortAllScatterTasks(const TBinding& binding) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC24, "AbortAllScatterTasks", (Binding, binding));

        for (auto& [cookie, task] : std::exchange(ScatterTasks, {})) {
            Y_VERIFY(task.Origin);
            Y_VERIFY(task.Origin == binding);

            for (const ui32 nodeId : task.PendingNodes) {
                const auto it = DirectBoundNodes.find(nodeId);
                Y_VERIFY(it != DirectBoundNodes.end());
                TBoundNode& info = it->second;
                const size_t n = info.ScatterTasks.erase(cookie);
                Y_VERIFY(n == 1);
            }
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigScatter::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC25, "TEvNodeConfigScatter", (Binding, Binding), (Sender, ev->Sender),
            (Cookie, ev->Cookie), (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

        if (Binding && Binding->Expected(*ev)) {
            IssueScatterTask(false, std::move(ev->Get()->Record));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigGather::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC26, "TEvNodeConfigGather", (Sender, ev->Sender), (Cookie, ev->Cookie),
            (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

        const ui32 senderNodeId = ev->Sender.NodeId();
        if (const auto it = DirectBoundNodes.find(senderNodeId); it != DirectBoundNodes.end() && it->second.Expected(*ev)) {
            TBoundNode& info = it->second;
            auto& record = ev->Get()->Record;
            if (const auto jt = ScatterTasks.find(record.GetCookie()); jt != ScatterTasks.end()) {
                const size_t n = info.ScatterTasks.erase(jt->first);
                Y_VERIFY(n == 1);

                TScatterTask& task = jt->second;
                record.Swap(&task.CollectedReplies.emplace_back());
                const size_t m = task.PendingNodes.erase(senderNodeId);
                Y_VERIFY(m == 1);
                if (task.PendingNodes.empty()) {
                    CompleteScatterTask(task);
                    ScatterTasks.erase(jt);
                }
            } else {
                Y_VERIFY_DEBUG(false);
            }
        }
    }

} // NKikimr::NStorage
