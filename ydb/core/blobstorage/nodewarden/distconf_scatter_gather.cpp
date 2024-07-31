#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::IssueScatterTask(std::optional<TActorId> actorId, TEvScatter&& request) {
        ui64 cookie = NextScatterCookie++;
        if (cookie == 0) {
            cookie = NextScatterCookie++;
        }
        STLOG(PRI_DEBUG, BS_NODE, NWDC21, "IssueScatterTask", (Request, request), (Cookie, cookie));
        Y_ABORT_UNLESS(actorId || Binding);
        const auto [it, inserted] = ScatterTasks.try_emplace(cookie, actorId ? std::nullopt : Binding, std::move(request),
            Scepter, actorId.value_or(TActorId()));
        Y_ABORT_UNLESS(inserted);
        TScatterTask& task = it->second;
        PrepareScatterTask(cookie, task);
        for (auto& [nodeId, info] : DirectBoundNodes) {
            IssueScatterTaskForNode(nodeId, info, cookie, task);
        }
        CheckCompleteScatterTask(it);
    }

    void TDistributedConfigKeeper::CheckCompleteScatterTask(TScatterTasks::iterator it) {
        TScatterTask& task = it->second;
        if (task.PendingNodes.empty() && !task.AsyncOperationsPending) {
            TScatterTask temp = std::move(task);
            ScatterTasks.erase(it);
            CompleteScatterTask(temp);
        }
    }

    void TDistributedConfigKeeper::FinishAsyncOperation(ui64 cookie) {
        if (const auto it = ScatterTasks.find(cookie); it != ScatterTasks.end()) {
            TScatterTask& task = it->second;
            Y_ABORT_UNLESS(task.AsyncOperationsPending);
            if (!--task.AsyncOperationsPending) {
                CheckCompleteScatterTask(it);
            }
        }
    }

    void TDistributedConfigKeeper::IssueScatterTaskForNode(ui32 nodeId, TBoundNode& info, ui64 cookie, TScatterTask& task) {
        auto ev = std::make_unique<TEvNodeConfigScatter>();
        ev->Record.CopyFrom(task.Request);
        ev->Record.SetCookie(cookie);
        SendEvent(nodeId, info, std::move(ev));
        info.ScatterTasks.insert(cookie);
        task.PendingNodes.insert(nodeId);
    }

    void TDistributedConfigKeeper::CompleteScatterTask(TScatterTask& task) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC22, "CompleteScatterTask", (Request, task.Request));

        if (task.Origin) {
            Y_ABORT_UNLESS(Binding); // when binding is dropped, all scatter tasks must be dropped too
            Y_ABORT_UNLESS(Binding == task.Origin); // binding must not change
        }

        PerformScatterTask(task); // do the local part

        if (task.Origin) {
            auto reply = std::make_unique<TEvNodeConfigGather>();
            task.Response.Swap(&reply->Record);
            SendEvent(*Binding, std::move(reply));
        } else if (task.ActorId) {
            auto ev = std::make_unique<TEvNodeConfigGather>();
            task.Response.Swap(&ev->Record);
            Send(task.ActorId, ev.release());
        } else {
            ProcessGather(task.Scepter.lock() == Scepter ? &task.Response : nullptr);
        }
    }

    void TDistributedConfigKeeper::AbortScatterTask(ui64 cookie, ui32 nodeId) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC23, "AbortScatterTask", (Cookie, cookie), (NodeId, nodeId));

        const auto it = ScatterTasks.find(cookie);
        Y_ABORT_UNLESS(it != ScatterTasks.end());
        TScatterTask& task = it->second;

        const size_t n = task.PendingNodes.erase(nodeId);
        Y_ABORT_UNLESS(n == 1);
        CheckCompleteScatterTask(it);
    }

    void TDistributedConfigKeeper::AbortAllScatterTasks(const TBinding& binding) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC24, "AbortAllScatterTasks", (Binding, binding));

        for (auto& [cookie, task] : std::exchange(ScatterTasks, {})) {
            Y_ABORT_UNLESS(task.Origin);
            Y_ABORT_UNLESS(task.Origin == binding);

            for (const ui32 nodeId : task.PendingNodes) {
                const auto it = DirectBoundNodes.find(nodeId);
                Y_ABORT_UNLESS(it != DirectBoundNodes.end());
                TBoundNode& info = it->second;
                const size_t n = info.ScatterTasks.erase(cookie);
                Y_ABORT_UNLESS(n == 1);
            }
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigScatter::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC25, "TEvNodeConfigScatter", (Binding, Binding), (Sender, ev->Sender),
            (Cookie, ev->Cookie), (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

        if (Binding && Binding->Expected(*ev)) {
            IssueScatterTask(std::nullopt, std::move(ev->Get()->Record));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigGather::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC26, "TEvNodeConfigGather", (Sender, ev->Sender), (Cookie, ev->Cookie),
            (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

        const ui32 senderNodeId = ev->Sender.NodeId();
        if (const auto it = DirectBoundNodes.find(senderNodeId); it != DirectBoundNodes.end() && it->second.Expected(*ev)) {
            TBoundNode& info = it->second;
            auto& record = ev->Get()->Record;
            const ui64 cookie = record.GetCookie();
            if (const auto jt = ScatterTasks.find(cookie); jt != ScatterTasks.end()) {
                const size_t n = info.ScatterTasks.erase(cookie);
                Y_ABORT_UNLESS(n == 1);

                TScatterTask& task = jt->second;
                record.Swap(&task.CollectedResponses.emplace_back());
                const size_t m = task.PendingNodes.erase(senderNodeId);
                Y_ABORT_UNLESS(m == 1);
                CheckCompleteScatterTask(jt);
            } else {
                Y_DEBUG_ABORT_UNLESS(!info.ScatterTasks.contains(cookie));
            }
        }
    }

} // NKikimr::NStorage
