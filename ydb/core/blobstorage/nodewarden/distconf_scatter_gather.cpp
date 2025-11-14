#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::IssueScatterTask(TScatterTaskOrigin&& origin, TEvScatter&& request,
            std::span<TNodeIdentifier> addedNodes) {
        ui64 cookie = NextScatterCookie++;
        if (cookie == 0) {
            cookie = NextScatterCookie++;
        }
        if (std::holds_alternative<TActorId>(origin)) {
            Y_ABORT_UNLESS(std::get<TActorId>(origin));
        }
        STLOG(PRI_DEBUG, BS_NODE, NWDC21, "IssueScatterTask", (Request, request), (Cookie, cookie), (Origin, origin),
            (Binding, Binding), (Scepter, Scepter ? std::make_optional(Scepter->Id) : std::nullopt),
            (AddedNodes, addedNodes));
        const auto [it, inserted] = ScatterTasks.try_emplace(cookie, std::move(origin), std::move(request), ScepterCounter);
        Y_ABORT_UNLESS(inserted);
        TScatterTask& task = it->second;
        PrepareScatterTask(cookie, task);
        if (!std::holds_alternative<TScatterTaskOriginTargeted>(task.Origin)) { // issue scatter task downward unless this one is targeted
            for (auto& [nodeId, info] : DirectBoundNodes) {
                IssueScatterTaskForNode(nodeId, info, cookie, task);
            }
            for (const TNodeIdentifier& nodeId : addedNodes) {
                auto ev = std::make_unique<TEvNodeConfigScatter>();
                ev->Record.CopyFrom(task.Request);
                ev->Record.SetCookie(cookie);
                ev->Record.SetTargeted(true);
                auto interconnectSessionId = SubscribeToPeerNode(nodeId.NodeId(), TActorId());
                auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(nodeId.NodeId()), SelfId(),
                    ev.release(), 0, 0);
                if (interconnectSessionId) {
                    handle->Rewrite(TEvInterconnect::EvForward, interconnectSessionId);
                }
                TActivationContext::Send(handle.release());
                const auto [it, inserted] = task.PendingNodes.insert(nodeId.NodeId());
                Y_ABORT_UNLESS(inserted);
                AddedNodesScatterTasks.emplace(nodeId.NodeId(), cookie);
            }
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

        if (std::holds_alternative<TBinding>(task.Origin)) {
            Y_ABORT_UNLESS(Binding); // when binding is dropped, all scatter tasks must be dropped too
            Y_ABORT_UNLESS(Binding == std::get<TBinding>(task.Origin)); // binding must not change
        }

        PerformScatterTask(task); // do the local part

        std::visit(TOverloaded{
            [&](const TBinding& binding) {
                auto reply = std::make_unique<TEvNodeConfigGather>();
                task.Response.Swap(&reply->Record);
                SendEvent(binding, std::move(reply));
            },
            [&](const TActorId& actorId) {
                auto ev = std::make_unique<TEvNodeConfigGather>();
                task.Response.Swap(&ev->Record);
                Send(actorId, ev.release());
            },
            [&](const TScatterTaskOriginFsm&) {
                ProcessGather(task.ScepterCounter == ScepterCounter ? &task.Response : nullptr);
            },
            [&](const TScatterTaskOriginTargeted& origin) {
                auto reply = std::make_unique<TEvNodeConfigGather>();
                task.Response.Swap(&reply->Record);
                auto handle = std::make_unique<IEventHandle>(origin.Sender, SelfId(), reply.release(), 0, origin.Cookie);
                Y_ABORT_UNLESS(origin.InterconnectSessionId);
                handle->Rewrite(TEvInterconnect::EvForward, origin.InterconnectSessionId);
                TActivationContext::Send(handle.release());
            }
        }, task.Origin);
    }

    void TDistributedConfigKeeper::AbortScatterTask(ui64 cookie, ui32 nodeId) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC23, "AbortScatterTask", (Cookie, cookie), (NodeId, nodeId));

        const auto it = ScatterTasks.find(cookie);
        Y_VERIFY_S(it != ScatterTasks.end(), "Cookie# " << cookie << " NodeId# " << nodeId);
        TScatterTask& task = it->second;

        const size_t n = task.PendingNodes.erase(nodeId);
        Y_ABORT_UNLESS(n == 1);
        CheckCompleteScatterTask(it);
    }

    void TDistributedConfigKeeper::AbortAllScatterTasks(const std::optional<TBinding>& binding) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC24, "AbortAllScatterTasks", (Binding, binding));

        for (auto& [cookie, task] : std::exchange(ScatterTasks, {})) {
            auto getAborted = [&] {
                auto ev = std::make_unique<TEvNodeConfigGather>();
                task.Response.SetAborted(true);
                task.Response.Swap(&ev->Record);
                return ev.release();
            };
            std::visit(TOverloaded{
                [&](const TBinding& origin) {
                    Y_ABORT_UNLESS(origin == binding);
                },
                [&](const TActorId& actorId) { // terminate the task prematurely -- and notify actor
                    Y_ABORT_UNLESS(!binding);
                    Send(actorId, getAborted());
                },
                [&](const TScatterTaskOriginFsm&) {
                },
                [&](const TScatterTaskOriginTargeted& origin) { // notify operation aborted
                    auto handle = std::make_unique<IEventHandle>(origin.Sender, SelfId(), getAborted(), 0, origin.Cookie);
                    Y_ABORT_UNLESS(origin.InterconnectSessionId);
                    handle->Rewrite(TEvInterconnect::EvForward, origin.InterconnectSessionId);
                    TActivationContext::Send(handle.release());
                }
            }, task.Origin);

            for (const ui32 nodeId : task.PendingNodes) {
                if (const auto it = DirectBoundNodes.find(nodeId); it != DirectBoundNodes.end()) {
                    TBoundNode& info = it->second;
                    const size_t n = info.ScatterTasks.erase(cookie);
                    Y_ABORT_UNLESS(n == 1);
                } else {
                    const size_t n = AddedNodesScatterTasks.erase({nodeId, cookie});
                    Y_ABORT_UNLESS(n == 1);
                }
            }
        }

        Y_ABORT_UNLESS(AddedNodesScatterTasks.empty());
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigScatter::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC25, "TEvNodeConfigScatter", (Binding, Binding), (Sender, ev->Sender),
            (Cookie, ev->Cookie), (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

        if (auto& record = ev->Get()->Record; Binding && Binding->Expected(*ev)) {
            IssueScatterTask(*Binding, std::move(record));
        } else if (record.GetTargeted()) {
            IssueScatterTask(TScatterTaskOriginTargeted{ev->Sender, ev->Cookie, ev->InterconnectSession}, std::move(record));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigGather::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC26, "TEvNodeConfigGather", (Sender, ev->Sender), (Cookie, ev->Cookie),
            (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

        auto& record = ev->Get()->Record;
        const ui64 cookie = record.GetCookie();

        const ui32 senderNodeId = ev->Sender.NodeId();
        const auto it = DirectBoundNodes.find(senderNodeId);

        if (it != DirectBoundNodes.end() ? it->second.Expected(*ev) : AddedNodesScatterTasks.erase({senderNodeId, cookie})) {
            if (const auto jt = ScatterTasks.find(cookie); jt != ScatterTasks.end()) {
                if (it != DirectBoundNodes.end()) {
                    const size_t n = it->second.ScatterTasks.erase(cookie);
                    Y_ABORT_UNLESS(n == 1);
                }

                TScatterTask& task = jt->second;
                record.Swap(&task.CollectedResponses.emplace_back());
                const size_t m = task.PendingNodes.erase(senderNodeId);
                Y_ABORT_UNLESS(m == 1);
                CheckCompleteScatterTask(jt);
            }
            if (it == DirectBoundNodes.end()) { // maybe we don't need this node subscribed anymore
                UnsubscribeInterconnect(senderNodeId);
            }
        }
    }

} // NKikimr::NStorage
