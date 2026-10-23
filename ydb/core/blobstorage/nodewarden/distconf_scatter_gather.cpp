#include "distconf.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    namespace {
        bool IsTimedScatterRequest(const NKikimrBlobStorage::TEvNodeConfigScatter& request) {
            return request.HasCollectConfigs() || request.HasProposeStorageConfig();
        }
    }

    void TDistributedConfigKeeper::IssueScatterTask(TScatterTaskOrigin&& origin, TEvScatter&& request,
            std::span<const TNodeIdentifier> targetedNodes) {
        ui64 cookie = NextScatterCookie++;
        if (cookie == 0) {
            cookie = NextScatterCookie++;
        }
        if (std::holds_alternative<TActorId>(origin)) {
            Y_ABORT_UNLESS(std::get<TActorId>(origin));
        }
        const bool timed = IsTimedScatterRequest(request);
        if (timed) {
            request.SetTimeoutUs(Min<ui64>(request.HasTimeoutUs() ? request.GetTimeoutUs() : 20'000'000, 20'000'000));
        }
        YDB_LOG_DEBUG("IssueScatterTask",
            {"marker", "NWDC21"},
            {"request", request},
            {"cookie", cookie},
            {"origin", origin},
            {"binding", Binding},
            {"scepter", Scepter ? std::make_optional(Scepter->Id) : std::nullopt},
            {"targetedNodes", targetedNodes});
        const auto [it, inserted] = ScatterTasks.try_emplace(cookie, std::move(origin), std::move(request), ScepterCounter,
            TActivationContext::Monotonic());
        Y_ABORT_UNLESS(inserted);
        TScatterTask& task = it->second;
        if (timed && !task.Request.GetTimeoutUs()) {
            CheckCompleteScatterTask(it);
            return;
        }
        if (timed) {
            const ui64 timeoutUs = task.Request.GetTimeoutUs();
            Schedule(TDuration::MicroSeconds(Min<ui64>(10'000'000, timeoutUs / 2)),
                     new TEvPrivate::TEvScatterTaskTimeout(cookie, true));
            Schedule(TDuration::MicroSeconds(timeoutUs), new TEvPrivate::TEvScatterTaskTimeout(cookie, false));
        }
        PrepareScatterTask(cookie, task);
        if (!std::holds_alternative<TScatterTaskOriginTargeted>(task.Origin)) { // issue scatter task downward unless this one is targeted
            for (auto& [nodeId, info] : DirectBoundNodes) {
                IssueScatterTaskForNode(nodeId, info, cookie, task);
            }
            for (const TNodeIdentifier& node : targetedNodes) {
                IssueAddedNodeScatterTask(node.NodeId(), cookie, task);
                const auto [it, inserted] = task.PendingNodes.insert(node.NodeId());
                Y_ABORT_UNLESS(inserted);
                AddedNodesScatterTasks.emplace(node.NodeId(), cookie);
            }
        }
        CheckCompleteScatterTask(it);
    }

    void TDistributedConfigKeeper::PrepareScatterTaskRequest(ui64 cookie, const TScatterTask& task,
            ui32 subtreeHeight, TEvScatter& request) {
        request.CopyFrom(task.Request);
        request.SetCookie(cookie);
        if (IsTimedScatterRequest(task.Request)) {
            const TMonotonic deadline = task.StartTimestamp + TDuration::MicroSeconds(task.Request.GetTimeoutUs());
            const TMonotonic now = TActivationContext::Monotonic();
            const ui64 remainingUs = now < deadline ? (deadline - now).MicroSeconds() : 0;
            const ui64 height = ui64(subtreeHeight) + 1;
            const ui64 localBudgetUs = remainingUs > 10'000'000 ? 10'000'000 : 0;
            request.SetTimeoutUs(localBudgetUs + (remainingUs - localBudgetUs) * height / (height + 1));
        }
    }

    void TDistributedConfigKeeper::IssueAddedNodeScatterTask(ui32 nodeId, ui64 cookie, TScatterTask& task) {
        auto ev = std::make_unique<TEvNodeConfigScatter>();
        PrepareScatterTaskRequest(cookie, task, 0, ev->Record);
        ev->Record.SetTargeted(true);
        auto interconnectSessionId = SubscribeToPeerNode(nodeId, TActorId());
        auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(nodeId), SelfId(), ev.release(), 0, 0);
        if (interconnectSessionId) {
            handle->Rewrite(TEvInterconnect::EvForward, interconnectSessionId);
        }
        TActivationContext::Send(handle.release());
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

    void TDistributedConfigKeeper::ExpireScatterTaskLocalContribution(TScatterTask& task) {
        task.LocalContributionExpired = true;
        task.AsyncOperationsPending = 0;
        if (const ui64 cookie = std::exchange(task.PersistCookie, 0)) {
            CancelPersistConfig(cookie);
        }
        if (const TActorId actorId = std::exchange(task.ConfigReaderActor, {})) {
            Send(actorId, new TEvents::TEvPoison);
        }
        if (const TActorId actorId = std::exchange(task.TargetedPropositionValidationActor, {})) {
            Send(actorId, new TEvents::TEvPoison);
        }
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvScatterTaskTimeout::TPtr ev) {
        const auto it = ScatterTasks.find(ev->Get()->Cookie);
        if (it == ScatterTasks.end()) {
            return;
        }
        TScatterTask& task = it->second;
        ExpireScatterTaskLocalContribution(task);
        if (!ev->Get()->LocalOnly) {
            for (const ui32 nodeId : std::exchange(task.PendingNodes, {})) {
                if (const auto jt = DirectBoundNodes.find(nodeId); jt != DirectBoundNodes.end()) {
                    Y_ABORT_UNLESS(jt->second.ScatterTasks.erase(it->first) == 1);
                    if (!jt->second.ReportedSubtreeHeight) {
                        jt->second.ExpiredScatterTasks.insert(it->first);
                    }
                } else {
                    Y_ABORT_UNLESS(AddedNodesScatterTasks.erase({nodeId, it->first}) == 1);
                    UnsubscribeQueue.insert(nodeId);
                }
            }
        }
        CheckCompleteScatterTask(it);
    }

    void TDistributedConfigKeeper::IssueScatterTaskForNode(ui32 nodeId, TBoundNode& info, ui64 cookie, TScatterTask& task) {
        if (!info.ReportedSubtreeHeight && !info.ExpiredScatterTasks.empty()
            && IsTimedScatterRequest(task.Request)) {
            return;
        }
        auto ev = std::make_unique<TEvNodeConfigScatter>();
        PrepareScatterTaskRequest(cookie, task, info.GetSubtreeHeight(), ev->Record);
        if (ev->Record.HasCollectConfigs() && !info.SupportsMetadataOnlyCollect) {
            ev->Record.MutableCollectConfigs()->SetMetadataOnly(false);
        }
        SendEvent(nodeId, info, std::move(ev));
        info.ScatterTasks.insert(cookie);
        task.PendingNodes.insert(nodeId);
    }

    void TDistributedConfigKeeper::CompleteScatterTask(TScatterTask& task) {
        YDB_LOG_DEBUG("CompleteScatterTask",
            {"marker", "NWDC22"},
            {"request", task.Request});

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
        YDB_LOG_DEBUG("AbortScatterTask",
            {"marker", "NWDC23"},
            {"cookie", cookie},
            {"nodeId", nodeId});

        const auto it = ScatterTasks.find(cookie);
        Y_VERIFY_S(it != ScatterTasks.end(), "Cookie# " << cookie << " NodeId# " << nodeId);
        TScatterTask& task = it->second;

        const size_t n = task.PendingNodes.erase(nodeId);
        Y_ABORT_UNLESS(n == 1);
        CheckCompleteScatterTask(it);
    }

    void TDistributedConfigKeeper::AbortAllScatterTasks(const std::optional<TBinding>& binding) {
        YDB_LOG_DEBUG("AbortAllScatterTasks",
            {"marker", "NWDC24"},
            {"binding", binding});

        for (auto& [cookie, task] : std::exchange(ScatterTasks, {})) {
            ExpireScatterTaskLocalContribution(task);
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
                    if (!info.ReportedSubtreeHeight && IsTimedScatterRequest(task.Request)) {
                        info.ExpiredScatterTasks.insert(cookie);
                    }
                } else {
                    const size_t n = AddedNodesScatterTasks.erase({nodeId, cookie});
                    Y_ABORT_UNLESS(n == 1);
                    UnsubscribeQueue.insert(nodeId);
                }
            }
        }

        Y_ABORT_UNLESS(AddedNodesScatterTasks.empty());
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigScatter::TPtr ev) {
        YDB_LOG_DEBUG("TEvNodeConfigScatter",
            {"marker", "NWDC25"},
            {"binding", Binding},
            {"sender", ev->Sender},
            {"cookie", ev->Cookie},
            {"sessionId", ev->InterconnectSession},
            {"record", ev->Get()->Record});

        if (auto& record = ev->Get()->Record; Binding && Binding->Expected(*ev)) {
            IssueScatterTask(*Binding, std::move(record));
        } else if (record.GetTargeted()) {
            IssueScatterTask(TScatterTaskOriginTargeted{ev->Sender, ev->Cookie, ev->InterconnectSession}, std::move(record));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigGather::TPtr ev) {
        YDB_LOG_DEBUG("TEvNodeConfigGather",
            {"marker", "NWDC26"},
            {"sender", ev->Sender},
            {"cookie", ev->Cookie},
            {"sessionId", ev->InterconnectSession},
            {"record", ev->Get()->Record});

        auto& record = ev->Get()->Record;
        const ui64 cookie = record.GetCookie();

        const ui32 senderNodeId = ev->Sender.NodeId();
        const auto it = DirectBoundNodes.find(senderNodeId);

        if (it != DirectBoundNodes.end() ? it->second.Expected(*ev) : AddedNodesScatterTasks.erase({senderNodeId, cookie})) {
            if (it != DirectBoundNodes.end() && it->second.ExpiredScatterTasks.erase(cookie)) {
                return;
            }
            if (const auto jt = ScatterTasks.find(cookie); jt != ScatterTasks.end()) {
                if (it != DirectBoundNodes.end()) {
                    const size_t n = it->second.ScatterTasks.erase(cookie);
                    Y_ABORT_UNLESS(n == 1);
                }

                TScatterTask& task = jt->second;
                const bool unexpectedMetadata = record.HasCollectConfigs() && record.GetCollectConfigs().GetMetadataOnly()
                    && (!task.Request.HasCollectConfigs() || !task.Request.GetCollectConfigs().GetMetadataOnly()
                        || it == DirectBoundNodes.end() || !it->second.SupportsMetadataOnlyCollect);
                if (!unexpectedMetadata) {
                    record.Swap(&task.CollectedResponses.emplace_back());
                }
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
