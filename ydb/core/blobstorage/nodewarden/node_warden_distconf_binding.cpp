#include "node_warden_distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC11, "TEvNodesInfo");

        // create a vector of peer static nodes
        bool iAmStatic = false;
        std::vector<ui32> nodeIds;
        const ui32 selfNodeId = SelfId().NodeId();
        for (const auto& item : ev->Get()->Nodes) {
            if (item.NodeId == selfNodeId) {
                iAmStatic = item.IsStatic;
                SelfHost = item.ResolveHost;
                SelfPort = item.Port;
            } else if (item.IsStatic) {
                nodeIds.push_back(item.NodeId);
            }
        }
        std::sort(nodeIds.begin(), nodeIds.end());

        // do not start configuration negotiation for dynamic nodes
        if (!iAmStatic) {
            Y_VERIFY(NodeIds.empty());
            return;
        }

        // check if some nodes were deleted -- we have to unbind them
        bool bindingReset = false;
        bool changes = false;
        for (auto prevIt = NodeIds.begin(), curIt = nodeIds.begin(); prevIt != NodeIds.end() || curIt != nodeIds.end(); ) {
            if (prevIt == NodeIds.end() || *curIt < *prevIt) { // node added
                ++curIt;
                changes = true;
            } else if (curIt == NodeIds.end() || *prevIt < *curIt) { // node deleted
                const ui32 nodeId = *prevIt++;
                UnbindNode(nodeId, "node vanished");
                if (Binding && Binding->NodeId == nodeId) {
                    bindingReset = true;
                }
                changes = true;
            } else {
                Y_VERIFY(*prevIt == *curIt);
                ++prevIt;
                ++curIt;
            }
        }

        if (!changes) {
            return;
        }

        if (bindingReset) {
            AbortBinding("node vanished");
        }

        // issue updates
        NodeIds = std::move(nodeIds);
        BindQueue.Update(NodeIds);
        IssueNextBindRequest();
    }

    void TDistributedConfigKeeper::IssueNextBindRequest() {
        CheckRootNodeStatus();
        if (!Binding && AllBoundNodes.size() < NodeIds.size() && RootState == ERootState::INITIAL) {
            const TMonotonic now = TActivationContext::Monotonic();
            TMonotonic closest;
            if (std::optional<ui32> nodeId = BindQueue.Pick(now, &closest)) {
                Binding.emplace(*nodeId, ++BindingCookie);

                if (const auto [it, inserted] = SubscribedSessions.try_emplace(Binding->NodeId); it->second) {
                    Binding->SessionId = it->second;
                    SendEvent(*Binding, std::make_unique<TEvNodeConfigPush>(AllBoundNodes));
                } else if (inserted) {
                    TActivationContext::Send(new IEventHandle(TEvInterconnect::EvConnectNode, 0,
                        TActivationContext::InterconnectProxy(Binding->NodeId), SelfId(), nullptr,
                        it->first));
                }

                STLOG(PRI_DEBUG, BS_NODE, NWDC29, "Initiated bind", (Binding, Binding));
            } else if (closest != TMonotonic::Max() && !Scheduled) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC30, "Delaying bind");
                TActivationContext::Schedule(closest, new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {}, nullptr, 0));
                Scheduled = true;
            } else {
                STLOG(PRI_DEBUG, BS_NODE, NWDC01, "No bind options");
            }
        }
    }

    void TDistributedConfigKeeper::Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        const TActorId sessionId = ev->Sender;

        STLOG(PRI_DEBUG, BS_NODE, NWDC14, "TEvNodeConnected", (NodeId, nodeId));

        // update subscription information
        const auto [it, inserted] = SubscribedSessions.try_emplace(nodeId, sessionId);
        Y_VERIFY(!inserted);
        Y_VERIFY(!it->second);
        it->second = sessionId;

        if (Binding && Binding->NodeId == nodeId) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC09, "Continuing bind", (Binding, Binding));
            Binding->SessionId = ev->Sender;
            SendEvent(*Binding, std::make_unique<TEvNodeConfigPush>(AllBoundNodes));
        }

        // in case of obsolete subscriptions
        UnsubscribeInterconnect(nodeId);
    }

    void TDistributedConfigKeeper::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;

        const auto it = SubscribedSessions.find(nodeId);
        Y_VERIFY(it != SubscribedSessions.end());
        Y_VERIFY(!it->second || it->second == ev->Sender);
        SubscribedSessions.erase(it);

        STLOG(PRI_DEBUG, BS_NODE, NWDC07, "TEvNodeDisconnected", (NodeId, nodeId));

        UnbindNode(nodeId, "disconnection");

        if (Binding && Binding->NodeId == nodeId) {
            AbortBinding("disconnection", false);
        }
    }

    void TDistributedConfigKeeper::Handle(TEvents::TEvUndelivered::TPtr ev) {
        auto& msg = *ev->Get();
        if (msg.SourceType == TEvents::TSystem::Subscribe) {
            const ui32 nodeId = ev->Cookie;
            STLOG(PRI_DEBUG, BS_NODE, NWDC15, "TEvUndelivered for subscription", (NodeId, nodeId));
            UnbindNode(nodeId, "disconnection");
        }
    }

    void TDistributedConfigKeeper::UnsubscribeInterconnect(ui32 nodeId) {
        if (Binding && Binding->NodeId == nodeId) {
            return;
        }
        if (DirectBoundNodes.contains(nodeId)) {
            return;
        }
        if (const auto it = SubscribedSessions.find(nodeId); it != SubscribedSessions.end() && it->second) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, it->second, SelfId(), nullptr, 0));
            SubscribedSessions.erase(it);
        }
    }

    void TDistributedConfigKeeper::AbortBinding(const char *reason, bool sendUnbindMessage) {
        if (Binding) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC03, "AbortBinding", (Binding, Binding), (Reason, reason));

            const TBinding binding = *std::exchange(Binding, std::nullopt);

            if (binding.SessionId && sendUnbindMessage) {
                SendEvent(binding, std::make_unique<TEvNodeConfigUnbind>());
            }

            AbortAllScatterTasks(binding);

            IssueNextBindRequest();

            for (const auto& [nodeId, info] : DirectBoundNodes) {
                SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId()));
            }

            UnsubscribeInterconnect(binding.NodeId);
        }
    }

    void TDistributedConfigKeeper::HandleWakeup() {
        Y_VERIFY(Scheduled);
        Scheduled = false;
        IssueNextBindRequest();
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigReversePush::TPtr ev) {
        const ui32 senderNodeId = ev->Sender.NodeId();
        Y_VERIFY(senderNodeId != SelfId().NodeId());
        auto& record = ev->Get()->Record;

        STLOG(PRI_DEBUG, BS_NODE, NWDC17, "TEvNodeConfigReversePush", (NodeId, senderNodeId), (Cookie, ev->Cookie),
            (SessionId, ev->InterconnectSession), (Binding, Binding), (Record, record));

        if (!Binding || !Binding->Expected(*ev)) {
            return; // possible race with unbinding
        }

        Y_VERIFY(Binding->RootNodeId || ScatterTasks.empty());

        // check if this binding was accepted and if it is acceptable from our point of view
        if (record.GetRejected()) {
            AbortBinding("binding rejected by peer", false);
        } else if (const ui32 prevRootNodeId = std::exchange(Binding->RootNodeId, record.GetRootNodeId()); prevRootNodeId != Binding->RootNodeId) {
            if (Binding->RootNodeId == SelfId().NodeId()) {
                AbortBinding("binding cycle");
            } else {
                STLOG(PRI_DEBUG, BS_NODE, NWDC13, "Binding updated", (Binding, Binding), (PrevRootNodeId, prevRootNodeId));
            }
            if (prevRootNodeId != GetRootNodeId()) {
                for (const auto& [nodeId, info] : DirectBoundNodes) {
                    SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId()));
                }
            }
        }
    }

    void TDistributedConfigKeeper::AddBound(ui32 nodeId, TEvNodeConfigPush *msg) {
        if (const auto [it, inserted] = AllBoundNodes.try_emplace(nodeId, 1); inserted) {
            if (msg) {
                msg->Record.AddNewBoundNodeIds(nodeId);
            }
            if (nodeId != SelfId().NodeId()) {
                BindQueue.Disable(nodeId);
            }
        } else {
            ++it->second;
        }
    }

    void TDistributedConfigKeeper::DeleteBound(ui32 nodeId, TEvNodeConfigPush *msg) {
        const auto it = AllBoundNodes.find(nodeId);
        Y_VERIFY(it != AllBoundNodes.end());
        if (!--it->second) {
            AllBoundNodes.erase(it);
            if (msg) {
                msg->Record.AddDeletedBoundNodeIds(nodeId);
            }
            if (nodeId != SelfId().NodeId()) {
                BindQueue.Enable(nodeId);
            }
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigPush::TPtr ev) {
        const ui32 senderNodeId = ev->Sender.NodeId();
        Y_VERIFY(senderNodeId != SelfId().NodeId());
        auto& record = ev->Get()->Record;

        STLOG(PRI_DEBUG, BS_NODE, NWDC02, "TEvNodeConfigPush", (NodeId, senderNodeId), (Cookie, ev->Cookie),
            (SessionId, ev->InterconnectSession), (Binding, Binding), (Record, record));

        // check if we can't accept this message (or else it would make a cycle)
        if (record.GetInitial() && senderNodeId == GetRootNodeId()) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC28, "TEvNodeConfigPush rejected", (NodeId, senderNodeId),
                (Cookie, ev->Cookie), (SessionId, ev->InterconnectSession), (Binding, Binding),
                (Record, record));
            SendEvent(*ev, TEvNodeConfigReversePush::MakeRejected());
            return;
        }

        if (!record.GetInitial() && !DirectBoundNodes.contains(senderNodeId)) {
            return; // just a race with rejected request
        }

        // abort current outgoing binding if there is a race
        if (Binding && senderNodeId == Binding->NodeId && senderNodeId < SelfId().NodeId()) {
            AbortBinding("mutual binding");
        }

        // subscribe for interconnect session
        if (const auto [it, inserted] = SubscribedSessions.try_emplace(senderNodeId); inserted) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Subscribe, IEventHandle::FlagTrackDelivery,
                ev->InterconnectSession, SelfId(), nullptr, senderNodeId));
        } else {
            Y_VERIFY(!it->second || it->second == ev->InterconnectSession);
        }

        std::unique_ptr<TEvNodeConfigPush> pushEv;
        auto getPushEv = [&] {
            if (Binding && Binding->SessionId && !pushEv) {
                pushEv = std::make_unique<TEvNodeConfigPush>();
            }
            return pushEv.get();
        };

        // insert new connection into map (if there is none)
        const auto [it, inserted] = DirectBoundNodes.try_emplace(senderNodeId, ev->Cookie, ev->InterconnectSession);
        TBoundNode& info = it->second;
        if (inserted) {
            SendEvent(senderNodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId()));
            AddBound(senderNodeId, getPushEv());
            for (auto& [cookie, task] : ScatterTasks) {
                IssueScatterTaskForNode(senderNodeId, info, cookie, task);
            }
        } else if (ev->Cookie != info.Cookie || ev->InterconnectSession != info.SessionId) {
            STLOG(PRI_CRIT, BS_NODE, NWDC12, "distributed configuration protocol violation: cookie/session mismatch",
                (Sender, ev->Sender),
                (Cookie, ev->Cookie),
                (SessionId, ev->InterconnectSession),
                (ExpectedCookie, info.Cookie),
                (ExpectedSessionId, info.SessionId));
            Y_VERIFY_DEBUG(false);
            return;
        }

        // process added items
        for (const ui32 nodeId : record.GetNewBoundNodeIds()) {
            if (info.BoundNodeIds.insert(nodeId).second) {
                AddBound(nodeId, getPushEv());
            } else {
                STLOG(PRI_CRIT, BS_NODE, NWDC04, "distributed configuration protocol violation: adding duplicate item",
                    (Sender, ev->Sender),
                    (Cookie, ev->Cookie),
                    (SessionId, ev->InterconnectSession),
                    (Record, record),
                    (NodeId, nodeId));
                Y_VERIFY_DEBUG(false);
            }
        }

        // process deleted items
        for (const ui32 nodeId : record.GetDeletedBoundNodeIds()) {
            if (info.BoundNodeIds.erase(nodeId)) {
                DeleteBound(nodeId, getPushEv());
            } else {
                STLOG(PRI_CRIT, BS_NODE, NWDC05, "distributed configuration protocol violation: deleting nonexisting item",
                    (Sender, ev->Sender),
                    (Cookie, ev->Cookie),
                    (SessionId, ev->InterconnectSession),
                    (Record, record),
                    (NodeId, nodeId));
                Y_VERIFY_DEBUG(false);
            }
        }

        if (pushEv && pushEv->IsUseful()) {
            SendEvent(*Binding, std::move(pushEv));
        }

        CheckRootNodeStatus();
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigUnbind::TPtr ev) {
        const ui32 senderNodeId = ev->Sender.NodeId();
        Y_VERIFY(senderNodeId != SelfId().NodeId());

        STLOG(PRI_DEBUG, BS_NODE, NWDC16, "TEvNodeConfigUnbind", (NodeId, senderNodeId), (Cookie, ev->Cookie),
            (SessionId, ev->InterconnectSession), (Binding, Binding));

        if (const auto it = DirectBoundNodes.find(senderNodeId); it != DirectBoundNodes.end() && it->second.Expected(*ev)) {
            UnbindNode(it->first, "explicit unbind request");
        }
    }

    void TDistributedConfigKeeper::UnbindNode(ui32 nodeId, const char *reason) {
        if (const auto it = DirectBoundNodes.find(nodeId); it != DirectBoundNodes.end()) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC06, "UnbindNode", (NodeId, nodeId), (Reason, reason));

            TBoundNode& info = it->second;

            std::unique_ptr<TEvNodeConfigPush> pushEv;
            auto getPushEv = [&] {
                if (Binding && Binding->SessionId && !pushEv) {
                    pushEv = std::make_unique<TEvNodeConfigPush>();
                }
                return pushEv.get();
            };

            DeleteBound(nodeId, getPushEv());
            for (const ui32 boundNodeId : info.BoundNodeIds) {
                DeleteBound(boundNodeId, getPushEv());
            }

            if (pushEv && pushEv->IsUseful()) {
                SendEvent(*Binding, std::move(pushEv));
            }

            // abort all unprocessed scatter tasks
            for (const ui64 cookie : info.ScatterTasks) {
                AbortScatterTask(cookie, nodeId);
            }

            DirectBoundNodes.erase(it);

            IssueNextBindRequest();

            UnsubscribeInterconnect(nodeId);
        }
    }

    ui32 TDistributedConfigKeeper::GetRootNodeId() const {
        return Binding && Binding->RootNodeId ? Binding->RootNodeId : SelfId().NodeId();
    }

} // NKikimr::NStorage
