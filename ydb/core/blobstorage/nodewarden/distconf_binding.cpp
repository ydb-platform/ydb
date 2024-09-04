#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC11, "TEvNodesInfo");

        // create a vector of peer static nodes
        std::vector<ui32> nodeIds;
        const ui32 selfNodeId = SelfId().NodeId();
        for (const auto& item : ev->Get()->Nodes) {
            if (item.NodeId == selfNodeId) {
                SelfNode = TNodeIdentifier(item.ResolveHost, item.Port, selfNodeId);
                Y_ABORT_UNLESS(IsSelfStatic == item.IsStatic);
            }
            if (item.IsStatic) {
                nodeIds.push_back(item.NodeId);
            }
        }
        std::sort(nodeIds.begin(), nodeIds.end());

        // do not start configuration negotiation for dynamic nodes
        if (!IsSelfStatic) {
            Y_ABORT_UNLESS(NodeIds.empty());
            ApplyStaticNodeIds(nodeIds);
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
                Y_ABORT_UNLESS(*prevIt == *curIt);
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
    }

    void TDistributedConfigKeeper::IssueNextBindRequest() {
        Y_DEBUG_ABORT_UNLESS(IsSelfStatic);
        CheckRootNodeStatus();
        if (RootState == ERootState::INITIAL && !Binding && AllBoundNodes.size() < NodeIds.size()) {
            const TMonotonic now = TActivationContext::Monotonic();
            TMonotonic closest;
            if (std::optional<ui32> nodeId = BindQueue.Pick(now, &closest)) {
                Y_ABORT_UNLESS(*nodeId != SelfId().NodeId());
                Binding.emplace(*nodeId, ++BindingCookie);

                const TActorId sessionId = SubscribeToPeerNode(Binding->NodeId, TActorId());
                if (sessionId) {
                    BindToSession(sessionId);
                }

                STLOG(PRI_DEBUG, BS_NODE, NWDC29, "Initiated bind", (NodeId, nodeId), (Binding, Binding),
                    (SessionId, sessionId));
            } else if (closest != TMonotonic::Max() && !Scheduled) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC30, "Delaying bind");
                TActivationContext::Schedule(closest, new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {}, nullptr, 0));
                Scheduled = true;
            } else {
                STLOG(PRI_DEBUG, BS_NODE, NWDC01, "No bind options");
            }
        }
    }

    void TDistributedConfigKeeper::BindToSession(TActorId sessionId) {
        Y_ABORT_UNLESS(Binding);
        Binding->SessionId = sessionId;

        auto ev = std::make_unique<TEvNodeConfigPush>();
        auto& record = ev->Record;

        record.SetInitial(true);

        for (const auto& [nodeId, node] : AllBoundNodes) {
            auto *boundNode = record.AddBoundNodes();
            nodeId.Serialize(boundNode->MutableNodeId());
            boundNode->MutableMeta()->CopyFrom(node.Configs.back());
        }

        SendEvent(*Binding, std::move(ev));
    }

    void TDistributedConfigKeeper::Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        const TActorId sessionId = ev->Sender;
        const ui64 cookie = ev->Cookie;

        STLOG(PRI_DEBUG, BS_NODE, NWDC14, "TEvNodeConnected", (NodeId, nodeId), (SessionId, sessionId), (Cookie, cookie),
            (CookieInFlight, SubscriptionCookieMap.contains(cookie)),
            (SubscriptionExists, SubscribedSessions.contains(nodeId)));

        if (!IsSelfStatic) {
            return OnStaticNodeConnected(nodeId, sessionId);
        }

        // check subscription map, do we really have this subscription in flight
        const auto cookieIt = SubscriptionCookieMap.find(cookie);
        if (cookieIt == SubscriptionCookieMap.end()) {
            // we are not going to subscribe with this subscription; check if we have any other pending subscriptions
            // for this node, and unsubscribe if not
            if (!SubscribedSessions.contains(nodeId)) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, ev->Sender, SelfId(), nullptr, 0));
            }
            return;
        }
        Y_ABORT_UNLESS(nodeId == cookieIt->second);
        SubscriptionCookieMap.erase(cookieIt);

        // update subscription information
        const auto it = SubscribedSessions.find(nodeId);
        Y_ABORT_UNLESS(it != SubscribedSessions.end());
        TSessionSubscription& subs = it->second;

        // check the cookie
        Y_ABORT_UNLESS(subs.SubscriptionCookie == cookie);
        subs.SubscriptionCookie = 0;

        // check if the session actor has changed; if so, we have to simulate disconnection first, because this means
        // a race -- we started subscribing when knowing other session actor, and now it got changed
        if (subs.SessionId && sessionId != subs.SessionId) {
            HandleDisconnect(nodeId, subs.SessionId);
        }
        subs.SessionId = sessionId;

        if (Binding && Binding->NodeId == nodeId) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC09, "Continuing bind", (Binding, Binding));
            BindToSession(sessionId);
        }
    }

    void TDistributedConfigKeeper::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        const TActorId sessionId = ev->Sender;
        const ui64 cookie = ev->Cookie;

        STLOG(PRI_DEBUG, BS_NODE, NWDC07, "TEvNodeDisconnected", (NodeId, nodeId), (SessionId, sessionId), (Cookie, cookie));

        if (!IsSelfStatic) {
            return OnStaticNodeDisconnected(nodeId, sessionId);
        }

        const auto it = SubscribedSessions.find(nodeId);
        if (it == SubscribedSessions.end()) {
            return; // some kind of a race with unsubscription, we don't need this notification anymore
        }
        TSessionSubscription& subs = it->second;

        if (subs.SubscriptionCookie && cookie != subs.SubscriptionCookie) {
            return; // a race with other TEvNodeDisconnected, don't care, ignore
        }

        if (subs.SubscriptionCookie) {
            const auto cookieIt = SubscriptionCookieMap.find(subs.SubscriptionCookie);
            Y_ABORT_UNLESS(cookieIt != SubscriptionCookieMap.end());
            Y_ABORT_UNLESS(cookieIt->second == nodeId);
            SubscriptionCookieMap.erase(cookieIt);
        }

        const TActorId subsSessionId = subs.SessionId;
        SubscribedSessions.erase(it);

        HandleDisconnect(nodeId, subsSessionId);
    }

    void TDistributedConfigKeeper::HandleDisconnect(ui32 nodeId, TActorId sessionId) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC56, "HandleDisconnect", (NodeId, nodeId), (SessionId, sessionId));

        OnDynamicNodeDisconnected(nodeId, sessionId);

        UnbindNode(nodeId, "disconnection");

        if (Binding && Binding->NodeId == nodeId) {
            AbortBinding("disconnection", false);
        }
    }

    void TDistributedConfigKeeper::UnsubscribeInterconnect(ui32 nodeId) {
        if (Binding && Binding->NodeId == nodeId) {
            return;
        }
        if (DirectBoundNodes.contains(nodeId)) {
            return;
        }
        if (ConnectedDynamicNodes.contains(nodeId)) {
            return;
        }
        if (const auto it = SubscribedSessions.find(nodeId); it != SubscribedSessions.end()) {
            TSessionSubscription& subs = it->second;
            STLOG(PRI_DEBUG, BS_NODE, NWDC55, "UnsubscribeInterconnect", (NodeId, nodeId), (Subscription, subs));
            if (subs.SubscriptionCookie) {
                // we are waiting for TEvNodeConnected, so just drop it; we will generate unsubscribe when we get that
                // TEvNodeConnected
                const auto jt = SubscriptionCookieMap.find(subs.SubscriptionCookie);
                Y_ABORT_UNLESS(jt != SubscriptionCookieMap.end());
                Y_ABORT_UNLESS(jt->second == nodeId);
                SubscriptionCookieMap.erase(jt);
            } else {
                // we already had TEvNodeConnected, so we have to unsubscribe
                Y_ABORT_UNLESS(subs.SessionId);
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, subs.SessionId, SelfId(),
                    nullptr, 0));
            }
            SubscribedSessions.erase(it);
        }
    }

    TActorId TDistributedConfigKeeper::SubscribeToPeerNode(ui32 nodeId, TActorId sessionId) {
        const auto [it, inserted] = SubscribedSessions.try_emplace(nodeId, sessionId);
        TSessionSubscription& subs = it->second;
        STLOG(PRI_DEBUG, BS_NODE, NWDC54, "SubscribeToPeerNode", (NodeId, nodeId), (SessionId, sessionId),
            (Inserted, inserted), (Subscription, subs), (NextSubscribeCookie, NextSubscribeCookie));
        if (inserted) {
            // start subscription; always subscribe to the proxy to get the latest working session actor
            subs.SubscriptionCookie = NextSubscribeCookie++;
            SubscriptionCookieMap.emplace(subs.SubscriptionCookie, nodeId);
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Subscribe, 0,
                TActivationContext::InterconnectProxy(nodeId), SelfId(), nullptr, subs.SubscriptionCookie));
        } else if (!sessionId) {
            // we don't have the actual session and and only want to establish new connection; however, we have already
            // sent the subscription message, because this entry is not the new one, so just relax here and do nothing
        } else if (!subs.SessionId) {
            // we don't have confirmed session id and never received message from the peer node, but now we got one;
            // however, this may be a phantom session, which is dead already, and we can't use this actor identifier for
            // anything, but we may want to store it in order to compare with future messages to detect sudden session
            // change (this will mean reconnection)
            Y_ABORT_UNLESS(subs.SubscriptionCookie); // ensure we are expecting TEvNodeConnected
            subs.SessionId = sessionId;
        } else if (sessionId != subs.SessionId) {
            // we have session id actor changed; if this happens after we receive TEvNodeConnected, this is protocol
            // violation, some bug in message handling logic (it can't happen, because messages from further sessions
            // can be received only after TEvNodeDisconnected, which drops this record); if this happens before that,
            // we just have to simulate disconnection hoping that subscription brings us to this new session actor
            Y_ABORT_UNLESS(subs.SubscriptionCookie);
            HandleDisconnect(nodeId, std::exchange(subs.SessionId, sessionId));
        }
        return subs.SessionId;
    }

    void TDistributedConfigKeeper::AbortBinding(const char *reason, bool sendUnbindMessage) {
        if (Binding) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC03, "AbortBinding", (Binding, Binding), (Reason, reason));

            const TBinding binding = *std::exchange(Binding, std::nullopt);

            if (binding.SessionId && sendUnbindMessage) {
                SendEvent(binding, std::make_unique<TEvNodeConfigUnbind>());
            }

            AbortAllScatterTasks(binding);

            ApplyConfigUpdateToDynamicNodes(true);

            FanOutReversePush(nullptr);

            UnsubscribeQueue.insert(binding.NodeId);
        }
    }

    void TDistributedConfigKeeper::HandleWakeup() {
        Y_ABORT_UNLESS(Scheduled);
        Scheduled = false;
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigReversePush::TPtr ev) {
        const ui32 senderNodeId = ev->Sender.NodeId();
        Y_ABORT_UNLESS(senderNodeId != SelfId().NodeId());
        auto& record = ev->Get()->Record;

        STLOG(PRI_DEBUG, BS_NODE, NWDC17, "TEvNodeConfigReversePush", (NodeId, senderNodeId), (Cookie, ev->Cookie),
            (SessionId, ev->InterconnectSession), (Binding, Binding), (Record, record));

        if (!Binding || !Binding->Expected(*ev)) {
            return; // possible race with unbinding
        }

        Y_ABORT_UNLESS(Binding->RootNodeId || ScatterTasks.empty());

        // check if this binding was accepted and if it is acceptable from our point of view
        if (record.GetRejected()) {
            AbortBinding("binding rejected by peer", false);
        } else {
            const bool configUpdate = record.HasCommittedStorageConfig() && (
                ApplyStorageConfig(record.GetCommittedStorageConfig()) || record.GetRecurseConfigUpdate());
            const ui32 prevRootNodeId = std::exchange(Binding->RootNodeId, record.GetRootNodeId());
            if (Binding->RootNodeId == SelfId().NodeId()) {
                AbortBinding("binding cycle");
            } else if (prevRootNodeId != GetRootNodeId() || configUpdate) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC13, "Binding updated", (Binding, Binding), (PrevRootNodeId, prevRootNodeId),
                    (ConfigUpdate, configUpdate));
                FanOutReversePush(configUpdate ? &StorageConfig.value() : nullptr, record.GetRecurseConfigUpdate());
            }
        }
    }

    void TDistributedConfigKeeper::UpdateBound(ui32 refererNodeId, TNodeIdentifier nodeId, const TStorageConfigMeta& meta, TEvNodeConfigPush *msg) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC18, "UpdateBound", (RefererNodeId, refererNodeId), (NodeId, nodeId), (Meta, meta));

        const auto [it, inserted] = AllBoundNodes.try_emplace(std::move(nodeId));
        TIndirectBoundNode& node = it->second;

        if (inserted) { // disable this node from target binding set, this is the first mention of this node
            BindQueue.Disable(it->first.NodeId());
        }

        // remember previous config meta
        const std::optional<TStorageConfigMeta> prev = node.Configs.empty()
            ? std::nullopt
            : std::make_optional(node.Configs.back());

        const auto [refIt, refInserted] = node.Refs.try_emplace(refererNodeId);
        if (refInserted) { // new entry
            refIt->second = node.Configs.emplace(node.Configs.end(), meta);
        } else { // update meta (even if it didn't change)
            refIt->second->CopyFrom(meta);
            node.Configs.splice(node.Configs.end(), node.Configs, refIt->second);
        }

        if (msg && prev != node.Configs.back()) { // update config for this node
            auto *boundNode = msg->Record.AddBoundNodes();
            it->first.Serialize(boundNode->MutableNodeId());
            boundNode->MutableMeta()->CopyFrom(node.Configs.back());
        }
    }

    void TDistributedConfigKeeper::DeleteBound(ui32 refererNodeId, const TNodeIdentifier& nodeId, TEvNodeConfigPush *msg) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC34, "DeleteBound", (RefererNodeId, refererNodeId), (NodeId, nodeId));

        const auto it = AllBoundNodes.find(nodeId);
        Y_ABORT_UNLESS(it != AllBoundNodes.end());
        TIndirectBoundNode& node = it->second;

        Y_ABORT_UNLESS(!node.Configs.empty());
        const TStorageConfigMeta prev = node.Configs.back();

        const auto refIt = node.Refs.find(refererNodeId);
        node.Configs.erase(refIt->second);
        node.Refs.erase(refIt);

        if (node.Refs.empty()) {
            AllBoundNodes.erase(it);
            BindQueue.Enable(nodeId.NodeId());
            if (msg) {
                nodeId.Serialize(msg->Record.AddDeletedBoundNodeIds());
            }
        } else if (msg && prev != node.Configs.back()) {
            auto *boundNode = msg->Record.AddBoundNodes();
            nodeId.Serialize(boundNode->MutableNodeId());
            boundNode->MutableMeta()->CopyFrom(node.Configs.back());
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigPush::TPtr ev) {
        const ui32 senderNodeId = ev->Sender.NodeId();
        Y_ABORT_UNLESS(senderNodeId != SelfId().NodeId());
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
            AbortBinding("mutual binding", true);
        }

        // ensure we have subscription to this node
        SubscribeToPeerNode(senderNodeId, ev->InterconnectSession);

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
            SendEvent(senderNodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(),
                StorageConfig ? &StorageConfig.value() : nullptr, false));
            for (auto& [cookie, task] : ScatterTasks) {
                IssueScatterTaskForNode(senderNodeId, info, cookie, task);
            }
        } else if (ev->Cookie != info.Cookie || ev->InterconnectSession != info.SessionId) {
            STLOG(PRI_CRIT, BS_NODE, NWDC12, "distributed configuration protocol violation: cookie/session mismatch",
                (Sender, ev->Sender),
                (Cookie, ev->Cookie),
                (SelfId, SelfId()),
                (SessionId, ev->InterconnectSession),
                (ExpectedCookie, info.Cookie),
                (ExpectedSessionId, info.SessionId));
            Y_DEBUG_ABORT();
            return;
        }

        // process updates
        for (const auto& item : record.GetBoundNodes()) {
            const auto& nodeId = item.GetNodeId();
            UpdateBound(senderNodeId, nodeId, item.GetMeta(), getPushEv());
            info.BoundNodeIds.insert(nodeId);
        }

        // process deleted items
        for (const auto& item : record.GetDeletedBoundNodeIds()) {
            const TNodeIdentifier nodeId(item);
            if (info.BoundNodeIds.erase(nodeId)) {
                DeleteBound(senderNodeId, nodeId, getPushEv());
            } else {
                STLOG(PRI_CRIT, BS_NODE, NWDC05, "distributed configuration protocol violation: deleting nonexisting item",
                    (Sender, ev->Sender),
                    (Cookie, ev->Cookie),
                    (SessionId, ev->InterconnectSession),
                    (Record, record),
                    (NodeId, nodeId));
                Y_DEBUG_ABORT();
            }
        }

        if (pushEv && pushEv->IsUseful()) {
            SendEvent(*Binding, std::move(pushEv));
        }

        CheckRootNodeStatus();
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigUnbind::TPtr ev) {
        const ui32 senderNodeId = ev->Sender.NodeId();
        Y_ABORT_UNLESS(senderNodeId != SelfId().NodeId());

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

            for (const TNodeIdentifier& boundNodeId : info.BoundNodeIds) {
                DeleteBound(nodeId, boundNodeId, getPushEv());
            }

            if (pushEv && pushEv->IsUseful()) {
                SendEvent(*Binding, std::move(pushEv));
            }

            // abort all unprocessed scatter tasks
            for (const ui64 cookie : info.ScatterTasks) {
                AbortScatterTask(cookie, nodeId);
            }

            DirectBoundNodes.erase(it);

            UnsubscribeQueue.insert(nodeId);

            CheckRootNodeStatus();
        }
    }

    ui32 TDistributedConfigKeeper::GetRootNodeId() const {
        return Binding && Binding->RootNodeId ? Binding->RootNodeId : SelfId().NodeId();
    }

    bool TDistributedConfigKeeper::PartOfNodeQuorum() const {
        return Scepter || (Binding && GetRootNodeId() != SelfId().NodeId());
    }

} // NKikimr::NStorage
