#include "distconf.h"
#include "distconf_quorum.h"
#include <ydb/core/protos/node_broker.pb.h>

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC11, "TEvNodesInfo");

        std::vector<std::tuple<TNodeIdentifier, TNodeLocation>> newNodeList;
        for (const auto& item : ev->Get()->Nodes) {
            if (item.IsStatic) {
                newNodeList.emplace_back(TNodeIdentifier(item.ResolveHost, item.Port, item.NodeId), item.Location);
            }
        }

        ApplyNewNodeList(newNodeList);
    }

    void TDistributedConfigKeeper::ApplyNewNodeList(std::span<std::tuple<TNodeIdentifier, TNodeLocation>> newNodeList) {
        // do not start configuration negotiation for dynamic nodes
        if (!IsSelfStatic) {
            std::optional<TString> expectedBridgePileName;
            if (Cfg->BridgeConfig) {
                Y_ABORT_UNLESS(Cfg->DynamicNodeConfig && Cfg->DynamicNodeConfig->HasNodeInfo());
                const auto& nodeInfo = Cfg->DynamicNodeConfig->GetNodeInfo();
                Y_ABORT_UNLESS(nodeInfo.HasLocation());
                expectedBridgePileName = TNodeLocation(nodeInfo.GetLocation()).GetBridgePileName();
            }

            std::vector<ui32> nodeIds;
            for (const auto& [item, location] : newNodeList) {
                if (location.GetBridgePileName() == expectedBridgePileName) {
                    nodeIds.push_back(item.NodeId());
                }
            }
            ApplyStaticNodeIds(nodeIds);
            return;
        }

        // find this node in new node list
        const ui32 selfNodeId = SelfId().NodeId();
        bool found = false;
        for (const auto& [item, location] : newNodeList) {
            if (item.NodeId() == selfNodeId) {
                SelfNode = item;
                SelfNodeBridgePileName = location.GetBridgePileName();
                found = true;
                break;
            }
        }
        Y_ABORT_UNLESS(found);

        // process all other nodes, find bindable ones (from our current pile) and build list of all nodes
        AllNodeIds.clear();
        NodesFromSamePile.clear();

        std::vector<ui32> nodeIdsForOutgoingBinding;
        std::vector<ui32> nodeIdsForIncomingBinding;
        std::vector<ui32> nodeIdsForPrimaryPileOutgoingBinding;

        for (const auto& [item, location] : newNodeList) {
            const ui32 nodeId = item.NodeId();
            AllNodeIds.insert(item.NodeId());

            // check if node is from the same pile (as this one)
            if (location.GetBridgePileName() == SelfNodeBridgePileName) {
                NodesFromSamePile.insert(item.NodeId());
                if (nodeId < selfNodeId) {
                    nodeIdsForOutgoingBinding.push_back(nodeId);
                } else if (selfNodeId < nodeId) {
                    nodeIdsForIncomingBinding.push_back(nodeId);
                }
            }

            // check if node is located in primary pile (and this one is not the primary) -- then it is suitable for
            // binding to primary pile
            if (BridgeInfo && !BridgeInfo->SelfNodePile->IsPrimary && location.GetBridgePileName() == BridgeInfo->PrimaryPile->Name) {
                nodeIdsForPrimaryPileOutgoingBinding.push_back(item.NodeId());
            }
        }

        // check if some nodes were deleted -- we have to unbind them
        bool bindingReset = false;
        auto applyChanges = [&](auto& prev, auto& cur) {
            std::ranges::sort(cur);
            for (auto prevIt = prev.begin(), curIt = cur.begin(); prevIt != prev.end() || curIt != cur.end(); ) {
                if (prevIt == prev.end() || (curIt != cur.end() && *curIt < *prevIt)) { // node added
                    ++curIt;
                } else if (curIt == cur.end() || *prevIt < *curIt) { // node deleted
                    const ui32 nodeId = *prevIt++;
                    UnbindNode(nodeId, "node vanished");
                    if (Binding && Binding->NodeId == nodeId) {
                        bindingReset = true;
                    }
                } else {
                    Y_ABORT_UNLESS(*prevIt == *curIt);
                    ++prevIt;
                    ++curIt;
                }
            }
            prev = std::move(cur);
        };
        applyChanges(NodeIdsForOutgoingBinding, nodeIdsForOutgoingBinding);
        applyChanges(NodeIdsForIncomingBinding, nodeIdsForIncomingBinding);
        applyChanges(NodeIdsForPrimaryPileOutgoingBinding, nodeIdsForPrimaryPileOutgoingBinding);
        if (bindingReset) {
            AbortBinding("node vanished");
        }

        // issue updates
        BindQueue.Update(NodeIdsForOutgoingBinding);
        RevBindQueue.Update(NodeIdsForIncomingBinding);
        PrimaryPileBindQueue.Update(NodeIdsForPrimaryPileOutgoingBinding);
    }

    std::optional<TBridgePileId> TDistributedConfigKeeper::ResolveNodePileId(const TNodeLocation& location) {
        if (const auto& bridgePileName = location.GetBridgePileName()) {
            if (const auto it = BridgePileNameMap.find(*bridgePileName); it != BridgePileNameMap.end()) {
                return it->second;
            } else {
                Y_DEBUG_ABORT_S("incorrect bridge pile name for node Location# " << location.ToString());
            }
        } else if (BridgePileNameMap) {
            Y_DEBUG_ABORT_S("missing bridge pile name for node Location# " << location.ToString());
        }
        return std::nullopt;
    }

    void TDistributedConfigKeeper::IssueNextBindRequest() {
        Y_DEBUG_ABORT_UNLESS(IsSelfStatic);

        if (RootState != ERootState::INITIAL || Binding || !InvokeQ.empty()) {
            return; // we are either doing something, or binding is already in progress
        }

        const TMonotonic now = TActivationContext::Monotonic();

        // try to bind to node from the same pile
        TMonotonic closest = TMonotonic::Max();
        if (std::optional<ui32> nodeId = BindQueue.Pick(now, &closest)) {
            return StartBinding(*nodeId);
        }

        // nothing to bind to from main bind queue, try reverse one
        TMonotonic revClosest = TMonotonic::Max();
        if (std::optional<ui32> nodeId = RevBindQueue.Pick(now, &revClosest)) {
            return StartBinding(*nodeId);
        }

        // no node from the same pile available, try to bind to primary pile (if we have quorum)
        TMonotonic primaryClosest = TMonotonic::Max();
        if (LocalPileQuorum && !BridgeInfo->SelfNodePile->IsPrimary) {
            if (std::optional<ui32> nodeIdFromPrimaryPile = PrimaryPileBindQueue.Pick(now, &primaryClosest)) {
                return StartBinding(*nodeIdFromPrimaryPile);
            }
        }

        // nothing to bind to
        closest = Max(closest, revClosest, primaryClosest);
        if (closest != TMonotonic::Max() && !Scheduled) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC30, "Delaying bind");
            TActivationContext::Schedule(closest, new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {}, nullptr, 0));
            Scheduled = true;
        }
    }

    void TDistributedConfigKeeper::StartBinding(ui32 nodeId) {
        Y_ABORT_UNLESS(!Binding);

        Y_ABORT_UNLESS(nodeId != SelfId().NodeId());
        Binding.emplace(nodeId, ++BindingCookie);

        const TActorId sessionId = SubscribeToPeerNode(Binding->NodeId, TActorId());
        if (sessionId) {
            BindToSession(sessionId);
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC29, "Initiated bind", (NodeId, nodeId), (Binding, Binding),
            (SessionId, sessionId));

        // abort any pending queries
        OpQueueOnError(TStringBuilder() << "binding is in progress Binding# " << Binding->ToString());

        // unbind any other piles
        UnbindNodesFromOtherPiles();
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

        for (auto it = Cache.begin(); it != Cache.end(); ++it) {
            AddCacheUpdate(record.MutableCacheUpdate(), it, false);
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

    void TDistributedConfigKeeper::AbortBinding(const char *reason, bool sendUnbindMessage, bool sendUpdate) {
        if (Binding) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC03, "AbortBinding", (Binding, Binding), (Reason, reason));

            const TBinding binding = *std::exchange(Binding, std::nullopt);

            if (binding.SessionId && sendUnbindMessage) {
                SendEvent(binding, std::make_unique<TEvNodeConfigUnbind>());
            }

            AbortAllScatterTasks(binding);

            ApplyConfigUpdateToDynamicNodes(true);

            if (sendUpdate) {
                FanOutReversePush(nullptr);
            }

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
            if (const ui32 rootNodeId = record.GetRootNodeId()) {
                StartBinding(rootNodeId);
            }
        } else {
            const bool configUpdate = record.HasCommittedStorageConfig() &&
                CheckBridgePeerRevPush(record.GetCommittedStorageConfig(), senderNodeId) &&
                (ApplyStorageConfig(record.GetCommittedStorageConfig()) || record.GetRecurseConfigUpdate());

            const bool bindingUpdate = !Binding || Binding->RootNodeId != record.GetRootNodeId();
            if (Binding) {
                if (record.GetRootNodeId() == SelfId().NodeId()) {
                    AbortBinding("binding cycle", /*sendUnbindMessage=*/ true, /*sendUpdate=*/ false);
                } else {
                    Binding->RootNodeId = record.GetRootNodeId();
                }
            }

            Y_ABORT_UNLESS(!Binding || Binding->RootNodeId != SelfId().NodeId());

            if (bindingUpdate || configUpdate) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC13, "Binding updated", (Binding, Binding), (BindingUpdate, bindingUpdate),
                    (ConfigUpdate, configUpdate));
                FanOutReversePush(configUpdate ? StorageConfig.get() : nullptr, record.GetRecurseConfigUpdate());
            }
        }

        // process cache updates, if needed
        if (record.HasCacheUpdate()) {
            auto *cacheUpdate = record.MutableCacheUpdate();
            ApplyCacheUpdates(cacheUpdate, senderNodeId);

            if (cacheUpdate->RequestedKeysSize()) {
                auto ev = std::make_unique<TEvNodeConfigPush>();
                for (const TString& key : cacheUpdate->GetRequestedKeys()) {
                    if (const auto it = Cache.find(key); it != Cache.end()) {
                        AddCacheUpdate(ev->Record.MutableCacheUpdate(), it, true);
                    }
                }
                SendEvent(*Binding, std::move(ev));
            }
        }
    }

    void TDistributedConfigKeeper::UpdateBound(ui32 refererNodeId, TNodeIdentifier nodeId, const TStorageConfigMeta& meta, TEvNodeConfigPush *msg) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC18, "UpdateBound", (RefererNodeId, refererNodeId), (NodeId, nodeId), (Meta, meta));

        const auto [it, inserted] = AllBoundNodes.try_emplace(std::move(nodeId));
        TIndirectBoundNode& node = it->second;
        QuorumValid &= !inserted;

        if (inserted) {
            const ui32 nodeId = it->first.NodeId();
            if (std::ranges::binary_search(NodeIdsForOutgoingBinding, nodeId)) {
                BindQueue.Disable(nodeId);
            } else if (std::ranges::binary_search(NodeIdsForIncomingBinding, nodeId)) {
                RevBindQueue.Disable(nodeId);
            }
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
            QuorumValid = false;
            if (std::ranges::binary_search(NodeIdsForOutgoingBinding, nodeId.NodeId())) {
                BindQueue.Enable(nodeId.NodeId());
            } else if (std::ranges::binary_search(NodeIdsForIncomingBinding, nodeId.NodeId())) {
                RevBindQueue.Enable(nodeId.NodeId());
            }
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
            (SessionId, ev->InterconnectSession), (Binding, Binding), (Record, record),
            (RootNodeId, GetRootNodeId()));

        if (!AllNodeIds.contains(senderNodeId)) {
            // node has been already deleted from the config, but new subscription is coming through -- ignoring it
            SendEvent(*ev, TEvNodeConfigReversePush::MakeRejected());
            return;
        }

        // check if we can't accept this message (or else it would make a cycle)
        if (record.GetInitial() && senderNodeId == GetRootNodeId()) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC28, "TEvNodeConfigPush rejected", (NodeId, senderNodeId),
                (Cookie, ev->Cookie), (SessionId, ev->InterconnectSession), (Binding, Binding),
                (Record, record));
            SendEvent(*ev, TEvNodeConfigReversePush::MakeRejected());
            return;
        }

        // check if this is connection from another pile
        if (record.GetInitial() && !NodesFromSamePile.contains(senderNodeId)) {
            Y_DEBUG_ABORT_UNLESS(BridgeInfo);
            if (!Binding && LocalPileQuorum && BridgeInfo->SelfNodePile->IsPrimary) {
                // we allow this node's connection as this is the primary pile AND we have majority of connected
                // nodes AND this is the root one
            } else {
                // this is either not the root node, or no quorum for connection
                auto response = TEvNodeConfigReversePush::MakeRejected();
                if (Binding && Binding->RootNodeId) {
                    // command peer to join this specific node
                    response->Record.SetRootNodeId(Binding->RootNodeId);
                }
                SendEvent(*ev, std::move(response));
                return;
            }
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
            auto response = std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), StorageConfig.get(), false);
            if (record.GetInitial() && record.HasCacheUpdate()) {
                auto *cache = record.MutableCacheUpdate();

                // scan existing cache keys to find the ones we need to ask and to report others
                THashSet<TString> keysToReport;
                for (const auto& [key, value] : Cache) {
                    keysToReport.insert(key);
                }
                for (const auto& item : cache->GetKeyValuePairs()) {
                    keysToReport.erase(item.GetKey());
                    const auto it = Cache.find(item.GetKey());
                    if (it == Cache.end() || it->second.Generation < item.GetGeneration()) {
                        response->Record.MutableCacheUpdate()->AddRequestedKeys(item.GetKey());
                    } else if (item.GetGeneration() < it->second.Generation) {
                        AddCacheUpdate(response->Record.MutableCacheUpdate(), it, true);
                    }
                }
                for (const TString& key : keysToReport) {
                    const auto it = Cache.find(key);
                    Y_ABORT_UNLESS(it != Cache.end());
                    AddCacheUpdate(response->Record.MutableCacheUpdate(), it, true);
                }
            }

            SendEvent(senderNodeId, info, std::move(response));
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

        // process cache items
        if (!record.GetInitial() && record.HasCacheUpdate()) {
            ApplyCacheUpdates(record.MutableCacheUpdate(), senderNodeId);
        }

        if (pushEv && pushEv->IsUseful()) {
            SendEvent(*Binding, std::move(pushEv));
        }
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

            OnSyncerUnboundNode(nodeId);
        }
    }

    ui32 TDistributedConfigKeeper::GetRootNodeId() const {
        return Binding && Binding->RootNodeId ? Binding->RootNodeId : SelfId().NodeId();
    }

    bool TDistributedConfigKeeper::PartOfNodeQuorum() const {
        return Scepter || (Binding && GetRootNodeId() != SelfId().NodeId());
    }

    void TDistributedConfigKeeper::UnbindNodesFromOtherPiles() {
        if (!BridgeInfo) {
            return;
        }
        std::vector<ui32> goingToUnbind;
        for (const auto& [nodeId, info] : DirectBoundNodes) {
            if (BridgeInfo->GetPileForNode(nodeId) != BridgeInfo->SelfNodePile) {
                goingToUnbind.push_back(nodeId);
            }
        }
        for (ui32 nodeId : goingToUnbind) {
            UnbindNode(nodeId, "primary pile scepter lost");
        }
    }

} // NKikimr::NStorage
