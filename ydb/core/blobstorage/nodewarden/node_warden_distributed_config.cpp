#include "node_warden_impl.h"
#include "node_warden_events.h"
#include "bind_queue.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper : public TActorBootstrapped<TDistributedConfigKeeper> {
        struct TEvPrivate {
            enum {
               EvProcessPendingEvent = EventSpaceBegin(TEvents::ES_PRIVATE),
            };
        };

        static constexpr ui64 OutgoingBindingCookie = 1;
        static constexpr ui64 IncomingBindingCookie = 2;

        struct TBinding {
            ui32 NodeId; // we have direct binding to this node
            ui32 RootNodeId; // this is the terminal node id for the whole binding chain
            ui64 Cookie; // binding cookie within the session
            TActorId SessionId; // session that connects to the node
            std::vector<std::unique_ptr<IEventBase>> PendingEvents;

            TBinding(ui32 nodeId, ui32 rootNodeId, ui64 cookie, TActorId sessionId)
                : NodeId(nodeId)
                , RootNodeId(rootNodeId)
                , Cookie(cookie)
                , SessionId(sessionId)
            {}

            bool Expected(IEventHandle& ev) const {
                return NodeId == ev.Sender.NodeId()
                    && Cookie == ev.Cookie
                    && SessionId == ev.InterconnectSession;
            }

            TString ToString() const {
                return TStringBuilder() << '{' << NodeId << '.' << RootNodeId << '/' << Cookie
                    << '@' << SessionId << '}';
            }
        };

        struct TBoundNode {
            ui64 Cookie;
            TActorId SessionId;
            THashSet<ui32> BoundNodeIds;

            TBoundNode(ui64 cookie, TActorId sessionId)
                : Cookie(cookie)
                , SessionId(sessionId)
            {}
        };

        // current most relevant storage config
        NKikimrBlobStorage::TStorageConfig StorageConfig;

        // outgoing binding
        std::optional<TBinding> Binding;
        ui64 BindingCookie = RandomNumber<ui64>();
        TBindQueue BindQueue;
        ui32 NumPeerNodes = 0;
        bool Scheduled = false;

        // incoming bindings
        THashMap<ui32, TBoundNode> DirectBoundNodes; // a set of nodes directly bound to this one
        THashMap<ui32, ui32> AllBoundNodes; // counter may be more than 2 in case of races, but not for long

        std::deque<TAutoPtr<IEventHandle>> PendingEvents;
        std::vector<ui32> NodeIds;

    public:
        void Bootstrap() {
            STLOG(PRI_DEBUG, BS_NODE, NWDC00, "Bootstrap");
            StorageConfig.SetGeneration(1);
            Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));
            Become(&TThis::StateWaitForList);
        }

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC11, "TEvNodesInfo");

            // create a vector of peer static nodes
            bool iAmStatic = false;
            std::vector<ui32> nodeIds;
            const ui32 selfNodeId = SelfId().NodeId();
            for (const auto& item : ev->Get()->Nodes) {
                if (item.NodeId == selfNodeId) {
                    iAmStatic = item.IsStatic;
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
                    UnbindNode(nodeId, true);
                    if (Binding && Binding->NodeId == nodeId) {
                        Binding.reset();
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

            // issue updates
            NodeIds = std::move(nodeIds);
            BindQueue.Update(NodeIds);
            NumPeerNodes = NodeIds.size() - 1;
            IssueNextBindRequest();

            if (bindingReset) {
                for (const auto& [nodeId, info] : DirectBoundNodes) {
                    SendEvent(nodeId, info.Cookie, info.SessionId, std::make_unique<TEvNodeConfigReversePush>(nullptr,
                        GetRootNodeId()));
                }
            }
        }

        void UnsubscribeInterconnect(ui32 nodeId, TActorId sessionId) {
            if (Binding && Binding->NodeId == nodeId) {
                return;
            }
            if (DirectBoundNodes.contains(nodeId)) {
                return;
            }
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, sessionId, SelfId(), nullptr, 0));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding to peer nodes

        void IssueNextBindRequest() {
            if (!Binding && AllBoundNodes.size() + 1 /* including this one */ < NumPeerNodes) {
                const TMonotonic now = TActivationContext::Monotonic();
                TMonotonic closest;
                if (std::optional<ui32> nodeId = BindQueue.Pick(now, &closest)) {
                    Binding.emplace(*nodeId, 0, ++BindingCookie, TActorId());
                    STLOG(PRI_DEBUG, BS_NODE, NWDC01, "Initiating bind", (Binding, Binding));
                    TActivationContext::Send(new IEventHandle(TEvInterconnect::EvConnectNode, 0,
                        TActivationContext::InterconnectProxy(Binding->NodeId), SelfId(), nullptr, OutgoingBindingCookie));
                } else if (closest != TMonotonic::Max() && !Scheduled) {
                    TActivationContext::Schedule(closest, new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {}, nullptr, 0));
                    Scheduled = true;
                }
            }
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
            const ui32 nodeId = ev->Get()->NodeId;
            STLOG(PRI_DEBUG, BS_NODE, NWDC14, "TEvNodeConnected", (NodeId, nodeId));

            if (ev->Cookie == OutgoingBindingCookie) {
                Y_VERIFY(Binding);
                Y_VERIFY(Binding->NodeId == nodeId);
                Binding->SessionId = ev->Sender;

                // send any accumulated events generated while we were waiting for the connection
                for (auto& ev : std::exchange(Binding->PendingEvents, {})) {
                    SendEvent(*Binding, std::move(ev));
                }

                STLOG(PRI_DEBUG, BS_NODE, NWDC09, "Continuing bind", (Binding, Binding));
                SendEvent(nodeId, Binding->Cookie, Binding->SessionId, std::make_unique<TEvNodeConfigPush>(&StorageConfig,
                    AllBoundNodes));
            }
        }

        void HandleWakeup() {
            Y_VERIFY(Scheduled);
            Scheduled = false;
            IssueNextBindRequest();
        }

        void Handle(TEvNodeConfigReversePush::TPtr ev) {
            const ui32 senderNodeId = ev->Sender.NodeId();
            Y_VERIFY(senderNodeId != SelfId().NodeId());
            auto& record = ev->Get()->Record;

            STLOG(PRI_DEBUG, BS_NODE, NWDC17, "TEvNodeConfigReversePush", (NodeId, senderNodeId), (Cookie, ev->Cookie),
                (SessionId, ev->InterconnectSession), (Binding, Binding), (Record, record));

            if (Binding && Binding->Expected(*ev)) {
                // check if this binding was accepted and if it is acceptable from our point of view
                bool rejected = record.GetRejected();
                const char *rejectReason = nullptr;
                bool rootUpdated = false;
                if (rejected) {
                    // applicable only for initial binding
                    Y_VERIFY_DEBUG(!Binding->RootNodeId);
                    rejectReason = "peer";
                } else {
                    const ui32 prevRootNodeId = std::exchange(Binding->RootNodeId, record.GetRootNodeId());
                    if (Binding->RootNodeId == SelfId().NodeId()) {
                        // root node changes and here we are in cycle -- break it
                        SendEvent(*Binding, std::make_unique<TEvNodeConfigUnbind>());
                        rejected = true;
                        rejectReason = "self";
                    } else if (prevRootNodeId != Binding->RootNodeId) {
                        if (prevRootNodeId) {
                            STLOG(PRI_DEBUG, BS_NODE, NWDC13, "Binding updated", (Binding, Binding));
                        } else {
                            STLOG(PRI_DEBUG, BS_NODE, NWDC07, "Binding established", (Binding, Binding));
                        }
                        rootUpdated = true;
                    }
                }

                if (rejected) {
                    STLOG(PRI_DEBUG, BS_NODE, NWDC06, "Binding rejected", (Binding, Binding), (Reason, rejectReason));

                    // binding needs to be reestablished
                    const TActorId sessionId = Binding->SessionId;
                    Binding.reset();
                    IssueNextBindRequest();

                    // unsubscribe from peer node unless there are incoming bindings active
                    UnsubscribeInterconnect(senderNodeId, sessionId);
                }

                // check if we have newer configuration from the peer
                bool configUpdated = false;
                if (record.HasStorageConfig()) {
                    const auto& config = record.GetStorageConfig();
                    if (StorageConfig.GetGeneration() < config.GetGeneration()) {
                        StorageConfig.Swap(record.MutableStorageConfig());
                        configUpdated = true;
                    }
                }

                // fan-out updates to the following peers
                if (configUpdated || rootUpdated) {
                    for (const auto& [nodeId, info] : DirectBoundNodes) {
                        SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(
                            configUpdated ? &StorageConfig : nullptr, GetRootNodeId()));
                    }
                }
            } else {
                // a race is possible when we have cancelled the binding, but there were updates in flight
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Binding requests from peer nodes

        void AddBound(ui32 nodeId, TEvNodeConfigPush *msg) {
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

        void DeleteBound(ui32 nodeId, TEvNodeConfigPush *msg) {
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

        void Handle(TEvNodeConfigPush::TPtr ev) {
            const ui32 senderNodeId = ev->Sender.NodeId();
            Y_VERIFY(senderNodeId != SelfId().NodeId());
            auto& record = ev->Get()->Record;

            STLOG(PRI_DEBUG, BS_NODE, NWDC02, "TEvNodeConfigPush", (NodeId, senderNodeId), (Cookie, ev->Cookie),
                (SessionId, ev->InterconnectSession), (Binding, Binding), (Record, record));

            // check if we can't accept this message (or else it would make a cycle)
            if (record.GetInitial()) {
                bool reject = Binding && senderNodeId == Binding->RootNodeId;
                if (!reject) {
                    for (const ui32 nodeId : record.GetNewBoundNodeIds()) {
                        if (nodeId == SelfId().NodeId()) {
                            reject = true;
                            break;
                        }
                    }
                }
                if (reject) {
                    STLOG(PRI_DEBUG, BS_NODE, NWDC03, "TEvNodeConfigPush rejected", (NodeId, senderNodeId),
                        (Cookie, ev->Cookie), (SessionId, ev->InterconnectSession), (Binding, Binding),
                        (Record, record));
                    SendEvent(*ev, TEvNodeConfigReversePush::MakeRejected());
                    return;
                }
            }

            // prepare configuration push down
            auto downEv = Binding
                ? std::make_unique<TEvNodeConfigPush>()
                : nullptr;

            // and configuration push up
            auto upEv = record.GetInitial()
                ? std::make_unique<TEvNodeConfigReversePush>(nullptr, GetRootNodeId())
                : nullptr;

            // insert new connection into map (if there is none)
            const auto [it, inserted] = DirectBoundNodes.try_emplace(senderNodeId, ev->Cookie, ev->InterconnectSession);
            TBoundNode& info = it->second;

            if (inserted) {
                if (!record.GetInitial()) {
                    // may be a race with rejected queries
                    DirectBoundNodes.erase(it);
                    return;
                } else {
                    // subscribe to the session -- we need to know when the channel breaks
                    TActivationContext::Send(new IEventHandle(TEvents::TSystem::Subscribe, 0, ev->InterconnectSession,
                        SelfId(), nullptr, IncomingBindingCookie));
                }

                // account newly bound node itself and add it to the record
                AddBound(senderNodeId, downEv.get());
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
                    AddBound(nodeId, downEv.get());
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
                    DeleteBound(nodeId, downEv.get());
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

            // process configuration update
            if (record.HasStorageConfig()) {
                const auto& config = record.GetStorageConfig();
                if (StorageConfig.GetGeneration() < config.GetGeneration()) {
                    StorageConfig.Swap(record.MutableStorageConfig());
                    if (downEv) {
                        downEv->Record.MutableStorageConfig()->CopyFrom(StorageConfig);
                    }

                    for (const auto& [nodeId, info] : DirectBoundNodes) {
                        if (nodeId != senderNodeId) {
                            SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(&StorageConfig, GetRootNodeId()));
                        }
                    }
                } else if (config.GetGeneration() < StorageConfig.GetGeneration() && upEv) {
                    upEv->Record.MutableStorageConfig()->CopyFrom(StorageConfig);
                }
            }

            if (downEv && (downEv->Record.HasStorageConfig() || downEv->Record.NewBoundNodeIdsSize() ||
                    downEv->Record.DeletedBoundNodeIdsSize())) {
                SendEvent(*Binding, std::move(downEv));
            }
            if (upEv) {
                SendEvent(senderNodeId, info, std::move(upEv));
            }
        }

        void Handle(TEvNodeConfigUnbind::TPtr ev) {
            const ui32 senderNodeId = ev->Sender.NodeId();
            Y_VERIFY(senderNodeId != SelfId().NodeId());

            STLOG(PRI_DEBUG, BS_NODE, NWDC16, "TEvNodeConfigUnbind", (NodeId, senderNodeId), (Cookie, ev->Cookie),
                (SessionId, ev->InterconnectSession), (Binding, Binding));

            if (const auto it = DirectBoundNodes.find(senderNodeId); it != DirectBoundNodes.end() &&
                    ev->Cookie == it->second.Cookie && ev->InterconnectSession == it->second.SessionId) {
                UnbindNode(it->first, false);
            } else {
                STLOG(PRI_CRIT, BS_NODE, NWDC08, "distributed configuration protocol violation: unexpected unbind event",
                    (Sender, ev->Sender),
                    (Cookie, ev->Cookie),
                    (SessionId, ev->InterconnectSession));

                Y_VERIFY_DEBUG(false);
            }
        }

        void UnbindNode(ui32 nodeId, bool byDisconnect) {
            if (const auto it = DirectBoundNodes.find(nodeId); it != DirectBoundNodes.end()) {
                TBoundNode& info = it->second;

                auto ev = Binding ? std::make_unique<TEvNodeConfigPush>() : nullptr;

                DeleteBound(nodeId, ev.get());
                for (const ui32 boundNodeId : info.BoundNodeIds) {
                    DeleteBound(boundNodeId, ev.get());
                }

                if (ev && ev->Record.DeletedBoundNodeIdsSize()) {
                    SendEvent(*Binding, std::move(ev));
                }

                const TActorId sessionId = info.SessionId;
                DirectBoundNodes.erase(it);

                if (!byDisconnect) {
                    UnsubscribeInterconnect(nodeId, sessionId);
                }

                IssueNextBindRequest();
            }
        }

        ui32 GetRootNodeId() const {
            return Binding && Binding->RootNodeId ? Binding->RootNodeId : SelfId().NodeId();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Event delivery

        void SendEvent(ui32 nodeId, ui64 cookie, TActorId sessionId, std::unique_ptr<IEventBase> ev) {
            Y_VERIFY(nodeId != SelfId().NodeId());
            auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(nodeId), SelfId(), ev.release(), 0, cookie);
            Y_VERIFY(sessionId);
            handle->Rewrite(TEvInterconnect::EvForward, sessionId);
            TActivationContext::Send(handle.release());
        }

        void SendEvent(TBinding& binding, std::unique_ptr<IEventBase> ev) {
            if (binding.SessionId) {
                SendEvent(binding.NodeId, binding.Cookie, binding.SessionId, std::move(ev));
            } else {
                binding.PendingEvents.push_back(std::move(ev));
            }
        }

        void SendEvent(IEventHandle& handle, std::unique_ptr<IEventBase> ev) {
            SendEvent(handle.Sender.NodeId(), handle.Cookie, handle.InterconnectSession, std::move(ev));
        }

        void SendEvent(ui32 nodeId, const TBoundNode& info, std::unique_ptr<IEventBase> ev) {
            SendEvent(nodeId, info.Cookie, info.SessionId, std::move(ev));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Connectivity handling

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
            const ui32 nodeId = ev->Get()->NodeId;
            STLOG(PRI_DEBUG, BS_NODE, NWDC15, "TEvNodeDisconnected", (NodeId, nodeId));
            UnbindNode(nodeId, true);
            if (Binding && Binding->NodeId == nodeId) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC10, "Binding aborted by disconnection", (Binding, Binding));

                Binding.reset();
                IssueNextBindRequest();

                for (const auto& [nodeId, info] : DirectBoundNodes) {
                    SendEvent(nodeId, info.Cookie, info.SessionId, std::make_unique<TEvNodeConfigReversePush>(nullptr,
                        GetRootNodeId()));
                }
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Consistency checking

        void ConsistencyCheck() {
#ifndef NDEBUG
            THashMap<ui32, ui32> refAllBoundNodeIds;
            for (const auto& [nodeId, info] : DirectBoundNodes) {
                ++refAllBoundNodeIds[nodeId];
                for (const ui32 boundNodeId : info.BoundNodeIds) {
                    ++refAllBoundNodeIds[boundNodeId];
                }
            }
            Y_VERIFY(AllBoundNodes == refAllBoundNodeIds);

            for (const auto& [nodeId, info] : DirectBoundNodes) {
                Y_VERIFY(std::binary_search(NodeIds.begin(), NodeIds.end(), nodeId));
            }
            if (Binding) {
                Y_VERIFY(std::binary_search(NodeIds.begin(), NodeIds.end(), Binding->NodeId));
            }
#endif
        }

        STFUNC(StateWaitForList) {
            switch (ev->GetTypeRewrite()) {
                case TEvInterconnect::TEvNodesInfo::EventType:
                    PendingEvents.push_front(std::move(ev));
                    [[fallthrough]];
                case TEvPrivate::EvProcessPendingEvent:
                    Y_VERIFY(!PendingEvents.empty());
                    StateFunc(PendingEvents.front());
                    PendingEvents.pop_front();
                    if (PendingEvents.empty()){
                        Become(&TThis::StateFunc);
                    } else {
                        TActivationContext::Send(new IEventHandle(TEvPrivate::EvProcessPendingEvent, 0, SelfId(), {}, nullptr, 0));
                    }
                    break;

                default:
                    PendingEvents.push_back(std::move(ev));
                    break;
            }
        }

        STFUNC(StateFunc) {
            STRICT_STFUNC_BODY(
                hFunc(TEvNodeConfigPush, Handle);
                hFunc(TEvNodeConfigReversePush, Handle);
                hFunc(TEvNodeConfigUnbind, Handle);
                hFunc(TEvInterconnect::TEvNodesInfo, Handle);
                hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
            ConsistencyCheck();
        }
    };

    void TNodeWarden::StartDistributedConfigKeeper() {
        DistributedConfigKeeperId = Register(new TDistributedConfigKeeper);
    }

    void TNodeWarden::ForwardToDistributedConfigKeeper(STATEFN_SIG) {
        ev->Rewrite(ev->GetTypeRewrite(), DistributedConfigKeeperId);
        TActivationContext::Send(ev.Release());
    }

} // NKikimr::NStorage
