#include "node_warden_impl.h"
#include "node_warden_events.h"
#include "bind_queue.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper : public TActorBootstrapped<TDistributedConfigKeeper> {
        struct TEvPrivate {
            enum {
               EvProcessPendingEvent = EventSpaceBegin(TEvents::ES_PRIVATE),
               EvQuorumCheckTimeout,
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

            TBinding(const TBinding& origin)
                : NodeId(origin.NodeId)
                , RootNodeId(origin.RootNodeId)
                , Cookie(origin.Cookie)
                , SessionId(origin.SessionId)
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

            friend bool operator ==(const TBinding& x, const TBinding& y) {
                return x.NodeId == y.NodeId && x.RootNodeId == y.RootNodeId && x.Cookie == y.Cookie && x.SessionId == y.SessionId;
            }

            friend bool operator !=(const TBinding& x, const TBinding& y) {
                return !(x == y);
            }
        };

        struct TBoundNode {
            ui64 Cookie;
            TActorId SessionId;
            THashSet<ui32> BoundNodeIds;
            THashSet<ui64> ScatterTasks; // unanswered scatter queries

            TBoundNode(ui64 cookie, TActorId sessionId)
                : Cookie(cookie)
                , SessionId(sessionId)
            {}
        };

        struct TScatterTask {
            std::optional<TBinding> Origin;
            THashSet<ui32> PendingNodes;
            NKikimrBlobStorage::TEvNodeConfigScatter Task;
            std::vector<NKikimrBlobStorage::TEvNodeConfigGather> CollectedReplies;

            TScatterTask(const std::optional<TBinding>& origin, NKikimrBlobStorage::TEvNodeConfigScatter&& task)
                : Origin(origin)
            {
                Task.Swap(&task);
            }
        };

        // current most relevant storage config
        NKikimrBlobStorage::TStorageConfig StorageConfig;

        // outgoing binding
        std::optional<TBinding> Binding;
        ui64 BindingCookie = RandomNumber<ui64>();
        TBindQueue BindQueue;
        bool Scheduled = false;

        // incoming bindings
        THashMap<ui32, TBoundNode> DirectBoundNodes; // a set of nodes directly bound to this one
        THashMap<ui32, ui32> AllBoundNodes; // counter may be more than 2 in case of races, but not for long

        // pending event queue
        std::deque<TAutoPtr<IEventHandle>> PendingEvents;
        std::vector<ui32> NodeIds;

        // scatter tasks
        ui64 NextScatterCookie = RandomNumber<ui64>();
        THashMap<ui64, TScatterTask> ScatterTasks;

        // root node operation
        enum class ERootState {
            INITIAL,
            QUORUM_CHECK_TIMEOUT,
            COLLECT_CONFIG,
        };
        static constexpr TDuration QuorumCheckTimeout = TDuration::Seconds(1); // time to wait after obtaining quorum
        ERootState RootState = ERootState::INITIAL;

    public:
        void Bootstrap() {
            STLOG(PRI_DEBUG, BS_NODE, NWDC00, "Bootstrap");
            StorageConfig.SetFingerprint(CalculateFingerprint(StorageConfig));
            Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));
            Become(&TThis::StateWaitForList);
        }

        TString CalculateFingerprint(const NKikimrBlobStorage::TStorageConfig& config) {
            NKikimrBlobStorage::TStorageConfig temp;
            temp.CopyFrom(config);
            temp.ClearFingerprint();

            TString s;
            const bool success = temp.SerializeToString(&s);
            Y_VERIFY(success);

            auto digest = NOpenSsl::NSha1::Calc(s.data(), s.size());
            return TString(reinterpret_cast<const char*>(digest.data()), digest.size());
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
                        AbortAllScatterTasks();
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
            IssueNextBindRequest();

            if (bindingReset) {
                for (const auto& [nodeId, info] : DirectBoundNodes) {
                    SendEvent(nodeId, info.Cookie, info.SessionId, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId()));
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
            CheckRootNodeStatus();
            if (!Binding && AllBoundNodes.size() < NodeIds.size() && RootState == ERootState::INITIAL) {
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
                SendEvent(nodeId, Binding->Cookie, Binding->SessionId, std::make_unique<TEvNodeConfigPush>(AllBoundNodes));
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
                Y_VERIFY(ScatterTasks.empty());

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

                // fan-out updates to the following peers
                if (rootUpdated) {
                    for (const auto& [nodeId, info] : DirectBoundNodes) {
                        SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId()));
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

            // configuration push message
            std::unique_ptr<TEvNodeConfigPush> pushEv;
            auto getPushEv = [&] {
                if (Binding && !pushEv) {
                    pushEv = std::make_unique<TEvNodeConfigPush>();
                }
                return pushEv.get();
            };

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
                AddBound(senderNodeId, getPushEv());
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

            if (pushEv) {
                SendEvent(*Binding, std::move(pushEv));
            }

            if (!Binding) {
                CheckRootNodeStatus();
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

                // abort all unprocessed scatter tasks
                for (const ui64 cookie : info.ScatterTasks) {
                    AbortScatterTask(cookie, nodeId);
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
        // Root node operation

        void CheckRootNodeStatus() {
            if (RootState == ERootState::INITIAL && !Binding && HasQuorum()) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC18, "Starting QUORUM_CHECK_TIMEOUT");
                TActivationContext::Schedule(QuorumCheckTimeout, new IEventHandle(TEvPrivate::EvQuorumCheckTimeout, 0,
                    SelfId(), {}, nullptr, 0));
                RootState = ERootState::QUORUM_CHECK_TIMEOUT;
            }
        }

        void HandleQuorumCheckTimeout() {
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

        void ProcessGather(NKikimrBlobStorage::TEvNodeConfigGather&& res) {
            switch (RootState) {
                case ERootState::COLLECT_CONFIG:
                    STLOG(PRI_DEBUG, BS_NODE, NWDC27, "ProcessGather(COLLECT_CONFIG)", (Res, res));
                    break;

                default:
                    break;
            }
        }

        bool HasQuorum() const {
            // we have strict majority of all nodes (including this one)
            return AllBoundNodes.size() + 1 > (NodeIds.size() + 1) / 2;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Scatter/gather logic

        void IssueScatterTask(bool locallyGenerated, NKikimrBlobStorage::TEvNodeConfigScatter&& task) {
            const ui64 cookie = NextScatterCookie++;
            STLOG(PRI_DEBUG, BS_NODE, NWDC21, "IssueScatterTask", (Task, task), (Cookie, cookie));
            Y_VERIFY(locallyGenerated || Binding);
            const auto [it, inserted] = ScatterTasks.try_emplace(cookie, locallyGenerated ? std::nullopt : Binding,
                std::move(task));
            Y_VERIFY(inserted);
            TScatterTask& scatterTask = it->second;
            for (auto& [nodeId, info] : DirectBoundNodes) {
                auto ev = std::make_unique<TEvNodeConfigScatter>();
                ev->Record.CopyFrom(scatterTask.Task);
                ev->Record.SetCookie(cookie);
                SendEvent(nodeId, info, std::move(ev));
                info.ScatterTasks.insert(cookie);
                scatterTask.PendingNodes.insert(nodeId);
            }
            if (scatterTask.PendingNodes.empty()) {
                CompleteScatterTask(scatterTask);
                ScatterTasks.erase(it);
            }
        }

        void CompleteScatterTask(TScatterTask& task) {
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

                case NKikimrBlobStorage::TEvNodeConfigScatter::kApplyConfigs:
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
                ProcessGather(std::move(res));
            }
        }

        void GenerateCollectConfigs(NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs *response, TScatterTask& task) {
            THashMap<std::tuple<ui64, TString>, NKikimrBlobStorage::TEvNodeConfigGather::TCollectConfigs::TItem*> configs;

            auto addConfig = [&](const NKikimrBlobStorage::TStorageConfig& config, const auto& nodeIds) {
                const auto key = std::make_tuple(config.GetGeneration(), config.GetFingerprint());
                auto& ptr = configs[key];
                if (!ptr) {
                    ptr = response->AddItems();
                    ptr->MutableConfig()->CopyFrom(config);
                }
                for (const ui32 nodeId : nodeIds) {
                    ptr->AddNodeIds(nodeId);
                }
            };

            addConfig(StorageConfig, std::initializer_list<ui32>{SelfId().NodeId()});

            for (const auto& reply : task.CollectedReplies) {
                if (reply.HasCollectConfigs()) {
                    for (const auto& item : reply.GetCollectConfigs().GetItems()) {
                        addConfig(item.GetConfig(), item.GetNodeIds());
                    }
                }
            }
        }

        void AbortScatterTask(ui64 cookie, ui32 nodeId) {
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

        void AbortAllScatterTasks() {
            STLOG(PRI_DEBUG, BS_NODE, NWDC24, "AbortAllScatterTasks");

            Y_VERIFY(Binding);

            for (auto& [cookie, task] : std::exchange(ScatterTasks, {})) {
                Y_VERIFY(task.Origin);
                Y_VERIFY(Binding == task.Origin);

                for (const ui32 nodeId : task.PendingNodes) {
                    const auto it = DirectBoundNodes.find(nodeId);
                    Y_VERIFY(it != DirectBoundNodes.end());
                    TBoundNode& info = it->second;
                    const size_t n = info.ScatterTasks.erase(cookie);
                    Y_VERIFY(n == 1);
                }
            }
        }

        void Handle(TEvNodeConfigScatter::TPtr ev) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC25, "TEvNodeConfigScatter", (Binding, Binding), (Sender, ev->Sender),
                (Cookie, ev->Cookie), (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

            if (Binding && Binding->Expected(*ev)) {
                IssueScatterTask(false, std::move(ev->Get()->Record));
            }
        }

        void Handle(TEvNodeConfigGather::TPtr ev) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC26, "TEvNodeConfigGather", (Sender, ev->Sender), (Cookie, ev->Cookie),
                (SessionId, ev->InterconnectSession), (Record, ev->Get()->Record));

            const ui32 senderNodeId = ev->Sender.NodeId();
            if (const auto it = DirectBoundNodes.find(senderNodeId); it != DirectBoundNodes.end()
                    && ev->Cookie == it->second.Cookie
                    && ev->InterconnectSession == it->second.SessionId) {
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

                AbortAllScatterTasks();
                Binding.reset();
                IssueNextBindRequest();

                for (const auto& [nodeId, info] : DirectBoundNodes) {
                    SendEvent(nodeId, info.Cookie, info.SessionId, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId()));
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

            for (const auto& [cookie, task] : ScatterTasks) {
                for (const ui32 nodeId : task.PendingNodes) {
                    const auto it = DirectBoundNodes.find(nodeId);
                    Y_VERIFY(it != DirectBoundNodes.end());
                    TBoundNode& info = it->second;
                    Y_VERIFY(info.ScatterTasks.contains(cookie));
                }
            }

            for (const auto& [nodeId, info] : DirectBoundNodes) {
                for (const ui64 cookie : info.ScatterTasks) {
                    const auto it = ScatterTasks.find(cookie);
                    Y_VERIFY(it != ScatterTasks.end());
                    TScatterTask& task = it->second;
                    Y_VERIFY(task.PendingNodes.contains(nodeId));
                }
            }

            for (const auto& [cookie, task] : ScatterTasks) {
                if (task.Origin) {
                    Y_VERIFY(Binding);
                    Y_VERIFY(task.Origin == Binding);
                } else { // locally-generated task
                    Y_VERIFY(RootState != ERootState::INITIAL);
                    Y_VERIFY(!Binding);
                }
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
                hFunc(TEvNodeConfigScatter, Handle);
                hFunc(TEvNodeConfigGather, Handle);
                hFunc(TEvInterconnect::TEvNodesInfo, Handle);
                hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                cFunc(TEvPrivate::EvQuorumCheckTimeout, HandleQuorumCheckTimeout);
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
