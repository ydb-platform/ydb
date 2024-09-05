#include "distconf.h"
#include "node_warden_impl.h"
#include <ydb/core/mind/dynamic_nameserver.h>

namespace NKikimr::NStorage {

    TDistributedConfigKeeper::TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg,
            const NKikimrBlobStorage::TStorageConfig& baseConfig, bool isSelfStatic)
        : IsSelfStatic(isSelfStatic)
        , Cfg(std::move(cfg))
        , BaseConfig(baseConfig)
        , InitialConfig(baseConfig)
    {
        UpdateFingerprint(&BaseConfig);
        InitialConfig.SetFingerprint(BaseConfig.GetFingerprint());
    }

    void TDistributedConfigKeeper::Bootstrap() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC00, "Bootstrap");

        auto ns = NNodeBroker::BuildNameserverTable(Cfg->NameserviceConfig);
        auto ev = std::make_unique<TEvInterconnect::TEvNodesInfo>();
        for (const auto& [nodeId, item] : ns->StaticNodeTable) {
            ev->Nodes.emplace_back(nodeId, item.Address, item.Host, item.ResolveHost, item.Port, item.Location);
        }
        Send(SelfId(), ev.release());

        // and subscribe for the node list too
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));

        // generate initial drive set and query stored configuration
        if (IsSelfStatic) {
            EnumerateConfigDrives(InitialConfig, SelfId().NodeId(), [&](const auto& /*node*/, const auto& drive) {
                DrivesToRead.push_back(drive.GetPath());
            });
            std::sort(DrivesToRead.begin(), DrivesToRead.end());
            ReadConfig();
        } else {
            StorageConfigLoaded = true;
        }

        Become(&TThis::StateWaitForInit);
    }

    void TDistributedConfigKeeper::PassAway() {
        for (const TActorId& actorId : ChildActors) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
        TActorBootstrapped::PassAway();
    }

    void TDistributedConfigKeeper::HandleGone(STATEFN_SIG) {
        const size_t numErased = ChildActors.erase(ev->Sender);
        Y_DEBUG_ABORT_UNLESS(numErased == 1);
    }

    void TDistributedConfigKeeper::Halt() {
        // TODO: implement
    }

    bool TDistributedConfigKeeper::ApplyStorageConfig(const NKikimrBlobStorage::TStorageConfig& config) {
        if (!StorageConfig || StorageConfig->GetGeneration() < config.GetGeneration()) {
            StorageConfig.emplace(config);
            if (ProposedStorageConfig && ProposedStorageConfig->GetGeneration() <= StorageConfig->GetGeneration()) {
                ProposedStorageConfig.reset();
            }
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenStorageConfig(*StorageConfig,
                ProposedStorageConfig ? &ProposedStorageConfig.value() : nullptr));
            if (IsSelfStatic) {
                PersistConfig({});
                ApplyConfigUpdateToDynamicNodes(false);
            }
            return true;
        } else if (StorageConfig->GetGeneration() && StorageConfig->GetGeneration() == config.GetGeneration() &&
                StorageConfig->GetFingerprint() != config.GetFingerprint()) {
            // TODO: fingerprint mismatch, abort operation
        }
        return false;
    }

    void TDistributedConfigKeeper::HandleConfigConfirm(STATEFN_SIG) {
        if (ev->Cookie) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC46, "HandleConfigConfirm", (Cookie, ev->Cookie),
                (ProposedStorageConfigCookie, ProposedStorageConfigCookie),
                (ProposedStorageConfigCookieUsage, ProposedStorageConfigCookieUsage));
            if (ev->Cookie == ProposedStorageConfigCookie && ProposedStorageConfigCookieUsage) {
                --ProposedStorageConfigCookieUsage;
            }
            FinishAsyncOperation(ev->Cookie);
        }
    }

    void TDistributedConfigKeeper::SendEvent(ui32 nodeId, ui64 cookie, TActorId sessionId, std::unique_ptr<IEventBase> ev) {
        Y_ABORT_UNLESS(nodeId != SelfId().NodeId());
        auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(nodeId), SelfId(), ev.release(), 0, cookie);
        Y_ABORT_UNLESS(sessionId);
        handle->Rewrite(TEvInterconnect::EvForward, sessionId);
        TActivationContext::Send(handle.release());
    }

    void TDistributedConfigKeeper::SendEvent(const TBinding& binding, std::unique_ptr<IEventBase> ev) {
        Y_ABORT_UNLESS(binding.SessionId);
        SendEvent(binding.NodeId, binding.Cookie, binding.SessionId, std::move(ev));
    }

    void TDistributedConfigKeeper::SendEvent(const IEventHandle& handle, std::unique_ptr<IEventBase> ev) {
        SendEvent(handle.Sender.NodeId(), handle.Cookie, handle.InterconnectSession, std::move(ev));
    }

    void TDistributedConfigKeeper::SendEvent(ui32 nodeId, const TBoundNode& info, std::unique_ptr<IEventBase> ev) {
        SendEvent(nodeId, info.Cookie, info.SessionId, std::move(ev));
    }

#ifndef NDEBUG
    void TDistributedConfigKeeper::ConsistencyCheck() {
        for (const auto& [nodeId, info] : DirectBoundNodes) {
            Y_ABORT_UNLESS(std::binary_search(NodeIds.begin(), NodeIds.end(), nodeId));
        }
        if (Binding) {
            Y_ABORT_UNLESS(std::binary_search(NodeIds.begin(), NodeIds.end(), Binding->NodeId));
        }

        for (const auto& [cookie, task] : ScatterTasks) {
            for (const ui32 nodeId : task.PendingNodes) {
                const auto it = DirectBoundNodes.find(nodeId);
                Y_ABORT_UNLESS(it != DirectBoundNodes.end());
                TBoundNode& info = it->second;
                Y_ABORT_UNLESS(info.ScatterTasks.contains(cookie));
            }
        }

        for (const auto& [nodeId, info] : DirectBoundNodes) {
            for (const ui64 cookie : info.ScatterTasks) {
                const auto it = ScatterTasks.find(cookie);
                Y_ABORT_UNLESS(it != ScatterTasks.end());
                TScatterTask& task = it->second;
                Y_ABORT_UNLESS(task.PendingNodes.contains(nodeId));
            }
        }

        for (const auto& [cookie, task] : ScatterTasks) {
            if (task.Origin) {
                Y_ABORT_UNLESS(Binding);
                Y_ABORT_UNLESS(task.Origin == Binding);
            }
        }

        for (const auto& [nodeId, subs] : SubscribedSessions) {
            bool okay = false;
            if (Binding && Binding->NodeId == nodeId) {
                Y_VERIFY_S(subs.SessionId == Binding->SessionId || !Binding->SessionId,
                    "Binding# " << Binding->ToString() << " Subscription# " << subs.ToString());
                okay = true;
            }
            if (const auto it = DirectBoundNodes.find(nodeId); it != DirectBoundNodes.end()) {
                Y_VERIFY_S(!subs.SessionId || subs.SessionId == it->second.SessionId, "sessionId# " << subs.SessionId
                    << " node.SessionId# " << it->second.SessionId);
                okay = true;
            }
            if (!subs.SessionId) {
                okay = true; // may be just obsolete subscription request
            }
            if (ConnectedDynamicNodes.contains(nodeId)) {
                okay = true;
            }
            Y_ABORT_UNLESS(okay);
            if (subs.SubscriptionCookie) {
                const auto it = SubscriptionCookieMap.find(subs.SubscriptionCookie);
                Y_ABORT_UNLESS(it != SubscriptionCookieMap.end());
                Y_ABORT_UNLESS(it->second == nodeId);
            }
        }
        for (const auto& [cookie, nodeId] : SubscriptionCookieMap) {
            const auto it = SubscribedSessions.find(nodeId);
            Y_ABORT_UNLESS(it != SubscribedSessions.end());
            const TSessionSubscription& subs = it->second;
            Y_VERIFY_S(subs.SubscriptionCookie == cookie, "SubscriptionCookie# " << subs.SubscriptionCookie
                << " cookie# " << cookie);
        }

        if (Binding) {
            Y_ABORT_UNLESS(SubscribedSessions.contains(Binding->NodeId));
        }
        for (const auto& [nodeId, info] : DirectBoundNodes) {
            Y_VERIFY_S(SubscribedSessions.contains(nodeId), "NodeId# " << nodeId);
        }

        Y_ABORT_UNLESS(!StorageConfig || CheckFingerprint(*StorageConfig));
        Y_ABORT_UNLESS(!ProposedStorageConfig || CheckFingerprint(*ProposedStorageConfig));
        Y_ABORT_UNLESS(CheckFingerprint(BaseConfig));
        Y_ABORT_UNLESS(!InitialConfig.GetFingerprint() || CheckFingerprint(InitialConfig));

        if (Scepter) {
            Y_ABORT_UNLESS(HasQuorum());
            Y_ABORT_UNLESS(RootState != ERootState::INITIAL && RootState != ERootState::ERROR_TIMEOUT);
            Y_ABORT_UNLESS(!Binding);
        } else {
            Y_ABORT_UNLESS(RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT);
        }
    }
#endif

    STFUNC(TDistributedConfigKeeper::StateWaitForInit) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC53, "StateWaitForInit event", (Type, ev->GetTypeRewrite()),
            (StorageConfigLoaded, StorageConfigLoaded), (NodeListObtained, NodeListObtained),
            (PendingEvents.size, PendingEvents.size()));

        auto processPendingEvents = [&] {
            if (PendingEvents.empty()) {
                Become(&TThis::StateFunc);
            } else {
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvProcessPendingEvent, 0, SelfId(), {}, nullptr, 0));
            }
        };

        bool change = false;
        const bool wasStorageConfigLoaded = StorageConfigLoaded;

        switch (ev->GetTypeRewrite()) {
            case TEvInterconnect::TEvNodesInfo::EventType:
                Handle(reinterpret_cast<TEvInterconnect::TEvNodesInfo::TPtr&>(ev));
                if (!NodeIds.empty() || !IsSelfStatic) {
                    change = !std::exchange(NodeListObtained, true);
                }
                break;

            case TEvPrivate::EvStorageConfigLoaded:
                Handle(reinterpret_cast<TEvPrivate::TEvStorageConfigLoaded::TPtr&>(ev));
                change = wasStorageConfigLoaded < StorageConfigLoaded;
                break;

            case TEvPrivate::EvProcessPendingEvent:
                Y_ABORT_UNLESS(!PendingEvents.empty());
                StateFunc(PendingEvents.front());
                PendingEvents.pop_front();
                processPendingEvents();
                break;

            default:
                PendingEvents.push_back(std::move(ev));
                break;
        }

        if (change && NodeListObtained && StorageConfigLoaded) {
            if (IsSelfStatic) {
                UpdateBound(SelfNode.NodeId(), SelfNode, *StorageConfig, nullptr);
                IssueNextBindRequest();
            }
            processPendingEvents();
        }
    }

    STFUNC(TDistributedConfigKeeper::StateFunc) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC15, "StateFunc", (Type, ev->GetTypeRewrite()), (Sender, ev->Sender),
            (SessionId, ev->InterconnectSession), (Cookie, ev->Cookie));
        const ui32 senderNodeId = ev->Sender.NodeId();
        if (ev->InterconnectSession && SubscribedSessions.contains(senderNodeId)) {
            // keep session actors intact
            SubscribeToPeerNode(senderNodeId, ev->InterconnectSession);
        }
        STRICT_STFUNC_BODY(
            hFunc(TEvNodeConfigPush, Handle);
            hFunc(TEvNodeConfigReversePush, Handle);
            hFunc(TEvNodeConfigUnbind, Handle);
            hFunc(TEvNodeConfigScatter, Handle);
            hFunc(TEvNodeConfigGather, Handle);
            hFunc(TEvNodeConfigInvokeOnRoot, Handle);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            cFunc(TEvPrivate::EvErrorTimeout, HandleErrorTimeout);
            hFunc(TEvPrivate::TEvStorageConfigLoaded, Handle);
            hFunc(TEvPrivate::TEvStorageConfigStored, Handle);
            fFunc(TEvBlobStorage::EvNodeWardenStorageConfigConfirm, HandleConfigConfirm);
            fFunc(TEvBlobStorage::EvNodeWardenDynamicConfigSubscribe, HandleDynamicConfigSubscribe);
            hFunc(TEvNodeWardenDynamicConfigPush, Handle);
            cFunc(TEvPrivate::EvReconnect, HandleReconnect);
            hFunc(NMon::TEvHttpInfo, Handle);
            fFunc(TEvents::TSystem::Gone, HandleGone);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
        for (ui32 nodeId : std::exchange(UnsubscribeQueue, {})) {
            UnsubscribeInterconnect(nodeId);
        }
        if (IsSelfStatic && StorageConfig && NodeListObtained) {
            IssueNextBindRequest();
        }
        ConsistencyCheck();
    }

    void TNodeWarden::StartDistributedConfigKeeper() {
        auto *appData = AppData();
        const bool isSelfStatic = !appData->DynamicNameserviceConfig || SelfId().NodeId() <= appData->DynamicNameserviceConfig->MaxStaticNodeId;
        DistributedConfigKeeperId = Register(new TDistributedConfigKeeper(Cfg, StorageConfig, isSelfStatic));
    }

    void TNodeWarden::ForwardToDistributedConfigKeeper(STATEFN_SIG) {
        ev->Rewrite(ev->GetTypeRewrite(), DistributedConfigKeeperId);
        TActivationContext::Send(ev.Release());
    }

} // NKikimr::NStorage

template<>
void Out<NKikimr::NStorage::TDistributedConfigKeeper::ERootState>(IOutputStream& s, NKikimr::NStorage::TDistributedConfigKeeper::ERootState state) {
    using E = decltype(state);
    switch (state) {
        case E::INITIAL:       s << "INITIAL";       return;
        case E::ERROR_TIMEOUT: s << "ERROR_TIMEOUT"; return;
        case E::IN_PROGRESS:   s << "IN_PROGRESS";   return;
        case E::RELAX:         s << "RELAX";         return;
    }
    Y_ABORT();
}

template<>
void Out<NKikimr::NStorage::TNodeIdentifier>(IOutputStream& s, const NKikimr::NStorage::TNodeIdentifier& value) {
    s << std::get<0>(value) << ':' << std::get<1>(value) << '/' << std::get<2>(value);
}
