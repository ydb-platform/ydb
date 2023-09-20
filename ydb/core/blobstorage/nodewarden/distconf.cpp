#include "distconf.h"
#include "node_warden_impl.h"

namespace NKikimr::NStorage {

    TDistributedConfigKeeper::TDistributedConfigKeeper(TIntrusivePtr<TNodeWardenConfig> cfg,
            const NKikimrBlobStorage::TStorageConfig& baseConfig)
        : Cfg(std::move(cfg))
        , BaseConfig(baseConfig)
        , InitialConfig(baseConfig)
    {
        UpdateFingerprint(&BaseConfig);
        InitialConfig.SetFingerprint(BaseConfig.GetFingerprint());
    }

    void TDistributedConfigKeeper::Bootstrap() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC00, "Bootstrap");

        // TODO: maybe extract list of nodes from the initial storage config?
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));

        // generate initial drive set and query stored configuration
        EnumerateConfigDrives(InitialConfig, SelfId().NodeId(), [&](const auto& /*node*/, const auto& drive) {
            DrivesToRead.push_back(drive.GetPath());
        });
        std::sort(DrivesToRead.begin(), DrivesToRead.end());

        auto query = std::bind(&TThis::ReadConfig, TActivationContext::ActorSystem(), SelfId(), DrivesToRead, Cfg, 0);
        Send(MakeIoDispatcherActorId(), new TEvInvokeQuery(std::move(query)));
        Become(&TThis::StateWaitForInit);
    }

    void TDistributedConfigKeeper::Halt() {
        // TODO: implement
    }

    bool TDistributedConfigKeeper::ApplyStorageConfig(const NKikimrBlobStorage::TStorageConfig& config) {
        if (!StorageConfig || StorageConfig->GetGeneration() < config.GetGeneration()) {
            StorageConfig.emplace(config);
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenStorageConfig(*StorageConfig));
            if (ProposedStorageConfig && ProposedStorageConfig->GetGeneration() <= StorageConfig->GetGeneration()) {
                ProposedStorageConfig.reset();
            }
            PersistConfig({});
            return true;
        } else if (StorageConfig->GetGeneration() && StorageConfig->GetGeneration() == config.GetGeneration() &&
                StorageConfig->GetFingerprint() != config.GetFingerprint()) {
            // TODO: fingerprint mismatch, abort operation
        }
        return false;
    }

    void TDistributedConfigKeeper::SendEvent(ui32 nodeId, ui64 cookie, TActorId sessionId, std::unique_ptr<IEventBase> ev) {
        Y_VERIFY(nodeId != SelfId().NodeId());
        auto handle = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(nodeId), SelfId(), ev.release(), 0, cookie);
        Y_VERIFY(sessionId);
        handle->Rewrite(TEvInterconnect::EvForward, sessionId);
        TActivationContext::Send(handle.release());
    }

    void TDistributedConfigKeeper::SendEvent(const TBinding& binding, std::unique_ptr<IEventBase> ev) {
        Y_VERIFY(binding.SessionId);
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

        for (const auto& [nodeId, sessionId] : SubscribedSessions) {
            bool okay = false;
            if (Binding && Binding->NodeId == nodeId) {
                Y_VERIFY(sessionId == Binding->SessionId);
                okay = true;
            }
            if (const auto it = DirectBoundNodes.find(nodeId); it != DirectBoundNodes.end()) {
                Y_VERIFY(!sessionId || sessionId == it->second.SessionId);
                okay = true;
            }
            if (!sessionId) {
                okay = true; // may be just obsolete subscription request
            }
            Y_VERIFY(okay);
        }

        if (Binding) {
            Y_VERIFY(SubscribedSessions.contains(Binding->NodeId));
        }
        for (const auto& [nodeId, info] : DirectBoundNodes) {
            Y_VERIFY(SubscribedSessions.contains(nodeId));
        }

        Y_VERIFY(!StorageConfig || CheckFingerprint(*StorageConfig));
        Y_VERIFY(!ProposedStorageConfig || CheckFingerprint(*ProposedStorageConfig));
        Y_VERIFY(CheckFingerprint(BaseConfig));
        Y_VERIFY(!InitialConfig.GetFingerprint() || CheckFingerprint(InitialConfig));
    }
#endif

    STFUNC(TDistributedConfigKeeper::StateWaitForInit) {
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
                NodeListObtained = change = true;
                break;

            case TEvPrivate::EvStorageConfigLoaded:
                Handle(reinterpret_cast<TEvPrivate::TEvStorageConfigLoaded::TPtr&>(ev));
                change = wasStorageConfigLoaded < StorageConfigLoaded;
                break;

            case TEvPrivate::EvProcessPendingEvent:
                Y_VERIFY(!PendingEvents.empty());
                StateFunc(PendingEvents.front());
                PendingEvents.pop_front();
                processPendingEvents();
                break;

            default:
                PendingEvents.push_back(std::move(ev));
                break;
        }

        if (NodeListObtained && StorageConfigLoaded && change) {
            UpdateBound(SelfNode.NodeId(), SelfNode, *StorageConfig, nullptr);
            IssueNextBindRequest();
            processPendingEvents();
        }
    }

    STFUNC(TDistributedConfigKeeper::StateFunc) {
        STRICT_STFUNC_BODY(
            hFunc(TEvNodeConfigPush, Handle);
            hFunc(TEvNodeConfigReversePush, Handle);
            hFunc(TEvNodeConfigUnbind, Handle);
            hFunc(TEvNodeConfigScatter, Handle);
            hFunc(TEvNodeConfigGather, Handle);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvPrivate::EvErrorTimeout, HandleErrorTimeout);
            hFunc(TEvPrivate::TEvStorageConfigLoaded, Handle);
            hFunc(TEvPrivate::TEvStorageConfigStored, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
        ConsistencyCheck();
    }

    void TNodeWarden::StartDistributedConfigKeeper() {
        return;
        DistributedConfigKeeperId = Register(new TDistributedConfigKeeper(Cfg, StorageConfig));
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
        case E::INITIAL:                    s << "INITIAL";                    return;
        case E::COLLECT_CONFIG:             s << "COLLECT_CONFIG";             return;
        case E::PROPOSE_NEW_STORAGE_CONFIG: s << "PROPOSE_NEW_STORAGE_CONFIG"; return;
        case E::ERROR_TIMEOUT:              s << "ERROR_TIMEOUT";              return;
    }
    Y_FAIL();
}

template<>
void Out<NKikimr::NStorage::TNodeIdentifier>(IOutputStream& s, const NKikimr::NStorage::TNodeIdentifier& value) {
    s << std::get<0>(value) << ':' << std::get<1>(value) << '/' << std::get<2>(value);
}
