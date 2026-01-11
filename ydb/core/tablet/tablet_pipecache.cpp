#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/hfunc.h>

#include <algorithm>

namespace NKikimr {

class TPipePerNodeCache : public TActor<TPipePerNodeCache> {
    TIntrusiveConstPtr<TPipePerNodeCacheConfig> Config;
    NTabletPipe::TClientConfig PipeConfig;

    struct TCounters {
        ::NMonitoring::TDynamicCounters::TCounterPtr Tablets;
        ::NMonitoring::TDynamicCounters::TCounterPtr Subscribers;
        ::NMonitoring::TDynamicCounters::TCounterPtr PipesActive;
        ::NMonitoring::TDynamicCounters::TCounterPtr PipesInactive;
        ::NMonitoring::TDynamicCounters::TCounterPtr PipesConnecting;
        ::NMonitoring::TDynamicCounters::TCounterPtr EventCreate;
        ::NMonitoring::TDynamicCounters::TCounterPtr EventConnectOk;
        ::NMonitoring::TDynamicCounters::TCounterPtr EventConnectFailure;
        ::NMonitoring::TDynamicCounters::TCounterPtr EventGracefulShutdown;
        ::NMonitoring::TDynamicCounters::TCounterPtr EventDisconnect;
        bool HaveCounters = false;

        explicit TCounters(::NMonitoring::TDynamicCounterPtr counters) {
            if (counters) {
                Tablets = counters->GetCounter("PipeCache/Tablets");
                Subscribers = counters->GetCounter("PipeCache/Subscribers");
                PipesActive = counters->GetCounter("PipeCache/Pipes/Active");
                PipesInactive = counters->GetCounter("PipeCache/Pipes/Inactive");
                PipesConnecting = counters->GetCounter("PipeCache/Pipes/Connecting");
                EventCreate = counters->GetCounter("PipeCache/Event/Create", true);
                EventConnectOk = counters->GetCounter("PipeCache/Event/ConnectOk", true);
                EventConnectFailure = counters->GetCounter("PipeCache/Event/ConnectFailure", true);
                EventGracefulShutdown = counters->GetCounter("PipeCache/Event/GracefulShutdown", true);
                EventDisconnect = counters->GetCounter("PipeCache/Event/Disconnect", true);
                HaveCounters = true;
            }
        }

        explicit operator bool() const {
            return HaveCounters;
        }
    };

    TCounters Counters;

    struct TNodeRequest {
        TActorId Sender;
        ui64 Cookie;

        TNodeRequest(TActorId sender, ui64 cookie)
            : Sender(sender)
            , Cookie(cookie)
        { }
    };

    struct TClientSubscription {
        ui64 SeqNo;
        ui64 Cookie;
    };

    struct TClientState {
        TActorId Client;
        THashMap<TActorId, TClientSubscription> Peers;
        ui64 LastSentSeqNo = 0;
        ui64 MaxForwardedSeqNo = Max<ui64>();
        TVector<TNodeRequest> NodeRequests;
        ui32 NodeId = 0;
        bool Connected = false;
    };

    struct TTabletState {
        bool ForceReconnect = false;

        THashMap<TActorId, TClientState> ByClient;
        THashMap<TActorId, TClientState*> ByPeer;

        // Multiple active clients for round-robin distribution
        TVector<TActorId> ActiveClients;
        ui32 NextClientIndex = 0;
        TInstant LastCreated;

        TClientState* FindClient(const TActorId& clientId) {
            auto it = ByClient.find(clientId);
            if (it == ByClient.end()) {
                return nullptr;
            }
            return &it->second;
        }

        TClientState* GetActive() {
            if (ActiveClients.empty()) {
                return nullptr;
            }
            return FindClient(ActiveClients[0]);
        }

        TClientState* GetNextActiveClient() {
            if (ActiveClients.empty()) {
                return nullptr;
            }
            // Round-robin selection among active clients
            NextClientIndex = (NextClientIndex + 1) % ActiveClients.size();
            return FindClient(ActiveClients[NextClientIndex]);
        }

        bool IsActive(const TActorId& client) const {
            return std::find(ActiveClients.begin(), ActiveClients.end(), client) != ActiveClients.end();
        }

        bool IsActive(TClientState* clientState) const {
            return IsActive(clientState->Client);
        }

        void AddActiveClient(const TActorId& client) {
            if (!IsActive(client)) {
                ActiveClients.push_back(client);
            }
        }

        void RemoveActiveClient(const TActorId& client) {
            auto it = std::find(ActiveClients.begin(), ActiveClients.end(), client);
            if (it != ActiveClients.end()) {
                ActiveClients.erase(it);
                if (NextClientIndex >= ActiveClients.size() && !ActiveClients.empty()) {
                    NextClientIndex = 0;
                }
            }
        }

        void Deactivate(const TActorId& client) {
            RemoveActiveClient(client);
        }

        // Count how many active clients are connected to a specific follower node
        ui32 CountClientsOnNode(ui32 nodeId) const {
            ui32 count = 0;
            for (const auto& clientId : ActiveClients) {
                auto it = ByClient.find(clientId);
                if (it != ByClient.end() && it->second.NodeId == nodeId && it->second.Connected) {
                    ++count;
                }
            }
            return count;
        }

        // Get a list of connected follower NodeIds
        THashSet<ui32> GetConnectedNodes() const {
            THashSet<ui32> nodes;
            for (const auto& clientId : ActiveClients) {
                auto it = ByClient.find(clientId);
                if (it != ByClient.end() && it->second.Connected && it->second.NodeId != 0) {
                    nodes.insert(it->second.NodeId);
                }
            }
            return nodes;
        }
    };

    struct TPeerState {
        THashSet<ui64> ConnectedToTablet;
    };

    using TByTablet = THashMap<ui64, TTabletState>;
    using TByPeer = THashMap<TActorId, TPeerState>;

    TByTablet ByTablet;
    TByPeer ByPeer;

    TTabletState* FindTablet(ui64 tablet) {
        auto it = ByTablet.find(tablet);
        if (it == ByTablet.end()) {
            return nullptr;
        }
        return &it->second;
    }

    TTabletState* EnsureTablet(ui64 tabletId) {
        auto it = ByTablet.find(tabletId);
        if (it != ByTablet.end()) {
            return &it->second;
        }
        if (Counters) {
            Counters.Tablets->Inc();
        }
        return &ByTablet[tabletId];
    }

    void ForgetTablet(ui64 tabletId) {
        if (ByTablet.erase(tabletId) && Counters) {
            Counters.Tablets->Dec();
        }
    }

    void RemoveClientPeer(TTabletState* tabletState, TClientState* clientState, const TActorId& peer) {
        clientState->Peers.erase(peer);

        // Remove old clients that no longer have any peers
        if (clientState->Peers.empty() && !tabletState->IsActive(clientState)) {
            TActorId client = clientState->Client;
            if (Counters) {
                if (clientState->Connected) {
                    Counters.PipesInactive->Dec();
                } else {
                    Counters.PipesConnecting->Dec();
                }
            }
            NTabletPipe::CloseClient(SelfId(), client);
            tabletState->ByClient.erase(client);
        }
    }

    void UnlinkOne(TActorId peer, ui64 tablet) {
        auto *tabletState = FindTablet(tablet);
        if (Y_UNLIKELY(!tabletState))
            return;

        auto byPeerIt = tabletState->ByPeer.find(peer);
        if (byPeerIt == tabletState->ByPeer.end())
            return;

        auto *clientState = byPeerIt->second;
        Y_ABORT_UNLESS(clientState, "Unexpected nullptr in tablet's ByPeer links");
        tabletState->ByPeer.erase(byPeerIt);

        RemoveClientPeer(tabletState, clientState, peer);

        // Avoid keeping dead tablets forever
        if (tabletState->ByClient.empty()) {
            ForgetTablet(tablet);
        }
    }

    void DropClient(ui64 tablet, TActorId client, bool connected, bool notDelivered, bool isDeleted) {
        auto *tabletState = FindTablet(tablet);
        if (Y_UNLIKELY(!tabletState))
            return;

        auto *clientState = tabletState->FindClient(client);
        if (!clientState)
            return;

        for (auto &kv : clientState->Peers) {
            const auto &peer = kv.first;
            const ui64 seqNo = kv.second.SeqNo;
            const ui64 cookie = kv.second.Cookie;
            const bool msgNotDelivered = notDelivered || seqNo > clientState->MaxForwardedSeqNo;
            Send(peer, new TEvPipeCache::TEvDeliveryProblem(tablet, connected, msgNotDelivered, isDeleted), 0, cookie);

            tabletState->ByPeer.erase(peer);

            RemovePeerTablet(peer, tablet);
        }
        clientState->Peers.clear();

        for (auto &req : clientState->NodeRequests) {
            Send(req.Sender, new TEvPipeCache::TEvGetTabletNodeResult(tablet, 0), 0, req.Cookie);
        }

        if (Counters) {
            if (clientState->Connected) {
                if (tabletState->IsActive(clientState)) {
                    Counters.PipesActive->Dec();
                } else {
                    Counters.PipesInactive->Dec();
                }
            } else {
                Counters.PipesConnecting->Dec();
            }
        }

        tabletState->ByClient.erase(client);
        tabletState->Deactivate(client);

        // Avoid keeping dead tablets forever
        if (tabletState->ByClient.empty()) {
            ForgetTablet(tablet);
        }
    }

    bool TryRemoveIdleClient(TTabletState *tabletState, const TActorId& clientId) {
        auto* cs = tabletState->FindClient(clientId);
        if (!cs || !cs->Peers.empty()) {
            return false;  // Client has peers, can't remove
        }
        if (Counters) {
            if (cs->Connected) {
                Counters.PipesActive->Dec();
            } else {
                Counters.PipesConnecting->Dec();
            }
        }
        NTabletPipe::CloseClient(SelfId(), clientId);
        tabletState->ByClient.erase(clientId);
        tabletState->RemoveActiveClient(clientId);
        return true;
    }

    void CleanupIdleClients(TTabletState *tabletState) {
        // Remove idle active clients (those with no peers)
        // Keep at least one client for immediate use
        TVector<TActorId> toRemove;
        for (const auto& clientId : tabletState->ActiveClients) {
            if (tabletState->ActiveClients.size() - toRemove.size() <= 1) {
                break;  // Keep at least one client
            }
            if (auto* cs = tabletState->FindClient(clientId)) {
                if (cs->Peers.empty() && cs->Connected) {
                    toRemove.push_back(clientId);
                }
            }
        }

        for (const auto& clientId : toRemove) {
            TryRemoveIdleClient(tabletState, clientId);
        }
    }

    TClientState* EnsureClient(TTabletState *tabletState, ui64 tabletId) {
        // Count connected clients among active ones
        ui32 connectedCount = 0;
        for (const auto& clientId : tabletState->ActiveClients) {
            if (auto* cs = tabletState->FindClient(clientId)) {
                if (cs->Connected) {
                    connectedCount++;
                }
            }
        }

        bool refreshExpired = Config->PipeRefreshTime &&
            Config->PipeRefreshTime < (TActivationContext::Now() - tabletState->LastCreated);

        // Periodically cleanup idle clients when we have many
        if (refreshExpired && tabletState->ActiveClients.size() > 1) {
            CleanupIdleClients(tabletState);
        }

        // Determine if we need to create a new client
        bool needNewClient = tabletState->ActiveClients.empty() ||
            tabletState->ForceReconnect ||
            (connectedCount < Config->MaxActiveClients &&
             tabletState->ActiveClients.size() < Config->MaxActiveClients &&
             refreshExpired);

        // When at MaxActiveClients limit and need new client (refresh or force), try to remove an idle client
        if (needNewClient && tabletState->ActiveClients.size() >= Config->MaxActiveClients) {
            // Copy the list to avoid iterator invalidation when removing
            TVector<TActorId> clientsCopy = tabletState->ActiveClients;
            for (const auto& clientId : clientsCopy) {
                if (TryRemoveIdleClient(tabletState, clientId)) {
                    break;
                }
            }
        }

        // Also try to rotate when refresh expired even if we didn't initially need a new client
        if (!needNewClient && refreshExpired &&
            tabletState->ActiveClients.size() >= Config->MaxActiveClients) {
            // Copy the list to avoid iterator invalidation when removing
            TVector<TActorId> clientsCopy = tabletState->ActiveClients;
            for (const auto& clientId : clientsCopy) {
                if (TryRemoveIdleClient(tabletState, clientId)) {
                    needNewClient = true;
                    break;
                }
            }
        }

        if (needNewClient && tabletState->ActiveClients.size() < Config->MaxActiveClients) {
            tabletState->ForceReconnect = false;
            if (Counters) {
                Counters.PipesConnecting->Inc();
                Counters.EventCreate->Inc();
            }
            TActorId newClient = Register(NTabletPipe::CreateClient(SelfId(), tabletId, PipeConfig));
            tabletState->LastCreated = TActivationContext::Now();
            auto& clientState = tabletState->ByClient[newClient];
            clientState.Client = newClient;
            tabletState->AddActiveClient(newClient);
            return &clientState;
        }

        // Round-robin among existing active clients
        TClientState* clientState = tabletState->GetNextActiveClient();
        Y_ABORT_UNLESS(clientState, "Missing expected client state for active client");
        return clientState;
    }

    void Handle(TEvPipeCache::TEvForcePipeReconnect::TPtr &ev) {
        const ui64 tablet = ev->Get()->TabletId;
        if (auto* tabletState = ByTablet.FindPtr(tablet)) {
            tabletState->ForceReconnect = true;
        }
    }

    void Handle(TEvPipeCache::TEvGetTabletNode::TPtr &ev) {
        const ui64 tablet = ev->Get()->TabletId;

        auto *tabletState = EnsureTablet(tablet);
        auto *clientState = EnsureClient(tabletState, tablet);

        if (clientState->Connected) {
            Send(ev->Sender, new TEvPipeCache::TEvGetTabletNodeResult(tablet, clientState->NodeId), 0, ev->Cookie);
            return;
        }

        clientState->NodeRequests.emplace_back(ev->Sender, ev->Cookie);
    }

    TPeerState* EnsurePeer(const TActorId& peer) {
        auto it = ByPeer.find(peer);
        if (it != ByPeer.end()) {
            return &it->second;
        }
        if (Counters) {
            Counters.Subscribers->Inc();
        }
        return &ByPeer[peer];
    }

    void RemovePeerTablet(const TActorId& peer, ui64 tablet) {
        auto it = ByPeer.find(peer);
        Y_ABORT_UNLESS(it != ByPeer.end());
        it->second.ConnectedToTablet.erase(tablet);
        if (it->second.ConnectedToTablet.empty()) {
            ForgetPeer(it);
        }
    }

    void ForgetPeer(TByPeer::iterator peerIt) {
        ByPeer.erase(peerIt);
        if (Counters) {
            Counters.Subscribers->Dec();
        }
    }

    void Handle(TEvPipeCache::TEvForward::TPtr &ev) {
        TEvPipeCache::TEvForward *msg = ev->Get();
        const ui64 tablet = msg->TabletId;
        const bool subscribe = msg->Options.Subscribe;
        const ui64 subscribeCookie = msg->Options.SubscribeCookie;
        const TActorId peer = ev->Sender;
        const ui64 cookie = ev->Cookie;
        NWilson::TTraceId traceId = std::move(ev->TraceId);

        // Don't create an empty tablet record that won't be used
        if (Y_UNLIKELY(!msg->Options.AutoConnect && !ByTablet.contains(tablet))) {
            if (subscribe) {
                Send(peer, new TEvPipeCache::TEvDeliveryProblem(tablet, false, true, false), 0, subscribeCookie);
            }
            return;
        }

        auto *tabletState = EnsureTablet(tablet);

        TClientState *clientState = nullptr;

        // Use the same pipe after subscription unless resubscribing
        if (!subscribe) {
            auto it = tabletState->ByPeer.find(peer);
            if (it != tabletState->ByPeer.end()) {
                clientState = it->second;
            }
        }

        // Ensure there's a valid pipe for sending messages
        if (!clientState) {
            if (Y_LIKELY(msg->Options.AutoConnect)) {
                clientState = EnsureClient(tabletState, tablet);
            } else if (Y_UNLIKELY(subscribe)) {
                // Use the last active client or send the delivery problem immediately
                // Note that this is not a typical use-case, implemented for completeness
                clientState = tabletState->GetActive();
                if (!clientState) {
                    Send(peer, new TEvPipeCache::TEvDeliveryProblem(tablet, false, true, false), 0, subscribeCookie);
                    return;
                }
            } else {
                return;
            }
        }

        Y_ABORT_UNLESS(clientState);

        if (subscribe) {
            TClientState *&link = tabletState->ByPeer[peer];
            if (link != clientState) {
                if (Y_UNLIKELY(link)) {
                    // Resubscribing to a new pipe
                    RemoveClientPeer(tabletState, link, peer);
                } else {
                    // Register new peer to tablet connection
                    EnsurePeer(peer)->ConnectedToTablet.insert(tablet);
                }
                link = clientState;
            }
            const ui64 seqNo = ++clientState->LastSentSeqNo;
            clientState->Peers[peer] = { seqNo, subscribeCookie };
            NTabletPipe::SendDataWithSeqNo(peer, clientState->Client, msg->Ev.Release(), seqNo, cookie, std::move(traceId));
        } else {
            NTabletPipe::SendData(peer, clientState->Client, msg->Ev.Release(), cookie, std::move(traceId));
        }
    }

    void Handle(TEvPipeCache::TEvUnlink::TPtr &ev) {
        TEvPipeCache::TEvUnlink *msg = ev->Get();
        const ui64 tablet = msg->TabletId;
        const TActorId peer = ev->Sender;

        auto byPeerIt = ByPeer.find(peer);
        if (byPeerIt == ByPeer.end())
            return;

        auto &connectedTo = byPeerIt->second.ConnectedToTablet;
        if (tablet == 0) { // unlink everything
            for (ui64 x : connectedTo)
                UnlinkOne(peer, x);
            ForgetPeer(byPeerIt);
            return;
        } else {
            UnlinkOne(peer, tablet);
            connectedTo.erase(tablet);
            if (connectedTo.empty()) {
                ForgetPeer(byPeerIt);
            }
            return;
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev) {
        const TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        if (msg->Status != NKikimrProto::OK) {
            if (Counters) {
                Counters.EventConnectFailure->Inc();
            }
            return DropClient(msg->TabletId, msg->ClientId, false, true, msg->Dead);
        } else {
            if (Counters) {
                Counters.EventConnectOk->Inc();
            }
        }

        auto *tabletState = FindTablet(msg->TabletId);
        if (tabletState) {
            auto *clientState = tabletState->FindClient(msg->ClientId);
            if (clientState) {
                clientState->NodeId = msg->ServerId.NodeId();
                clientState->Connected = true;
                if (Counters) {
                    if (tabletState->IsActive(clientState)) {
                        Counters.PipesActive->Inc();
                    } else {
                        Counters.PipesInactive->Inc();
                    }
                    Counters.PipesConnecting->Dec();
                }
                auto nodeRequests = std::move(clientState->NodeRequests);
                for (auto &req : nodeRequests) {
                    Send(req.Sender, new TEvPipeCache::TEvGetTabletNodeResult(msg->TabletId, clientState->NodeId), 0, req.Cookie);
                }

                // Check for duplicate connections to the same follower node
                // If this is a duplicate and we have many clients, close an idle duplicate
                if (Config->MaxActiveClients > 1 && clientState->NodeId != 0) {
                    ui32 duplicateCount = tabletState->CountClientsOnNode(clientState->NodeId);
                    if (duplicateCount > 1) {
                        // We have multiple connections to the same follower - try to close an idle one
                        TVector<TActorId> clientsCopy = tabletState->ActiveClients;
                        for (const auto& otherId : clientsCopy) {
                            if (otherId == msg->ClientId) {
                                continue;  // Don't close the one we just connected
                            }
                            auto* otherClient = tabletState->FindClient(otherId);
                            if (otherClient && otherClient->NodeId == clientState->NodeId &&
                                otherClient->Connected && otherClient->Peers.empty()) {
                                // Found an idle duplicate - close it and force reconnect
                                TryRemoveIdleClient(tabletState, otherId);
                                tabletState->ForceReconnect = true;
                                break;
                            }
                        }
                    }
                }
                return;
            }
        }

        // Unknown client (dropped before it connected)
        NTabletPipe::CloseClient(SelfId(), msg->ClientId);
    }

    void Handle(TEvTabletPipe::TEvClientShuttingDown::TPtr &ev) {
        const auto *msg = ev->Get();

        if (Counters) {
            Counters.EventGracefulShutdown->Inc();
        }

        TTabletState *tabletState = FindTablet(msg->TabletId);
        if (tabletState) {
            TClientState *clientState = tabletState->FindClient(msg->ClientId);
            if (clientState) {
                clientState->MaxForwardedSeqNo = msg->MaxForwardedSeqNo;
            }
            if (tabletState->IsActive(msg->ClientId)) {
                Y_ABORT_UNLESS(clientState, "Missing expected client state for active client");
                if (Counters && Y_LIKELY(clientState->Connected)) {
                    Counters.PipesInactive->Inc();
                    Counters.PipesActive->Dec();
                }
                tabletState->Deactivate(msg->ClientId);
                if (clientState->Peers.empty()) {
                    if (Counters) {
                        if (Y_LIKELY(clientState->Connected)) {
                            Counters.PipesInactive->Dec();
                        } else {
                            Counters.PipesConnecting->Dec();
                        }
                    }
                    NTabletPipe::CloseClient(SelfId(), msg->ClientId);
                    tabletState->ByClient.erase(msg->ClientId);

                    // Avoid keeping dead tablets forever
                    if (tabletState->ByClient.empty()) {
                        ForgetTablet(msg->TabletId);
                    }
                }
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev) {
        const TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();

        if (Counters) {
            Counters.EventDisconnect->Inc();
        }

        DropClient(msg->TabletId, msg->ClientId, true, false, false);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_PIPE_SERVER;
    }

    TPipePerNodeCache(const TIntrusivePtr<TPipePerNodeCacheConfig> &config)
        : TActor(&TThis::StateWork)
        , Config(config)
        , PipeConfig(Config->PipeConfig)
        , Counters(Config->Counters)
    {
        PipeConfig.ExpectShutdown = true;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvGetTabletNode, Handle);
            hFunc(TEvPipeCache::TEvForcePipeReconnect, Handle);
            hFunc(TEvPipeCache::TEvForward, Handle);
            hFunc(TEvPipeCache::TEvUnlink, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientShuttingDown, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }
};

NTabletPipe::TClientConfig TPipePerNodeCacheConfig::DefaultPipeConfig() {
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .RetryLimitCount = 3,
    };
    return config;
}

NTabletPipe::TClientConfig TPipePerNodeCacheConfig::DefaultPersistentPipeConfig() {
    NTabletPipe::TClientConfig config;
    config.CheckAliveness = true;
    config.RetryPolicy = {
        .RetryLimitCount = 30,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(500),
        .BackoffMultiplier = 2,
    };
    return config;
}

IActor* CreatePipePerNodeCache(const TIntrusivePtr<TPipePerNodeCacheConfig> &config) {
    return new TPipePerNodeCache(config);
}

TActorId MakePipePerNodeCacheID(EPipePerNodeCache kind) {
    char x[12] = "PipeCache";
    switch (kind) {
        case EPipePerNodeCache::Leader:
            x[9] = 'A';
            break;
        case EPipePerNodeCache::Follower:
            x[9] = 'F';
            break;
        case EPipePerNodeCache::Persistent:
            x[9] = 'P';
            break;
    }
    return TActorId(0, TStringBuf(x, 12));
}

TActorId MakePipePerNodeCacheID(bool allowFollower) {
    return MakePipePerNodeCacheID(allowFollower ? EPipePerNodeCache::Follower : EPipePerNodeCache::Leader);
}

}
