#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_pipe.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr {

class TPipePeNodeCache : public TActor<TPipePeNodeCache> {
    TIntrusiveConstPtr<TPipePeNodeCacheConfig> Config;
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

        TActorId LastClient;
        TInstant LastCreated;

        TClientState* FindClient(const TActorId& clientId) {
            auto it = ByClient.find(clientId);
            if (it == ByClient.end()) {
                return nullptr;
            }
            return &it->second;
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

    void UnlinkOne(TActorId peer, ui64 tablet) {
        auto *tabletState = FindTablet(tablet);
        if (Y_UNLIKELY(!tabletState))
            return;

        auto byPeerIt = tabletState->ByPeer.find(peer);
        if (byPeerIt == tabletState->ByPeer.end())
            return;

        auto *clientState = byPeerIt->second;
        Y_VERIFY(clientState, "Unexpected nullptr in tablet's ByPeer links");
        clientState->Peers.erase(peer);
        tabletState->ByPeer.erase(byPeerIt);

        // Remove old clients that no longer have any peers
        if (clientState->Peers.empty() && clientState->Client != tabletState->LastClient) {
            auto clientId = clientState->Client;
            if (Counters) {
                if (clientState->Connected) {
                    Counters.PipesInactive->Dec();
                } else {
                    Counters.PipesConnecting->Dec();
                }
            }
            NTabletPipe::CloseClient(SelfId(), clientId);
            tabletState->ByClient.erase(clientId);
        }

        // Avoid keeping dead tablets forever
        if (tabletState->ByClient.empty() && !tabletState->LastClient) {
            ForgetTablet(tablet);
        }
    }

    void DropClient(ui64 tablet, TActorId client, bool notDelivered) {
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
            Send(peer, new TEvPipeCache::TEvDeliveryProblem(tablet, msgNotDelivered), 0, cookie);

            tabletState->ByPeer.erase(peer);

            auto byPeerIt = ByPeer.find(peer);
            Y_VERIFY(byPeerIt != ByPeer.end());
            byPeerIt->second.ConnectedToTablet.erase(tablet);
            if (byPeerIt->second.ConnectedToTablet.empty()) {
                ForgetPeer(byPeerIt);
            }
        }
        clientState->Peers.clear();

        for (auto &req : clientState->NodeRequests) {
            Send(req.Sender, new TEvPipeCache::TEvGetTabletNodeResult(tablet, 0), 0, req.Cookie);
        }

        if (Counters) {
            if (clientState->Connected) {
                if (tabletState->LastClient == client) {
                    Counters.PipesActive->Dec();
                } else {
                    Counters.PipesInactive->Dec();
                }
            } else {
                Counters.PipesConnecting->Dec();
            }
        }

        tabletState->ByClient.erase(client);
        if (tabletState->LastClient == client) {
            tabletState->LastClient = TActorId();
        }

        // Avoid keeping dead tablets forever
        if (tabletState->ByClient.empty() && !tabletState->LastClient) {
            ForgetTablet(tablet);
        }
    }

    TClientState* EnsureClient(TTabletState *tabletState, ui64 tabletId) {
        TClientState *clientState = nullptr;
        if (!tabletState->LastClient || tabletState->ForceReconnect || Config->PipeRefreshTime && tabletState->ByClient.size() < 2 && Config->PipeRefreshTime < (TActivationContext::Now() - tabletState->LastCreated)) {
            tabletState->ForceReconnect = false;
            // Remove current client if it is idle
            if (tabletState->LastClient) {
                clientState = tabletState->FindClient(tabletState->LastClient);
                Y_VERIFY(clientState);
                if (clientState->Peers.empty()) {
                    if (Counters) {
                        if (clientState->Connected) {
                            Counters.PipesActive->Dec();
                        } else {
                            Counters.PipesConnecting->Dec();
                        }
                    }
                    NTabletPipe::CloseClient(SelfId(), tabletState->LastClient);
                    tabletState->ByClient.erase(tabletState->LastClient);
                    tabletState->LastClient = TActorId();
                }
            }
            if (Counters) {
                Counters.PipesConnecting->Inc();
                Counters.EventCreate->Inc();
            }
            tabletState->LastClient = Register(NTabletPipe::CreateClient(SelfId(), tabletId, PipeConfig));
            tabletState->LastCreated = TActivationContext::Now();
            clientState = &tabletState->ByClient[tabletState->LastClient];
            clientState->Client = tabletState->LastClient;
        } else {
            clientState = tabletState->FindClient(tabletState->LastClient);
            Y_VERIFY(clientState, "Missing expected client state for active client");
        }
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

    void ForgetPeer(TByPeer::iterator peerIt) {
        ByPeer.erase(peerIt);
        if (Counters) {
            Counters.Subscribers->Dec();
        }
    }

    void Handle(TEvPipeCache::TEvForward::TPtr &ev) {
        TEvPipeCache::TEvForward *msg = ev->Get();
        const ui64 tablet = msg->TabletId;
        const bool subscribe = msg->Subscribe;
        const ui64 subscribeCookie = msg->SubscribeCookie;
        const TActorId peer = ev->Sender;
        const ui64 cookie = ev->Cookie;
        NWilson::TTraceId traceId = std::move(ev->TraceId);

        auto *tabletState = EnsureTablet(tablet);

        TClientState *clientState = nullptr;

        // Prefer using the same pipe after subscription
        if (!subscribe) {
            auto it = tabletState->ByPeer.find(peer);
            if (it != tabletState->ByPeer.end()) {
                clientState = it->second;
            }
        }

        // Ensure there's a valid pipe for sending messages
        if (!clientState) {
            clientState = EnsureClient(tabletState, tablet);
        }

        if (subscribe) {
            TClientState *&link = tabletState->ByPeer[peer];
            if (link != clientState) {
                if (Y_UNLIKELY(link)) {
                    // Resubscribing to a new pipe
                    link->Peers.erase(peer);
                    if (link->Peers.empty()) {
                        auto oldClient = link->Client;
                        Y_VERIFY(oldClient != tabletState->LastClient);
                        if (Counters) {
                            if (link->Connected) {
                                Counters.PipesInactive->Dec();
                            } else {
                                Counters.PipesConnecting->Dec();
                            }
                        }
                        NTabletPipe::CloseClient(SelfId(), oldClient);
                        tabletState->ByClient.erase(oldClient);
                    }
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
            return DropClient(msg->TabletId, msg->ClientId, true);
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
                    if (tabletState->LastClient == msg->ClientId) {
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
            if (tabletState->LastClient == msg->ClientId) {
                Y_VERIFY(clientState, "Missing expected client state for active client");
                if (Counters && Y_LIKELY(clientState->Connected)) {
                    Counters.PipesInactive->Inc();
                    Counters.PipesActive->Dec();
                }
                tabletState->LastClient = TActorId();
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

        DropClient(msg->TabletId, msg->ClientId, false);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_PIPE_SERVER;
    }

    TPipePeNodeCache(const TIntrusivePtr<TPipePeNodeCacheConfig> &config)
        : TActor(&TThis::StateWork)
        , Config(config)
        , PipeConfig(Config->PipeConfig)
        , Counters(Config->Counters)
    {
        PipeConfig.ExpectShutdown = true;
    }

    STFUNC(StateWork) {
        Y_UNUSED(ctx);
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

IActor* CreatePipePeNodeCache(const TIntrusivePtr<TPipePeNodeCacheConfig> &config) {
    return new TPipePeNodeCache(config);
}

TActorId MakePipePeNodeCacheID(bool allowFollower) {
    char x[12] = "PipeCache";
    x[9] = allowFollower ? 'F' : 'A';
    return TActorId(0, TStringBuf(x, 12));
}

}
