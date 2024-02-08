#include "ic_mock.h"
#include <ydb/library/actors/core/interconnect.h>
#include <util/system/yield.h>
#include <thread>
#include <deque>

namespace NActors {

    class TInterconnectMock::TImpl {
        enum {
            EvInject = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvCheckSession,
            EvRam,
        };

        struct TEvInject : TEventLocal<TEvInject, EvInject> {
            std::deque<std::unique_ptr<IEventHandle>> Messages;
            const TScopeId OriginScopeId;
            const ui64 SenderSessionId;

            TEvInject(std::deque<std::unique_ptr<IEventHandle>>&& messages, const TScopeId& originScopeId, ui64 senderSessionId)
                : Messages(std::move(messages))
                , OriginScopeId(originScopeId)
                , SenderSessionId(senderSessionId)
            {}
        };

        class TProxyMockActor;

        class TConnectionState {
            struct TPeerInfo {
                TRWMutex Mutex;
                TActorSystem *ActorSystem = nullptr;
                TActorId ProxyId;
            };

            const ui64 Key;
            TPeerInfo PeerInfo[2];
            std::atomic_uint64_t SessionId = 0;

        public:
            TConnectionState(ui64 key)
                : Key(key)
            {}

            void Attach(ui32 nodeId, TActorSystem *as, const TActorId& actorId) {
                TPeerInfo *peer = GetPeer(nodeId);
                auto guard = TWriteGuard(peer->Mutex);
                Y_ABORT_UNLESS(!peer->ActorSystem);
                peer->ActorSystem = as;
                peer->ProxyId = actorId;
                as->DeferPreStop([peer] {
                    auto guard = TWriteGuard(peer->Mutex);
                    peer->ActorSystem = nullptr;
                });
            }

            void Inject(ui32 peerNodeId, std::deque<std::unique_ptr<IEventHandle>>&& messages,
                    const TScopeId& originScopeId, ui64 senderSessionId) {
                TPeerInfo *peer = GetPeer(peerNodeId);
                auto guard = TReadGuard(peer->Mutex);
                if (peer->ActorSystem) {
                    peer->ActorSystem->Send(new IEventHandle(peer->ProxyId, TActorId(), new TEvInject(std::move(messages),
                        originScopeId, senderSessionId)));
                } else {
                    for (auto&& ev : messages) {
                        TActivationContext::Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::Disconnected));
                    }
                }
            }

            ui64 GetValidSessionId() const {
                return SessionId;
            }

            void InvalidateSessionId(ui32 peerNodeId) {
                ++SessionId;
                TPeerInfo *peer = GetPeer(peerNodeId);
                auto guard = TReadGuard(peer->Mutex);
                if (peer->ActorSystem) {
                    peer->ActorSystem->Send(new IEventHandle(EvCheckSession, 0, peer->ProxyId, {}, nullptr, 0));
                }
            }

        private:
            TPeerInfo *GetPeer(ui32 nodeId) {
                if (nodeId == ui32(Key)) {
                    return PeerInfo;
                } else if (nodeId == ui32(Key >> 32)) {
                    return PeerInfo + 1;
                } else {
                    Y_ABORT();
                }
            }
        };

        class TProxyMockActor : public TActor<TProxyMockActor> {
            class TSessionMockActor : public TActor<TSessionMockActor> {
                std::map<TActorId, ui64> Subscribers;
                TProxyMockActor* const Proxy;
                std::deque<std::unique_ptr<IEventHandle>> Queue;

            public:
                const ui64 SessionId;

            public:
                TSessionMockActor(TProxyMockActor *proxy, ui64 sessionId)
                    : TActor(&TThis::StateFunc)
                    , Proxy(proxy)
                    , SessionId(sessionId)
                {}

                static constexpr char ActorName[] = "SESSION_MOCK_ACTOR";

                void Terminate() {
                    for (auto&& ev : std::exchange(Queue, {})) {
                        TActivationContext::Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::Disconnected));
                    }
                    for (const auto& kv : Subscribers) {
                        Send(kv.first, new TEvInterconnect::TEvNodeDisconnected(Proxy->PeerNodeId), 0, kv.second);
                    }
                    Y_ABORT_UNLESS(Proxy->Session == this);
                    Proxy->Session = nullptr;
                    PassAway();
                }

                void HandleForward(TAutoPtr<IEventHandle> ev) {
                    if (CheckNodeStatus(ev)) {
                        if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
                            Subscribe(ev->Sender, ev->Cookie);
                        }
                        if (Queue.empty()) {
                            TActivationContext::Send(new IEventHandle(EvRam, 0, SelfId(), {}, {}, 0));
                        }
                        Queue.emplace_back(ev.Release());
                    }
                }

                void HandleRam() {
                    if (SessionId != Proxy->State.GetValidSessionId()) {
                        Terminate();
                    } else {
                        Proxy->PeerInject(std::exchange(Queue, {}));
                    }
                }

                void Handle(TEvInterconnect::TEvConnectNode::TPtr ev) {
                    if (CheckNodeStatus(ev)) {
                        Subscribe(ev->Sender, ev->Cookie);
                    }
                }

                void Handle(TEvents::TEvSubscribe::TPtr ev) {
                    if (CheckNodeStatus(ev)) {
                        Subscribe(ev->Sender, ev->Cookie);
                    }
                }

                void Handle(TEvents::TEvUnsubscribe::TPtr ev) {
                    if (CheckNodeStatus(ev)) {
                        Subscribers.erase(ev->Sender);
                    }
                }

                void HandlePoison() {
                    Proxy->Disconnect();
                }

                STRICT_STFUNC(StateFunc,
                    fFunc(TEvInterconnect::EvForward, HandleForward)
                    hFunc(TEvInterconnect::TEvConnectNode, Handle)
                    hFunc(TEvents::TEvSubscribe, Handle)
                    hFunc(TEvents::TEvUnsubscribe, Handle)
                    hFunc(TEvInterconnect::TEvNodeInfo, HandleNodeInfo)
                    cFunc(TEvents::TSystem::Poison, HandlePoison)
                    cFunc(EvRam, HandleRam)
                )

            private:
                enum EPeerNodeStatus {
                    UNKNOWN,
                    EXISTS,
                    MISSING
                };

                bool IsWaitingForNodeInfo = false;
                std::deque<std::unique_ptr<IEventHandle>> WaitingConnections;
                EPeerNodeStatus PeerNodeStatus = EPeerNodeStatus::UNKNOWN;

                void Subscribe(const TActorId& actorId, ui64 cookie) {
                    Subscribers[actorId] = cookie;
                    Send(actorId, new TEvInterconnect::TEvNodeConnected(Proxy->PeerNodeId), 0, cookie);
                }

                template <typename TEvent>
                bool CheckNodeStatus(TAutoPtr<TEventHandle<TEvent>>& ev) {
                    if (PeerNodeStatus != EPeerNodeStatus::EXISTS) {
                        std::unique_ptr<IEventHandle> tmp(ev.Release());
                        CheckNonexistentNode(tmp);
                        return false;
                    }
                    return true;
                }

                bool CheckNodeStatus(TAutoPtr<IEventHandle>& ev) {
                    if (PeerNodeStatus != EPeerNodeStatus::EXISTS) {
                        std::unique_ptr<IEventHandle> tmp(ev.Release());
                        CheckNonexistentNode(tmp);
                        return false;
                    }
                    return true;
                }

                void CheckNonexistentNode(std::unique_ptr<IEventHandle>& ev) {
                    if (PeerNodeStatus == EPeerNodeStatus::UNKNOWN) {
                        WaitingConnections.emplace_back(ev.release());
                        if (!IsWaitingForNodeInfo) {
                            Send(Proxy->Common->NameserviceId, new TEvInterconnect::TEvGetNode(Proxy->PeerNodeId));
                            IsWaitingForNodeInfo = true;
                        }
                    } else if (PeerNodeStatus == EPeerNodeStatus::MISSING) {
                        switch (ev->GetTypeRewrite()) {
                            case TEvInterconnect::EvForward:
                                if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
                                    Send(ev->Sender, new TEvInterconnect::TEvNodeDisconnected(Proxy->PeerNodeId), 0, ev->Cookie);
                                }
                                TActivationContext::Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::Disconnected));
                                break;

                            case TEvents::TEvSubscribe::EventType:
                            case TEvInterconnect::TEvConnectNode::EventType:
                                Send(ev->Sender, new TEvInterconnect::TEvNodeDisconnected(Proxy->PeerNodeId), 0, ev->Cookie);
                                break;

                            case TEvents::TEvUnsubscribe::EventType:
                                break;

                            default:
                                Y_ABORT();
                        }
                    }
                }

                void HandleNodeInfo(TEvInterconnect::TEvNodeInfo::TPtr ev) {
                    Y_ABORT_UNLESS(IsWaitingForNodeInfo);
                    if (!ev->Get()->Node) {
                        PeerNodeStatus = EPeerNodeStatus::MISSING;
                    } else {
                        PeerNodeStatus = EPeerNodeStatus::EXISTS;
                    }
                    IsWaitingForNodeInfo = false;
                    while (!WaitingConnections.empty()) {
                        TAutoPtr<IEventHandle> tmp(WaitingConnections.front().release());
                        WaitingConnections.pop_front();
                        Receive(tmp);
                    }
                }
            };

            friend class TSessionMockActor;

            const ui32 NodeId;
            const ui32 PeerNodeId;
            TConnectionState& State;
            const TInterconnectProxyCommon::TPtr Common;
            TSessionMockActor *Session = nullptr;

        public:
            TProxyMockActor(ui32 nodeId, ui32 peerNodeId, TConnectionState& state, TInterconnectProxyCommon::TPtr common)
                : TActor(&TThis::StateFunc)
                , NodeId(nodeId)
                , PeerNodeId(peerNodeId)
                , State(state)
                , Common(std::move(common))
            {}

            static constexpr char ActorName[] = "PROXY_MOCK_ACTOR";

            void Registered(TActorSystem *as, const TActorId& parent) override {
                TActor::Registered(as, parent);
                State.Attach(NodeId, as, SelfId());
            }

            void Handle(TEvInject::TPtr ev) {
                auto *msg = ev->Get();
                if (Session && Session->SessionId != msg->SenderSessionId) {
                    return; // drop messages from other sessions
                }
                if (auto *session = GetSession()) {
                    for (auto&& ev : ev->Get()->Messages) {
                        auto fw = std::make_unique<IEventHandle>(
                            session->SelfId(),
                            ev->Type,
                            ev->Flags & ~IEventHandle::FlagForwardOnNondelivery,
                            ev->Recipient,
                            ev->Sender,
                            ev->ReleaseChainBuffer(),
                            ev->Cookie,
                            msg->OriginScopeId,
                            std::move(ev->TraceId)
                        );
                        if (!Common->EventFilter || Common->EventFilter->CheckIncomingEvent(*fw, Common->LocalScopeId)) {
                            TActivationContext::Send(fw.release());
                        }
                    }
                }
            }

            void PassAway() override {
                Disconnect();
                TActor::PassAway();
            }

            TSessionMockActor *GetSession() {
                CheckSession();
                if (!Session) {
                    Session = new TSessionMockActor(this, State.GetValidSessionId());
                    RegisterWithSameMailbox(Session);
                }
                return Session;
            }

            void HandleSessionEvent(TAutoPtr<IEventHandle> ev) {
                auto *session = GetSession();
                InvokeOtherActor(*session, &TSessionMockActor::Receive, ev);
            }

            void Disconnect() {
                State.InvalidateSessionId(PeerNodeId);
                if (Session) {
                    Session->Terminate();
                }
            }

            void CheckSession() {
                if (Session && Session->SessionId != State.GetValidSessionId()) {
                    Session->Terminate();
                }
            }

            void PeerInject(std::deque<std::unique_ptr<IEventHandle>>&& messages) {
                Y_ABORT_UNLESS(Session);
                return State.Inject(PeerNodeId, std::move(messages), Common->LocalScopeId, Session->SessionId);
            }

            STRICT_STFUNC(StateFunc,
                cFunc(TEvents::TSystem::Poison, PassAway)
                fFunc(TEvInterconnect::EvForward, HandleSessionEvent)
                fFunc(TEvInterconnect::EvConnectNode, HandleSessionEvent)
                fFunc(TEvents::TSystem::Subscribe, HandleSessionEvent)
                fFunc(TEvents::TSystem::Unsubscribe, HandleSessionEvent)
                cFunc(TEvInterconnect::EvDisconnect, Disconnect)
                IgnoreFunc(TEvInterconnect::TEvClosePeerSocket)
                IgnoreFunc(TEvInterconnect::TEvCloseInputSession)
                cFunc(TEvInterconnect::EvPoisonSession, Disconnect)
                hFunc(TEvInject, Handle)
                cFunc(EvCheckSession, CheckSession)
            )
        };

        std::unordered_map<ui64, TConnectionState> States;

    public:
        IActor *CreateProxyMock(ui32 nodeId, ui32 peerNodeId, TInterconnectProxyCommon::TPtr common) {
            Y_ABORT_UNLESS(nodeId != peerNodeId);
            Y_ABORT_UNLESS(nodeId);
            Y_ABORT_UNLESS(peerNodeId);
            const ui64 key = std::min(nodeId, peerNodeId) | ui64(std::max(nodeId, peerNodeId)) << 32;
            auto it = States.try_emplace(key, key).first;
            return new TProxyMockActor(nodeId, peerNodeId, it->second, std::move(common));
        }
    };

    TInterconnectMock::TInterconnectMock()
        : Impl(std::make_unique<TImpl>())
    {}

    TInterconnectMock::~TInterconnectMock()
    {}

    IActor *TInterconnectMock::CreateProxyMock(ui32 nodeId, ui32 peerNodeId, TInterconnectProxyCommon::TPtr common) {
        return Impl->CreateProxyMock(nodeId, peerNodeId, std::move(common));
    }

} // NActors
