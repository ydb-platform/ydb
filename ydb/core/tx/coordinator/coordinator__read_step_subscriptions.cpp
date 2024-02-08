#include "coordinator_impl.h"

#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NFlatTxCoordinator {

    class TTxCoordinator::TReadStepSubscriptionManager : public TActor<TReadStepSubscriptionManager> {
    public:
        explicit TReadStepSubscriptionManager(ui64 coordinatorId)
            : TActor(&TThis::StateWork)
            , CoordinatorId(coordinatorId)
        { }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::COORDINATOR_READ_STEP_SUBSCRIPTION_MANAGER;
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                sFunc(TEvents::TEvPoison, PassAway);
                hFunc(TEvPrivate::TEvReadStepSubscribed, Handle);
                hFunc(TEvPrivate::TEvReadStepUnsubscribed, Handle);
                hFunc(TEvPrivate::TEvReadStepUpdated, Handle);
                hFunc(TEvPrivate::TEvPipeServerDisconnected, Handle);
                hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

    private:
        struct TSubscriberState {
            ui64 SeqNo = 0;
            ui64 Cookie;
            ui64 NextStep;
            TActorId SessionId;
            TActorId ServerId;
        };

        struct TSessionState {
            THashSet<TActorId> Subscribers;
        };

        struct TServerState {
            THashSet<TActorId> Subscribers;
        };

    private:
        void Handle(TEvPrivate::TEvReadStepSubscribed::TPtr& ev) {
            auto* msg = ev->Get();
            auto& subscriber = Subscribers[msg->Sender];
            if (msg->SeqNo < subscriber.SeqNo) {
                // ignore out-of-order subscriptions
                return;
            }
            subscriber.SeqNo = msg->SeqNo;
            subscriber.Cookie = msg->Cookie;
            subscriber.NextStep = msg->NextStep;
            if (subscriber.SessionId) {
                RemoveSessionSubscriber(subscriber.SessionId, msg->Sender);
                subscriber.SessionId = { };
            }
            if (subscriber.ServerId) {
                RemoveServerSubscriber(subscriber.ServerId, msg->Sender);
                subscriber.ServerId = { };
            }

            if (msg->Sender.NodeId() != SelfId().NodeId()) {
                Y_ABORT_UNLESS(msg->InterconnectSession);
                auto& session = SubscribeToSession(msg->InterconnectSession);
                session.Subscribers.insert(msg->Sender);
                subscriber.SessionId = msg->InterconnectSession;
            } else {
                Y_ABORT_UNLESS(!msg->InterconnectSession);
            }

            if (msg->PipeServer) {
                auto& server = Servers[msg->PipeServer];
                server.Subscribers.insert(msg->Sender);
                subscriber.ServerId = msg->PipeServer;
            }

            SendViaSession(
                    subscriber.SessionId,
                    msg->Sender,
                    new TEvTxProxy::TEvSubscribeReadStepResult(CoordinatorId, subscriber.SeqNo, msg->LastStep, msg->NextStep),
                    0, subscriber.Cookie);
        }

        void Handle(TEvPrivate::TEvReadStepUnsubscribed::TPtr& ev) {
            auto* msg = ev->Get();
            auto it = Subscribers.find(msg->Sender);
            if (it == Subscribers.end()) {
                return;
            }
            auto& subscriber = it->second;
            if (subscriber.SeqNo > msg->SeqNo) {
                return;
            }
            if (subscriber.SessionId) {
                RemoveSessionSubscriber(subscriber.SessionId, msg->Sender);
            }
            Subscribers.erase(it);
        }

        void Handle(TEvPrivate::TEvReadStepUpdated::TPtr& ev) {
            auto* msg = ev->Get();
            for (auto& pr : Subscribers) {
                const auto& subscriberId = pr.first;
                auto& subscriber = pr.second;
                if (subscriber.NextStep < msg->NextStep) {
                    subscriber.NextStep = msg->NextStep;
                    SendViaSession(
                        subscriber.SessionId,
                        subscriberId,
                        new TEvTxProxy::TEvSubscribeReadStepUpdate(CoordinatorId, subscriber.SeqNo, msg->NextStep),
                        0, subscriber.Cookie);
                }
            }
        }

        TSessionState& SubscribeToSession(const TActorId& sessionId) {
            Y_ABORT_UNLESS(sessionId);
            auto it = Sessions.find(sessionId);
            if (it != Sessions.end()) {
                return it->second;
            }
            auto& session = Sessions[sessionId];
            Send(sessionId, new TEvents::TEvSubscribe, IEventHandle::FlagTrackDelivery);
            return session;
        }

        void SendViaSession(const TActorId& sessionId, const TActorId& target, IEventBase* event, ui32 flags, ui64 cookie) {
            THolder<IEventHandle> ev = MakeHolder<IEventHandle>(target, SelfId(), event, flags, cookie);

            if (sessionId) {
                ev->Rewrite(TEvInterconnect::EvForward, sessionId);
            }

            TActivationContext::Send(ev.Release());
        }

        void Handle(TEvPrivate::TEvPipeServerDisconnected::TPtr& ev) {
            RemoveServer(ev->Get()->ServerId);
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
            Y_UNUSED(ev);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
            RemoveSession(ev->Sender);
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            RemoveSession(ev->Sender);
        }

        void RemoveSessionSubscriber(const TActorId& sessionId, const TActorId& subscriberId) {
            auto it = Sessions.find(sessionId);
            if (it != Sessions.end()) {
                it->second.Subscribers.erase(subscriberId);
            }
        }

        void RemoveSession(const TActorId& sessionId) {
            auto it = Sessions.find(sessionId);
            if (it == Sessions.end()) {
                return;
            }
            for (const auto& subscriberId : it->second.Subscribers) {
                auto itSubscriber = Subscribers.find(subscriberId);
                if (itSubscriber != Subscribers.end()) {
                    if (itSubscriber->second.ServerId) {
                        RemoveServerSubscriber(itSubscriber->second.ServerId, subscriberId);
                    }
                    Subscribers.erase(itSubscriber);
                }
            }
            Sessions.erase(it);
        }

        void RemoveServerSubscriber(const TActorId& serverId, const TActorId& subscriberId) {
            auto it = Servers.find(serverId);
            if (it != Servers.end()) {
                it->second.Subscribers.erase(subscriberId);
            }
        }

        void RemoveServer(const TActorId& serverId) {
            auto it = Servers.find(serverId);
            if (it == Servers.end()) {
                return;
            }
            for (const auto& subscriberId : it->second.Subscribers) {
                auto itSubscriber = Subscribers.find(subscriberId);
                if (itSubscriber != Subscribers.end()) {
                    if (itSubscriber->second.SessionId) {
                        RemoveSessionSubscriber(itSubscriber->second.SessionId, subscriberId);
                    }
                    Subscribers.erase(itSubscriber);
                }
            }
            Servers.erase(it);
        }

    private:
        const ui64 CoordinatorId;
        THashMap<TActorId, TSubscriberState> Subscribers;
        THashMap<TActorId, TSessionState> Sessions;
        THashMap<TActorId, TServerState> Servers;
    };

    struct TTxCoordinator::TTxSubscribeReadStep : public TTransactionBase<TTxCoordinator> {
        const TActorId PipeServer;
        TEvTxProxy::TEvSubscribeReadStep::TPtr Ev;
        ui64 LastAcquiredStep;

        TTxSubscribeReadStep(TTxCoordinator* self, const TActorId& pipeServer, TEvTxProxy::TEvSubscribeReadStep::TPtr&& ev)
            : TTransactionBase(self)
            , PipeServer(pipeServer)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_SUBSCRIBE_READ_STEP; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            LastAcquiredStep = Self->VolatileState.LastAcquired;

            // We currently have to force a read/write transaction.
            NIceDb::TNiceDb db(txc.DB);
            Schema::SaveState(db, Schema::State::AcquireReadStepLast, LastAcquiredStep);

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (PipeServer && !Self->PipeServers.contains(PipeServer)) {
                // Pipe server disconnected before we finished the transaction
                // We just ignore this subscription as implicitly discarded
                return;
            }
            const ui64 lastStep = LastAcquiredStep;
            const ui64 nextStep = Max(Self->VolatileState.LastSentStep, Self->VolatileState.LastAcquired);
            ctx.Send(Self->EnsureReadStepSubscriptionManager(ctx), new TEvPrivate::TEvReadStepSubscribed(PipeServer, Ev, lastStep, nextStep));
        }
    };

    struct TTxCoordinator::TTxUnsubscribeReadStep : public TTransactionBase<TTxCoordinator> {
        TEvTxProxy::TEvUnsubscribeReadStep::TPtr Ev;

        TTxUnsubscribeReadStep(TTxCoordinator* self, TEvTxProxy::TEvUnsubscribeReadStep::TPtr&& ev)
            : TTransactionBase(self)
            , Ev(std::move(ev))
        { }

        TTxType GetTxType() const override { return TXTYPE_UNSUBSCRIBE_READ_STEP; }

        bool Execute(TTransactionContext&, const TActorContext&) override {
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(Self->ReadStepSubscriptionManager, new TEvPrivate::TEvReadStepUnsubscribed(Ev));
        }
    };

    TActorId TTxCoordinator::EnsureReadStepSubscriptionManager(const TActorContext& ctx) {
        if (!ReadStepSubscriptionManager) {
            // We want this actor on a separate mailbox
            // This way fanout subscription sends will not block coordinator
            ReadStepSubscriptionManager = ctx.Register(new TReadStepSubscriptionManager(TabletID()));
        }

        return ReadStepSubscriptionManager;
    }

    void TTxCoordinator::Handle(TEvTxProxy::TEvSubscribeReadStep::TPtr& ev, const TActorContext& ctx) {
        const TActorId pipeServer = PipeServers.contains(ev->Recipient) ? ev->Recipient : TActorId{ };
        Execute(new TTxSubscribeReadStep(this, pipeServer, std::move(ev)), ctx);
    }

    void TTxCoordinator::Handle(TEvTxProxy::TEvUnsubscribeReadStep::TPtr& ev, const TActorContext& ctx) {
        Execute(new TTxUnsubscribeReadStep(this, std::move(ev)), ctx);
    }

} // namespace NKikimr::NFlatTxCoordinator
