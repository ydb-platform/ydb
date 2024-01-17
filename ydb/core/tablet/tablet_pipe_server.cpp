#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

namespace NTabletPipe {

    class TServer : public TActor<TServer> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TABLET_PIPE_SERVER;
        }

        TServer(ui64 tabletId, const TActorId& clientId, const TActorId& interconnectSession, ui32 features, ui64 connectCookie)
            : TActor(&TThis::StateInactive)
            , TabletId(tabletId)
            , ClientId(clientId)
            , InterconnectSession(interconnectSession)
            , ConnectCookie(connectCookie)
            , Features(features)
            , NeedUnsubscribe(false)
            , Leader(true)
            , Connected(false)
        {
            Y_ABORT_UNLESS(tabletId != 0);
        }

    private:
        STFUNC(StateActive) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTabletPipe::TEvPush, Handle);
                HFunc(TEvTabletPipe::TEvMessage, Handle);
                FFunc(TEvTabletPipe::EvSend, HandleSend); // only for direct (one-node) sends, for generic send see EvPush
                HFunc(TEvTabletPipe::TEvPeerClosed, Handle);
                HFunc(TEvTabletPipe::TEvShutdown, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
                HFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                HFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        STFUNC(StateInactive) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTabletPipe::TEvActivate, Handle);
                HFunc(TEvTabletPipe::TEvShutdown, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
            }
        }

        STFUNC(StateWaitingSubscribe) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvInterconnect::TEvNodeConnected, Handle);
                HFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                HFunc(TEvents::TEvUndelivered, Handle);
                HFunc(TEvTabletPipe::TEvShutdown, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
            }
        }

        void Handle(TEvTabletPipe::TEvPush::TPtr& ev, const TActorContext& ctx) {
            const auto& msg = *ev->Get();
            const auto& record = msg.Record;
            Y_ABORT_UNLESS(record.GetTabletId() == TabletId);
            const TActorId sender = ActorIdFromProto(record.GetSender());
            LOG_DEBUG_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " Push Sender# " << sender << " EventType# " << record.GetType()
                << (HadShutdown ? " ignored after shutdown" : ""));
            if (HadShutdown) {
                return;
            }

            Y_ABORT_UNLESS(!msg.GetPayloadCount() || (msg.GetPayloadCount() == 1 && !record.HasBuffer()));
            TEventSerializationInfo serializationInfo{
                .IsExtendedFormat = record.GetExtendedFormat(),
            };
            auto buffer = msg.GetPayloadCount()
                ? MakeIntrusive<TEventSerializedData>(TRope(msg.GetPayload(0)), std::move(serializationInfo))
                : MakeIntrusive<TEventSerializedData>(record.GetBuffer(), std::move(serializationInfo));

            auto result = std::make_unique<IEventHandle>(
                    ev->InterconnectSession,
                    record.GetType(),
                    0,
                    ctx.SelfID,
                    sender,
                    std::move(buffer),
                    record.GetCookie(),
                    ev->OriginScopeId,
                    std::move(ev->TraceId));
            result->Rewrite(record.GetType(), RecipientId);
            ctx.ExecutorThread.Send(result.release());

            if (ui64 seqNo = record.GetSeqNo()) {
                LOG_TRACE_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                    << " Applying new remote seqNo " << seqNo);
                MaxForwardedSeqNo = Max(MaxForwardedSeqNo, seqNo);
            }
        }

        void Handle(TEvTabletPipe::TEvMessage::TPtr& ev, const TActorContext& ctx) {
            auto* msg = ev->Get();
            const auto& originalSender = msg->Sender;
            LOG_DEBUG_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " Message Sender# " << originalSender << " EventType# " << ev->Type
                << (HadShutdown ? " ignored after shutdown" : ""));
            if (HadShutdown) {
                return;
            }

            THolder<IEventHandle> result;
            if (msg->HasEvent()) {
                result = MakeHolder<IEventHandle>(ctx.SelfID, originalSender,
                        msg->ReleaseEvent().Release(), 0, ev->Cookie, nullptr,
                        std::move(ev->TraceId));
            } else {
                result = MakeHolder<IEventHandle>(
                        msg->Type, 0, ctx.SelfID, originalSender,
                        msg->ReleaseBuffer(), ev->Cookie, nullptr,
                        std::move(ev->TraceId));
            }

            result->Rewrite(msg->Type, RecipientId);
            ctx.ExecutorThread.Send(result.Release());

            if (ui64 seqNo = msg->GetSeqNo()) {
                LOG_TRACE_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                    << " Applying new local seqNo " << seqNo);
                MaxForwardedSeqNo = Max(MaxForwardedSeqNo, seqNo);
            }
        }

        void HandleSend(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            const auto& originalSender = ev->Recipient;
            LOG_DEBUG_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " HandleSend Sender# " << originalSender << " EventType# " << ev->Type
                << (HadShutdown ? " ignored after shutdown" : ""));
            if (HadShutdown) {
                return;
            }

            IEventHandle* result;
            if (ev->HasEvent()) {
                result = new IEventHandle(ctx.SelfID, originalSender, ev->ReleaseBase().Release(), 0, ev->Cookie, nullptr,
                        std::move(ev->TraceId));
            } else {
                result = new IEventHandle(ev->Type, 0, ctx.SelfID, originalSender, ev->ReleaseChainBuffer(), ev->Cookie,
                        nullptr, std::move(ev->TraceId));
            }

            result->Rewrite(ev->Type, RecipientId);
            ctx.ExecutorThread.Send(result);
        }

        void Handle(TEvTabletPipe::TEvShutdown::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            if (!Connected) {
                // Just stop if not connected yet
                SendToClient(ctx, new TEvTabletPipe::TEvPeerClosed(TabletId, ClientId, ctx.SelfID));
                Reset(ctx);
                return;
            }
            if (Features & NKikimrTabletPipe::FEATURE_GRACEFUL_SHUTDOWN) {
                SendToClient(ctx, new TEvTabletPipe::TEvPeerShutdown(TabletId, ClientId, ctx.SelfID, MaxForwardedSeqNo));
            }
            HadShutdown = true;
        }

        void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            SendToClient(ctx, new TEvTabletPipe::TEvPeerClosed(TabletId, ClientId, ctx.SelfID));
            Reset(ctx);
        }

        void Handle(TEvTabletPipe::TEvPeerClosed::TPtr& ev, const TActorContext& ctx) {
            Y_ABORT_UNLESS(ev->Get()->Record.GetTabletId() == TabletId);
            LOG_DEBUG_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " Got PeerClosed from# " << ev->Sender);
            Reset(ctx);
        }

        void Reset(const TActorContext& ctx) {
            if (Connected) {
                ctx.Send(RecipientId, new TEvTabletPipe::TEvServerDisconnected(TabletId, ClientId, ctx.SelfID));
            }
            ctx.Send(OwnerId, new TEvTabletPipe::TEvServerDestroyed(TabletId, ClientId, ctx.SelfID));
            UnsubscribeAndDie(ctx);
        }

        void Handle(TEvTabletPipe::TEvActivate::TPtr& ev, const TActorContext& ctx) {
            OwnerId = ev->Get()->OwnerId;
            RecipientId = ev->Get()->RecipientId;
            Leader = ev->Get()->Leader;
            Generation = ev->Get()->Generation;
            Y_ABORT_UNLESS(OwnerId);
            Y_ABORT_UNLESS(RecipientId);
            if (InterconnectSession) {
                NeedUnsubscribe = true;
                ctx.Send(InterconnectSession, new TEvents::TEvSubscribe(), IEventHandle::FlagTrackDelivery);
                Become(&TThis::StateWaitingSubscribe);
            } else {
                OnConnected(ctx);
            }
        }

        void OnConnected(const TActorContext& ctx) {
            Become(&TThis::StateActive);
            SendToClient(ctx, new TEvTabletPipe::TEvConnectResult(NKikimrProto::OK, TabletId, ClientId, ctx.SelfID, Leader, Generation), IEventHandle::FlagTrackDelivery, ConnectCookie);
            ctx.Send(RecipientId, new TEvTabletPipe::TEvServerConnected(TabletId, ClientId, ctx.SelfID, InterconnectSession));
            Connected = true;
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            OnConnected(ctx);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext& ctx) {
            LOG_ERROR_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " NodeDisconnected NodeId# " << ev->Get()->NodeId);
            NeedUnsubscribe = false;
            Reset(ctx);
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
            // Either interconnect session or remote client no longer exist
            LOG_INFO_S(ctx, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " Undelivered Target# " << ev->Sender << " Type# " << ev->Get()->SourceType << " Reason# " << ev->Get()->Reason);
            Reset(ctx);
        }

        void UnsubscribeAndDie(const TActorContext& ctx) {
            if (NeedUnsubscribe)
                ctx.Send(InterconnectSession, new TEvents::TEvUnsubscribe());

            Die(ctx);
        }

        void SendToClient(const TActorContext& ctx, TAutoPtr<IEventBase> msg, ui32 flags = 0, ui64 cookie = 0) {
            TAutoPtr<IEventHandle> ev = new IEventHandle(ClientId, ctx.SelfID, msg.Release(), flags, cookie);

            if (InterconnectSession) {
                ev->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
            }

            ctx.ExecutorThread.Send(ev);
        }

    private:
        const ui64 TabletId;
        const TActorId ClientId;
        TActorId OwnerId;
        TActorId RecipientId;
        TActorId InterconnectSession;
        ui64 MaxForwardedSeqNo = 0;
        const ui64 ConnectCookie;
        const ui32 Features;
        bool NeedUnsubscribe;
        bool Leader;
        bool Connected;
        bool HadShutdown = false;
        ui32 Generation = 0;
    };

    class TConnectAcceptor: public IConnectAcceptor {
    public:
        explicit TConnectAcceptor(ui64 tabletId)
            : TabletId(tabletId)
            , Active(false)
            , Stopped(false)
        {
        }

        TActorId Accept(TEvTabletPipe::TEvConnect::TPtr &ev, TActorIdentity owner, TActorId recipientId, bool leader, ui64 generation) override {
            Y_ABORT_UNLESS(ev->Get()->Record.GetTabletId() == TabletId);
            const TActorId clientId = ActorIdFromProto(ev->Get()->Record.GetClientId());
            IActor* server = CreateServer(TabletId, clientId, ev->InterconnectSession, ev->Get()->Record.GetFeatures(), ev->Cookie);
            TActorId serverId = TActivationContext::Register(server);
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " Accept Connect Originator# " << ev->Sender);
            ServerIds.insert(serverId);
            ActivateServer(TabletId, serverId, owner, recipientId, leader, generation);
            return serverId;
        }

        void Reject(TEvTabletPipe::TEvConnect::TPtr &ev, TActorIdentity owner, NKikimrProto::EReplyStatus status, bool leader) override {
            Y_ABORT_UNLESS(ev->Get()->Record.GetTabletId() == TabletId);
            const TActorId clientId = ActorIdFromProto(ev->Get()->Record.GetClientId());
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " Reject Connect Originator# " << ev->Sender);
            owner.Send(clientId, new TEvTabletPipe::TEvConnectResult(status, TabletId, clientId, TActorId(), leader, 0));
        }

        void Stop(TActorIdentity owner) override {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]" << " Stop");
            for (const auto& serverId : ServerIds) {
                owner.Send(serverId, new TEvTabletPipe::TEvShutdown);
            }
            ActivatePending.clear();
            Stopped = true;
        }

        void Detach(TActorIdentity owner) override {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]" << " Detach");
            for (const auto& serverId : ServerIds) {
                CloseServer(owner, serverId);
            }
            Active = false;
            ServerIds.clear();
            ActivatePending.clear();
        }

        TActorId Enqueue(TEvTabletPipe::TEvConnect::TPtr &ev, TActorIdentity owner) override {
            Y_UNUSED(owner);
            Y_ABORT_UNLESS(ev->Get()->Record.GetTabletId() == TabletId);
            const TActorId clientId = ActorIdFromProto(ev->Get()->Record.GetClientId());
            IActor* server = CreateServer(TabletId, clientId, ev->InterconnectSession, ev->Get()->Record.GetFeatures(), ev->Cookie);
            TActorId serverId = TActivationContext::Register(server);
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]"
                << " Enqueue Connect Originator# " << ev->Sender);
            ServerIds.insert(serverId);
            ActivatePending.insert(serverId);
            return serverId;
        }

        void Activate(TActorIdentity owner, TActorId recipientId, bool leader, ui64 generation) override {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::PIPE_SERVER, "[" << TabletId << "]" << " Activate");
            for (const auto& serverId : ActivatePending) {
                ActivateServer(TabletId, serverId, owner, recipientId, leader, generation);
            }

            ActivatePending.clear();
            Active = true;
        }

        void Erase(TEvTabletPipe::TEvServerDestroyed::TPtr &ev) override {
            const auto& serverId = ev->Get()->ServerId;
            ServerIds.erase(serverId);
            ActivatePending.erase(serverId);
        }

        bool IsActive() const override {
            return Active;
        }

        bool IsStopped() const override {
            return Stopped;
        }

    private:
    private:
        const ui64 TabletId;
        THashSet<TActorId> ServerIds;
        THashSet<TActorId> ActivatePending;
        bool Active;
        bool Stopped;
    };

    IActor* CreateServer(ui64 tabletId, const TActorId& clientId, const TActorId& interconnectSession, ui32 features, ui64 connectCookie) {
        return new TServer(tabletId, clientId, interconnectSession, features, connectCookie);
    }

    IConnectAcceptor* CreateConnectAcceptor(ui64 tabletId) {
        return new TConnectAcceptor(tabletId);
    }

    void ActivateServer(ui64 tabletId, TActorId serverId, TActorIdentity owner, TActorId recipientId, bool leader, ui64 generation) {
        owner.Send(serverId, new TEvTabletPipe::TEvActivate(tabletId, owner, recipientId, leader, generation));
    }

    void CloseServer(TActorIdentity owner, TActorId serverId) {
        owner.Send(serverId, new TEvents::TEvPoisonPill);
    }
}

}
