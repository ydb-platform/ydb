#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/util/queue_oneone_inplace.h>
#include <library/cpp/random_provider/random_provider.h>


#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
    #error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::PIPE_CLIENT, "TClient[" << TabletId << "] " << stream << " " << ctx.SelfID)

#define BLOG_I(stream) LOG_INFO_S(ctx, NKikimrServices::PIPE_CLIENT, "TClient[" << TabletId << "] " << stream << " " << ctx.SelfID)

#define BLOG_ERROR(stream) LOG_ERROR_S(ctx, NKikimrServices::PIPE_CLIENT, "TClient[" << TabletId << "] " << stream << " " << ctx.SelfID)

#define BLOG_TRACE(stream) LOG_TRACE_S(ctx, NKikimrServices::PIPE_CLIENT, "TClient[" << TabletId << "] " << stream << " " << ctx.SelfID)

namespace NKikimr {

namespace NTabletPipe {

    class TClient : public TActorBootstrapped<TClient> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TABLET_PIPE_CLIENT;
        }

        TClient(const TActorId& owner, ui64 tabletId, const TClientConfig& config)
            : Owner(owner)
            , TabletId(tabletId)
            , Config(config)
            , IsShutdown(false)
            , PayloadQueue(new TPayloadQueue())
            , Leader(true)
        {
            Y_ABORT_UNLESS(tabletId != 0);
        }

        void Bootstrap(const TActorContext& ctx) {
            BLOG_D("::Bootstrap");

            Lookup(ctx);
        }

    private:
        void StateLookup(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                FFunc(TEvTabletPipe::EvSend, HandleSendQueued);
                FFunc(TEvTabletPipe::EvMessage, HandleSendQueued);
                HFunc(TEvTabletPipe::TEvShutdown, HandleConnect);
                HFunc(TEvents::TEvPoisonPill, HandleConnect);
                HFunc(TEvTabletResolver::TEvForwardResult, HandleLookup);
                HFunc(TEvInterconnect::TEvNodeDisconnected, HandleRelaxed);
                HFunc(TEvTabletPipe::TEvConnectResult, HandleOutdated);
            }
        }

        void StateConnectNode(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                FFunc(TEvTabletPipe::EvSend, HandleSendQueued);
                FFunc(TEvTabletPipe::EvMessage, HandleSendQueued);
                HFunc(TEvTabletPipe::TEvShutdown, HandleConnect);
                HFunc(TEvents::TEvPoisonPill, HandleConnect);
                HFunc(TEvInterconnect::TEvNodeConnected, HandleConnectNode);
                HFunc(TEvInterconnect::TEvNodeDisconnected, HandleConnect);
                HFunc(TEvTabletPipe::TEvConnectResult, HandleOutdated);
            }
        }

        void StateConnect(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                FFunc(TEvTabletPipe::EvSend, HandleSendQueued);
                FFunc(TEvTabletPipe::EvMessage, HandleSendQueued);
                HFunc(TEvTabletPipe::TEvShutdown, HandleConnect);
                HFunc(TEvents::TEvPoisonPill, HandleConnect);
                HFunc(TEvents::TEvUndelivered, HandleConnect);
                HFunc(TEvTabletPipe::TEvPeerClosed, HandleConnect);
                HFunc(TEvTabletPipe::TEvConnectResult, HandleConnect);
                HFunc(TEvInterconnect::TEvNodeDisconnected, HandleConnect);
            }
        }

        void StateWait(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                FFunc(TEvTabletPipe::EvSend, HandleSendQueued);
                FFunc(TEvTabletPipe::EvMessage, HandleSendQueued);
                HFunc(TEvTabletPipe::TEvShutdown, HandleConnect);
                HFunc(TEvents::TEvPoisonPill, HandleConnect);
                HFunc(TEvTabletPipe::TEvClientRetry, HandleWait);
                HFunc(TEvInterconnect::TEvNodeDisconnected, HandleRelaxed);
                HFunc(TEvTabletPipe::TEvConnectResult, HandleOutdated);
            }
        }

        void StateCheckDead(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvents::TEvPoisonPill, Handle);
                HFunc(TEvTabletPipe::TEvClientCheckDelay, Handle);
                HFunc(TEvTabletPipe::TEvClientConnected, Handle);
                HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                HFunc(TEvHive::TEvResponseHiveInfo, Handle);
                HFunc(TEvInterconnect::TEvNodeDisconnected, HandleRelaxed);
                HFunc(TEvTabletPipe::TEvConnectResult, HandleOutdated);
                // ignore everything else
            }
        }

        void StateWork(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                FFunc(TEvTabletPipe::EvSend, HandleSend);
                FFunc(TEvTabletPipe::EvMessage, HandleSend);
                HFunc(TEvTabletPipe::TEvShutdown, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
                HFunc(TEvents::TEvUndelivered, Handle);
                HFunc(TEvTabletPipe::TEvPeerClosed, Handle);
                HFunc(TEvTabletPipe::TEvPeerShutdown, Handle);
                HFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                HFunc(TEvTabletPipe::TEvConnectResult, HandleOutdated);
            }
        }

        bool IsLocalNode(const TActorContext& ctx) const {
            auto leader = GetTabletLeader();
            return leader.NodeId() == 0 || ctx.ExecutorThread.ActorSystem->NodeId == leader.NodeId();
        }

        TActorId GetTabletLeader() const {
            return Config.ConnectToUserTablet ? LastKnownLeaderTablet : LastKnownLeader;
        }

        void HandleSendQueued(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            BLOG_D("queue send");
            Y_ABORT_UNLESS(!IsShutdown);
            PayloadQueue->Push(ev.Release());
        }

        void HandleSend(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            BLOG_D("send");
            Y_ABORT_UNLESS(!IsShutdown);
            Push(ctx, ev);
        }

        ui32 GenerateConnectFeatures() const {
            ui32 features = 0;
            if (Config.ExpectShutdown) {
                features |= NKikimrTabletPipe::FEATURE_GRACEFUL_SHUTDOWN;
            }
            return features;
        }

        void HandleLookup(TEvTabletResolver::TEvForwardResult::TPtr& ev, const TActorContext &ctx) {
            Y_ABORT_UNLESS(ev->Get()->TabletID == TabletId);

            if (ev->Get()->Status != NKikimrProto::OK || (Config.ConnectToUserTablet && !ev->Get()->TabletActor)) {
                BLOG_D("forward result error, check reconnect");
                return TryToReconnect(ctx);
            }

            LastKnownLeaderTablet = ev->Get()->TabletActor;
            LastKnownLeader = ev->Get()->Tablet;
            LastCacheEpoch = ev->Get()->CacheEpoch;

            if (IsLocalNode(ctx)) {
                BLOG_D("forward result local node, try to connect");
                UnsubscribeNetworkSession(ctx);
                Connect(ctx);
            } else {
                const ui32 nodeId = GetTabletLeader().NodeId();
                BLOG_D("forward result remote node " << nodeId);
                if (InterconnectNodeId == nodeId) {
                    // Already connected to correct remote node
                    Y_ABORT_UNLESS(InterconnectSessionId);
                    Connect(ctx);
                } else {
                    // Connect to a new remote node
                    ConnectNode(nodeId, ctx);
                }
            }
        }

        void ConnectNode(ui32 nodeId, const TActorContext &ctx) {
            UnsubscribeNetworkSession(ctx);

            TActorId proxy = TActivationContext::InterconnectProxy(nodeId);
            if (!proxy) {
                BLOG_ERROR("remote node " << nodeId << " on broken proxy");
                NotifyNodeProblem(nodeId, ctx);
                return NotifyConnectFail(ctx);
            }

            InterconnectNodeId = nodeId;
            InterconnectProxyId = proxy;
            ctx.Send(proxy, new TEvInterconnect::TEvConnectNode(), 0, ++InterconnectCookie);
            Become(&TThis::StateConnectNode);
        }

        void HandleConnectNode(TEvInterconnect::TEvNodeConnected::TPtr& ev, const TActorContext &ctx) {
            if (ev->Cookie != InterconnectCookie) {
                BLOG_D("ignored outdated TEvNodeConnected");
                return;
            }

            BLOG_D("remote node connected");
            Y_ABORT_UNLESS(!InterconnectSessionId);
            InterconnectSessionId = ev->Sender;
            Connect(ctx);
        }

        void HandleRelaxed(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext &ctx) {
            if (ev->Cookie != InterconnectCookie) {
                BLOG_D("ignored outdated TEvNodeDisconnected");
                return;
            }

            BLOG_D("remote node disonnected while connecting, check retry");
            if (InterconnectSessionId) {
                Y_ABORT_UNLESS(ev->Sender == InterconnectSessionId);
            }
            NotifyNodeProblem(ctx);
            ForgetNetworkSession();
        }

        void HandleConnect(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext &ctx) {
            if (ev->Cookie != InterconnectCookie) {
                BLOG_D("ignored outdated TEvNodeDisconnected");
                return;
            }

            BLOG_D("remote node disonnected while connecting, check retry");
            if (InterconnectSessionId) {
                Y_ABORT_UNLESS(ev->Sender == InterconnectSessionId);
            }
            NotifyNodeProblem(ctx);
            ForgetNetworkSession();
            TryToReconnect(ctx);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext &ctx) {
            if (ev->Cookie != InterconnectCookie) {
                BLOG_D("ignored outdated TEvNodeDisconnected");
                return;
            }

            BLOG_D("remote node disconnected while working, drop pipe");
            NotifyNodeProblem(ctx);
            ForgetNetworkSession();
            NotifyDisconnect(ctx);
        }

        void Connect(const TActorContext &ctx) {
            SendEvent(new IEventHandle(GetTabletLeader(), ctx.SelfID, new TEvTabletPipe::TEvConnect(TabletId, ctx.SelfID, GenerateConnectFeatures()),
                IEventHandle::FlagTrackDelivery, ++ConnectCookie), ctx);
            Become(&TThis::StateConnect);
        }

        void HandleConnect(TEvTabletPipe::TEvPeerClosed::TPtr& ev, const TActorContext &ctx) {
            if (ev->InterconnectSession != InterconnectSessionId) {
                // Ingnore TEvPeerClosed from an unexpected interconnect session
                BLOG_D("ignore outdated peer closed from " << ev->Sender);
                return;
            }

            BLOG_D("peer closed while connecting, check reconnect");
            return TryToReconnect(ctx);
        }

        void HandleConnect(TEvTabletPipe::TEvConnectResult::TPtr& ev, const TActorContext &ctx) {
            if (ev->InterconnectSession != InterconnectSessionId || ev->Cookie != 0 && ev->Cookie != ConnectCookie) {
                // Ignore TEvConnectResult from an unexpected interconnect session or retry attempt
                BLOG_D("ignored outdated connection result from " << ev->Sender);
                ctx.Send(ev->Sender, new TEvTabletPipe::TEvPeerClosed(TabletId, ctx.SelfID, ev->Sender));
                return;
            }

            const auto &record = ev->Get()->Record;
            Y_ABORT_UNLESS(record.GetTabletId() == TabletId);

            ServerId = ActorIdFromProto(record.GetServerId());
            Leader = record.GetLeader();
            Generation = record.GetGeneration();
            SupportsDataInPayload = record.GetSupportsDataInPayload();

            Y_ABORT_UNLESS(!ServerId || record.GetStatus() == NKikimrProto::OK);
            BLOG_D("connected with status " << record.GetStatus() << " role: " << (Leader ? "Leader" : "Follower"));

            if (!ServerId) {
                return TryToReconnect(ctx);
            }

            Become(&TThis::StateWork);

            ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(TabletId, NKikimrProto::OK, ctx.SelfID, ServerId,
                                                                  Leader, false, Generation));

            BLOG_D("send queued");
            while (TAutoPtr<IEventHandle> x = PayloadQueue->Pop())
                Push(ctx, x);

            PayloadQueue.Destroy();

            if (IsShutdown) {
                BLOG_D("shutdown pipe due to pending shutdown request");
                return NotifyDisconnect(ctx);
            }
        }

        void HandleOutdated(TEvTabletPipe::TEvConnectResult::TPtr& ev, const TActorContext &ctx) {
            BLOG_D("ignored outdated connection result from " << ev->Sender);
            ctx.Send(ev->Sender, new TEvTabletPipe::TEvPeerClosed(TabletId, ctx.SelfID, ev->Sender));
        }

        void HandleConnect(TEvents::TEvUndelivered::TPtr& ev, const TActorContext &ctx) {
            const auto* msg = ev->Get();
            if (msg->SourceType != TEvTabletPipe::TEvConnect::EventType || ev->Cookie != ConnectCookie) {
                BLOG_D("ignored unexpected TEvUndelivered for event " << msg->SourceType << " with cookie " << ev->Cookie);
                return;
            }

            BLOG_D("connect request undelivered");
            TryToReconnect(ctx);
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
            const auto* msg = ev->Get();
            if (msg->SourceType == TEvTabletPipe::TEvConnect::EventType) {
                // We have connected already, ignore undelivered notifications from older attempts
                BLOG_D("ignored unexpected TEvUndelivered for event " << msg->SourceType << " with cookie " << ev->Cookie);
                return;
            }

            BLOG_D("pipe event not delivered, drop pipe");
            return NotifyDisconnect(ctx);
        }

        void Handle(TEvTabletPipe::TEvPeerClosed::TPtr& ev, const TActorContext& ctx) {
            if (ev->InterconnectSession != InterconnectSessionId) {
                // Ingnore TEvPeerClosed from an unexpected interconnect session
                BLOG_D("ignore outdated peer closed from " << ev->Sender);
                return;
            }

            Y_ABORT_UNLESS(ev->Get()->Record.GetTabletId() == TabletId);
            BLOG_D("peer closed");
            return NotifyDisconnect(ctx);
        }

        void Handle(TEvTabletPipe::TEvPeerShutdown::TPtr& ev, const TActorContext& ctx) {
            Y_ABORT_UNLESS(ev->Get()->Record.GetTabletId() == TabletId);
            BLOG_D("peer shutdown");
            if (Y_LIKELY(Config.ExpectShutdown)) {
                ctx.Send(Owner, new TEvTabletPipe::TEvClientShuttingDown(
                        TabletId, ctx.SelfID, ServerId, ev->Get()->GetMaxForwardedSeqNo()));
            }
        }

        void HandleConnect(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
            BLOG_D("poison pill while connecting");
            Y_UNUSED(ev);
            if (ServerId)
                ctx.Send(ServerId, new TEvTabletPipe::TEvPeerClosed(TabletId, ctx.SelfID, ServerId));

            return NotifyConnectFail(ctx);
        }

        void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            BLOG_D("received poison pill");
            ctx.Send(ServerId, new TEvTabletPipe::TEvPeerClosed(TabletId, ctx.SelfID, ServerId));
            return NotifyDisconnect(ctx);
        }

        void Handle(TEvTabletPipe::TEvShutdown::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            BLOG_D("received shutdown");
            ctx.Send(ServerId, new TEvTabletPipe::TEvPeerClosed(TabletId, SelfId(), ServerId));
            return NotifyDisconnect(ctx);
        }

        void HandleConnect(TEvTabletPipe::TEvShutdown::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            BLOG_D("received pending shutdown");
            IsShutdown = true;
        }

        void HandleWait(TEvTabletPipe::TEvClientRetry::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            BLOG_D("client retry");

            LastKnownLeaderTablet = TActorId();
            LastKnownLeader = TActorId();

            Lookup(ctx);
        }

        void RequestHiveInfo(ui64 hiveTabletId) {
            static TClientConfig clientConfig({.RetryLimitCount = 3, .MinRetryTime = TDuration::MilliSeconds(300)});
            HiveClient = Register(CreateClient(SelfId(), hiveTabletId, clientConfig));
            NTabletPipe::SendData(SelfId(), HiveClient, new TEvHive::TEvRequestHiveInfo(TabletId, false));
        }

        // check aliveness section
        void Handle(TEvHive::TEvResponseHiveInfo::TPtr &ev, const TActorContext &ctx) {
            const auto &record = ev->Get()->Record;
            if (record.HasForwardRequest() && (++CurrentHiveForwards < MAX_HIVE_FORWARDS)) {
                BLOG_I("hive request forwarded to " << record.GetForwardRequest().GetHiveTabletId());
                CloseClient(ctx, HiveClient);
                RequestHiveInfo(record.GetForwardRequest().GetHiveTabletId());
                return;
            }
            bool definitelyDead = false;
            if (record.GetTablets().empty()) {
                definitelyDead = true; // the tablet wasn't found in Hive
            } else {
                const auto &info = record.GetTablets(0);
                Y_ABORT_UNLESS(info.GetTabletID() == TabletId);
                if (!info.HasState() || info.GetState() == 202/*THive::ETabletState::Deleting*/) {
                    definitelyDead = true;
                }
            }

            ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(TabletId, NKikimrProto::ERROR, SelfId(), TActorId(), Leader, definitelyDead, Generation));
            return Die(ctx);
        }

        void Handle(TEvTabletPipe::TEvClientCheckDelay::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);

            if (HiveUidFromTabletID(TabletId) == 0)
                BLOG_ERROR("trying to check aliveness of hand-made tablet! would definitely fail");

            const ui64 hiveTabletId = AppData()->DomainsInfo->GetHive();
            RequestHiveInfo(hiveTabletId);
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
            if (HiveClient != ev->Sender)
                return;
            auto *msg = ev->Get();
            if (msg->Status != NKikimrProto::OK) {
                ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(TabletId, NKikimrProto::ERROR, SelfId(), TActorId(), Leader, false, Generation));
                return Die(ctx);
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
            if (HiveClient != ev->Sender)
                return;
            ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(TabletId, NKikimrProto::ERROR, SelfId(), TActorId(), Leader, false, Generation));
            return Die(ctx);
        }

        void NotifyConnectFail(const TActorContext &ctx) {
            UnsubscribeNetworkSession(ctx);
            if (Config.CheckAliveness && !IsReservedTabletId(TabletId)) {
                BLOG_D("connect failed, check aliveness");

                if (!Config.RetryPolicy)
                    BLOG_ERROR("check aliveness w/o retry policy, possible perfomance hit");

                Become(&TThis::StateCheckDead, RetryState.MakeCheckDelay(), new TEvTabletPipe::TEvClientCheckDelay());
            } else {
                BLOG_D("connect failed");
                ctx.Send(Owner, new TEvTabletPipe::TEvClientConnected(TabletId, NKikimrProto::ERROR, SelfId(), TActorId(), Leader, false, Generation));
                return Die(ctx);
            }
        }

        void NotifyDisconnect(const TActorContext &ctx) {
            BLOG_D("notify reset");
            ctx.Send(Owner, new TEvTabletPipe::TEvClientDestroyed(TabletId, ctx.SelfID, ServerId));
            return Die(ctx);
        }

        void NotifyNodeProblem(const TActorContext &ctx) {
            NotifyNodeProblem(InterconnectNodeId, ctx);
        }

        void NotifyNodeProblem(ui32 nodeId, const TActorContext &ctx) {
            ctx.Send(MakeTabletResolverID(), new TEvTabletResolver::TEvNodeProblem(nodeId, LastCacheEpoch));
        }

        void ForgetNetworkSession() {
            InterconnectSessionId = { };
            InterconnectProxyId = { };
            InterconnectNodeId = 0;
            ++InterconnectCookie;
        }

        void UnsubscribeNetworkSession(const TActorContext& ctx) {
            if (InterconnectNodeId) {
                ctx.Send(InterconnectSessionId ? InterconnectSessionId : InterconnectProxyId, new TEvents::TEvUnsubscribe());
                ForgetNetworkSession();
            }
        }

        void Lookup(const TActorContext& ctx) {
            BLOG_D("lookup");
            TEvTabletResolver::TEvForward::TResolveFlags resolveFlags;
            resolveFlags.SetAllowFollower(Config.AllowFollower);
            resolveFlags.SetForceFollower(Config.ForceFollower);
            resolveFlags.SetPreferLocal(Config.PreferLocal);
            resolveFlags.SetForceLocal(Config.ForceLocal);

            ctx.Send(MakeTabletResolverID(), new TEvTabletResolver::TEvForward(TabletId, nullptr, resolveFlags));
            Become(&TThis::StateLookup);
        }

        void TryToReconnect(const TActorContext& ctx) {
            if (LastKnownLeaderTablet)
                ctx.Send(MakeTabletResolverID(), new TEvTabletResolver::TEvTabletProblem(TabletId, LastKnownLeaderTablet));

            LastKnownLeaderTablet = TActorId();
            LastKnownLeader = TActorId();

            TDuration waitDuration;
            if (Config.RetryPolicy && RetryState.IsAllowedToRetry(waitDuration, Config.RetryPolicy)) {
                if (waitDuration == TDuration::Zero()) {
                    BLOG_D("immediate retry");
                    Lookup(ctx);
                } else {
                    BLOG_D("schedule retry");
                    ctx.Schedule(waitDuration, new TEvTabletPipe::TEvClientRetry);
                    Become(&TThis::StateWait);
                }
            } else {
                return NotifyConnectFail(ctx);
            }
        }

        struct TEventParts {
            ui32 Type;
            TActorId Sender;
            THolder<IEventBase> Event;
            TIntrusivePtr<TEventSerializedData> Buffer;
            ui64 SeqNo;

            explicit TEventParts(TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvTabletPipe::EvSend: {
                        Type = ev->Type;
                        Sender = ev->Sender;
                        if (ev->HasEvent()) {
                            if (ev->HasBuffer()) {
                                // Grab both the buffer and the event
                                Buffer = ev->GetChainBuffer();
                            }
                            Event.Reset(ev->ReleaseBase().Release());
                        } else {
                            Buffer = ev->ReleaseChainBuffer();
                        }
                        SeqNo = 0;
                        break;
                    }
                    case TEvTabletPipe::EvMessage: {
                        auto* msg = ev->Get<TEvTabletPipe::TEvMessage>();
                        Type = msg->Type;
                        Sender = msg->Sender;
                        Event = msg->ReleaseEvent();
                        Buffer = msg->ReleaseBuffer();
                        SeqNo = msg->GetSeqNo();
                        break;
                    }
                    default:
                        Y_UNREACHABLE();
                }
            }

            THolder<TEvTabletPipe::TEvPush> ToRemotePush(ui64 tabletId, ui64 cookie, bool supportsDataInPayload) {
                if (!Buffer) {
                    Y_ABORT_UNLESS(Event, "Sending an empty event without a buffer");
                    TAllocChunkSerializer serializer;
                    Event->SerializeToArcadiaStream(&serializer);
                    Buffer = serializer.Release(Event->CreateSerializationInfo());
                }

                auto msg = MakeHolder<TEvTabletPipe::TEvPush>(tabletId, Type, Sender, Buffer, cookie,
                    Buffer->GetSerializationInfo(), supportsDataInPayload);

                if (SeqNo) {
                    msg->SetSeqNo(SeqNo);
                }

                return msg;
            }
        };

        void Push(const TActorContext& ctx, TAutoPtr<IEventHandle>& ev) {
            BLOG_D("push event to server");

            if (!InterconnectSessionId) {
                switch (ev->GetTypeRewrite()) {
                    case TEvTabletPipe::EvMessage: {
                        // Send local self -> server message without conversions
                        THolder<TEvTabletPipe::TEvMessage> msg(ev->Release<TEvTabletPipe::TEvMessage>().Release());
                        ctx.ExecutorThread.Send(new IEventHandle(ServerId, SelfId(), msg.Release(),
                                IEventHandle::FlagTrackDelivery, ev->Cookie, nullptr, std::move(ev->TraceId)));
                        break;
                    }
                    case TEvTabletPipe::EvSend: {
                        // Repackage event in a self -> sender form with an original type
                        THolder<IEventHandle> directEv;
                        if (ev->HasEvent()) {
                            directEv = MakeHolder<IEventHandle>(ev->Sender, SelfId(), ev->ReleaseBase().Release(),
                                    IEventHandle::FlagTrackDelivery, ev->Cookie, nullptr, std::move(ev->TraceId));
                        } else {
                            directEv = MakeHolder<IEventHandle>(ev->Type, IEventHandle::FlagTrackDelivery,
                                    ev->Sender, SelfId(), ev->ReleaseChainBuffer(), ev->Cookie, nullptr,
                                    std::move(ev->TraceId));
                        }
                        // Rewrite into EvSend and send to the server
                        directEv->Rewrite(TEvTabletPipe::EvSend, ServerId);
                        ctx.ExecutorThread.Send(directEv.Release());
                        break;
                    }
                    default:
                        Y_UNREACHABLE();
                }
            } else {
                THolder<TEvTabletPipe::TEvPush> msg = TEventParts(ev).ToRemotePush(TabletId, ev->Cookie,
                    SupportsDataInPayload);

                // Send a remote self -> server message
                SendEvent(new IEventHandle(ServerId, SelfId(), msg.Release(), IEventHandle::FlagTrackDelivery, 0,
                    nullptr, std::move(ev->TraceId)), ctx);
            }
        }

        void SendEvent(IEventHandle* ev, const TActorContext& ctx) {
            LOG_DEBUG(ctx, NKikimrServices::PIPE_CLIENT, "TClient[%" PRIu64 "]::SendEvent %s", TabletId,
                ctx.SelfID.ToString().c_str());

            if (InterconnectSessionId) {
                Y_ABORT_UNLESS(ev->Recipient.NodeId() == InterconnectNodeId,
                    "Sending event to %s via remote node %" PRIu32, ev->Recipient.ToString().c_str(), InterconnectNodeId);
                ev->Rewrite(TEvInterconnect::EvForward, InterconnectSessionId);
            } else {
                Y_ABORT_UNLESS(ev->Recipient.NodeId() == ctx.SelfID.NodeId(),
                    "Sending event to %s via local node %" PRIu32, ev->Recipient.ToString().c_str(), ctx.SelfID.NodeId());
            }

            ctx.ExecutorThread.Send(ev);
        }

        void Die(const TActorContext& ctx) override {
            if (HiveClient)
                CloseClient(ctx, HiveClient);
            UnsubscribeNetworkSession(ctx);
            IActor::Die(ctx);
        }

    private:
        const TActorId Owner;
        const ui64 TabletId;
        const TClientConfig Config;
        bool IsShutdown;
        TActorId LastKnownLeader;
        TActorId LastKnownLeaderTablet;
        ui64 LastCacheEpoch = 0;
        ui32 InterconnectNodeId = 0;
        ui64 InterconnectCookie = 0;
        ui64 ConnectCookie = 0;
        TActorId InterconnectProxyId;
        TActorId InterconnectSessionId;
        TActorId ServerId;
        typedef TOneOneQueueInplace<IEventHandle*, 32> TPayloadQueue;
        TAutoPtr<TPayloadQueue, TPayloadQueue::TPtrCleanDestructor> PayloadQueue;
        TClientRetryState RetryState;
        bool Leader;
        ui64 Generation = 0;
        TActorId HiveClient;
        ui32 CurrentHiveForwards = 0;
        static constexpr ui32 MAX_HIVE_FORWARDS = 10;
        bool SupportsDataInPayload = false;
    };

    IActor* CreateClient(const TActorId& owner, ui64 tabletId, const TClientConfig& config) {
        return new TClient(owner, tabletId, config);
    }

    void SendData(TActorId self, TActorId clientId, IEventBase *payload, ui64 cookie, NWilson::TTraceId traceId) {
        auto ev = new IEventHandle(clientId, self, payload, 0, cookie, nullptr, std::move(traceId));
        ev->Rewrite(TEvTabletPipe::EvSend, clientId);
        TActivationContext::Send(ev);
    }

    void SendData(TActorId self, TActorId clientId, THolder<IEventBase>&& payload, ui64 cookie, NWilson::TTraceId traceId) {
        SendData(self, clientId, payload.Release(), cookie, std::move(traceId));
    }

    void SendDataWithSeqNo(TActorId self, TActorId clientId, IEventBase *payload, ui64 seqNo, ui64 cookie, NWilson::TTraceId traceId) {
        auto event = MakeHolder<TEvTabletPipe::TEvMessage>(self, THolder<IEventBase>(payload));
        event->SetSeqNo(seqNo);
        auto ev = MakeHolder<IEventHandle>(clientId, self, event.Release(), 0, cookie, nullptr, std::move(traceId));
        TActivationContext::Send(ev.Release());
    }

    void SendData(TActorId self, TActorId clientId, ui32 eventType, TIntrusivePtr<TEventSerializedData> buffer, ui64 cookie, NWilson::TTraceId traceId) {
        auto ev = new IEventHandle(eventType, 0, clientId, self, buffer, cookie, nullptr, std::move(traceId));
        ev->Rewrite(TEvTabletPipe::EvSend, clientId);
        TActivationContext::Send(ev);
    }

    void SendData(const TActorContext& ctx, const TActorId& clientId, IEventBase* payload, ui64 cookie, NWilson::TTraceId traceId) {
        auto ev = new IEventHandle(clientId, ctx.SelfID, payload, 0, cookie, nullptr, std::move(traceId));
        ev->Rewrite(TEvTabletPipe::EvSend, clientId);
        ctx.ExecutorThread.Send(ev);
    }

    void SendData(const TActorContext& ctx, const TActorId& clientId, ui32 eventType, TIntrusivePtr<TEventSerializedData> buffer, ui64 cookie, NWilson::TTraceId traceId) {
        auto ev = new IEventHandle(eventType, 0, clientId, ctx.SelfID, buffer, cookie, nullptr, std::move(traceId));
        ev->Rewrite(TEvTabletPipe::EvSend, clientId);
        ctx.ExecutorThread.Send(ev);
    }

    void ShutdownClient(const TActorContext& ctx, const TActorId& clientId) {
        ctx.Send(clientId, new TEvTabletPipe::TEvShutdown);
    }

    void CloseClient(const TActorContext& ctx, const TActorId& clientId) {
        ctx.Send(clientId, new TEvents::TEvPoisonPill);
    }

    void CloseClient(TActorIdentity self, TActorId clientId) {
        self.Send(clientId, new TEvents::TEvPoisonPill());
    }

    void CloseAndForgetClient(TActorIdentity self, TActorId &clientId) {
        if (clientId) {
            CloseClient(self, clientId);
            clientId = TActorId();
        }
    }

    bool TClientRetryState::IsAllowedToRetry(TDuration& wait, const TClientRetryPolicy& policy) {
        if (RetryNumber == 0) {
           wait = policy.DoFirstRetryInstantly ? TDuration::Zero() : policy.MinRetryTime;
        } else {
            const ui64 baseRetryDuration = RetryDuration.GetValue() * policy.BackoffMultiplier;
            const ui64 croppedRetryDuration = Min(policy.MaxRetryTime.GetValue(), baseRetryDuration);
            const ui64 randomizedRetryDuration = croppedRetryDuration * AppData()->RandomProvider->Uniform(100, 115) / 100;
            wait = TDuration::FromValue(randomizedRetryDuration);
            wait = Max(policy.MinRetryTime, wait);
        }
        ++RetryNumber;
        RetryDuration = wait;
        return !policy.RetryLimitCount || RetryNumber <= policy.RetryLimitCount;
    }

    TDuration TClientRetryState::MakeCheckDelay() {
        const ui64 randomizedRetryDuration = RetryDuration.GetValue() * AppData()->RandomProvider->Uniform(100, 133) / 100;
        return TDuration::FromValue(randomizedRetryDuration);
    }
}

}
