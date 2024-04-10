#include "lease_holder.h"
#include "node_broker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/util/should_continue.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/random/random.h>

namespace NKikimr {
namespace NNodeBroker {

using namespace NMon;

class TLeaseHolder : public TActorBootstrapped<TLeaseHolder> {
private:
    struct TEvPrivate {
        enum EEv {
            EvExpire = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvExpire : TEventLocal<TEvExpire, EvExpire> {};
    };

public:
    using TBase = TActorBootstrapped<TLeaseHolder>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::NODE_BROKER_LEASE_HOLDER;
    }

    TLeaseHolder(TInstant expire)
        : LastPingEpoch(0)
        , Expire(expire)
    {

    }

    void Bootstrap(const TActorContext &ctx)
    {
        NActors::TMon* mon = AppData(ctx)->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "lease", "Dynamic node lease status",
                                   false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
        }

        Become(&TThis::StatePing);

        ScheduleExpire(ctx);
        Ping(ctx);
    }

private:
    STFUNC(StateIdle)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleIdle);
            HFunc(TEvHttpInfo, Handle);
            HFunc(TEvPrivate::TEvExpire, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleIdle);
            HFunc(TEvTabletPipe::TEvClientConnected, HandleIdle);
            IgnoreFunc(TEvNodeBroker::TEvExtendLeaseResponse);

        default:
            Y_ABORT("TLeaseHolder::StateIdle unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    STFUNC(StatePing)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHttpInfo, Handle);
            HFunc(TEvNodeBroker::TEvExtendLeaseResponse, Handle);
            HFunc(TEvPrivate::TEvExpire, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

        default:
            Y_ABORT("TLeaseHolder::StatePing unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void Die(const TActorContext &ctx)
    {
        if (NodeBrokerPipe)
            NTabletPipe::CloseClient(ctx, NodeBrokerPipe);
        TBase::Die(ctx);
    }

    void OnPipeDestroyedIdle(const TActorContext &ctx)
    {
        NTabletPipe::CloseClient(ctx, NodeBrokerPipe);
        NodeBrokerPipe = TActorId();
    }

    void HandleIdle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->ClientId == NodeBrokerPipe && ev->Get()->Status != NKikimrProto::OK)
            OnPipeDestroyedIdle(ctx);
    }

    void HandleIdle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->ClientId == NodeBrokerPipe)
            OnPipeDestroyedIdle(ctx);
    }

    void HandleIdle(TEvents::TEvWakeup::TPtr &, const TActorContext &ctx) {
        Become(&TThis::StatePing);
        Ping(ctx);
    }

    void OnPipeDestroyed(const TActorContext &ctx)
    {
        NTabletPipe::CloseClient(ctx, NodeBrokerPipe);
        NodeBrokerPipe = TActorId();

        Ping(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->ClientId == NodeBrokerPipe && ev->Get()->Status != NKikimrProto::OK)
            OnPipeDestroyed(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->ClientId == NodeBrokerPipe)
            OnPipeDestroyed(ctx);
    }

    void Connect(const TActorContext &ctx)
    {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, MakeNodeBrokerID(), config);
        NodeBrokerPipe = ctx.Register(pipe);
    }

    void Handle(TEvNodeBroker::TEvExtendLeaseResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;

        // Error means Node Broker doesn't know about this node.
        // Node is either already expired or its ID is banned.
        Y_ABORT_UNLESS(rec.GetNodeId() == ctx.SelfID.NodeId());
        if (rec.GetStatus().GetCode() != NKikimrNodeBroker::TStatus::OK) {
            LOG_ERROR(ctx, NKikimrServices::NODE_BROKER, "Cannot extend lease: %s",
                      rec.GetStatus().GetReason().data());
            return;
        }

        Expire = TInstant::MicroSeconds(rec.GetExpire());
        LastResponse = ctx.Now();
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Node has now extended lease expiring " << ToString(Expire));

        if (rec.HasEpoch()) {
            LastPingEpoch = rec.GetEpoch().GetId();
            EpochEnd = TInstant::FromValue(rec.GetEpoch().GetEnd());

            if (Expire != TInstant::Max()) {
                Y_ABORT_UNLESS(Expire > EpochEnd);
                Y_ABORT_UNLESS(rec.GetExpire() == rec.GetEpoch().GetNextEnd());

                ui64 window = (Expire - EpochEnd).GetValue() / 2;
                Y_ABORT_UNLESS(window);

                NextPing = EpochEnd + TDuration::FromValue(RandomNumber<ui64>(window));
            }
        } else {
            NextPing = ctx.Now() + (Expire - ctx.Now()) / 2;
        }

        if (Expire != TInstant::Max())
            ctx.Schedule(NextPing - ctx.Now(), new TEvents::TEvWakeup());
        else
            NextPing = TInstant::Max();

        Become(&TThis::StateIdle);
    }

    void Ping(const TActorContext &ctx)
    {
        if (!NodeBrokerPipe)
            Connect(ctx);

        auto request = MakeHolder<TEvNodeBroker::TEvExtendLeaseRequest>();
        request->Record.SetNodeId(ctx.SelfID.NodeId());
        NTabletPipe::SendData(ctx, NodeBrokerPipe, request.Release());
    }

    void Handle(TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Lease expires: " << ToString(Expire) << Endl
                    << "Last lease extension: " << ToString(LastResponse) << Endl
                    << "Last ping epoch: " << LastPingEpoch << Endl
                    << "Epoch end: " << ToString(EpochEnd) << Endl
                    << "Next ping at: " << ToString(NextPing) << Endl;
            }
        }
        ctx.Send(ev->Sender, new TEvHttpInfoRes(str.Str()));
    }

    void ScheduleExpire(const TActorContext &ctx)
    {
        if (Expire != TInstant::Max())
            ctx.Schedule(Expire - ctx.Now(), new TEvPrivate::TEvExpire);
    }

    void Handle(TEvPrivate::TEvExpire::TPtr &,const TActorContext &ctx)
    {
        if (Expire <= ctx.Now())
            StopNode(ctx);
        else
            ScheduleExpire(ctx);
    }

    void StopNode(const TActorContext &ctx)
    {
        LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER, "Stop node upon lease expiration (exit code 2)");
        AppData(ctx)->KikimrShouldContinue->ShouldStop(2);
    }

    TString ToString(TInstant t) const
    {
        if (t == TInstant::Max())
            return "NEVER";
        return t.ToRfc822StringLocal();
    }

private:
    TActorId NodeBrokerPipe;
    ui64 LastPingEpoch;
    TInstant EpochEnd;
    TInstant Expire;
    TInstant LastResponse;
    TInstant NextPing;
};

IActor *CreateLeaseHolder(TInstant expire)
{
    return new TLeaseHolder(expire);
}

} // NNodeBroker
} // NKikimr
