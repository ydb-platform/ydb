#include "msgbus_servicereq.h"
#include <ydb/core/client/server/msgbus_securereq.h>
#include <ydb/library/actors/interconnect/load.h>
#include <ydb/library/actors/interconnect/slowpoke_actor.h>
#include <ydb/library/actors/core/interconnect.h>

#include <algorithm>

namespace NKikimr {
namespace NMsgBusProxy {

class TInterconnectDebugActor : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TInterconnectDebugActor>> {
    enum {
        EvLoadFinished = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TLoadResult {
        ui64 ThroughputBytes = 0;
        ui64 ThroughputSamples = 0;
        ui64 RttSamples = 0;
        ui64 DurationUs = 0;
        ui64 MaxRttGapUs = 0;

        TLoadResult() = default;

        explicit TLoadResult(const NInterconnect::TLoadActorStats& stats)
            : ThroughputBytes(stats.ThroughputBytes)
            , ThroughputSamples(stats.ThroughputSamples)
            , RttSamples(stats.TotalRttSamples)
            , DurationUs(stats.ThroughputWindow.MicroSeconds())
            , MaxRttGapUs(stats.MaxRttGap.MicroSeconds())
        {}

        void Add(const TLoadResult& other) {
            ThroughputBytes += other.ThroughputBytes;
            ThroughputSamples += other.ThroughputSamples;
            RttSamples += other.RttSamples;
            DurationUs = std::max(DurationUs, other.DurationUs);
            MaxRttGapUs = std::max(MaxRttGapUs, other.MaxRttGapUs);
        }

        void Fill(NKikimrClient::TResponse::TInterconnectLoadResult* result) const {
            result->SetThroughputBytes(ThroughputBytes);
            result->SetThroughputSamples(ThroughputSamples);
            result->SetRttSamples(RttSamples);
            result->SetDurationUs(DurationUs);
            result->SetMaxRttGapUs(MaxRttGapUs);
        }
    };

    struct TEvLoadFinished : TEventLocal<TEvLoadFinished, EvLoadFinished> {
        TLoadResult Result;

        explicit TEvLoadFinished(const NInterconnect::TLoadActorStats& stats)
            : Result(stats)
        {}
    };

    std::function<void(const TActorContext&)> Callback;
    bool WaitForCompletion = false;
    ui32 PendingLoadActors = 0;
    TLoadResult LoadResult;

    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TInterconnectDebugActor>>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TInterconnectDebugActor(NKikimrClient::TInterconnectDebug& record, NMsgBusProxy::TBusMessageContext& msg)
        : TBase(msg)
    {
        TBase::SetSecurityToken(record.GetSecurityToken());
        TBase::SetPeerName(msg.GetPeerName());
        TBase::SetRequireAdminAccess(true);

        if (record.HasNumSlowpokeActors()) {
            const ui32 num = record.GetNumSlowpokeActors();
            const ui32 poolId = record.GetPoolId();
            const TDuration sleepMin = TDuration::FromValue(record.GetSleepMin());
            const TDuration sleepMax = TDuration::FromValue(record.GetSleepMax());
            const TDuration rescheduleMin = TDuration::FromValue(record.GetRescheduleMin());
            const TDuration rescheduleMax = TDuration::FromValue(record.GetRescheduleMax());
            const TDuration duration = TDuration::FromValue(record.GetDuration());
            Callback = [=](const TActorContext& ctx) {
                for (ui32 i = 0; i < num; ++i) {
                    ctx.Register(new TSlowpokeActor(duration, sleepMin, sleepMax, rescheduleMin, rescheduleMax),
                        TMailboxType::HTSwap, poolId);
                }
            };
        } else if (record.HasPoisonSessionNodeId()) {
            const ui32 nodeId = record.GetPoisonSessionNodeId();
            Callback = [=](const TActorContext& ctx) {
                ctx.Send(TActivationContext::InterconnectProxy(nodeId), new NActors::TEvInterconnect::TEvPoisonSession);
            };
        } else if (record.HasCloseInputSessionNodeId()) {
            const ui32 nodeId = record.GetCloseInputSessionNodeId();
            Callback = [=](const TActorContext& ctx) {
                ctx.Send(TActivationContext::InterconnectProxy(nodeId), new NActors::TEvInterconnect::TEvCloseInputSession);
            };
        } else if (record.HasClosePeerSocketNodeId()) {
            const ui32 nodeId = record.GetClosePeerSocketNodeId();
            Callback = [=](const TActorContext& ctx) {
                ctx.Send(TActivationContext::InterconnectProxy(nodeId), new NActors::TEvInterconnect::TEvClosePeerSocket);
            };
        } else {
            NInterconnect::TLoadParams params;
            params.Name = record.GetName();
            params.Channel = record.GetChannel();
            const auto &hops = record.GetHops();
            params.NodeHops = {hops.begin(), hops.end()};
            params.SizeMin = record.GetSizeMin();
            params.SizeMax = record.GetSizeMax();
            params.InFlyMax = record.GetInFlyMax();
            params.IntervalMin = TDuration::MicroSeconds(record.GetIntervalMin());
            params.IntervalMax = TDuration::MicroSeconds(record.GetIntervalMax());
            params.SoftLoad = record.GetSoftLoad();
            params.Duration = TDuration::MicroSeconds(record.GetDuration());
            params.UseProtobufWithPayload = record.GetUseProtobufWithPayload();
            params.RdmaMode = record.GetRdmaMode();
            WaitForCompletion = record.GetWaitForCompletion();
            PendingLoadActors = record.GetNumLoadActors();
            const bool hasServicePool = record.HasServicePool();
            const TString servicePool = record.GetServicePool();
            const ui32 numLoadActors = PendingLoadActors;
            const bool waitForCompletion = WaitForCompletion;
            Callback = [params = std::move(params), hasServicePool, servicePool, numLoadActors,
                        waitForCompletion](const TActorContext& ctx) {
                ui32 poolId = 0;
                if (hasServicePool) {
                    auto *appData = AppData(ctx);
                    if (const auto it = appData->ServicePools.find(servicePool); it != appData->ServicePools.end()) {
                        poolId = it->second;
                    }
                }
                NInterconnect::TFinishCallback finishCallback;
                if (waitForCompletion) {
                    const TActorId replyTo = ctx.SelfID;
                    finishCallback = [replyTo](const TActorContext& ctx, TString&&,
                                               const NInterconnect::TLoadActorStats& stats) {
                        ctx.Send(replyTo, new TEvLoadFinished(stats));
                    };
                }
                for (ui32 i = 0; i < numLoadActors; ++i) {
                    ctx.Register(NInterconnect::CreateLoadActor(params, finishCallback), TMailboxType::HTSwap, poolId);
                }
            };
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        Callback(ctx);

        if (WaitForCompletion && PendingLoadActors) {
            TBase::Become(&TInterconnectDebugActor::StateWait);
            return;
        }

        ReplyAndDie(ctx);
    }

    void Handle(TEvLoadFinished::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(PendingLoadActors);

        LoadResult.Add(ev->Get()->Result);

        if (!--PendingLoadActors) {
            ReplyAndDie(ctx);
        }
    }

    void ReplyAndDie(const TActorContext& ctx) {
        auto response = MakeHolder<TBusResponse>();
        response->Record.SetStatus(MSTATUS_OK);
        if (WaitForCompletion) {
            LoadResult.Fill(response->Record.MutableInterconnectLoadResult());
        }
        SendReplyMove(response.Release());

        Die(ctx);
    }

    STRICT_STFUNC(StateWait,
        HFunc(TEvLoadFinished, Handle);
        CFunc(TEvents::TSystem::PoisonPill, TBase::Cancel);
    )
};

IActor *CreateMessageBusInterconnectDebug(NMsgBusProxy::TBusMessageContext& msg) {
    NKikimrClient::TInterconnectDebug& record = static_cast<TBusInterconnectDebug *>(msg.GetMessage())->Record;
    return new TInterconnectDebugActor(record, msg);
}

} // NMsgBusProxy
} // NKikimr
