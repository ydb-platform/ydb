#include "msgbus_servicereq.h"
#include <ydb/core/client/server/msgbus_securereq.h>
#include <ydb/library/actors/interconnect/load.h>
#include <ydb/library/actors/interconnect/slowpoke_actor.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TInterconnectDebugActor : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TInterconnectDebugActor>> {
    std::function<void(const TActorContext&)> Callback;

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
            Callback = [=](const TActorContext& ctx) {
                ui32 poolId = 0;
                if (record.HasServicePool()) {
                    auto *appData = AppData(ctx);
                    if (const auto it = appData->ServicePools.find(record.GetServicePool()); it != appData->ServicePools.end()) {
                        poolId = it->second;
                    }
                }
                ctx.Register(NInterconnect::CreateLoadActor(params), TMailboxType::HTSwap, poolId);
            };
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        Callback(ctx);

        auto response = MakeHolder<TBusResponse>();
        response->Record.SetStatus(MSTATUS_OK);
        SendReplyMove(response.Release());

        Die(ctx);
    }
};

IActor *CreateMessageBusInterconnectDebug(NMsgBusProxy::TBusMessageContext& msg) {
    NKikimrClient::TInterconnectDebug& record = static_cast<TBusInterconnectDebug *>(msg.GetMessage())->Record;
    return new TInterconnectDebugActor(record, msg);
}

} // NMsgBusProxy
} // NKikimr
