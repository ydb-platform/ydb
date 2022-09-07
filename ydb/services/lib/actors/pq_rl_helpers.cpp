#include "pq_rl_helpers.h"

#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NGRpcProxy::V1 {

TRlHelpers::TRlHelpers(NGRpcService::IRequestCtxBase* reqCtx, ui64 blockSize, const TDuration& waitDuration)
    : Request(reqCtx)
    , BlockSize(blockSize)
    , WaitDuration(waitDuration)
    , PayloadBytes(0)
{
    Y_VERIFY(Request);
}

bool TRlHelpers::IsQuotaRequired() const {
    Y_VERIFY(MeteringMode.Defined());
    return MeteringMode == NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS && Request->GetRlPath();
}

bool TRlHelpers::MaybeRequestQuota(ui64 amount, EWakeupTag tag, const TActorContext& ctx) {
    if (RlActor) {
        return false;
    }

    const auto selfId = ctx.SelfID;
    const auto as = ctx.ActorSystem();

    auto onSendAllowed = [selfId, as, tag]() {
        as->Send(selfId, new TEvents::TEvWakeup(tag));
    };

    auto onSendTimeout = [selfId, as]() {
        as->Send(selfId, new TEvents::TEvWakeup(EWakeupTag::RlNoResource));
    };

    RlActor = NRpcService::RateLimiterAcquireUseSameMailbox(
        *Request, amount, WaitDuration,
        std::move(onSendAllowed), std::move(onSendTimeout), ctx);
    return true;
}

void TRlHelpers::OnWakeup(EWakeupTag tag) {
    switch (tag) {
        case EWakeupTag::RlInit:
        case EWakeupTag::RlAllowed:
        case EWakeupTag::RlNoResource:
            RlActor = {};
            break;
        default:
            break;
    }
}

const TMaybe<NKikimrPQ::TPQTabletConfig::EMeteringMode>& TRlHelpers::GetMeteringMode() const {
    return MeteringMode;
}

void TRlHelpers::SetMeteringMode(NKikimrPQ::TPQTabletConfig::EMeteringMode mode) {
    MeteringMode = mode;
}

ui64 TRlHelpers::CalcRuConsumption(ui64 payloadSize) {
    if (!IsQuotaRequired()) {
        return 0;
    }

    const ui64 remainder = BlockSize - (PayloadBytes % BlockSize);
    PayloadBytes += payloadSize;

    if (payloadSize > remainder) {
        return Max<ui64>(1, (payloadSize - remainder) / BlockSize);
    }

    return 0;
}

}
