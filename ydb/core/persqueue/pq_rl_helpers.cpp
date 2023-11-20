#include "pq_rl_helpers.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_board/subscriber.h>

namespace NKikimr::NPQ {

TRlHelpers::TRlHelpers(const std::optional<TString>& topicPath, const TRlContext ctx, ui64 blockSize, bool subscribe, const TDuration& waitDuration)
    : TStreamRequestUnitsCalculator(blockSize)
    , TopicPath(topicPath)
    , Ctx(ctx)
    , Subscribe(subscribe)
    , WaitDuration(waitDuration)
{
    Y_DEBUG_ABORT_UNLESS(!subscribe || (topicPath && subscribe));
}

void TRlHelpers::Bootstrap(const TActorId selfId, const NActors::TActorContext& ctx) {
    if (Subscribe) {
        SubscriberId = ctx.Register(CreateSchemeBoardSubscriber(selfId, *TopicPath));
    }
}

void TRlHelpers::PassAway(const TActorId selfId) {
    TActorIdentity id(selfId);
    if (RlActor) {
        id.Send(RlActor, new TEvents::TEvPoison());
    }
    if (SubscriberId) {
        id.Send(SubscriberId, new TEvents::TEvPoison());
    }
}

bool TRlHelpers::IsQuotaInflight() const {
    return !!RlActor;
}

bool TRlHelpers::IsQuotaRequired() const {
    Y_ABORT_UNLESS(MeteringMode.Defined());
    return MeteringMode == NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS && Ctx;
}

void TRlHelpers::RequestInitQuota(ui64 amount, const TActorContext& ctx) {
    RequestQuota(amount, EWakeupTag::RlInit, EWakeupTag::RlInitNoResource, ctx);
}

void TRlHelpers::RequestDataQuota(ui64 amount, const TActorContext& ctx) {
    RequestQuota(amount, EWakeupTag::RlAllowed, EWakeupTag::RlNoResource, ctx);
}

bool TRlHelpers::MaybeRequestQuota(ui64 amount, EWakeupTag tag, const TActorContext& ctx) {
    if (IsQuotaInflight()) {
        return false;
    }

    RequestQuota(amount, tag, EWakeupTag::RlNoResource, ctx);
    return true;
}

void TRlHelpers::RequestQuota(ui64 amount, EWakeupTag success, EWakeupTag timeout, const TActorContext& ctx) {
    const auto selfId = ctx.SelfID;
    const auto as = ctx.ActorSystem();

    auto onSendAllowed = [selfId, as, success]() {
        as->Send(selfId, new TEvents::TEvWakeup(success));
    };

    auto onSendTimeout = [selfId, as, timeout]() {
        as->Send(selfId, new TEvents::TEvWakeup(timeout));
    };

    RlActor = NRpcService::RateLimiterAcquireUseSameMailbox(
        Ctx.GetPath(), amount, WaitDuration,
        std::move(onSendAllowed), std::move(onSendTimeout), ctx);
}

void TRlHelpers::OnWakeup(EWakeupTag tag) {
    switch (tag) {
        case EWakeupTag::RlInit:
        case EWakeupTag::RlInitNoResource:
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

    return CalcConsumption(payloadSize);
}

void TRlHelpers::Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
    auto* result = ev->Get();
    auto status = result->DescribeSchemeResult.GetStatus();
    if (status != NKikimrScheme::EStatus::StatusSuccess) {
        //LOG_ERROR("Describe database '" << Database << "' error: " << status);
        return;
    }

    MeteringMode = result->DescribeSchemeResult.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig().GetMeteringMode();
}

}
