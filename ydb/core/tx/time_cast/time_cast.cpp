#include "time_cast.h"

#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <queue>

namespace NKikimr {

ui64 TMediatorTimecastEntry::Get(ui64 tabletId) const {
    Y_UNUSED(tabletId);
    return AtomicGet(Step);
}

void TMediatorTimecastEntry::Update(ui64 step, ui64 *exemption, ui64 exsz) {
    Y_VERIFY(exsz == 0, "exemption lists not supported yet");
    Y_UNUSED(exemption);
    Y_UNUSED(exsz);

    AtomicSet(Step, step);
}

class TMediatorTimecastProxy : public TActor<TMediatorTimecastProxy> {
    struct TWaiter {
        TActorId Sender;
        ui64 TabletId;
        ui64 PlanStep;

        TWaiter() = default;
        TWaiter(const TActorId& sender, ui64 tabletId, ui64 planStep)
            : Sender(sender)
            , TabletId(tabletId)
            , PlanStep(planStep)
        { }

        // An inverse based on (PlanStep, TabletId) tuple
        inline bool operator<(const TWaiter& rhs) const {
            return rhs.PlanStep < PlanStep ||
                (rhs.PlanStep == PlanStep && rhs.TabletId < TabletId);
        }
    };

    struct TMediatorBucket {
        TIntrusivePtr<TMediatorTimecastEntry> Entry;
        std::priority_queue<TWaiter> Waiters;
    };

    struct TMediator {
        const ui32 BucketsSz;
        TArrayHolder<TMediatorBucket> Buckets;

        TActorId PipeClient;

        TMediator(ui32 bucketsSz)
            : BucketsSz(bucketsSz)
            , Buckets(new TMediatorBucket[bucketsSz])
        {}
    };

    struct TTabletInfo {
        ui64 MediatorTabletId;
        TMediator* Mediator;
        ui32 BucketId;
        ui32 RefCount = 0;
    };

    THashMap<ui64, TMediator> Mediators; // mediator tablet -> info
    THashMap<ui64, TTabletInfo> Tablets;

    TMediator& MediatorInfo(ui64 mediator, const NKikimrSubDomains::TProcessingParams &processing) {
        auto pr = Mediators.try_emplace(mediator, processing.GetTimeCastBucketsPerMediator());
        if (!pr.second) {
            Y_VERIFY(pr.first->second.BucketsSz == processing.GetTimeCastBucketsPerMediator());
        }
        return pr.first->second;
    }

    void RegisterMediator(ui64 mediatorTabletId, TMediator &mediator, const TActorContext &ctx) {
        TAutoPtr<TEvMediatorTimecast::TEvWatch> ev(new TEvMediatorTimecast::TEvWatch());
        for (ui32 bucketId = 0, e = mediator.BucketsSz; bucketId != e; ++bucketId) {
            auto &x = mediator.Buckets[bucketId];
            if (!x.Entry)
                continue;
            if (x.Entry.RefCount() == 1) {
                x.Entry.Drop();
                x.Waiters = { };
                continue;
            }

            ev->Record.AddBucket(bucketId);
        }

        if (ev->Record.BucketSize()) {
            mediator.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, mediatorTabletId));
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
                << " SEND to# " << mediatorTabletId << " Mediator " << ev->ToString());
            NTabletPipe::SendData(ctx, mediator.PipeClient, ev.Release());
        }
    }

    void SubscribeBucket(ui64 mediatorTabletId, TMediator &mediator, ui32 bucketId, const TActorContext &ctx) {
        if (mediator.PipeClient) {
            TAutoPtr<TEvMediatorTimecast::TEvWatch> ev(new TEvMediatorTimecast::TEvWatch(bucketId));
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
                << " SEND to# " << mediatorTabletId << " Mediator " << ev->ToString());
            NTabletPipe::SendData(ctx, mediator.PipeClient, ev.Release());
        } else {
            RegisterMediator(mediatorTabletId, mediator, ctx);
        }
    }

    void TryResync(const TActorId &pipeClient, ui64 tabletId, const TActorContext &ctx) {
        for (auto &xpair : Mediators) {
            const ui64 mediatorTabletId = xpair.first;
            TMediator &mediator = xpair.second;

            if (mediator.PipeClient == pipeClient) {
                Y_VERIFY(tabletId == mediatorTabletId);
                mediator.PipeClient = TActorId();
                RegisterMediator(mediatorTabletId, mediator, ctx);
                return;
            }
        }
    }

    void Handle(TEvMediatorTimecast::TEvRegisterTablet::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvUnregisterTablet::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvWaitPlanStep::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvUpdate::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
 
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_MEDIATOR_ACTOR;
    } 
 
    TMediatorTimecastProxy()
        : TActor(&TThis::StateFunc)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvMediatorTimecast::TEvRegisterTablet, Handle);
            HFunc(TEvMediatorTimecast::TEvUnregisterTablet, Handle);
            HFunc(TEvMediatorTimecast::TEvWaitPlanStep, Handle);
            HFunc(TEvMediatorTimecast::TEvUpdate, Handle);

            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }
};

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvRegisterTablet::TPtr &ev, const TActorContext &ctx) {
    const TEvMediatorTimecast::TEvRegisterTablet *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE " << msg->ToString());
    const ui64 tabletId = msg->TabletId;
    const NKikimrSubDomains::TProcessingParams &processingParams = msg->ProcessingParams;

    auto& tabletInfo = Tablets[tabletId];

    TMediators mediators(processingParams);
    Y_VERIFY(mediators.List().size());
    const ui64 mediatorTabletId = mediators.Select(tabletId);
    tabletInfo.MediatorTabletId = mediatorTabletId;

    TTimeCastBuckets buckets(processingParams);
    const ui32 bucketId = buckets.Select(tabletId);
    tabletInfo.BucketId = bucketId;

    TMediator &mediator = MediatorInfo(mediatorTabletId, processingParams);
    tabletInfo.Mediator = &mediator;

    Y_VERIFY(bucketId < mediator.BucketsSz);
    auto &bucket = mediator.Buckets[bucketId];
    if (!bucket.Entry)
        bucket.Entry = new TMediatorTimecastEntry();

    ++tabletInfo.RefCount;

    TAutoPtr<TEvMediatorTimecast::TEvRegisterTabletResult> result(
        new TEvMediatorTimecast::TEvRegisterTabletResult(tabletId, bucket.Entry));
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " SEND to# " << ev->Sender.ToString() << " Sender " << result->ToString());
    ctx.Send(ev->Sender, result.Release());

    SubscribeBucket(mediatorTabletId, mediator, bucketId, ctx);
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvUnregisterTablet::TPtr &ev, const TActorContext &ctx) {
    const auto *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE " << msg->ToString());
    const ui64 tabletId = msg->TabletId;

    auto it = Tablets.find(tabletId);
    if (it != Tablets.end()) {
        Y_VERIFY(it->second.RefCount > 0);
        if (0 == --it->second.RefCount) {
            // Note: buckets are unsubscribed lazily when entry refcount reaches zero on reconnect
            Tablets.erase(it);
        }
    }
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvWaitPlanStep::TPtr &ev, const TActorContext &ctx) {
    const auto *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE " << msg->ToString());
    const ui64 tabletId = msg->TabletId;
    const ui64 planStep = msg->PlanStep;

    auto it = Tablets.find(tabletId);
    Y_VERIFY(it != Tablets.end(), "TEvWaitPlanStep TabletId# %" PRIu64 " is not subscribed", tabletId);

    TMediator &mediator = *it->second.Mediator;
    const ui32 bucketId = it->second.BucketId;
    auto &bucket = mediator.Buckets[bucketId];
    Y_VERIFY_DEBUG(bucket.Entry, "TEvWaitPlanStep TabletId# %" PRIu64 " has no timecast entry, possible race", tabletId);
    if (!bucket.Entry) {
        return;
    }

    const ui64 currentStep = bucket.Entry->Get(tabletId);
    if (currentStep < planStep) {
        bucket.Waiters.emplace(ev->Sender, tabletId, planStep);
        return;
    }

    // We don't need to wait when step is not in the future
    ctx.Send(ev->Sender, new TEvMediatorTimecast::TEvNotifyPlanStep(tabletId, currentStep));
}

void TMediatorTimecastProxy::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE EvClientConnected");
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        TryResync(msg->ClientId, msg->TabletId, ctx);
    }
}

void TMediatorTimecastProxy::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE EvClientDestroyed");
    TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
    TryResync(msg->ClientId, msg->TabletId, ctx);
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvUpdate::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE "<< ev->Get()->ToString());

    const NKikimrTxMediatorTimecast::TEvUpdate &record = ev->Get()->Record;
    Y_VERIFY(record.ExemptionSize() == 0, "exemption lists are not supported yet");

    const ui64 mediatorTabletId = record.GetMediator();
    auto it = Mediators.find(mediatorTabletId);
    if (it != Mediators.end()) {
        auto &mediator = it->second;
        Y_VERIFY(record.GetBucket() < mediator.BucketsSz);
        auto &bucket = mediator.Buckets[record.GetBucket()];
        switch (bucket.Entry.RefCount()) {
            case 0:
                break;
            case 1:
                bucket.Entry.Drop();
                bucket.Waiters = { };
                break;
            default: {
                const ui64 step = record.GetTimeBarrier();
                bucket.Entry->Update(step, nullptr, 0);
                THashSet<ui64> processed; // a set of processed tablets
                while (!bucket.Waiters.empty()) {
                    const auto& top = bucket.Waiters.top();
                    if (step < top.PlanStep) {
                        break;
                    }
                    if (processed.insert(top.TabletId).second) {
                        ctx.Send(top.Sender, new TEvMediatorTimecast::TEvNotifyPlanStep(top.TabletId, step));
                    }
                    bucket.Waiters.pop();
                }
                break;
            }
        }
    }
}

IActor* CreateMediatorTimecastProxy() {
    return new TMediatorTimecastProxy();
}

}
