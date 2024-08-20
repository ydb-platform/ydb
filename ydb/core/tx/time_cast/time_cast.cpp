#include "time_cast.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/random_provider/random_provider.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <queue>

namespace NKikimr {

// We will unsubscribe from idle coordinators after 5 minutes
static constexpr TDuration MaxIdleCoordinatorSubscriptionTime = TDuration::Minutes(5);

ui64 TMediatorTimecastEntry::Get(ui64 tabletId) const {
    Y_UNUSED(tabletId);
    return AtomicGet(Step);
}

void TMediatorTimecastEntry::Update(ui64 step, ui64 *exemption, ui64 exsz) {
    Y_ABORT_UNLESS(exsz == 0, "exemption lists not supported yet");
    Y_UNUSED(exemption);
    Y_UNUSED(exsz);

    // Mediator time shouldn't go back while shards are running
    if (Get(0) < step) {
        AtomicSet(Step, step);
    }
}

class TMediatorTimecastProxy : public TActor<TMediatorTimecastProxy> {
    struct TEvPrivate {
        enum EEv {
            EvRetryCoordinator = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvEnd,
        };

        struct TEvRetryCoordinator : public TEventLocal<TEvRetryCoordinator, EvRetryCoordinator> {
            ui64 Coordinator;

            TEvRetryCoordinator(ui64 coordinator)
                : Coordinator(coordinator)
            {}
        };
    };

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
        std::vector<ui64> Coordinators;

        TActorId PipeClient;

        TMediator(ui32 bucketsSz)
            : BucketsSz(bucketsSz)
            , Buckets(new TMediatorBucket[bucketsSz])
        {}
    };

    struct TMediatorCoordinator {
        std::set<ui64> WaitingSteps;
        bool RetryPending = false;
        bool Subscribed = false;
    };

    struct TTabletInfo {
        ui64 MediatorTabletId;
        TMediator* Mediator;
        ui32 BucketId;
        ui32 RefCount = 0;
    };

    struct TCoordinatorSubscriber {
        TSet<ui64> Coordinators;
    };

    struct TCoordinator {
        TMediatorTimecastReadStep::TPtr ReadStep = new TMediatorTimecastReadStep;
        TActorId PipeClient;
        ui64 LastSentSeqNo = 0;
        ui64 LastConfirmedSeqNo = 0;
        ui64 LastObservedReadStep = 0;
        THashSet<TActorId> Subscribers;
        TMap<std::pair<ui64, TActorId>, ui64> SubscribeRequests; // (seqno, subscriber) -> Cookie
        TMap<std::pair<ui64, TActorId>, ui64> WaitRequests; // (step, subscriber) -> Cookie
        TMonotonic IdleStart;
    };

    THashMap<ui64, TMediator> Mediators; // mediator tablet -> info
    THashMap<ui64, TMediatorCoordinator> MediatorCoordinators; // coordinator tablet -> info
    THashMap<ui64, TTabletInfo> Tablets;

    ui64 LastSeqNo = 0;
    THashMap<ui64, TCoordinator> Coordinators;
    THashMap<TActorId, TCoordinatorSubscriber> CoordinatorSubscribers;

    TMediator& MediatorInfo(ui64 mediator, const NKikimrSubDomains::TProcessingParams &processing) {
        auto pr = Mediators.try_emplace(mediator, processing.GetTimeCastBucketsPerMediator());
        if (!pr.second) {
            Y_ABORT_UNLESS(pr.first->second.BucketsSz == processing.GetTimeCastBucketsPerMediator());
        }
        if (pr.first->second.Coordinators.empty()) {
            pr.first->second.Coordinators.assign(
                processing.GetCoordinators().begin(),
                processing.GetCoordinators().end());
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
        ResyncCoordinator(tabletId, pipeClient, ctx);

        auto it = Mediators.find(tabletId);
        if (it == Mediators.end()) {
            return;
        }

        TMediator &mediator = it->second;
        if (mediator.PipeClient == pipeClient) {
            mediator.PipeClient = TActorId();
            RegisterMediator(tabletId, mediator, ctx);
        }
    }

    void SyncCoordinator(ui64 coordinatorId, TCoordinator &coordinator, const TActorContext &ctx);
    void ResyncCoordinator(ui64 coordinatorId, const TActorId &pipeClient, const TActorContext &ctx);
    void NotifyCoordinatorWaiters(ui64 coordinatorId, TCoordinator &coordinator, const TActorContext &ctx);

    void Handle(TEvMediatorTimecast::TEvRegisterTablet::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvUnregisterTablet::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvWaitPlanStep::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvUpdate::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvRetryCoordinator::TPtr &ev, const TActorContext &ctx);

    // Client requests for readstep subscriptions
    void Handle(TEvMediatorTimecast::TEvSubscribeReadStep::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvUnsubscribeReadStep::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorTimecast::TEvWaitReadStep::TPtr &ev, const TActorContext &ctx);

    // Coordinator replies for readstep subscriptions
    void Handle(TEvTxProxy::TEvSubscribeReadStepResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProxy::TEvSubscribeReadStepUpdate::TPtr &ev, const TActorContext &ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_MEDIATOR_TIMECAST_ACTOR;
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

            HFunc(TEvMediatorTimecast::TEvSubscribeReadStep, Handle);
            HFunc(TEvMediatorTimecast::TEvUnsubscribeReadStep, Handle);
            HFunc(TEvMediatorTimecast::TEvWaitReadStep, Handle);

            HFunc(TEvTxProxy::TEvSubscribeReadStepResult, Handle);
            HFunc(TEvTxProxy::TEvSubscribeReadStepUpdate, Handle);

            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvPrivate::TEvRetryCoordinator, Handle);
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
    Y_ABORT_UNLESS(mediators.List().size());
    const ui64 mediatorTabletId = mediators.Select(tabletId);
    tabletInfo.MediatorTabletId = mediatorTabletId;

    TTimeCastBuckets buckets(processingParams);
    const ui32 bucketId = buckets.Select(tabletId);
    tabletInfo.BucketId = bucketId;

    TMediator &mediator = MediatorInfo(mediatorTabletId, processingParams);
    tabletInfo.Mediator = &mediator;

    Y_ABORT_UNLESS(bucketId < mediator.BucketsSz);
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
        Y_ABORT_UNLESS(it->second.RefCount > 0);
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
    Y_ABORT_UNLESS(it != Tablets.end(), "TEvWaitPlanStep TabletId# %" PRIu64 " is not subscribed", tabletId);

    TMediator &mediator = *it->second.Mediator;
    const ui32 bucketId = it->second.BucketId;
    auto &bucket = mediator.Buckets[bucketId];
    Y_DEBUG_ABORT_UNLESS(bucket.Entry, "TEvWaitPlanStep TabletId# %" PRIu64 " has no timecast entry, possible race", tabletId);
    if (!bucket.Entry) {
        return;
    }

    const ui64 currentStep = bucket.Entry->Get(tabletId);
    if (currentStep < planStep) {
        bucket.Waiters.emplace(ev->Sender, tabletId, planStep);
        for (ui64 coordinatorId : mediator.Coordinators) {
            TMediatorCoordinator &coordinator = MediatorCoordinators[coordinatorId];
            if (coordinator.WaitingSteps.insert(planStep).second && !coordinator.RetryPending) {
                Send(MakePipePerNodeCacheID(false),
                    new TEvPipeCache::TEvForward(
                        new TEvTxProxy::TEvRequirePlanSteps(coordinatorId, planStep),
                        coordinatorId,
                        !coordinator.Subscribed));
                coordinator.Subscribed = true;
            }
        }
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

void TMediatorTimecastProxy::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
    auto *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE EvDeliveryProblem " << msg->TabletId);
    auto it = MediatorCoordinators.find(msg->TabletId);
    if (it != MediatorCoordinators.end()) {
        Y_DEBUG_ABORT_UNLESS(!it->second.RetryPending);
        Schedule(TDuration::MilliSeconds(5), new TEvPrivate::TEvRetryCoordinator(msg->TabletId));
        it->second.RetryPending = true;
        it->second.Subscribed = false;
    }
}

void TMediatorTimecastProxy::Handle(TEvPrivate::TEvRetryCoordinator::TPtr &ev, const TActorContext &ctx) {
    auto *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE EvRetryCoordinator " << msg->Coordinator);
    auto it = MediatorCoordinators.find(msg->Coordinator);
    if (it != MediatorCoordinators.end() && it->second.RetryPending) {
        it->second.RetryPending = false;
        if (!it->second.WaitingSteps.empty()) {
            Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(
                    new TEvTxProxy::TEvRequirePlanSteps(msg->Coordinator, it->second.WaitingSteps),
                    msg->Coordinator,
                    true));
            it->second.Subscribed = true;
        }
    }
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvUpdate::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID.ToString()
        << " HANDLE "<< ev->Get()->ToString());

    const NKikimrTxMediatorTimecast::TEvUpdate &record = ev->Get()->Record;

    const ui64 mediatorTabletId = record.GetMediator();
    auto it = Mediators.find(mediatorTabletId);
    if (it != Mediators.end()) {
        auto &mediator = it->second;
        Y_ABORT_UNLESS(record.GetBucket() < mediator.BucketsSz);
        auto &bucket = mediator.Buckets[record.GetBucket()];
        const ui64 step = record.GetTimeBarrier();
        switch (bucket.Entry.RefCount()) {
            case 0:
                break;
            case 1:
                bucket.Entry.Drop();
                bucket.Waiters = { };
                break;
            default: {
                bucket.Entry->Update(step, nullptr, 0);
                THashSet<std::pair<TActorId, ui64>> processed; // a set of processed tablets
                while (!bucket.Waiters.empty()) {
                    const auto& top = bucket.Waiters.top();
                    if (step < top.PlanStep) {
                        break;
                    }
                    if (processed.insert(std::make_pair(top.Sender, top.TabletId)).second) {
                        ctx.Send(top.Sender, new TEvMediatorTimecast::TEvNotifyPlanStep(top.TabletId, step));
                    }
                    bucket.Waiters.pop();
                }
                break;
            }
        }
        for (ui64 coordinatorId : mediator.Coordinators) {
            auto it = MediatorCoordinators.find(coordinatorId);
            if (it != MediatorCoordinators.end()) {
                while (!it->second.WaitingSteps.empty() && *it->second.WaitingSteps.begin() <= step) {
                    it->second.WaitingSteps.erase(it->second.WaitingSteps.begin());
                }
            }
        }
    }
}

void TMediatorTimecastProxy::SyncCoordinator(ui64 coordinatorId, TCoordinator &coordinator, const TActorContext &ctx) {
    const ui64 seqNo = LastSeqNo;

    if (!coordinator.PipeClient) {
        ui64 maxDelay = 100 + TAppData::RandomProvider->GenRand64() % 50;
        auto retryPolicy = NTabletPipe::TClientRetryPolicy{
            .RetryLimitCount = 6 /* delays: 0, 10, 20, 40, 80, 100-150 */,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(maxDelay),
        };
        coordinator.PipeClient = ctx.RegisterWithSameMailbox(
            NTabletPipe::CreateClient(ctx.SelfID, coordinatorId, retryPolicy));
    }

    coordinator.LastSentSeqNo = seqNo;
    NTabletPipe::SendData(ctx, coordinator.PipeClient, new TEvTxProxy::TEvSubscribeReadStep(coordinatorId, seqNo));
}

void TMediatorTimecastProxy::ResyncCoordinator(ui64 coordinatorId, const TActorId &pipeClient, const TActorContext &ctx) {
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }

    auto &coordinator = itCoordinator->second;
    if (coordinator.PipeClient != pipeClient) {
        return;
    }

    coordinator.PipeClient = TActorId();
    if (coordinator.Subscribers.empty()) {
        // Just forget disconnected idle coordinators
        Coordinators.erase(itCoordinator);
        return;
    }

    ++LastSeqNo;
    SyncCoordinator(coordinatorId, coordinator, ctx);
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvSubscribeReadStep::TPtr &ev, const TActorContext &ctx) {
    const auto *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());

    const ui64 coordinatorId = msg->CoordinatorId;
    auto &subscriber = CoordinatorSubscribers[ev->Sender];
    auto &coordinator = Coordinators[coordinatorId];
    subscriber.Coordinators.insert(coordinatorId);
    coordinator.Subscribers.insert(ev->Sender);
    ui64 seqNo = ++LastSeqNo;
    auto key = std::make_pair(seqNo, ev->Sender);
    coordinator.SubscribeRequests[key] = ev->Cookie;
    SyncCoordinator(coordinatorId, coordinator, ctx);
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvUnsubscribeReadStep::TPtr &ev, const TActorContext &ctx) {
    const auto *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());

    auto &subscriber = CoordinatorSubscribers[ev->Sender];
    if (msg->CoordinatorId == 0) {
        // Unsubscribe from all coordinators
        for (ui64 coordinatorId : subscriber.Coordinators) {
            auto &coordinator = Coordinators[coordinatorId];
            coordinator.Subscribers.erase(ev->Sender);
            if (coordinator.Subscribers.empty()) {
                coordinator.IdleStart = ctx.Monotonic();
            }
        }
        subscriber.Coordinators.clear();
    } else if (subscriber.Coordinators.contains(msg->CoordinatorId)) {
        // Unsubscribe from specific coordinator
        auto &coordinator = Coordinators[msg->CoordinatorId];
        coordinator.Subscribers.erase(ev->Sender);
        if (coordinator.Subscribers.empty()) {
            coordinator.IdleStart = ctx.Monotonic();
        }
        subscriber.Coordinators.erase(msg->CoordinatorId);
    }

    if (subscriber.Coordinators.empty()) {
        // Don't track unnecessary subscribers
        CoordinatorSubscribers.erase(ev->Sender);
    }
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvWaitReadStep::TPtr &ev, const TActorContext &ctx) {
    const auto *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());

    const ui64 coordinatorId = msg->CoordinatorId;
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }
    auto &coordinator = itCoordinator->second;

    if (!coordinator.Subscribers.contains(ev->Sender)) {
        return;
    }

    const ui64 step = coordinator.LastObservedReadStep;
    const ui64 waitReadStep = msg->ReadStep;
    if (waitReadStep <= step) {
        ctx.Send(ev->Sender,
            new TEvMediatorTimecast::TEvNotifyReadStep(coordinatorId, step),
            0, ev->Cookie);
        return;
    }

    auto key = std::make_pair(waitReadStep, ev->Sender);
    coordinator.WaitRequests[key] = ev->Cookie;
}

void TMediatorTimecastProxy::Handle(TEvTxProxy::TEvSubscribeReadStepResult::TPtr &ev, const TActorContext &ctx) {
    const auto &record = ev->Get()->Record;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE TEvSubscribeReadStepResult " << record.ShortDebugString());

    const ui64 coordinatorId = record.GetCoordinatorID();
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }
    auto &coordinator = itCoordinator->second;

    bool updated = false;
    const ui64 nextReadStep = record.GetNextAcquireStep();
    if (coordinator.LastObservedReadStep < nextReadStep) {
        coordinator.LastObservedReadStep = nextReadStep;
        coordinator.ReadStep->Update(nextReadStep);
        updated = true;
    }

    const ui64 seqNo = record.GetSeqNo();
    if (coordinator.LastConfirmedSeqNo < seqNo) {
        coordinator.LastConfirmedSeqNo = seqNo;

        const ui64 lastReadStep = record.GetLastAcquireStep();
        for (auto it = coordinator.SubscribeRequests.begin(); it != coordinator.SubscribeRequests.end();) {
            const ui64 waitSeqNo = it->first.first;
            if (seqNo < waitSeqNo) {
                break;
            }
            const TActorId subscriberId = it->first.second;
            const ui64 cookie = it->second;
            if (coordinator.Subscribers.contains(subscriberId)) {
                ctx.Send(subscriberId,
                    new TEvMediatorTimecast::TEvSubscribeReadStepResult(
                        coordinatorId,
                        lastReadStep,
                        nextReadStep,
                        coordinator.ReadStep),
                    0, cookie);
            }
            it = coordinator.SubscribeRequests.erase(it);
        }
    }

    if (updated) {
        NotifyCoordinatorWaiters(coordinatorId, coordinator, ctx);
    }
}

void TMediatorTimecastProxy::Handle(TEvTxProxy::TEvSubscribeReadStepUpdate::TPtr &ev, const TActorContext &ctx) {
    const auto &record = ev->Get()->Record;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE TEvSubscribeReadStepUpdate " << record.ShortDebugString());

    const ui64 coordinatorId = record.GetCoordinatorID();
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }
    auto &coordinator = itCoordinator->second;

    const ui64 nextReadStep = record.GetNextAcquireStep();
    if (coordinator.LastObservedReadStep < nextReadStep) {
        coordinator.LastObservedReadStep = nextReadStep;
        coordinator.ReadStep->Update(nextReadStep);

        NotifyCoordinatorWaiters(coordinatorId, coordinator, ctx);
    }

    // Unsubscribe from idle coordinators
    if (coordinator.Subscribers.empty() && (ctx.Monotonic() - coordinator.IdleStart) >= MaxIdleCoordinatorSubscriptionTime) {
        if (coordinator.PipeClient) {
            NTabletPipe::CloseClient(ctx, coordinator.PipeClient);
            coordinator.PipeClient = TActorId();
        }

        Coordinators.erase(itCoordinator);
    }
}

void TMediatorTimecastProxy::NotifyCoordinatorWaiters(ui64 coordinatorId, TCoordinator &coordinator, const TActorContext &ctx) {
    const ui64 step = coordinator.LastObservedReadStep;
    for (auto it = coordinator.WaitRequests.begin(); it != coordinator.WaitRequests.end();) {
        const ui64 waitStep = it->first.first;
        if (step < waitStep) {
            break;
        }
        const TActorId subscriberId = it->first.second;
        const ui64 cookie = it->second;
        if (coordinator.Subscribers.contains(subscriberId)) {
            ctx.Send(subscriberId,
                new TEvMediatorTimecast::TEvNotifyReadStep(coordinatorId, step),
                0, cookie);
        }
        it = coordinator.WaitRequests.erase(it);
    }
}



IActor* CreateMediatorTimecastProxy() {
    return new TMediatorTimecastProxy();
}

}
