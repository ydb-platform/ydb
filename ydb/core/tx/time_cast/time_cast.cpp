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

ui64 TMediatorTimecastSharedEntry::Get() const noexcept {
    return Step.load(std::memory_order_acquire);
}

void TMediatorTimecastSharedEntry::Set(ui64 step) noexcept {
    ui64 current = Step.load(std::memory_order_relaxed);
    Step.store(Max(current, step), std::memory_order_release);
}

TMediatorTimecastEntry::TMediatorTimecastEntry(
        const TMediatorTimecastSharedEntry::TPtr& safeStep,
        const TMediatorTimecastSharedEntry::TPtr& latestStep) noexcept
    : SafeStep(safeStep)
    , LatestStep(latestStep)
{
    Y_ABORT_UNLESS(SafeStep);
    Y_ABORT_UNLESS(LatestStep);
}

TMediatorTimecastEntry::~TMediatorTimecastEntry() noexcept {
    // nothing
}

ui64 TMediatorTimecastEntry::Get(ui64 tabletId) const noexcept {
    Y_UNUSED(tabletId);
    ui64 latest = LatestStep->Get();
    ui64 frozen = FrozenStep.load(std::memory_order_relaxed);
    ui64 safe = SafeStep->Get();
    return Max(Min(latest, frozen), safe);
}

ui64 TMediatorTimecastEntry::GetFrozenStep() const noexcept {
    return FrozenStep.load(std::memory_order_relaxed);
}

void TMediatorTimecastEntry::SetFrozenStep(ui64 step) noexcept {
    FrozenStep.store(step, std::memory_order_relaxed);
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

        struct TCompare {
            constexpr bool operator()(const TWaiter& a, const TWaiter& b) const noexcept {
                return a.PlanStep < b.PlanStep;
            }
        };

        struct TInverseCompare {
            constexpr bool operator()(const TWaiter& a, const TWaiter& b) const noexcept {
                return b.PlanStep < a.PlanStep;
            }
        };
    };

    using TWaiterSet = std::multiset<TWaiter, TWaiter::TCompare>;
    using TWaiterQueue = std::priority_queue<TWaiter, std::vector<TWaiter>, TWaiter::TInverseCompare>;

    struct TTabletWaiter {
        ui64 PlanStep;
        ui64 TabletId;

        TTabletWaiter() = default;
        TTabletWaiter(ui64 planStep, ui64 tabletId)
            : PlanStep(planStep)
            , TabletId(tabletId)
        {}

        inline bool operator<(const TTabletWaiter& rhs) const noexcept {
            return PlanStep < rhs.PlanStep ||
                (PlanStep == rhs.PlanStep && TabletId < rhs.TabletId);
        }
    };

    using TTabletWaiterSet = std::set<TTabletWaiter>;

    struct TMediator;
    struct TMediatorBucket;

    struct TTabletInfo : public TIntrusiveListItem<TTabletInfo> {
        const ui64 TabletId;
        TMediator* Mediator = nullptr;
        ui32 BucketId = 0;
        ui32 RefCount = 0;
        TMediatorTimecastEntry::TPtr Entry;
        TWaiterSet Waiters;
        ui64 SubscriptionId = 0;
        ui64 FrozenStep = Max<ui64>();
        ui64 MinStep = 0;
        bool Synced = false;

        explicit TTabletInfo(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TMediatorBucket {
        const TMediatorTimecastSharedEntry::TPtr SafeStep = new TMediatorTimecastSharedEntry;
        const TMediatorTimecastSharedEntry::TPtr LatestStep = new TMediatorTimecastSharedEntry;
        THashSet<ui64> Tablets;
        TIntrusiveList<TTabletInfo> PendingSubscribe;
        TIntrusiveList<TTabletInfo> PendingUpdate;
        TWaiterQueue Waiters;
        TTabletWaiterSet TabletWaiters;
        ui64 SubscriptionId = 0;
        bool WatchSent = false;
        bool WatchSynced = false;

        bool AddNewTablet(ui64 tabletId) {
            return Tablets.insert(tabletId).second;
        }
    };

    struct TMediator {
        const ui64 TabletId;
        const ui32 BucketsSz;
        TArrayHolder<TMediatorBucket> Buckets;
        std::vector<ui64> Coordinators;

        TActorId PipeClient;

        TMediator(ui64 tabletId, ui32 bucketsSz)
            : TabletId(tabletId)
            , BucketsSz(bucketsSz)
            , Buckets(new TMediatorBucket[bucketsSz])
        {}
    };

    struct TMediatorCoordinator {
        std::set<ui64> WaitingSteps;
        bool RetryPending = false;
        bool Subscribed = false;
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
    ui64 NextWatchSubscriptionId = 1;

    TMediator& MediatorInfo(ui64 mediator, const NKikimrSubDomains::TProcessingParams& processing) {
        auto it = Mediators.find(mediator);
        if (it == Mediators.end()) {
            auto res = Mediators.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(mediator),
                std::forward_as_tuple(mediator, processing.GetTimeCastBucketsPerMediator()));
            it = res.first;
        } else {
            Y_ABORT_UNLESS(it->second.BucketsSz == processing.GetTimeCastBucketsPerMediator());
        }

        // Note: some older tablets may have an empty list of coordinators
        // We fill the list of coordinators the first time we observe a non-empty list
        if (it->second.Coordinators.empty()) {
            it->second.Coordinators.assign(
                processing.GetCoordinators().begin(),
                processing.GetCoordinators().end());
        }

        return it->second;
    }

    const TActorId& MediatorPipe(TMediator& mediator, const TActorContext& ctx) {
        if (!mediator.PipeClient) {
            mediator.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, mediator.TabletId));
        }
        return mediator.PipeClient;
    }

    void SendGranularWatch(TMediator& mediator, ui32 bucketId, const TActorContext& ctx) {
        auto& bucket = mediator.Buckets[bucketId];

        while (!bucket.PendingSubscribe.Empty()) {
            bucket.PendingSubscribe.PopFront();
        }
        while (!bucket.PendingUpdate.Empty()) {
            bucket.PendingUpdate.PopFront();
        }

        if (!AppData(ctx)->FeatureFlags.GetEnableGranularTimecast()) {
            bucket.SubscriptionId = Max<ui64>();
            return;
        }

        bucket.SubscriptionId = NextWatchSubscriptionId++;
        auto req = std::make_unique<TEvMediatorTimecast::TEvGranularWatch>(bucketId, bucket.SubscriptionId);
        req->Record.SetMinStep(bucket.LatestStep->Get());

        for (ui64 tabletId : bucket.Tablets) {
            auto& tabletInfo = Tablets.at(tabletId);
            Y_ABORT_UNLESS(tabletInfo.Mediator == &mediator);
            Y_ABORT_UNLESS(tabletInfo.BucketId == bucketId);
            tabletInfo.SubscriptionId = bucket.SubscriptionId;
            bucket.PendingSubscribe.PushBack(&tabletInfo);
            tabletInfo.FrozenStep = Max<ui64>(); // will be unfrozen unless reported as frozen
            tabletInfo.MinStep = tabletInfo.Entry->Get(); // will not freeze below this step
            tabletInfo.Synced = false;
            req->Record.AddTablets(tabletId);
        }

        const auto& client = MediatorPipe(mediator, ctx);
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
            << " SEND to Mediator# " << mediator.TabletId << " " << req->ToString());
        NTabletPipe::SendData(ctx, client, req.release());
    }

    void SendGranularWatchAdd(TMediator& mediator, ui32 bucketId, ui64 tabletId, const TActorContext& ctx) {
        auto& bucket = mediator.Buckets[bucketId];

        Y_ABORT_UNLESS(bucket.Tablets.contains(tabletId));
        auto& tabletInfo = Tablets.at(tabletId);
        Y_ABORT_UNLESS(tabletInfo.Mediator == &mediator);
        Y_ABORT_UNLESS(tabletInfo.BucketId == bucketId);

        if (bucket.SubscriptionId == Max<ui64>()) {
            tabletInfo.SubscriptionId = Max<ui64>();
            return;
        }

        tabletInfo.SubscriptionId = NextWatchSubscriptionId++;
        bucket.PendingSubscribe.PushBack(&tabletInfo);
        tabletInfo.FrozenStep = Max<ui64>(); // will be unfrozen unless reported as frozen
        tabletInfo.MinStep = tabletInfo.Entry->Get(); // will not freeze below this step
        tabletInfo.Synced = false;

        auto req = std::make_unique<TEvMediatorTimecast::TEvGranularWatchModify>(bucketId, tabletInfo.SubscriptionId);
        req->Record.AddAddTablets(tabletId);

        const auto& client = MediatorPipe(mediator, ctx);
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
            << " SEND to Mediator# " << mediator.TabletId << " " << req->ToString());
        NTabletPipe::SendData(ctx, client, req.release());
    }

    void SendGranularWatchRemove(TMediator& mediator, ui32 bucketId, ui64 tabletId, const TActorContext& ctx) {
        auto& bucket = mediator.Buckets[bucketId];

        Y_ABORT_UNLESS(bucket.Tablets.contains(tabletId));
        auto& tabletInfo = Tablets.at(tabletId);
        Y_ABORT_UNLESS(tabletInfo.Mediator == &mediator);
        Y_ABORT_UNLESS(tabletInfo.BucketId == bucketId);
        tabletInfo.SubscriptionId = Max<ui64>();
        tabletInfo.Synced = false;
        bucket.PendingSubscribe.Remove(&tabletInfo);
        bucket.PendingUpdate.Remove(&tabletInfo);
        bucket.Tablets.erase(tabletId);

        if (bucket.SubscriptionId == Max<ui64>()) {
            return;
        }

        auto req = std::make_unique<TEvMediatorTimecast::TEvGranularWatchModify>(bucketId, NextWatchSubscriptionId++);
        req->Record.AddRemoveTablets(tabletId);

        const auto& client = MediatorPipe(mediator, ctx);
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
            << " SEND to Mediator# " << mediator.TabletId << " " << req->ToString());
        NTabletPipe::SendData(ctx, client, req.release());
    }

    void RegisterMediator(TMediator& mediator, const TActorContext& ctx) {
        auto req = std::make_unique<TEvMediatorTimecast::TEvWatch>();

        for (ui32 bucketId = 0, e = mediator.BucketsSz; bucketId != e; ++bucketId) {
            auto& bucket = mediator.Buckets[bucketId];
            if (bucket.Tablets.empty()) {
                // There are no tablets interested in this bucket, avoid unnecessary watches
                bucket.WatchSent = false;
                bucket.Waiters = { };
                continue;
            }

            SendGranularWatch(mediator, bucketId, ctx);

            req->Record.AddBucket(bucketId);
            bucket.WatchSent = true;
            bucket.WatchSynced = false;
        }

        if (req->Record.BucketSize()) {
            const auto& client = MediatorPipe(mediator, ctx);
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
                << " SEND to Mediator# " << mediator.TabletId << " " << req->ToString());
            NTabletPipe::SendData(ctx, client, req.release());
        }
    }

    void SubscribeBucket(TMediator& mediator, ui32 bucketId, ui64 tabletId, const TActorContext& ctx) {
        auto& bucket = mediator.Buckets[bucketId];
        Y_ABORT_UNLESS(bucket.Tablets.contains(tabletId));
        if (bucket.WatchSent) {
            SendGranularWatchAdd(mediator, bucketId, tabletId, ctx);
        } else {
            SendGranularWatch(mediator, bucketId, ctx);

            auto req = std::make_unique<TEvMediatorTimecast::TEvWatch>(bucketId);

            const auto& client = MediatorPipe(mediator, ctx);
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
                << " SEND to Mediator# " << mediator.TabletId << " " << req->ToString());
            NTabletPipe::SendData(ctx, client, req.release());

            bucket.WatchSent = true;
        }
    }

    void TryResync(const TActorId& pipeClient, ui64 tabletId, const TActorContext& ctx) {
        ResyncCoordinator(tabletId, pipeClient, ctx);

        auto it = Mediators.find(tabletId);
        if (it == Mediators.end()) {
            return;
        }

        TMediator& mediator = it->second;
        if (mediator.PipeClient == pipeClient) {
            mediator.PipeClient = TActorId();
            RegisterMediator(mediator, ctx);
        }
    }

    void SyncCoordinator(ui64 coordinatorId, TCoordinator& coordinator, const TActorContext& ctx);
    void ResyncCoordinator(ui64 coordinatorId, const TActorId& pipeClient, const TActorContext& ctx);
    void NotifyCoordinatorWaiters(ui64 coordinatorId, TCoordinator& coordinator, const TActorContext& ctx);

    void Handle(TEvMediatorTimecast::TEvRegisterTablet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvUnregisterTablet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvWaitPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvUpdate::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvGranularUpdate::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRetryCoordinator::TPtr& ev, const TActorContext& ctx);

    // Client requests for readstep subscriptions
    void Handle(TEvMediatorTimecast::TEvSubscribeReadStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvUnsubscribeReadStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvWaitReadStep::TPtr& ev, const TActorContext& ctx);

    // Coordinator replies for readstep subscriptions
    void Handle(TEvTxProxy::TEvSubscribeReadStepResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProxy::TEvSubscribeReadStepUpdate::TPtr& ev, const TActorContext& ctx);

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
            HFunc(TEvMediatorTimecast::TEvGranularUpdate, Handle);

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

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvRegisterTablet::TPtr& ev, const TActorContext& ctx) {
    const TEvMediatorTimecast::TEvRegisterTablet* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());
    const ui64 tabletId = msg->TabletId;
    const NKikimrSubDomains::TProcessingParams& processingParams = msg->ProcessingParams;

    auto it = Tablets.find(tabletId);
    if (it == Tablets.end()) {
        auto res = Tablets.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(tabletId),
            std::forward_as_tuple(tabletId));
        it = res.first;
    }
    auto& tabletInfo = it->second;

    TMediators mediators(processingParams);
    Y_ABORT_UNLESS(mediators.List().size());
    const ui64 mediatorTabletId = mediators.Select(tabletId);

    TTimeCastBuckets buckets(processingParams);
    const ui32 bucketId = buckets.Select(tabletId);

    TMediator& mediator = MediatorInfo(mediatorTabletId, processingParams);

    Y_ABORT_UNLESS(bucketId < mediator.BucketsSz);
    auto& bucket = mediator.Buckets[bucketId];

    if (tabletInfo.Mediator && (tabletInfo.Mediator != &mediator || tabletInfo.BucketId != bucketId)) {
        // Tablet unexpectedly changed their mediator/bucket
        SendGranularWatchRemove(*tabletInfo.Mediator, tabletInfo.BucketId, tabletId, ctx);
        tabletInfo.Mediator = nullptr;
        tabletInfo.Entry.Reset();
    }

    tabletInfo.Mediator = &mediator;
    tabletInfo.BucketId = bucketId;
    if (!tabletInfo.Entry) {
        tabletInfo.Entry = new TMediatorTimecastEntry(bucket.SafeStep, bucket.LatestStep);
    }

    ++tabletInfo.RefCount;
    if (bucket.AddNewTablet(tabletId)) {
        // New tablet for this bucket, make sure we subscribe
        SubscribeBucket(mediator, bucketId, tabletId, ctx);
    }

    TAutoPtr<TEvMediatorTimecast::TEvRegisterTabletResult> result(
        new TEvMediatorTimecast::TEvRegisterTabletResult(tabletId, tabletInfo.Entry));
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " SEND to Sender# " << ev->Sender << " " << result->ToString());
    ctx.Send(ev->Sender, result.Release());
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvUnregisterTablet::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());
    const ui64 tabletId = msg->TabletId;

    auto it = Tablets.find(tabletId);
    if (it == Tablets.end()) {
        return;
    }

    auto& tabletInfo = it->second;

    Y_ABORT_UNLESS(tabletInfo.RefCount > 0);
    if (0 == --tabletInfo.RefCount) {
        if (tabletInfo.Mediator) {
            SendGranularWatchRemove(*tabletInfo.Mediator, tabletInfo.BucketId, tabletId, ctx);
        }
        // Note: buckets are unsubscribed lazily when entry refcount reaches zero on reconnect
        Tablets.erase(it);
    }
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvWaitPlanStep::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());
    const ui64 tabletId = msg->TabletId;
    const ui64 planStep = msg->PlanStep;

    auto it = Tablets.find(tabletId);
    Y_ABORT_UNLESS(it != Tablets.end(), "TEvWaitPlanStep TabletId# %" PRIu64 " is not subscribed", tabletId);
    auto& tabletInfo = it->second;

    Y_DEBUG_ABORT_UNLESS(tabletInfo.Entry, "TEvWaitPlanStep TabletId# %" PRIu64 " has no timecast entry, possible race", tabletId);
    if (!tabletInfo.Entry) {
        return;
    }

    Y_ABORT_UNLESS(tabletInfo.Mediator);
    TMediator& mediator = *tabletInfo.Mediator;
    auto& bucket = mediator.Buckets[tabletInfo.BucketId];

    const ui64 currentStep = tabletInfo.Entry->Get(tabletId);
    if (currentStep < planStep) {
        if (bucket.LatestStep->Get() < planStep) {
            bucket.Waiters.emplace(ev->Sender, tabletId, planStep);
            for (ui64 coordinatorId : mediator.Coordinators) {
                TMediatorCoordinator& coordinator = MediatorCoordinators[coordinatorId];
                if (coordinator.WaitingSteps.insert(planStep).second && !coordinator.RetryPending) {
                    Send(MakePipePerNodeCacheID(false),
                        new TEvPipeCache::TEvForward(
                            new TEvTxProxy::TEvRequirePlanSteps(coordinatorId, planStep),
                            coordinatorId,
                            !coordinator.Subscribed));
                    coordinator.Subscribed = true;
                }
            }
        } else {
            // Either frozen or not synchronized yet
            tabletInfo.Waiters.emplace(ev->Sender, tabletId, planStep);
            bucket.TabletWaiters.insert(TTabletWaiter(planStep, tabletId));
        }
        return;
    }

    // We don't need to wait when step is not in the future
    ctx.Send(ev->Sender, new TEvMediatorTimecast::TEvNotifyPlanStep(tabletId, currentStep));
}

void TMediatorTimecastProxy::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE EvClientConnected");
    TEvTabletPipe::TEvClientConnected* msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        TryResync(msg->ClientId, msg->TabletId, ctx);
    }
}

void TMediatorTimecastProxy::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE EvClientDestroyed");
    TEvTabletPipe::TEvClientDestroyed* msg = ev->Get();
    TryResync(msg->ClientId, msg->TabletId, ctx);
}

void TMediatorTimecastProxy::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE EvDeliveryProblem " << msg->TabletId);
    auto it = MediatorCoordinators.find(msg->TabletId);
    if (it != MediatorCoordinators.end()) {
        Y_DEBUG_ABORT_UNLESS(!it->second.RetryPending);
        Schedule(TDuration::MilliSeconds(5), new TEvPrivate::TEvRetryCoordinator(msg->TabletId));
        it->second.RetryPending = true;
        it->second.Subscribed = false;
    }
}

void TMediatorTimecastProxy::Handle(TEvPrivate::TEvRetryCoordinator::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
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

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvUpdate::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << ev->Get()->ToString());

    const NKikimrTxMediatorTimecast::TEvUpdate& record = ev->Get()->Record;

    const ui64 mediatorTabletId = record.GetMediator();
    auto it = Mediators.find(mediatorTabletId);
    if (it != Mediators.end()) {
        auto& mediator = it->second;
        Y_ABORT_UNLESS(record.GetBucket() < mediator.BucketsSz);
        auto& bucket = mediator.Buckets[record.GetBucket()];
        const ui64 step = record.GetTimeBarrier();
        switch (bucket.SafeStep.RefCount()) {
            case 0:
                break;
            case 1:
                bucket.Waiters = { };
                bucket.TabletWaiters = { };
                [[fallthrough]];
            default: {
                bucket.SafeStep->Set(step);
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
                while (!bucket.TabletWaiters.empty()) {
                    const auto& candidate = *bucket.TabletWaiters.begin();
                    if (step < candidate.PlanStep) {
                        break;
                    }
                    auto it = Tablets.find(candidate.TabletId);
                    if (it != Tablets.end()) {
                        auto& tabletInfo = it->second;
                        while (!tabletInfo.Waiters.empty()) {
                            const auto& top = *tabletInfo.Waiters.begin();
                            if (step < top.PlanStep) {
                                break;
                            }
                            if (processed.insert(std::make_pair(top.Sender, top.TabletId)).second) {
                                ctx.Send(top.Sender, new TEvMediatorTimecast::TEvNotifyPlanStep(top.TabletId, step));
                            }
                            tabletInfo.Waiters.erase(tabletInfo.Waiters.begin());
                        }
                    }
                    bucket.TabletWaiters.erase(bucket.TabletWaiters.begin());
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

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvGranularUpdate::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());

    const ui64 mediatorTabletId = msg->Record.GetMediator();
    auto it = Mediators.find(mediatorTabletId);
    if (it == Mediators.end()) {
        // Ignore messages from unknown mediators
        // Note: we don't clean up mediators, so this shouldn't happen
        return;
    }

    auto& mediator = it->second;
    const ui32 bucketId = msg->Record.GetBucket();
    if (bucketId >= mediator.BucketsSz) {
        LOG_CRIT_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
            << " got update from Mediator# " << mediatorTabletId << " Bucket# " << bucketId
            << " expecting only " << mediator.BucketsSz << " buckets");
        return;
    }

    auto& bucket = mediator.Buckets[bucketId];
    if (msg->Record.GetSubscriptionId() < bucket.SubscriptionId) {
        // Ignore messages from old subscriptions
        return;
    }

    if (msg->Record.FrozenTabletsSize() != msg->Record.FrozenStepsSize()) {
        LOG_CRIT_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
            << " got update from Mediator# " << mediatorTabletId << " Bucket# " << bucketId
            << " with mismatched frozen records");
        return;
    }

    size_t frozenSize = msg->Record.FrozenTabletsSize();
    size_t unfrozenSize = msg->Record.UnfrozenTabletsSize();

    for (size_t i = 0; i < frozenSize; ++i) {
        ui64 tabletId = msg->Record.GetFrozenTablets(i);
        ui64 step = msg->Record.GetFrozenSteps(i);
        if (!bucket.Tablets.contains(tabletId)) {
            // We are not interested in this tablet
            continue;
        }
        auto& tabletInfo = Tablets.at(tabletId);
        if (msg->Record.GetSubscriptionId() < tabletInfo.SubscriptionId) {
            // We have resubscribed to this tablet and this update must be ignored
            continue;
        }
        // This tablet is now frozen at this step
        tabletInfo.FrozenStep = step;
        // Update frozen steps for synchronized tablets
        if (tabletInfo.Synced && bucket.WatchSynced) {
            bucket.PendingUpdate.PushBack(&tabletInfo);
        }
    }

    for (size_t i = 0; i < unfrozenSize; ++i) {
        ui64 tabletId = msg->Record.GetUnfrozenTablets(i);
        if (!bucket.Tablets.contains(tabletId)) {
            // We are not interested in this tablet
            continue;
        }
        auto& tabletInfo = Tablets.at(tabletId);
        if (msg->Record.GetSubscriptionId() < tabletInfo.SubscriptionId) {
            // We have resubscribed to this tablet and this update must be ignored
            continue;
        }
        // This tablet is no longer frozen
        tabletInfo.FrozenStep = Max<ui64>();
        // Update frozen steps for synchronized tablets
        if (tabletInfo.Synced && bucket.WatchSynced) {
            bucket.PendingUpdate.PushBack(&tabletInfo);
        }
    }

    while (!bucket.PendingSubscribe.Empty()) {
        auto* tabletInfo = bucket.PendingSubscribe.Front();
        if (msg->Record.GetSubscriptionId() < tabletInfo->SubscriptionId) {
            // This tablet (and all others) is not subscribed yet
            break;
        }
        // This tablet is now subscribed and needs to be updated
        bucket.PendingUpdate.PushBack(tabletInfo);
        tabletInfo->Synced = true;
    }

    bucket.SubscriptionId = msg->Record.GetSubscriptionId();

    const ui64 latestStep = msg->Record.GetLatestStep();

    if (!bucket.WatchSynced) {
        // We may have connected to a lagging mediator, make sure we don't sync too early
        if (latestStep < bucket.LatestStep->Get()) {
            return;
        }

        // We have reached an expected state, now this bucket is synchronized
        bucket.WatchSynced = true;
    } else if (latestStep < bucket.LatestStep->Get()) {
        // This should never happen, but since we need to protect against
        // mediator time jumping backwards for running instances we will ignore
        // this update. Note that the current state is already updated, it's
        // just not published yet, and will be published later.
        LOG_CRIT_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
            << " got update from Mediator# " << mediatorTabletId
            << " with LatestStep# " << latestStep
            << " previous LatestStep# " << bucket.LatestStep->Get());
        return;
    }

    TIntrusiveList<TTabletInfo> checkWaiters;

    // Process frozen step updates
    while (!bucket.PendingUpdate.Empty()) {
        auto* tabletInfo = bucket.PendingUpdate.PopFront();
        ui64 frozenStep = Max(tabletInfo->FrozenStep, tabletInfo->MinStep);
        tabletInfo->Entry->SetFrozenStep(frozenStep);
        // Note: all waiters in tabletInfo are always below LatestStep
        if (!tabletInfo->Waiters.empty() && tabletInfo->Waiters.begin()->PlanStep <= frozenStep) {
            checkWaiters.PushBack(tabletInfo);
        }
    }

    // Publish the new latest step
    bucket.LatestStep->Set(latestStep);

    THashSet<std::pair<TActorId, ui64>> processed; // a set of processed tablets
    while (!bucket.Waiters.empty()) {
        const auto& top = bucket.Waiters.top();
        if (latestStep < top.PlanStep) {
            break;
        }
        auto it = Tablets.find(top.TabletId);
        if (it != Tablets.end() && bucket.Tablets.contains(top.TabletId)) {
            auto& tabletInfo = it->second;
            ui64 frozenStep = Max(tabletInfo.FrozenStep, tabletInfo.MinStep);
            if (tabletInfo.Synced && top.PlanStep <= frozenStep) {
                // Send update immediately
                if (processed.insert(std::make_pair(top.Sender, top.TabletId)).second) {
                    ctx.Send(top.Sender, new TEvMediatorTimecast::TEvNotifyPlanStep(top.TabletId, Min(latestStep, frozenStep)));
                }
            } else {
                // Move this waiter to the per-tablet waiter
                tabletInfo.Waiters.insert(top);
                bucket.TabletWaiters.insert(TTabletWaiter(top.PlanStep, top.TabletId));
            }
        }
        bucket.Waiters.pop();
    }

    while (!checkWaiters.Empty()) {
        auto* tabletInfo = checkWaiters.PopFront();
        ui64 frozenStep = Max(tabletInfo->FrozenStep, tabletInfo->MinStep);
        ui64 step = Min(latestStep, frozenStep);
        while (!tabletInfo->Waiters.empty()) {
            const auto& top = *tabletInfo->Waiters.begin();
            if (step < top.PlanStep) {
                break;
            }
            if (processed.insert(std::make_pair(top.Sender, top.TabletId)).second) {
                ctx.Send(top.Sender, new TEvMediatorTimecast::TEvNotifyPlanStep(top.TabletId, step));
            }
            // Current queue may have more waiters for the same plan step, but
            // all of them will be processed and erased in this loop.
            bucket.TabletWaiters.erase(TTabletWaiter(top.PlanStep, top.TabletId));
            tabletInfo->Waiters.erase(tabletInfo->Waiters.begin());
        }
    }
}

void TMediatorTimecastProxy::SyncCoordinator(ui64 coordinatorId, TCoordinator& coordinator, const TActorContext& ctx) {
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

void TMediatorTimecastProxy::ResyncCoordinator(ui64 coordinatorId, const TActorId& pipeClient, const TActorContext& ctx) {
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }

    auto& coordinator = itCoordinator->second;
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

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvSubscribeReadStep::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());

    const ui64 coordinatorId = msg->CoordinatorId;
    auto& subscriber = CoordinatorSubscribers[ev->Sender];
    auto& coordinator = Coordinators[coordinatorId];
    subscriber.Coordinators.insert(coordinatorId);
    coordinator.Subscribers.insert(ev->Sender);
    ui64 seqNo = ++LastSeqNo;
    auto key = std::make_pair(seqNo, ev->Sender);
    coordinator.SubscribeRequests[key] = ev->Cookie;
    SyncCoordinator(coordinatorId, coordinator, ctx);
}

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvUnsubscribeReadStep::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());

    auto& subscriber = CoordinatorSubscribers[ev->Sender];
    if (msg->CoordinatorId == 0) {
        // Unsubscribe from all coordinators
        for (ui64 coordinatorId : subscriber.Coordinators) {
            auto& coordinator = Coordinators[coordinatorId];
            coordinator.Subscribers.erase(ev->Sender);
            if (coordinator.Subscribers.empty()) {
                coordinator.IdleStart = ctx.Monotonic();
            }
        }
        subscriber.Coordinators.clear();
    } else if (subscriber.Coordinators.contains(msg->CoordinatorId)) {
        // Unsubscribe from specific coordinator
        auto& coordinator = Coordinators[msg->CoordinatorId];
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

void TMediatorTimecastProxy::Handle(TEvMediatorTimecast::TEvWaitReadStep::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE " << msg->ToString());

    const ui64 coordinatorId = msg->CoordinatorId;
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }
    auto& coordinator = itCoordinator->second;

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

void TMediatorTimecastProxy::Handle(TEvTxProxy::TEvSubscribeReadStepResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE TEvSubscribeReadStepResult " << record.ShortDebugString());

    const ui64 coordinatorId = record.GetCoordinatorID();
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }
    auto& coordinator = itCoordinator->second;

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

void TMediatorTimecastProxy::Handle(TEvTxProxy::TEvSubscribeReadStepUpdate::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TIMECAST, "Actor# " << ctx.SelfID
        << " HANDLE TEvSubscribeReadStepUpdate " << record.ShortDebugString());

    const ui64 coordinatorId = record.GetCoordinatorID();
    auto itCoordinator = Coordinators.find(coordinatorId);
    if (itCoordinator == Coordinators.end()) {
        return;
    }
    auto& coordinator = itCoordinator->second;

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

void TMediatorTimecastProxy::NotifyCoordinatorWaiters(ui64 coordinatorId, TCoordinator& coordinator, const TActorContext& ctx) {
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
