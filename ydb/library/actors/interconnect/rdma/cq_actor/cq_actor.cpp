#include "cq_actor.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/actors/interconnect/logging/logging.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/events.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/rdma_qp_cache_impl.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>
#include <library/cpp/threading/bounded_queue/bounded_queue.h>

#include <contrib/libs/ibdrv/include/infiniband/verbs.h>

#include <atomic>
#include <cerrno>

#include <util/system/hp_timer.h>

#define YDB_LOG_THIS_FILE_COMPONENT ::NActorsServices::INTERCONNECT

using namespace NActors;

namespace NInterconnect::NRdma {

using TCqFactory = std::function<ICq::TPtr(const TRdmaCtx*, std::shared_ptr<IMemPool>, IQpCache::TPtr qpCache, TCqDestroyer cqDestroyer)>;

static const TDuration PeriodicActionInterval = TDuration::Seconds(1);

struct TRdmaActorParams {
    TRdmaActorParams(const TRdmaRuntimeParams& params) noexcept
        : KeepQpPreinited(params.KeepQpPreinited)
        , MaxWrPerQp(params.MaxWrPerQp)
        , QpActorPoolId(params.QpActorPoolId)
    {}
    ui16 KeepQpPreinited;
    ui16 MaxWrPerQp;
    ui32 QpActorPoolId;
};

struct TQpCacheCounters {
    explicit TQpCacheCounters(NMonitoring::TDynamicCounters* counters)
        : Counters(counters ? counters : new NMonitoring::TDynamicCounters())
        , Hits(Counters->GetCounter("RdmaQpCacheHits", true))
        , Misses(Counters->GetCounter("RdmaQpCacheMisses", true))
        , QpsCreated(Counters->GetCounter("RdmaQpCacheQpsCreated", true))
        , FillTimeUs(Counters->GetHistogram(
            "RdmaQpCacheFillTimeUs", NMonitoring::ExponentialHistogram(13, 2, 1024)))
    {}

    NMonitoring::TDynamicCounterPtr Counters;
    NMonitoring::TDynamicCounters::TCounterPtr Hits;
    NMonitoring::TDynamicCounters::TCounterPtr Misses;
    NMonitoring::TDynamicCounters::TCounterPtr QpsCreated;
    NMonitoring::THistogramPtr FillTimeUs;
};

struct TEvPrivateFillCache : NActors::TEventLocal<TEvPrivateFillCache, (ui32)ERdma::EvPrivateFillCache> {
    explicit TEvPrivateFillCache(ICq::TPtr cq)
        : Cq(std::move(cq))
    {}

    ICq::TPtr Cq;
};

struct TEvPrivateDoFillCache : NActors::TEventLocal<TEvPrivateDoFillCache, (ui32)ERdma::EvPrivateDoFillCache> {
};

struct TEvPrivateStopCache : NActors::TEventLocal<TEvPrivateStopCache, (ui32)ERdma::EvPrivateStopCache> {
};

class TQpDeleter {
public:
    TQpDeleter(const TActorSystem* actorSystem, TActorId destroyerActorId)
        : ActorSystem(actorSystem)
        , DestroyerActorId(destroyerActorId)
    {}

    void operator()(TQueuePair* qp) noexcept {
        ActorSystem->Send(DestroyerActorId,
            new TEvPrivateDestroyQp(std::unique_ptr<TQueuePair>(qp)));
    }

private:
    const TActorSystem* const ActorSystem;
    const TActorId DestroyerActorId;
};

class TQpCache final : public IQpCache {
public:
    enum class EFillStatus {
        Ready,
        NoResources,
        Error,
    };

    TQpCache(ui16 keepQpPreinited, TActorId filler, const TActorSystem* actorSystem,
            std::shared_ptr<TQpCacheCounters> counters)
        : Cache(Max<size_t>(2, FastClp2(static_cast<size_t>(keepQpPreinited))))
        , MaxCachedQps(keepQpPreinited)
        , CacheFiller(filler)
        , ActorSystem(actorSystem)
        , Counters(std::move(counters))
    {}

    ICq::TGetQpResult GetQp(const NActors::TActorContext& actorCtx, ICq::TPtr cq) noexcept {
        TQueuePair::TPtr qp;
        if (Cache.Dequeue(qp)) {
            const size_t cachedQps = CachedQps.fetch_sub(1, std::memory_order_acq_rel);
            Y_DEBUG_ABORT_UNLESS(cachedQps > 0);
            Counters->Hits->Inc();
            return qp;
        }

        if (Stopped.load(std::memory_order_acquire)) {
            return ICq::TQpCacheError();
        }

        Counters->Misses->Inc();
        FillRequested.store(true, std::memory_order_release);
        if (!actorCtx.Send(CacheFiller, new TEvPrivateFillCache(std::move(cq)))) {
            return ICq::TQpCacheError();
        }
        return ICq::TQpCacheEmpty();
    }

    void Stop() noexcept {
        if (Stopped.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        DrainLock.Acquire();
        TQueuePair::TPtr qp;
        while(Cache.Dequeue(qp)) {
            const size_t cachedQps = CachedQps.fetch_sub(1, std::memory_order_relaxed);
            Y_DEBUG_ABORT_UNLESS(cachedQps > 0);
            qp.reset();
        }
        ActorSystem->Send(CacheFiller, new TEvPrivateStopCache);
    }

    bool IsStopped() const noexcept {
        return Stopped.load(std::memory_order_acquire);
    }

    bool IsFillRequested() const noexcept {
        return FillRequested.load(std::memory_order_acquire);
    }

    bool HasCachedQps() const noexcept {
        return CachedQps.load(std::memory_order_acquire) != 0;
    }

    bool TryEnqueue(TQueuePair::TPtr qp) noexcept {
        size_t cachedQps = CachedQps.load(std::memory_order_relaxed);
        while (cachedQps < MaxCachedQps) {
            if (CachedQps.compare_exchange_weak(cachedQps, cachedQps + 1,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                if (Cache.Enqueue(std::move(qp))) {
                    return true;
                }
                CachedQps.fetch_sub(1, std::memory_order_release);
                return false;
            }
        }
        return false;
    }

    template<typename TCb>
    EFillStatus TryAcquireAndFeed(TCb cb) noexcept {
        // Do not start a refill concurrently with cache draining.
        if (DrainLock.TryAcquire()) {
            // Requests arriving during the fill must remain visible after this
            // cycle completes and trigger another refill if consumers drain it.
            FillRequested.store(false, std::memory_order_release);
            EFillStatus fillStatus = cb();
            DrainLock.Release();
            return fillStatus;
        }
        return EFillStatus::Error;
    }
private:
    NThreading::TBoundedQueue<TQueuePair::TPtr> Cache;
    const size_t MaxCachedQps;
    NActors::TActorId CacheFiller;
    const TActorSystem* const ActorSystem;
    std::shared_ptr<TQpCacheCounters> Counters;
    std::atomic<bool> FillRequested = false;
    std::atomic<bool> Stopped = false;
    std::atomic<size_t> CachedQps = 0;
    TSpinLock DrainLock;
};

struct TEvPrivateRegisterCache : NActors::TEventLocal<TEvPrivateRegisterCache, (ui32)ERdma::EvPrivateRegisterCache> {
    TEvPrivateRegisterCache(std::weak_ptr<TQpCache> cache)
        : Cache(cache)
    {}
    std::weak_ptr<TQpCache> Cache;
};

class TQpCacheFillerActor : public TActor<TQpCacheFillerActor> {
public:
    TQpCacheFillerActor(const TRdmaCtx* ctx, ui16 cacheSize, ui16 maxWr, TActorId cqActorId,
            TActorId verbsDestroyerActorId, std::shared_ptr<TQpCacheCounters> counters)
        : TActor(&TQpCacheFillerActor::StateFunc)
        , Ctx(ctx)
        , CacheSize(cacheSize)
        , MaxWr(maxWr)
        , CqActorId(cqActorId)
        , VerbsDestroyerActorId(verbsDestroyerActorId)
        , Counters(std::move(counters))
    {}

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivateFillCache, Handle);
        hFunc(TEvPrivateDoFillCache, Handle);
        hFunc(TEvPrivateRegisterCache, Handle);
        hFunc(TEvPrivateStopCache, Handle);
    )

    void Handle(TEvPrivateFillCache::TPtr& ev) {
        auto cache = Cache.lock();
        if (!cache || cache->IsStopped()) {
            Notify(ev->Sender, TQpCache::EFillStatus::Error);
            return;
        }
        if (cache->HasCachedQps()) {
            Notify(ev->Sender, TQpCache::EFillStatus::Ready);
            return;
        }
        // The request may have been queued while the previous fill was in progress
        // and handled after it completed. Notify it with that fill result instead
        // of starting another fill for an already replenished cache.
        if (!cache->IsFillRequested()) {
            Notify(ev->Sender, LastFillStatus);
            return;
        }

        Waiters.push_back(ev->Sender);
        if (!FillScheduled) {
            FillScheduled = true;
            FillCq = std::move(ev->Get()->Cq);
            Send(SelfId(), new TEvPrivateDoFillCache);
        }
    }

    void Handle(TEvPrivateDoFillCache::TPtr&) {
        FillScheduled = false;
        auto cq = std::move(FillCq);
        auto cache = Cache.lock();
        int terminalError = 0;
        if (cache) {
            const ui64 fillStart = GetCycleCountFast();
            LastFillStatus = cache->TryAcquireAndFeed([&] {
                bool ready = false;
                for (size_t i = 0; i < CacheSize; ++i) {
                    auto qp = std::unique_ptr<TQueuePair>(new TQueuePair());
                    const int error = qp->Init(const_cast<TRdmaCtx*>(Ctx), cq.get(), MaxWr);
                    if (error) {
                        if (!IsResourceExhaustion(error)) {
                            terminalError = error;
                            return TQpCache::EFillStatus::Error;
                        }
                        return ready
                            ? TQpCache::EFillStatus::Ready
                            : TQpCache::EFillStatus::NoResources;
                    }
                    Counters->QpsCreated->Inc();
                    TQueuePair::TPtr sharedQp(qp.release(), TQpDeleter(
                        TlsActivationContext->ActorSystem(), VerbsDestroyerActorId));
                    if (!cache->TryEnqueue(std::move(sharedQp))) {
                        return TQpCache::EFillStatus::Ready;
                    }
                    ready = true;
                    // A notification does not reserve the QP, but waking one
                    // waiter per new QP avoids a thundering herd during refill.
                    NotifyOneWaiter(TQpCache::EFillStatus::Ready);
                }
                return TQpCache::EFillStatus::Ready;
            });
            if (LastFillStatus != TQpCache::EFillStatus::Error) {
                Counters->FillTimeUs->Collect(
                    NHPTimer::GetSeconds(GetCycleCountFast() - fillStart) * 1'000'000.0);
            }
        } else {
            LastFillStatus = TQpCache::EFillStatus::Error;
        }
        // If there are more waiters than the refill produced QPs, let them
        // retry and request the next cycle after the cache is drained.
        NotifyWaiters(LastFillStatus);
        if (terminalError) {
            Send(CqActorId, new TEvPrivateInvalidateCq(Ctx, std::move(cq), terminalError));
        }
    }

    static bool IsResourceExhaustion(int error) noexcept {
        return error == ENOMEM || error == ENOSPC || error == EAGAIN || error == ENOBUFS;
    }

    void NotifyOneWaiter(TQpCache::EFillStatus status) {
        if (!Waiters.empty()) {
            const TActorId waiter = Waiters.back();
            Waiters.pop_back();
            Notify(waiter, status);
        }
    }

    void NotifyWaiters(TQpCache::EFillStatus status) {
        for (const auto& waiter : Waiters) {
            Notify(waiter, status);
        }
        Waiters.clear();
    }

    void Notify(TActorId waiter, TQpCache::EFillStatus status) {
        switch (status) {
            case TQpCache::EFillStatus::Ready:
                Send(waiter, new TEvQpCacheReady);
                break;
            case TQpCache::EFillStatus::NoResources:
                Send(waiter, new TEvQpCacheNoResources);
                break;
            case TQpCache::EFillStatus::Error:
                Send(waiter, new TEvQpCacheError);
                break;
        }
    }

    void Handle(TEvPrivateRegisterCache::TPtr& ev) {
        Cache = ev->Get()->Cache;
    }

    void Handle(TEvPrivateStopCache::TPtr&) {
        NotifyWaiters(TQpCache::EFillStatus::Error);
        PassAway();
    }
    const TRdmaCtx* const Ctx;
    const ui16 CacheSize;
    const ui16 MaxWr;
    const TActorId CqActorId;
    const TActorId VerbsDestroyerActorId;
    std::shared_ptr<TQpCacheCounters> Counters;
    std::weak_ptr<TQpCache> Cache;
    std::vector<TActorId> Waiters;
    ICq::TPtr FillCq;
    TQpCache::EFillStatus LastFillStatus = TQpCache::EFillStatus::Error;
    bool FillScheduled = false;
};

class TVerbsDestroyerActor : public TActor<TVerbsDestroyerActor> {
public:
    TVerbsDestroyerActor()
        : TActor(&TVerbsDestroyerActor::StateFunc)
    {}

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivateDestroyQp, Handle);
        hFunc(TEvPrivateDestroyCq, Handle);
    )

    void Handle(TEvPrivateDestroyQp::TPtr& ev) {
        ev->Get()->Qp.reset();
    }

    void Handle(TEvPrivateDestroyCq::TPtr& ev) {
        ev->Get()->Cq.reset();
    }
};

class TCqActor: public TInterconnectLoggingBase, public TActorBootstrapped<TCqActor> {
    class TAsyncEventDesctiptor : public TSharedDescriptor {
    public:
        TAsyncEventDesctiptor(const TRdmaCtx* ctx)
            : Ctx(ctx)
        {}
        int GetDescriptor() override {
            return Ctx->GetContext()->async_fd;
        }
        const TRdmaCtx* GetContext() const {
            return Ctx;
        }
    private:
        const TRdmaCtx* Ctx;
    };

public:
    TCqActor(TCqFactory cqFactory, TRdmaActorParams params, NMonitoring::TDynamicCounters* counters)
        : TInterconnectLoggingBase("CqActor")
        , CqFactory(std::move(cqFactory))
        , RdmaActorParams(params)
        , QpCacheCounters(std::make_shared<TQpCacheCounters>(counters))
    {}

    ~TCqActor() {
        for (auto& [ctx, state] : CqMap) {
            Y_UNUSED(ctx);
            if (auto* data = std::get_if<TCtxData>(&state)) {
                data->Cq->StopQpCache();
            }
        }
    }

    static constexpr IActor::EActivityType ActorActivityType() {
        return IActor::EActivityType::INTERCONNECT_RDMA_CQ;
    }

    void Bootstrap() {
        Become(&TCqActor::StateFunc);

        TActorSystem* as = TlsActivationContext->AsActorContext().ActorSystem();

        if (IRcBufAllocator* rcBufAlloc = as->GetRcBufAllocator()) {
            if (TRdmaAllocatorWithFallback* rdmaAlloc = dynamic_cast<TRdmaAllocatorWithFallback*>(rcBufAlloc)) {
                MemPool = rdmaAlloc->GetRdmaMemPool();
            }
        }

        VerbsDestroyerActorId = Register(new TVerbsDestroyerActor(),
                TMailboxType::HTSwap, RdmaActorParams.QpActorPoolId);

        Schedule(PeriodicActionInterval, new TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvGetCqHandle, Handle);
        hFunc(NActors::TEvPollerRegisterResult, Handle);
        hFunc(NActors::TEvPollerReady, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvPrivateInvalidateCq, Handle);
    )

    bool RegisterForAsyncEvent(const TRdmaCtx* ctx) {
        int flags = fcntl(ctx->GetContext()->async_fd, F_GETFL);
        int ret = fcntl(ctx->GetContext()->async_fd, F_SETFL, flags | O_NONBLOCK);
        if (ret < 0) {
            return false;
        }
        auto rv = Send(MakePollerActorId(),
            new NActors::TEvPollerRegister(new TAsyncEventDesctiptor(ctx), SelfId(), SelfId()));
        Y_ABORT_UNLESS(rv, "actor system poller service must present");
        return true;
    }

    void Handle(TEvGetCqHandle::TPtr& ev) {
        auto rdmaCtx = ev->Get()->Ctx;
        auto it = CqMap.find(rdmaCtx);
        ICq::TPtr cqPtr = nullptr;
        if (it == CqMap.end()) {
            if (RegisterForAsyncEvent(rdmaCtx)) {
                CqMap.emplace(rdmaCtx, TWaitPollerReg(ev));
                return;
            }
            YDB_LOG_ERROR("Unable to register",
                {"marker", "ICRDMA"},
                {"asyncFd", rdmaCtx->GetContext()->async_fd});
        } else if (it->second.index() == 0) {
            cqPtr = std::get<0>(it->second).Cq;
        } else {
            Y_ABORT_UNLESS(it->second.index() == 1);
            std::get<1>(it->second).Waiters.emplace_back(ev);
            return;
        }

        // nullptr in case of err
        ev->Get()->CqPtr = cqPtr;

        TlsActivationContext->Send(ev->Sender, std::unique_ptr<TEvGetCqHandle>(ev->Release().Release()));
        Y_UNUSED(it);
    }

    void Handle(NActors::TEvPollerRegisterResult::TPtr& ev) {
        YDB_LOG_DEBUG("Got TEvPollerRegisterResult",
            {"marker", "ICRDMA"},
            {"fd", ev->Get()->Socket.Get()->GetDescriptor()});
        auto rdmaCtx = static_cast<TAsyncEventDesctiptor*>(ev->Get()->Socket.Get())->GetContext();
        std::shared_ptr<TQpCache> qpCache;
        if (RdmaActorParams.KeepQpPreinited > 0) {
            auto fillerActor = Register(new TQpCacheFillerActor(rdmaCtx,
                RdmaActorParams.KeepQpPreinited, RdmaActorParams.MaxWrPerQp,
                SelfId(), VerbsDestroyerActorId, QpCacheCounters),
                TMailboxType::HTSwap, RdmaActorParams.QpActorPoolId);
            const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();
            qpCache = std::make_shared<TQpCache>(RdmaActorParams.KeepQpPreinited,
                fillerActor, actorSystem, QpCacheCounters);
            Send(fillerActor, new TEvPrivateRegisterCache(qpCache));
        }

        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();
        const TActorId destroyerActorId = VerbsDestroyerActorId;
        TCqDestroyer cqDestroyer = [actorSystem, destroyerActorId](ICq* cq) noexcept {
            actorSystem->Send(destroyerActorId,
                new TEvPrivateDestroyCq(std::unique_ptr<ICq>(cq)));
        };
        auto cqPtr = CqFactory(rdmaCtx, MemPool, qpCache, std::move(cqDestroyer));
        auto it = CqMap.find(rdmaCtx);
        Y_ABORT_UNLESS(it != CqMap.end());
        Y_ABORT_UNLESS(it->second.index() == 1);
        auto waiters = std::get<1>(it->second).Waiters;
        if (cqPtr) {
            it->second = TCtxData {
                .Cq = cqPtr,
                .AsyncEventToken = ev->Get()->PollerToken,
            };
            for (auto p : waiters) {
                p->Get()->CqPtr = cqPtr;
                TlsActivationContext->Send(p->Sender, std::unique_ptr<TEvGetCqHandle>(p->Release().Release()));
            }
            std::get<0>(it->second).AsyncEventToken->Request(true, false);
        } else {
            if (qpCache) {
                qpCache->Stop();
            }
            for (auto p : waiters) {
                TlsActivationContext->Send(p->Sender, std::unique_ptr<TEvGetCqHandle>(p->Release().Release()));
            }
            CqMap.erase(it);
        }
    }

    void Handle(NActors::TEvPollerReady::TPtr& ev) {
        auto* desc = static_cast<TAsyncEventDesctiptor*>(ev->Get()->Socket.Get());
        for (;;) {
            switch (ProcessIbvAsyncEvent(desc)) {
                case EProcessAsyncEventResult::Processed:
                    continue;

                case EProcessAsyncEventResult::WouldBlock: {
                    auto it = CqMap.find(desc->GetContext());
                    Y_ABORT_UNLESS(it != CqMap.end());
                    Y_ABORT_UNLESS(it->second.index() == 0);
                    if (std::get<0>(it->second).AsyncEventToken->RequestReadNotificationAfterWouldBlock()) {
                        continue;
                    }
                    return;
                }

                case EProcessAsyncEventResult::ContextRemoved:
                    return;
            }
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        NActors::TMonotonic time = TlsActivationContext->AsActorContext().Monotonic();
        if (MemPool) {
            MemPool->Tick(time);
        }

        Schedule(PeriodicActionInterval, new TEvents::TEvWakeup());
    }
private:

    static TString GetAsyncEventDbg(const TRdmaCtx* ctx, const ibv_async_event& event) {
        switch (event.event_type) {
            /* QP events */
            case IBV_EVENT_QP_FATAL:
                return Sprintf("QP fatal event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_QP_REQ_ERR:
                return Sprintf("QP Requestor error for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_QP_ACCESS_ERR:
                return Sprintf("QP access error event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_COMM_EST:
                return Sprintf("QP communication established event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_SQ_DRAINED:
                return Sprintf("QP Send Queue drained event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_PATH_MIG:
                return Sprintf("QP Path migration loaded event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_PATH_MIG_ERR:
                return Sprintf("QP Path migration error event for QP with handle %p\n", event.element.qp);
            case IBV_EVENT_QP_LAST_WQE_REACHED:
                return Sprintf("QP last WQE reached event for QP with handle %p\n", event.element.qp);

            /* CQ events */
            case IBV_EVENT_CQ_ERR:
                return Sprintf("CQ error for CQ with handle %p\n", event.element.cq);

            /* SRQ events */
            case IBV_EVENT_SRQ_ERR:
                return Sprintf("SRQ error for SRQ with handle %p\n", event.element.srq);
            case IBV_EVENT_SRQ_LIMIT_REACHED:
                return Sprintf("SRQ limit reached event for SRQ with handle %p\n", event.element.srq);

            /* Port events */
            case IBV_EVENT_PORT_ACTIVE:
                return Sprintf("Port active event for port number %d\n", event.element.port_num);
            case IBV_EVENT_PORT_ERR:
                return Sprintf("Port error event for port number %d\n", event.element.port_num);
            case IBV_EVENT_LID_CHANGE:
                return Sprintf("LID change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_PKEY_CHANGE:
                return Sprintf("P_Key table change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_GID_CHANGE:
                return Sprintf("GID table change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_SM_CHANGE:
                return Sprintf("SM change event for port number %d\n", event.element.port_num);
            case IBV_EVENT_CLIENT_REREGISTER:
                return Sprintf("Client reregister event for port number %d\n", event.element.port_num);

            /* RDMA device events */
            case IBV_EVENT_DEVICE_FATAL:
                return Sprintf("Fatal error event for device %s\n", ctx->GetDeviceName());

            default:
                return Sprintf("Unknown event (%d)\n", event.event_type);
        }
    }

    void ProcessCqErr(auto it) {
        TCtxData& c = std::get<0>(it->second);
        YDB_LOG_ERROR("CQ/SRQ error issued on ctx notify all pending cq callbacks",
            {"marker", "ICRDMA"},
            {"rdmaCtx", it->first->ToString().data()});
        c.Cq->NotifyErr();
    }

    void Handle(TEvPrivateInvalidateCq::TPtr& ev) {
        auto it = CqMap.find(ev->Get()->Ctx);
        if (it == CqMap.end() || it->second.index() != 0) {
            return;
        }

        TCtxData& data = std::get<0>(it->second);
        if (data.Cq != ev->Get()->Cq) {
            return;
        }

        YDB_LOG_ERROR("Terminal error while filling RDMA QP cache",
            {"marker", "ICRDMA"},
            {"rdmaCtx", ev->Get()->Ctx->ToString().data()},
            {"error", ev->Get()->Error},
            {"strerror", strerror(ev->Get()->Error)});
        data.Cq->NotifyErr();
        CqMap.erase(it);
    }

    enum class EProcessAsyncEventResult {
        Processed,
        WouldBlock,
        ContextRemoved,
    };

    EProcessAsyncEventResult ProcessIbvAsyncEvent(const TAsyncEventDesctiptor* desc) {
        auto rdmaCtx = desc->GetContext();
        ibv_async_event async_event;
        for (;;) {
            if (!ibv_get_async_event(rdmaCtx->GetContext(), &async_event)) {
                break;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            Y_ABORT_UNLESS(error == EAGAIN || error == EWOULDBLOCK,
                "ibv_get_async_event failed with errno# %d", error);
            return EProcessAsyncEventResult::WouldBlock;
        }

        auto it = CqMap.find(rdmaCtx);
        Y_ABORT_UNLESS(it != CqMap.end());

        YDB_LOG_DEBUG("RDMA async event issued on ctx",
            {"marker", "ICRDMA"},
            {"rdmaCtx", rdmaCtx->ToString().data()},
            {"eventDbg", GetAsyncEventDbg(rdmaCtx, async_event).data()});

        switch (async_event.event_type) {
            case IBV_EVENT_CQ_ERR:
            case IBV_EVENT_SRQ_ERR:
            /*  Docs say:
                All async events that ibv_get_async_event() returns must be
                acknowledged using ibv_ack_async_event().  To avoid races,
                destroying an object (CQ, SRQ or QP) will wait for all affiliated
                events for the object to be acknowledged; this avoids an
                application retrieving an affiliated event after the corresponding
                object has already been destroyed.

                So, to avoid deadlock we need ack event before decrementing refcount
            */
                ProcessCqErr(it);
                ibv_ack_async_event(&async_event);
                CqMap.erase(it);
                return EProcessAsyncEventResult::ContextRemoved;
            default:
                break;
        }
        ibv_ack_async_event(&async_event);
        return EProcessAsyncEventResult::Processed;
    }

    struct TCtxData {
        ICq::TPtr Cq;
        NActors::TPollerToken::TPtr AsyncEventToken;
    };
    struct TWaitPollerReg {
        TWaitPollerReg(TEvGetCqHandle::TPtr& ev)
        {
            Waiters.emplace_back(ev);
        }
        std::vector<TEvGetCqHandle::TPtr> Waiters;
    };
    std::map<const TRdmaCtx*, std::variant<TCtxData, TWaitPollerReg>> CqMap;
    TCqFactory CqFactory;
    std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;
    const TRdmaActorParams RdmaActorParams;
    std::shared_ptr<TQpCacheCounters> QpCacheCounters;

    TActorId VerbsDestroyerActorId;
};

NActors::IActor* CreateCqActor(const TRdmaRuntimeParams& runtimeParams, ECqMode mode, NMonitoring::TDynamicCounters* counters) {
    switch (mode) {
        case NInterconnect::NRdma::ECqMode::POLLING:
            return new TCqActor([runtimeParams, counters](const TRdmaCtx* ctx, std::shared_ptr<IMemPool> memPool, IQpCache::TPtr qpCache, TCqDestroyer cqDestroyer) {
                return CreateSimpleCq(ctx, TlsActivationContext->AsActorContext().ActorSystem(), runtimeParams, std::move(memPool), counters, std::move(qpCache), std::move(cqDestroyer));
            }, runtimeParams, counters);

        case NInterconnect::NRdma::ECqMode::EVENT:
            return new TCqActor([runtimeParams, counters](const TRdmaCtx* ctx, std::shared_ptr<IMemPool> memPool, IQpCache::TPtr qpCache, TCqDestroyer cqDestroyer) {
                return CreateSimpleEventDrivenCq(ctx, TlsActivationContext->AsActorContext().ActorSystem(), runtimeParams, std::move(memPool), counters, std::move(qpCache), std::move(cqDestroyer));
            }, runtimeParams, counters);
    }
}

NActors::TActorId MakeCqActorId() {
    char x[12] = {'I', 'C', 'R', 'D', 'M', 'A', 'C', 'Q', '\xDE', '\xAD', '\xBE', '\xEF'};
    return TActorId(0, TStringBuf(std::begin(x), std::end(x)));
}

}
