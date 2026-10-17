#include "rdma_impl.h"
#include <util/stream/output.h>
#include <util/thread/lfqueue.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/datetime/base.h>

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/metric_sub_registry.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/thread.h>
#include <util/system/yield.h>

namespace NInterconnect::NRdma {

class TSimpleCq: public TSimpleCqBase {
public:
    TSimpleCq(NActors::TActorSystem* as, size_t sz, NMonitoring::TDynamicCounters* c) noexcept
        : TSimpleCqBase(as, sz, c, true)
    {}

    int Init(const TRdmaCtx* ctx, const TRdmaRuntimeParams& params, std::shared_ptr<IMemPool> memPool) noexcept {
        return TSimpleCqBase::Init(ctx, params, std::move(memPool), nullptr);
    }

    virtual ~TSimpleCq() {
        // For simple polling mode CQ, we just can destroy ibv CQ without any issues just aftre joining to the thread
        Cont.store(false, std::memory_order_relaxed);
        if (Thread.Running()) {
            Thread.Join();
        }

        DrainWaiters();
        Y_UNUSED(DestroyCq());
    }
};

class TSimpleEventDrivenCq: public TSimpleCqBase {
public:
    TSimpleEventDrivenCq(NActors::TActorSystem* as, size_t sz, NMonitoring::TDynamicCounters* c) noexcept
        : TSimpleCqBase(as, sz, c, false)
    {}

    int Init(const TRdmaCtx* ctx, const TRdmaRuntimeParams& params, std::shared_ptr<IMemPool> memPool) noexcept {
        CompChannel = ibv_create_comp_channel(ctx->GetContext());
        if (!CompChannel) {
            return errno;
        }

        int err = TSimpleCqBase::Init(ctx, params, std::move(memPool), CompChannel);
        if (err) {
            return err;
        }

        err = ibv_req_notify_cq(Cq, 0);
        if (err) {
            return errno;
        }

        return 0;
    }

    void NotifyErr() noexcept override {
        DoNotifyErr();
        WakeUntilFinished();
    }

    void Idle() noexcept override final {
        struct ibv_cq *evCq = nullptr;
        void *evCtx = nullptr;

        int err = ibv_get_cq_event(CompChannel, &evCq, &evCtx);
        if (err) {
            if (errno != EINTR) {
                DoNotifyErr();
                return;
            }
        }

        if (!evCq) {
            return;
        }
        // TODO: batch ack
        ibv_ack_cq_events(evCq, 1);
        err = ibv_req_notify_cq(evCq, 0);
        if (err) {
            Cerr << "Couldn't request CQ notification\n" << Endl;
            DoNotifyErr();
            Y_DEBUG_ABORT_UNLESS(false);
        }
    }

    virtual ~TSimpleEventDrivenCq() {
        // For event driven CQ stopping is a bit complicated

        // 1. Lock the verbs builder. This prevents possibility to add new WR. Not nessesearly but just to be sure
        // No deadlock here - the builder routine is protected by TryLock semantic.
        VerbsBuildingState.Lock.Acquire();

        // 2. Set flag to exit from loop
        Cont.store(false, std::memory_order_relaxed);

        // 3. Send signal to the thread to interrupt waiting on the read syscall ()
        // NOTE: There is a tiny chanse the signal was send before thread blocked on the read syscall
        // so in this case repeat send signal until cq thread finished
        if (Thread.Running()) {
            WakeUntilFinished();
            // 4. As usual, join and destroy CQ
            Thread.Join();
        }

        DrainWaiters();
        const int destroyErr = DestroyCq();

        // 5. Destroy completion event channel
        if (!destroyErr && CompChannel && ibv_destroy_comp_channel(CompChannel)) {
            // https://www.rdmamojo.com/2012/10/26/ibv_destroy_comp_channel
            Cerr << "Unable to destroy completion event channel, errno: " << errno << Endl;
            // it should not happen, but if it happens it is not a fatal error for production
            Y_DEBUG_ABORT_UNLESS(false);
        }
    }
private:
    ibv_comp_channel* CompChannel = nullptr;
};

ICq::TPtr CreateSimpleCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, TRdmaRuntimeParams runtimeParams, std::shared_ptr<IMemPool> memPool, NMonitoring::TDynamicCounters* counter) noexcept {
    return CreateCq<TSimpleCq>(ctx, as, runtimeParams, std::move(memPool), counter);
}

ICq::TPtr CreateSimpleEventDrivenCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, TRdmaRuntimeParams runtimeParams, std::shared_ptr<IMemPool> memPool, NMonitoring::TDynamicCounters* counter) noexcept {
    return CreateCq<TSimpleEventDrivenCq>(ctx, as, runtimeParams, std::move(memPool), counter);
}

} // namespace NInterconnect::NRdma
