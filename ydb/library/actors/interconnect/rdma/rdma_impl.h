#pragma once

#include "ctx.h"
#include "events.h"
#include "rdma.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/metric_sub_registry.h>

#include <util/thread/lfqueue.h>
#include <util/system/thread.h>

#include <span>

namespace NInterconnect::NRdma {

NMonitoring::TDynamicCounterPtr MakeCounters(NMonitoring::TDynamicCounters* counters);

class TWr;

class TIbVerbsBuilderImpl final : public IIbVerbsBuilder {
public:
    TIbVerbsBuilderImpl(size_t hint) noexcept {
        WorkBuf.reserve(hint);
    }

    void AddReadVerb(void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize,
        std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> ioCb) noexcept;
    size_t GetVerbsNum() const noexcept;
    ibv_send_wr* BuildListOfVerbs(std::vector<TWr*>& preparedWr) noexcept;

private:
    struct TWrVerbData {
        ibv_sge Sg;
        ibv_send_wr Wr;
        std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> IoCb;
    };
    std::vector<TWrVerbData> WorkBuf;
};
class TCqCommon : public ICq {
public:
    TCqCommon(NActors::TActorSystem* as)
        : As(as)
        , Cq(nullptr)
    {}

    virtual ~TCqCommon();

    virtual void ReturnWr(IWr*) noexcept = 0;

    ibv_cq* GetCq() noexcept {
        return Cq;
    }

    int Init(const TRdmaCtx* ctx, int maxCqe) noexcept {
        Cq = ibv_create_cq(ctx->GetContext(), maxCqe, nullptr, nullptr, 0);
        if (!Cq) {
            return errno;
        }
        return 0;
    }

    int Do(std::span<ibv_wc> wc) noexcept {
        return ibv_poll_cq(Cq, wc.size(), &wc.front());
    }

    void Idle() noexcept {
        SpinLockPause();
    }
protected:
    NActors::TActorSystem* const As;
private:
    ibv_cq* Cq;
};

class TWr : public ICq::IWr {
public:
    TWr(ui64 id, TCqCommon* cqCommon) noexcept
        : Id(id)
        , CqCommon(cqCommon)
    {}

    TWr(const TWr&) = delete;
    TWr& operator=(const TWr&) = delete;
    TWr(TWr&& wr) noexcept = default;

    ui64 GetId() noexcept override {
        return Id;
    }

    void Release() noexcept override {
        Cb = TCb();
        CqCommon->ReturnWr(this);
    }

    void Reply(NActors::TActorSystem* as, const ibv_wc* wc) noexcept {
        if (Cb) {
            if (wc) {
                if (wc->status == IBV_WC_SUCCESS) {
                    Cb(as, TEvRdmaIoDone::Success());
                } else {
                    Cb(as, TEvRdmaIoDone::WcError(wc->status));
                }
            } else {
                Cb(as, TEvRdmaIoDone::CqError());
            }
            Cb = TCb();
        }
    }

    void ReplyCqErr(NActors::TActorSystem* as) noexcept {
        if (Cb) {
            Cb(as, TEvRdmaIoDone::CqError());
            Cb = TCb();
        }
    }

    void ReplyWrErr(NActors::TActorSystem* as, int err) noexcept {
        if (Cb) {
            Cb(as, TEvRdmaIoDone::WrError(err));
            Cb = TCb();
            CqCommon->ReturnWr(this);
        }
    }

    void AttachCb(std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> cb) noexcept {
        Cb = std::move(cb);
    }

    void ResetTimer() noexcept {
        Timer.Reset();
    }

    double GetTimePassed() const noexcept {
        return Timer.Passed();
    }

private:
    const ui64 Id;
    TCqCommon* const CqCommon;
    using TCb = std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)>;
    TCb Cb;
    THPTimer Timer;
};

template<class TCq>
static ICq::TPtr CreateCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, int maxCqe, int maxWr, NMonitoring::TDynamicCounters* counter) noexcept {
    if (maxCqe <= 0) {
        const ibv_device_attr& attr = ctx->GetDevAttr();
        maxCqe = attr.max_cqe;
    }
    if (maxWr <= 0) {
        maxWr = maxCqe;
    }
    auto p = std::make_shared<TCq>(as, maxWr, counter);
    int err = p->Init(ctx, maxCqe);
    if (err) {
        return nullptr;
    }
    err = p->Start();
    if (err) {
       return nullptr;
    }

    return p;
}

class TSimpleCqBase : public TCqCommon {
protected:
    struct TWaiterCtx {
        TWaiterCtx(std::shared_ptr<TQueuePair> qp, std::unique_ptr<IIbVerbsBuilder> verbsBuilder) noexcept
            : Qp(std::move(qp))
            , VerbsBuilder(std::move(verbsBuilder))
        {}
        size_t GetVerbsNum() const noexcept {
            return static_cast<TIbVerbsBuilderImpl*>(VerbsBuilder.get())->GetVerbsNum();
        }

        ibv_send_wr* BuildListOfVerbs(std::vector<TWr*>& preparedWr) noexcept {
            return static_cast<TIbVerbsBuilderImpl*>(VerbsBuilder.get())->BuildListOfVerbs(preparedWr);
        }

        std::shared_ptr<TQueuePair> Qp;
        std::unique_ptr<IIbVerbsBuilder> VerbsBuilder;
    };

    void Stop() {
        Cont.store(false, std::memory_order_relaxed);
        if (Thread.Running())
            Thread.Join();
    }

public:
    TSimpleCqBase(NActors::TActorSystem* as, size_t sz, NMonitoring::TDynamicCounters* c) noexcept
        : TCqCommon(as)
        , Thread(ThreadFunc, this)
        , Err(false)
    {
        auto counter = MakeCounters(c);
        RdmaDeviceVerbTimeUs = counter->GetHistogram(
                    "RdmaDeviceVerbTimeUs", NMonitoring::ExplicitHistogram({0, 5, 10, 20, 50, 100, 200, 1000, 10000}));
        Allocated.store(0);
        WrBuf.reserve(sz);
        // Enumerate all work requests for this CQ
        for (size_t i = 0; i < sz; i++) {
            WrBuf.emplace_back(i, this);
        }

        // Fill queue
        for (size_t i = 0; i < sz; i++) {
            Queue.Enqueue(&WrBuf[i]);
        }
    }

    void ReturnWr(IWr* wr) noexcept override {
        Allocated.fetch_sub(1);
        Queue.Enqueue(static_cast<TWr*>(wr));
    }

    void NotifyErr() noexcept override {
        Err.store(true, std::memory_order_relaxed);
    }

    TWrStats GetWrStats() const noexcept override {
        ui64 allocated = Allocated.load();
        Y_DEBUG_ABORT_UNLESS(allocated <= WrBuf.size());
        return TWrStats {
            .Total = static_cast<ui32>(WrBuf.size()),
            .Ready = static_cast<ui32>(WrBuf.size() - allocated),
        };
    }

    std::optional<TErr> DoWrBatchAsync(std::shared_ptr<TQueuePair> qp, std::unique_ptr<IIbVerbsBuilder> builder) noexcept override {
        if (Err.load(std::memory_order_relaxed)) {
            return TErr();
        }
        Waiters.Enqueue(new TWaiterCtx(std::move(qp), std::move(builder)));
        return {};
    }

    bool ProcessWr(std::unique_ptr<TWaiterCtx>& ctx, std::vector<TWr*>& preparedWr) noexcept {
        if (ctx) {
            TWr* wr = nullptr;
            Queue.Dequeue(&wr);
            if (wr) {
                preparedWr.emplace_back(wr);
                if (preparedWr.size() < ctx->GetVerbsNum()) {
                    // we need more work requests
                    return true;
                }

                ibv_send_wr* wrList = ctx->BuildListOfVerbs(preparedWr);

                if (Err.load(std::memory_order_relaxed)) {
                    for (auto x : preparedWr) {
                        x->ReplyCqErr(As);
                    }
                } else {
                    Allocated.fetch_add(preparedWr.size());
                    ibv_send_wr* wrErr = nullptr;
                    int err = ctx->Qp->PostSend(wrList, &wrErr);
                    if (err) {
                        while (wrErr) {
                            TWr* x = &WrBuf[wrErr->wr_id];
                            x->ReplyWrErr(As, err);
                            wrErr = wrErr->next;
                        }
                    }
                }

                preparedWr.clear();
                ctx.reset();
                return true;
            } else {
                return false;
            }
        } else {
            TWaiterCtx* p = nullptr;
            Waiters.Dequeue(&p);
            ctx.reset(p);
            return true;
        }
        return false;
    }

    static void* ThreadFunc(void* p) {
        TThread::SetCurrentThreadName("RdmaCqThread");
        reinterpret_cast<TSimpleCqBase*>(p)->Loop();
        return nullptr;
    }

    void Loop() noexcept {
        std::unique_ptr<TWaiterCtx> curCtx;
        std::vector<TWr*> preparedWr;
        while (Cont.load(std::memory_order_relaxed)) {
            const constexpr size_t wcBatchSize = 16;
            std::array<ibv_wc, wcBatchSize> wcs;
            if (Err.load(std::memory_order_relaxed)) {
                HandleErr();
                Cont.store(false, std::memory_order_relaxed);
            } else {
                int rv = Do(wcs);
                if (rv < 0) {
                    //TODO: Is it correct err handling?
                    Err.store(true, std::memory_order_relaxed);
                } else if (rv == 0) {
                    if (!ProcessWr(curCtx, preparedWr)) {
                        Idle();
                    }
                } else {
                    Y_ABORT_UNLESS(static_cast<size_t>(rv) <= wcs.size(), "ibv_poll_cq returns more then requested");
                    HandleWc(wcs.data(), rv);
                }
            }
        }
    }

    void HandleErr() noexcept {
        for (size_t i = 0; i < WrBuf.size(); i++) {
            TWr* wr = &WrBuf[i];
            wr->ReplyCqErr(As);
            //This it terminal error. Cq should be recreated.
            //So no need to return wr in to the queue
        }
    }

    void HandleWc(ibv_wc* wc, size_t sz) noexcept {
        for (size_t i = 0; i < sz; i++, wc++) {
            TWr* wr = &WrBuf[wc->wr_id];
            double passed = wr->GetTimePassed();
            RdmaDeviceVerbTimeUs->Collect(passed * 1000000.0);
            wr->Reply(As, wc);
            ReturnWr(wr);
        }
    }

    int Start() noexcept {
        Cont.store(true, std::memory_order_relaxed);
        try {
            Thread.Start();
        } catch (std::exception& ex) {
            Cerr << "Unable to launch cq poller thread: " << ex.what() << Endl;
            return 1;
        }
        return 0;
    }

protected:
    TThread Thread;
    std::atomic<bool> Cont;

    std::vector<TWr> WrBuf;
    std::atomic<size_t> WrCurSz;
    // Queue is used to commnicate with client code (from actors)
    // It is possible to use Single Producer Multiple Consumer queue here but in this case
    // imlementation of Release() methos on IWr* will be musch more difficult
    TLockFreeQueue<TWr*> Queue;

    TLockFreeQueue<TWaiterCtx*> Waiters;
    std::atomic<bool> Err;
    std::atomic<ui64> Allocated;
    NMonitoring::THistogramPtr RdmaDeviceVerbTimeUs;
};

}
