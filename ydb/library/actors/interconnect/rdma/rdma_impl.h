#pragma once

#include "ctx.h"
#include "events.h"
#include "rdma.h"
#include "mem_pool.h"

#include <contrib/libs/ibdrv/include/infiniband/verbs.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/metric_sub_registry.h>
#include <library/cpp/threading/queue/mpsc_read_as_filled.h>



#include <util/thread/lfqueue.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/system/sanitizers.h>
#include <util/system/compiler.h>

#include <cerrno>
#include <span>

namespace NInterconnect::NRdma {

void Y_FORCE_INLINE MemoryOrderAcquire(void* addr) {
    atomic_thread_fence(std::memory_order_acquire);
#if defined(_tsan_enabled_)
    __tsan_acquire(addr);
#else
    Y_UNUSED(addr);
#endif
}

void Y_FORCE_INLINE MemoryOrderRlease(void* addr) {
    atomic_thread_fence(std::memory_order_release);
#if defined(_tsan_enabled_)
    __tsan_release(addr);
#else
    Y_UNUSED(addr);
#endif
}

NMonitoring::TDynamicCounterPtr MakeCounters(NMonitoring::TDynamicCounters* counters);

class TWr;

class TIbVerbsBuilderImpl final : public IIbVerbsBuilder {
public:
    TIbVerbsBuilderImpl(size_t hint) noexcept {
        WorkBuf.reserve(hint);
    }

    void AddReadVerb(void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize,
        std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> ioCb) noexcept;
    void AddSendVerb(TRcBuf packet,
        std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> ioCb) noexcept;
    size_t GetVerbsNum() const noexcept;
    ibv_send_wr* BuildListOfVerbs(std::vector<TWr*>& preparedWr, size_t deviceIndex) noexcept;

private:
    struct TWrVerbData {
        ibv_sge Sg;
        ibv_send_wr Wr;
        TRcBuf SendBuf;
        std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> IoCb;
    };
    std::vector<TWrVerbData> WorkBuf;
};

void SetSigHandler() noexcept;

class TSrq {
    struct TRecieveSlot {
        TRcBuf Buffer;
    };
    
public:
    static constexpr ui64 SRQ_WR_MASK = 1ull << 63;
    static constexpr int CqTerminalError = -1;

    struct TCmd {
        ui32 QpNum;
        enum ECmd {
            RegQp,
            DeregQp
        } Cmd;
        std::variant<NActors::TActorId> Target;
    };

    ibv_srq* Get() noexcept { return Srq; }

    int Init(const TRdmaCtx* ctx, const TRdmaRuntimeParams& params, std::shared_ptr<IMemPool> memPool) noexcept {
        if (!memPool || params.MaxSrqWr <= 0 || params.RecieveBufSz <= 0) {
            return EINVAL;
        }
        MemPool = std::move(memPool);
        RecieveBufSz = params.RecieveBufSz;
        DeviceIndex = ctx->GetDeviceIndex();

        struct ibv_srq_init_attr srqInitAttr;
        memset(&srqInitAttr, 0, sizeof(srqInitAttr));

        srqInitAttr.attr.max_wr = params.MaxSrqWr;
        srqInitAttr.attr.max_sge = 1;

        Srq = ibv_create_srq(ctx->GetProtDomain(), &srqInitAttr);
        if (!Srq) {
            return errno;
        }

        const ui64 maxWr = srqInitAttr.attr.max_wr;
        if (maxWr == 0) {
            if (const int destroyErr = Destroy()) {
                return destroyErr;
            }
            return EINVAL;
        }

        Slots.reserve(maxWr);
        PendingRefillSlots.reserve(maxWr);

        for (ui64 i = 0; i < maxWr; i++) {
            auto buffer = MemPool->AllocRcBuf(RecieveBufSz, IMemPool::EMPTY);
            if (!buffer) {
                if (const int destroyErr = Destroy()) {
                    return destroyErr;
                }
                return ENOMEM;
            }
            Slots.emplace_back(TRecieveSlot{std::move(*buffer)});
            auto region = TryExtractFromRcBuf(Slots.back().Buffer);
            if (region.Empty()) {
                if (const int destroyErr = Destroy()) {
                    return destroyErr;
                }
                return EINVAL;
            }
        }

        for (ui64 i = 0; i < maxWr; i++) {
            if (const int err = PostSlot(i, Slots[i].Buffer)) {
                if (const int destroyErr = Destroy()) {
                    return destroyErr;
                }
                return err;
            }
        }

        CommandsClosed = false;
        return 0;
    }

    void ProcessCommands() noexcept {
        for (;;) {
            std::unique_ptr<TCmd> cmd(Queue.Pop());
            if (!cmd) {
                return;
            }

            switch (cmd->Cmd) {
                case TCmd::RegQp:
                    QpActors[cmd->QpNum] = std::get<NActors::TActorId>(cmd->Target);
                    break;
                case TCmd::DeregQp:
                    QpActors.erase(cmd->QpNum);
                    break;
            }
        }
    }

    void DrainCommands() noexcept {
        for (;;) {
            std::unique_ptr<TCmd> cmd(Queue.Pop());
            if (!cmd) {
                return;
            }
        }
    }

    bool HasPendingRefillSlots() const noexcept {
        return !PendingRefillSlots.empty();
    }

    int RetryPendingRefillSlots(size_t maxSlots) noexcept {
        for (size_t i = 0; i < maxSlots && !PendingRefillSlots.empty(); ++i) {
            const ui64 id = PendingRefillSlots.back();
            PendingRefillSlots.pop_back();

            const auto refill = RefillSlot(id);
            switch (refill.Status) {
                case ERefillStatus::Ok:
                    break;
                case ERefillStatus::NoMemory:
                    PendingRefillSlots.push_back(id);
                    return 0;
                case ERefillStatus::Fatal:
                    return refill.Err;
            }
        }
        return 0;
    }

    int HandleWc(NActors::TActorSystem* as, ibv_wc* wc) noexcept {
        ui64 id = wc->wr_id & ~SRQ_WR_MASK;
        Y_DEBUG_ABORT_UNLESS(id < Slots.size());
        if (Y_UNLIKELY(id >= Slots.size())) {
            return CqTerminalError;
        }

        auto it = QpActors.find(wc->qp_num);
        const bool registered = it != QpActors.end();

        TRecieveSlot& slot = Slots[id];
        TRcBuf received;
        int receiveErr = 0;
        if (wc->status == IBV_WC_SUCCESS) {
            if (Y_UNLIKELY(wc->byte_len > slot.Buffer.GetSize())) {
                receiveErr = EMSGSIZE;
            } else {
                slot.Buffer.TrimBack(wc->byte_len);
                received = std::move(slot.Buffer);
            }
        }

        const auto refill = RefillSlot(id);
        if (refill.Status == ERefillStatus::NoMemory) {
            PendingRefillSlots.push_back(id);
        }

        if (!registered) {
            if (refill.Status == ERefillStatus::Fatal) {
                return refill.Err;
            }
            return receiveErr ? CqTerminalError : 0;
        }

        if (wc->status == IBV_WC_SUCCESS && !receiveErr) {
            as->Send(it->second, TEvRdmaIoReceiveDone::Success(std::move(received)));
        } else {
            as->Send(it->second, TEvRdmaIoReceiveDone::WcError(receiveErr ? receiveErr : wc->status));
        }

        if (refill.Status == ERefillStatus::Fatal) {
            return refill.Err;
        }
        return receiveErr ? CqTerminalError : 0;
    }

    void NotifyTerminalError(NActors::TActorSystem* as, int error) noexcept {
        if (!error) {
            return;
        }
        for (const auto& [qpNum, actorId] : QpActors) {
            Y_UNUSED(qpNum);
            if (error > 0) {
                as->Send(actorId, TEvRdmaIoReceiveDone::WrError(error));
            } else {
                as->Send(actorId, TEvRdmaIoReceiveDone::CqError());
            }
        }
        QpActors.clear();
    }

    bool EnqueueCmd(TCmd* cmd) noexcept {
        std::unique_ptr<TCmd> holder(cmd);
        TGuard<TSpinLock> guard(CommandsLock);
        if (CommandsClosed) {
            return false;
        }
        Queue.Push(holder.release());
        return true;
    }

    void CloseCommands() noexcept {
        TGuard<TSpinLock> guard(CommandsLock);
        CommandsClosed = true;
    }

    int Destroy() noexcept {
        CloseCommands();
        DrainCommands();
        if (Srq) {
            if (const int err = ibv_destroy_srq(Srq)) {
                Cerr << "Unable to destroy SRQ, err: " << err << ", errno: " << errno << Endl;
                Y_DEBUG_ABORT_UNLESS(false);
                return err;
            }
            Srq = nullptr;
        }
        Slots.clear();
        PendingRefillSlots.clear();
        QpActors.clear();
        MemPool.reset();
        RecieveBufSz = 0;
        DeviceIndex = 0;
        return 0;
    }

    ~TSrq() {
        Y_UNUSED(Destroy());
    }
private:
    enum class ERefillStatus {
        Ok,
        NoMemory,
        Fatal,
    };

    struct TRefillResult {
        ERefillStatus Status;
        int Err = 0;
    };

    TRefillResult RefillSlot(ui64 id) noexcept {
        if (Y_UNLIKELY(!MemPool || !Srq || id >= Slots.size())) {
            return {ERefillStatus::Fatal, EINVAL};
        }

        auto buffer = MemPool->AllocRcBuf(RecieveBufSz, IMemPool::EMPTY);
        if (!buffer) {
            return {ERefillStatus::NoMemory, ENOMEM};
        }

        if (const int err = PostSlot(id, *buffer)) {
            return {ERefillStatus::Fatal, err};
        }

        Slots[id].Buffer = std::move(*buffer);
        return {ERefillStatus::Ok};
    }

    int PostSlot(ui64 id, TRcBuf& buffer) noexcept {
        if (Y_UNLIKELY(!Srq || id >= Slots.size())) {
            return EINVAL;
        }

        auto region = TryExtractFromRcBuf(buffer);
        if (region.Empty()) {
            return EINVAL;
        }

        ibv_sge sg = {
            .addr = reinterpret_cast<ui64>(region.GetAddr()),
            .length = region.GetSize(),
            .lkey = region.GetLKey(DeviceIndex),
        };
        ibv_recv_wr wr = {
            .wr_id = SRQ_WR_MASK | id,
            .sg_list = &sg,
            .num_sge = 1,
        };
        ibv_recv_wr* badWr = nullptr;
        if (const int err = ibv_post_srq_recv(Srq, &wr, &badWr)) {
            Y_DEBUG_ABORT_UNLESS(badWr == &wr, "unexpected bad wr for single SRQ recv post");
            return err;
        }
        Y_DEBUG_ABORT_UNLESS(!badWr, "bad wr must not be set on successful SRQ recv post");
        if (Y_UNLIKELY(badWr)) {
            return EIO;
        }

        return 0;
    }

    ibv_srq* Srq = nullptr;
    std::shared_ptr<IMemPool> MemPool;
    int RecieveBufSz = 0;
    size_t DeviceIndex = 0;
    std::vector<TRecieveSlot> Slots;
    std::vector<ui64> PendingRefillSlots;
    NThreading::TReadAsFilledQueue<TCmd> Queue;
    TSpinLock CommandsLock;
    bool CommandsClosed = true;
    // TODO: replace with a paged radix/direct map for faster qp_num -> actor lookup on the receive hot path.
    absl::flat_hash_map<ui32, NActors::TActorId> QpActors;
};

class TCqCommon : public ICq {
public:
    TCqCommon(NActors::TActorSystem* as)
        : As(as)
        , Cq(nullptr)
    {}

    virtual ~TCqCommon() = default;

    virtual void ReturnWr(IWr*) noexcept = 0;

    ibv_cq* GetCq() noexcept {
        return Cq;
    }

    ibv_srq* GetSrq() noexcept {
        return Srq.Get();
    }

    int Init(const TRdmaCtx* ctx, const TRdmaRuntimeParams& params, std::shared_ptr<IMemPool> memPool, struct ibv_comp_channel* ch) noexcept {
        Cq = ibv_create_cq(ctx->GetContext(), params.MaxCqe, nullptr, ch, 0);
        if (!Cq) {
            return errno;
        }
        if (params.MaxSrqWr > 0) {
            if (auto err = Srq.Init(ctx, params, std::move(memPool))) {
                return err;
            }
        }
        return 0;
    }

    int Do(std::span<ibv_wc> wc) noexcept {
        return ibv_poll_cq(Cq, wc.size(), &wc.front());
    }

    virtual void Idle() noexcept {
        SpinLockPause();
    }

    void DestroyCq () noexcept {
        if (const int err = Srq.Destroy()) {
            Y_UNUSED(err);
        }
        if (Cq) {
            ibv_destroy_cq(Cq);
        }
    }

protected:
    NActors::TActorSystem* const As;
    ibv_cq* Cq;
    TSrq Srq;
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
        MemoryOrderAcquire(&Cb);
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
        MemoryOrderAcquire(&Cb);
        if (Cb) {
            Cb(as, TEvRdmaIoDone::CqError());
            Cb = TCb();
        }
    }

    void ReplyWrErr(NActors::TActorSystem* as, int err) noexcept {
        MemoryOrderAcquire(&Cb);
        if (Cb) {
            Cb(as, TEvRdmaIoDone::WrError(err));
            Cb = TCb();
            CqCommon->ReturnWr(this);
        }
    }

    void AttachCb(std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> cb) noexcept {
        Cb = std::move(cb);
        MemoryOrderRlease(&Cb);
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
static ICq::TPtr CreateCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, TRdmaRuntimeParams runtimeParams, std::shared_ptr<IMemPool> memPool, NMonitoring::TDynamicCounters* counter) noexcept {
    const ibv_device_attr& attr = ctx->GetDevAttr();
    if (runtimeParams.MaxCqe <= 0) {
        runtimeParams.MaxCqe = attr.max_cqe;
    }
    if (runtimeParams.MaxSrqWr < 0) {
        runtimeParams.MaxSrqWr = attr.max_srq_wr;
    }
    if (runtimeParams.MaxWr <= 0) {
        runtimeParams.MaxWr = runtimeParams.MaxCqe;
    }

    if (runtimeParams.MaxSrqWr > 0) {
        if (attr.max_srq <= 0 || attr.max_srq_wr <= 0 || attr.max_srq_sge < 1) {
            return nullptr; // or fail CQ creation
        }

        runtimeParams.MaxSrqWr = Min(runtimeParams.MaxSrqWr, attr.max_srq_wr);
    }

    auto p = std::make_shared<TCq>(as, runtimeParams.MaxWr, counter);
    int err = p->Init(ctx, runtimeParams, std::move(memPool));
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
            return static_cast<TIbVerbsBuilderImpl*>(VerbsBuilder.get())->BuildListOfVerbs(preparedWr, Qp->GetDeviceIndex());
        }

        std::shared_ptr<TQueuePair> Qp;
        std::unique_ptr<IIbVerbsBuilder> VerbsBuilder;
    };

public:
    TSimpleCqBase(NActors::TActorSystem* as, size_t sz, NMonitoring::TDynamicCounters* c, bool nonBlockingPolling) noexcept
        : TCqCommon(as)
        , Thread(ThreadFunc, this)
        , Finished(false)
        , NonBlockingPolling(nonBlockingPolling)
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
        SetTerminalError(TSrq::CqTerminalError);
    }

    void SetTerminalError(int error) noexcept {
        TerminalError.store(error ? error : TSrq::CqTerminalError, std::memory_order_relaxed);
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
        if (TerminalError.load(std::memory_order_relaxed)) {
            return TErr();
        }
        Waiters.Enqueue(new TWaiterCtx(std::move(qp), std::move(builder)));
        // If the thread may sleep we need to start Wr processing from caller thread. It is not a problem due to thread safe ibverbs api.
        // If we can't finish wr prosessing (no more wr to allocate without waiting) it means there are some verbs infligh so we can process it
        // from cq thread.
        if (!NonBlockingPolling) {
            while (true) {
                if (VerbsBuildingState.Lock.TryAcquire()) {
                    ProcessWr(VerbsBuildingState.CurCtx, VerbsBuildingState.PreparedWr, true);
                    VerbsBuildingState.Lock.Release();
                    return {};
                } else if (!MaybeIdle.load()) {
                    return {};
                }
            }
        }
        return {};
    }

    bool RegisterQpAsync(ui32 qpNum, NActors::TActorId actorId) noexcept override {
        if (TerminalError.load(std::memory_order_relaxed)) {
            return false;
        }
        auto cmd = new TSrq::TCmd{qpNum, TSrq::TCmd::RegQp, actorId};
        return Srq.EnqueueCmd(cmd);
    }

    bool DeregisterQpAsync(ui32 qpNum) noexcept override {
        if (TerminalError.load(std::memory_order_relaxed)) {
            return false;
        }
        auto cmd = new TSrq::TCmd{qpNum, TSrq::TCmd::DeregQp, NActors::TActorId()};
        return Srq.EnqueueCmd(cmd);
    }

    // Build RDMA verbs and post it
    // Returns false if it safe to sleep to wait for cq event
    bool ProcessWr(std::unique_ptr<TWaiterCtx>& ctx, std::vector<TWr*>& preparedWr, bool tryBuildAtOnce) noexcept {
        while (true) {
            if (ctx) {
                TWr* wr = nullptr;
                Queue.Dequeue(&wr);
                if (wr) {
                    preparedWr.emplace_back(wr);
                    if (preparedWr.size() < ctx->GetVerbsNum()) {
                        if (tryBuildAtOnce) {
                            continue;
                        } else {
                            return true;
                        }
                    }

                    ibv_send_wr* wrList = ctx->BuildListOfVerbs(preparedWr);

                    if (TerminalError.load(std::memory_order_relaxed)) {
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
                if (p == nullptr) {
                    // No wr to build
                    return false;
                }
                ctx.reset(p);
            }
        }
        return false;
    }

    static void* ThreadFunc(void* p) {
        TThread::SetCurrentThreadName("RdmaCqThread");
        SetSigHandler();
        TSimpleCqBase* cq = reinterpret_cast<TSimpleCqBase*>(p);
        cq->CqThreadId = pthread_self();
        cq->Loop();
        return nullptr;
    }

    void Loop() noexcept {
        while (Cont.load(std::memory_order_relaxed)) {
            const constexpr size_t wcBatchSize = 16;
            std::array<ibv_wc, wcBatchSize> wcs;
            if (TerminalError.load(std::memory_order_relaxed)) {
                HandleErr();
                Cont.store(false, std::memory_order_relaxed);
            } else {
                int rv = Do(wcs);
                if (rv < 0) {
                    //TODO: Is it correct err handling?
                    SetTerminalError(TSrq::CqTerminalError);
                } else if (rv == 0) {
                    bool idleAllowed = false;
                    MaybeIdle.store(true);
                    if (VerbsBuildingState.Lock.TryAcquire()) {
                        idleAllowed = !ProcessWr(VerbsBuildingState.CurCtx, VerbsBuildingState.PreparedWr, false);
                        if (!idleAllowed) {
                            MaybeIdle.store(false);
                        }
                        VerbsBuildingState.Lock.Release();
                    }
                    if (Srq.HasPendingRefillSlots()) {
                        static constexpr ui64 SrqRefillRetryPeriod = 1024;
                        if ((++SrqRefillRetryCounter & (SrqRefillRetryPeriod - 1)) == 0) {
                            if (auto error = Srq.RetryPendingRefillSlots(1)) {
                                SetTerminalError(error);
                                idleAllowed = false;
                                MaybeIdle.store(false);
                            }
                        }
                        if (Srq.HasPendingRefillSlots()) {
                            idleAllowed = false;
                            MaybeIdle.store(false);
                            SpinLockPause();
                        }
                    } else {
                        SrqRefillRetryCounter = 0;
                    }
                    if (idleAllowed) {
                        if (!TerminalError.load(std::memory_order_relaxed)) {
                            Idle();
                        }
                        MaybeIdle.store(false);
                    }
                } else {
                    Y_ABORT_UNLESS(static_cast<size_t>(rv) <= wcs.size(), "ibv_poll_cq returns more then requested");
                    HandleWc(wcs.data(), rv);
                }
            }
        }
        Finished.store(true, std::memory_order_relaxed);
    }

    void HandleErr() noexcept {
        Srq.CloseCommands();
        Srq.ProcessCommands();
        const int terminalError = TerminalError.load(std::memory_order_relaxed);
        Srq.NotifyTerminalError(As, terminalError);
        for (size_t i = 0; i < WrBuf.size(); i++) {
            TWr* wr = &WrBuf[i];
            wr->ReplyCqErr(As);
            //This it terminal error. Cq should be recreated.
            //So no need to return wr in to the queue
        }
    }



    void HandleWc(ibv_wc* wc, size_t sz) noexcept {
        for (size_t i = 0; i < sz; i++, wc++) {
            if (wc->wr_id & TSrq::SRQ_WR_MASK) {
                Srq.ProcessCommands();
                if (auto error = Srq.HandleWc(As, wc)) {
                    SetTerminalError(error);
                }
            } else {
                TWr* wr = &WrBuf[wc->wr_id];
                double passed = wr->GetTimePassed();
                RdmaDeviceVerbTimeUs->Collect(passed * 1000000.0);
                wr->Reply(As, wc);
                ReturnWr(wr);
            }
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

    void Awake() noexcept {
        if (CqThreadId) {
            pthread_kill(CqThreadId, SIGUSR1);
        }
    }

protected:
    TThread Thread;
    std::atomic<bool> Finished;
    std::atomic<bool> Cont;

    std::vector<TWr> WrBuf;
    std::atomic<size_t> WrCurSz;
    // Queue is used to commnicate with client code (from actors)
    // It is possible to use Single Producer Multiple Consumer queue here but in this case
    // imlementation of Release() methos on IWr* will be musch more difficult
    TLockFreeQueue<TWr*> Queue;

    TLockFreeQueue<TWaiterCtx*> Waiters;

    struct {
        std::unique_ptr<TWaiterCtx> CurCtx;
        std::vector<TWr*> PreparedWr;
        TSpinLock Lock; // Is used to protect VerbsBulding due to cuncurrent access from one poller thred and multiple actor system threads
    } VerbsBuildingState;

    std::atomic<bool> MaybeIdle = false;

    std::atomic<int> TerminalError = 0;
    const bool NonBlockingPolling;
    std::atomic<ui64> Allocated;
    ui64 SrqRefillRetryCounter = 0;
    NMonitoring::THistogramPtr RdmaDeviceVerbTimeUs;
private:
    pthread_t CqThreadId = {};
};

}
