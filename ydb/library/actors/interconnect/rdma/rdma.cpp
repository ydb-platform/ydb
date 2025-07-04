#include "rdma.h"
#include "ctx.h"
#include "events.h"
#include <util/stream/output.h>
#include <util/thread/lfqueue.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/datetime/base.h>
#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <thread>

namespace NInterconnect::NRdma {

class TCqCommon : public ICq {
public:
    TCqCommon(NActors::TActorSystem* as)
        : As(as)
        , Cq(nullptr)
    {}

    virtual ~TCqCommon() {
        if (Cq) {
            ibv_destroy_cq(Cq);
        }
    }

    virtual void ReturnWr(IWr*) noexcept = 0;

    ibv_cq* GetCq() noexcept {
        return Cq;
    }

    int Init(const TRdmaCtx* ctx, int max_cqe) noexcept {
        const ibv_device_attr& attr = ctx->GetDevAttr();
        if (max_cqe <= 0) {
            max_cqe = attr.max_cqe;
        }
        Cq = ibv_create_cq(ctx->GetContext(), max_cqe, nullptr, nullptr, 0);
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

    void ReplyErr(NActors::TActorSystem* as) noexcept {
        if (Cb) {
            Cb(as, TEvRdmaIoDone::CqError());
            Cb = TCb();
        }
    }

    void AttachCb(std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> cb) noexcept {
        Cb = std::move(cb);
    }

private:
    const ui64 Id;
    TCqCommon* const CqCommon;
    using TCb = std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)>;
    TCb Cb;
};

class TSimpleCq : public TCqCommon {
public:
    TSimpleCq(NActors::TActorSystem* as, size_t sz) noexcept
        : TCqCommon(as)
        , Err(false)
    {
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

    ~TSimpleCq() {
        Cont.store(false, std::memory_order_relaxed);
        if (Thread)
             Thread->join();
    }

    TAllocResult AllocWr(std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> cb) noexcept override  {
        if (Err.load(std::memory_order_relaxed)) {
            return TErr();
        }
        TWr* wr = nullptr;
        Queue.Dequeue(&wr);
        if (wr) {
            wr->AttachCb(std::move(cb));
            if (Err.load(std::memory_order_relaxed)) {
                return TErr();
            }
            return static_cast<IWr*>(wr);
        } else {
            return  TBusy();
        }
    }

    void ReturnWr(IWr* wr) noexcept override {
        Queue.Enqueue(static_cast<TWr*>(wr));
    }

    void NotifyErr() noexcept override {
        Err.store(true, std::memory_order_relaxed);
    }

    void Loop() noexcept {
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
                    Idle();
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
            wr->ReplyErr(As);
            Queue.Enqueue(wr);
        }
    }

    void HandleWc(ibv_wc* wc, size_t sz) noexcept {
        for (size_t i = 0; i < sz; i++, wc++) {
            TWr* wr = &WrBuf[wc->wr_id];
            wr->Reply(As, wc);
            Queue.Enqueue(wr); 
        }
    }

    int Start() noexcept {
        Cont.store(true, std::memory_order_relaxed);
        try {
            Thread.emplace(&TSimpleCq::Loop, this);
        } catch (std::exception& ex) {
            Cerr << "Unable to launch cq poller thread" << Endl;
            return 1;
        }
        return 0;
    }
private:
    std::optional<std::thread> Thread;
    std::atomic<bool> Cont;

    std::vector<TWr> WrBuf;
    std::atomic<size_t> WrCurSz;
    // Queue is used to commnicate with client code (from actors)
    // It is possible to use Single Producer Multiple Consumer queue here but in this case
    // imlementation of Release() methos on IWr* will be musch more difficult
    TLockFreeQueue<TWr*> Queue;
    std::atomic<bool> Err;
};

ICq::TPtr ICq::MakeSimpleCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, int max_cqe) noexcept {
    auto p = std::make_shared<TSimpleCq>(as, 16);
    int err = p->Init(ctx, max_cqe);
    if (err) {
        return nullptr;
    }
    err = p->Start();
    if (err) {
       return nullptr;
    }

    return p;
}

TQueuePair::~TQueuePair() {
    if (Qp) {
        ibv_destroy_qp(Qp);
    }
}

int TQueuePair::Init(TRdmaCtx* ctx, ICq* icq, int maxWr) noexcept {
    Ctx = ctx;
    ibv_cq* cq = icq->GetCq();
    Y_ABORT_UNLESS(cq);
    const ibv_device_attr& attr = ctx->GetDevAttr();

    if (maxWr < 0) {
        maxWr = attr.max_qp_wr;
    }

    ibv_qp_init_attr qpInitAttr;
    bzero(&qpInitAttr, sizeof(qpInitAttr));
    qpInitAttr.send_cq = cq;
    qpInitAttr.recv_cq = cq;
    qpInitAttr.cap.max_send_wr = static_cast<ui32>(maxWr);
    qpInitAttr.cap.max_recv_wr = static_cast<ui32>(maxWr);
    qpInitAttr.cap.max_send_sge = static_cast<ui32>(attr.max_sge);
    qpInitAttr.cap.max_recv_sge = static_cast<ui32>(attr.max_sge);
    qpInitAttr.qp_type = IBV_QPT_RC;

    TStringStream ss;
    ctx->Output(ss);

    Qp = ibv_create_qp(ctx->GetProtDomain(), &qpInitAttr);
    if (Qp) {
        return 0;
    } else {
        return errno;
    }
}

int TQueuePair::ToRtsState(TRdmaCtx* ctx, ui32 qpNum, const ibv_gid& gid, int mtuIndex) noexcept {
    // ibv_modify_qp() returns 0 on success, or the value of errno on
    //  failure (which indicates the failure reason).
    {   // modify QP to INIT
        struct ibv_qp_attr qpAttr;
        memset(&qpAttr, 0, sizeof(qpAttr));

        qpAttr.qp_state = IBV_QPS_INIT;
        qpAttr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        qpAttr.pkey_index = 0;
        qpAttr.port_num = ctx->GetPortNum();

        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (err) {
            return err;
        }
    }

    {   // modify QP to RTR
        struct ibv_qp_attr qpAttr;
        memset(&qpAttr, 0, sizeof(qpAttr));

        qpAttr.qp_state = IBV_QPS_RTR;
        qpAttr.path_mtu = (ibv_mtu)mtuIndex;
        qpAttr.dest_qp_num = qpNum;
        qpAttr.ah_attr.grh.dgid = gid;
        qpAttr.ah_attr.grh.sgid_index = ctx->GetGidIndex();
        qpAttr.ah_attr.grh.hop_limit = 1;
        qpAttr.ah_attr.is_global = 1;
        qpAttr.ah_attr.port_num = ctx->GetPortNum();
        qpAttr.max_dest_rd_atomic = 1;
        qpAttr.min_rnr_timer = 12;

        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
        if (err) {
            return err;
        }
    }

    {   // modify QP to RTS
        struct ibv_qp_attr  qpAttr;
        memset(&qpAttr, 0, sizeof(qpAttr));
        qpAttr.qp_state      = IBV_QPS_RTS;
        qpAttr.sq_psn        = 0;
        qpAttr.max_rd_atomic = 1;
        qpAttr.timeout       = 14;
        qpAttr.retry_cnt     = 4;
        qpAttr.rnr_retry     = 4;

        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
        if (err) {
            return err;
        }
    }

    return 0;
} 

int TQueuePair::SendRdmaReadWr(ui64 wrId, void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize) noexcept {
    ibv_sge sg = {
        .addr = (ui64)mrAddr,
        .length = dstSize,
        .lkey = mrlKey,
    };
    ibv_send_wr wr {
        .wr_id = wrId,
        .sg_list = &sg,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .rdma = {
                .remote_addr = (ui64)dstAddr,
                .rkey = dstRkey,
            },
        },
    };
    struct ibv_send_wr *bad_wr;

    return ibv_post_send(Qp, &wr, &bad_wr);
}

ui32 TQueuePair::GetQpNum() const noexcept {
   return Qp->qp_num;
}

void TQueuePair::Output(IOutputStream& os) const noexcept {
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    int err = ibv_query_qp(Qp, &attr,
        IBV_QP_STATE, &init_attr);

    os << GetQpNum() << ",";
    if (err) {
        os << "query err: " << err;
    } else {
        os << attr.qp_state;
    }
}

TRdmaCtx* TQueuePair::GetCtx() const noexcept {
    return Ctx;
}

}

template<>
void Out<ibv_qp_state>(IOutputStream& os, ibv_qp_state state) {
    switch (state) {
        case IBV_QPS_RESET:
            os << "QPS_RESET";
            break;
        case IBV_QPS_INIT:
            os << "QPS_INIT";
            break;
        case IBV_QPS_RTR:
            os << "QPS_RTR";
            break;
        case IBV_QPS_RTS:
            os << "QPS_RTS";
            break;
        case IBV_QPS_SQD:
            os << "QPS_SQD";
            break;
        case IBV_QPS_SQE:
            os << "QPS_SQE";
            break;
        case IBV_QPS_ERR:
            os << "QPS_ERR";
            break;
        case IBV_QPS_UNKNOWN:
            os << "QPS_UNKNOWN";
            break;
        default: 
            Y_DEBUG_ABORT_UNLESS(false, "unknown qp state");
            os << "???";
    }
}

template<>
void Out<std::unique_ptr<NInterconnect::NRdma::TQueuePair>>(IOutputStream& os, const std::unique_ptr<NInterconnect::NRdma::TQueuePair>& qp) {
    if (qp) {
        os << "[";
        qp->Output(os);
        os << "]";
    } else {
        os << "[none]"; 
    }
}

IOutputStream& operator<<(IOutputStream& os, const std::unique_ptr<NInterconnect::NRdma::TQueuePair>& qp) {
    Out<std::unique_ptr<NInterconnect::NRdma::TQueuePair>>(os, qp);
    return os;
}
