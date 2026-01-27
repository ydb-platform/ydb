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

NMonitoring::TDynamicCounterPtr MakeCounters(NMonitoring::TDynamicCounters* counters) {
    if (!counters) {
        static NMonitoring::TDynamicCounterPtr dummy(new NMonitoring::TDynamicCounters());
        return dummy;
    }
    return counters;
}

static void SigHandler(int) noexcept {
    // Empty handler. We just need to interrupt read syscall
}

void SetSigHandler() noexcept {
    struct sigaction sigActionData;
    sigemptyset(&sigActionData.sa_mask);
    sigActionData.sa_handler = &SigHandler;
    sigActionData.sa_flags = 0;
    sigaction(SIGUSR1, &sigActionData, nullptr);
}

class TSimpleCq: public TSimpleCqBase {
public:
    TSimpleCq(NActors::TActorSystem* as, size_t sz, NMonitoring::TDynamicCounters* c) noexcept
        : TSimpleCqBase(as, sz, c, true)
    {}

    int Init(const TRdmaCtx* ctx, int maxCqe) noexcept {
        return TSimpleCqBase::Init(ctx, maxCqe, nullptr);
    }

    virtual ~TSimpleCq() {
        // For simple polling mode CQ, we just can destroy ibv CQ without any issues just aftre joining to the thread
        Cont.store(false, std::memory_order_relaxed);
        if (Thread.Running())
            Thread.Join();

        DestroyCq();
    }
};

class TSimpleEventDrivenCq: public TSimpleCqBase {
public:
    TSimpleEventDrivenCq(NActors::TActorSystem* as, size_t sz, NMonitoring::TDynamicCounters* c) noexcept
        : TSimpleCqBase(as, sz, c, false)
    {}

    int Init(const TRdmaCtx* ctx, int maxCqe) noexcept {
        CompChannel = ibv_create_comp_channel(ctx->GetContext());
        if (!CompChannel) {
            return errno;
        }

        int err = TSimpleCqBase::Init(ctx, maxCqe, CompChannel);
        if (err) {
            return err;
        }

        err = ibv_req_notify_cq(Cq, 0);
        if (err) {
            return errno;
        }

        return 0;
    }

    void Idle() noexcept override final {
        struct ibv_cq *evCq = nullptr;
        void *evCtx = nullptr;

        int err = ibv_get_cq_event(CompChannel, &evCq, &evCtx);
        if (err) {
            if (errno != EINTR) {
                NotifyErr();
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
            NotifyErr();
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
        while (!Finished.load(std::memory_order_relaxed)) {
            Awake();
            if (Finished.load(std::memory_order_relaxed)) {
                break;
            }
            ThreadYield();
        }

        // 4. As usual, join and destroy CQ
        if (Thread.Running())
            Thread.Join();

        DestroyCq();

        // 5. Destroy completion event channel
        if (ibv_destroy_comp_channel(CompChannel)) {
            // https://www.rdmamojo.com/2012/10/26/ibv_destroy_comp_channel
            Cerr << "Unable to destroy completion event channel, errno: " << errno << Endl;
            // it should not happen, but if it happens it is not a fatal error for production
            Y_DEBUG_ABORT_UNLESS(false);
        }
    }
private:
    ibv_comp_channel* CompChannel;
};

ICq::TPtr CreateSimpleCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, int maxCqe, int maxWr, NMonitoring::TDynamicCounters* counter) noexcept {
    return CreateCq<TSimpleCq>(ctx, as, maxCqe, maxWr, counter);
}

ICq::TPtr CreateSimpleEventDrivenCq(const TRdmaCtx* ctx, NActors::TActorSystem* as, int maxCqe, int maxWr, NMonitoring::TDynamicCounters* counter) noexcept {
    return CreateCq<TSimpleEventDrivenCq>(ctx, as, maxCqe, maxWr, counter);
}

const int TQueuePair::UnknownQpState = IBV_QPS_UNKNOWN;

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

int TQueuePair::ToErrorState() noexcept {
    struct ibv_qp_attr qpAttr;
    memset(&qpAttr, 0, sizeof(qpAttr));

    qpAttr.qp_state = IBV_QPS_ERR;
    qpAttr.port_num = Ctx->GetPortNum();

    return ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE);
}

int TQueuePair::ToResetState() noexcept {
    struct ibv_qp_attr qpAttr;
    memset(&qpAttr, 0, sizeof(qpAttr));

    qpAttr.qp_state = IBV_QPS_RESET;
    qpAttr.port_num = Ctx->GetPortNum();

    return ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE);
}

int TQueuePair::ToRtsState(const THandshakeData& hd) noexcept {
    // ibv_modify_qp() returns 0 on success, or the value of errno on
    //  failure (which indicates the failure reason).
    {   // modify QP to INIT
        struct ibv_qp_attr qpAttr;
        memset(&qpAttr, 0, sizeof(qpAttr));

        qpAttr.qp_state = IBV_QPS_INIT;
        qpAttr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        qpAttr.pkey_index = 0;
        qpAttr.port_num = Ctx->GetPortNum();

        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (err) {
            return err;
        }
    }

    {   // modify QP to RTR
        struct ibv_qp_attr qpAttr;
        memset(&qpAttr, 0, sizeof(qpAttr));

        qpAttr.qp_state = IBV_QPS_RTR;
        qpAttr.path_mtu = (ibv_mtu)hd.MtuIndex;
        qpAttr.dest_qp_num = hd.QpNum;
        qpAttr.ah_attr.grh.dgid.global.subnet_prefix = hd.SubnetPrefix;
        qpAttr.ah_attr.grh.dgid.global.interface_id = hd.InterfaceId;
        qpAttr.ah_attr.grh.sgid_index = Ctx->GetGidIndex();
        qpAttr.ah_attr.grh.hop_limit = 1;
        qpAttr.ah_attr.is_global = 1;
        qpAttr.ah_attr.port_num = Ctx->GetPortNum();
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
        qpAttr.timeout       = 16;
        qpAttr.retry_cnt     = 2;
        qpAttr.rnr_retry     = 2;

        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
        if (err) {
            return err;
        }
    }

    return 0;
}

int TQueuePair::PostSend(struct ::ibv_send_wr *wr, struct ::ibv_send_wr **bad_wr) noexcept {
    return ibv_post_send(Qp, wr, bad_wr);
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
    if (Ctx) {
        os << ", ctx: " << *Ctx;
    }
}

TQueuePair::TQpState TQueuePair::GetState(bool forseUpdate) const noexcept {
    static_assert(sizeof(ibv_qp_state) <= sizeof(int));
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;

    if (LastState != UnknownQpState && !forseUpdate) {
        return TQpS { .State = LastState };
    }

    int err = ibv_query_qp(Qp, &attr, IBV_QP_STATE, &init_attr);

    if (err) {
        return TQpErr { .Err = err };
    }

    LastState = attr.qp_state;

    return TQpS { .State = attr.qp_state };
}

TRdmaCtx* TQueuePair::GetCtx() const noexcept {
    return Ctx;
}

ui32 TQueuePair::GetMinMtuIndex(ui32 mtuIndex) const noexcept {
    return std::min(mtuIndex, (ui32)Ctx->GetPortAttr().active_mtu);
}

size_t TQueuePair::GetDeviceIndex() const noexcept {
    return Ctx->GetDeviceIndex();
}

bool TQueuePair::IsRtsState(TQpS state) noexcept {
     enum ibv_qp_state qpState = static_cast<enum ibv_qp_state>(state.State);
     return qpState == IBV_QPS_RTS;
}

THandshakeData TQueuePair::GetHandshakeData() const noexcept {
    return THandshakeData {
        .QpNum = GetQpNum(),
        .SubnetPrefix = Ctx->GetGid().global.subnet_prefix,
        .InterfaceId = Ctx->GetGid().global.interface_id,
        .MtuIndex = Ctx->GetPortAttr().active_mtu
    };
}

void TIbVerbsBuilderImpl::AddReadVerb(void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize,
    std::function<void(NActors::TActorSystem* as, TEvRdmaIoDone*)> ioCb) noexcept
{
    WorkBuf.emplace_back(
        TWrVerbData {
            .Sg = {
                .addr = (ui64)mrAddr,
                .length = dstSize,
                .lkey = mrlKey,
            },
            .Wr = {
               .wr_id = 0/*wrId*/,
               .sg_list = nullptr,
               .num_sge = 1,
               .opcode = IBV_WR_RDMA_READ,
               .send_flags = IBV_SEND_SIGNALED,
               .wr = {
                   .rdma = {
                       .remote_addr = (ui64)dstAddr,
                       .rkey = dstRkey,
                    },
                },
            },
            .IoCb = std::move(ioCb)
        }
    );
}

ibv_send_wr* TIbVerbsBuilderImpl::BuildListOfVerbs(std::vector<TWr*>& wr) noexcept {
    Y_ABORT_UNLESS(wr.size() == WorkBuf.size());
    Y_ABORT_UNLESS(wr.size());

    WorkBuf[0].Wr.sg_list = &WorkBuf[0].Sg;
    WorkBuf[0].Wr.wr_id = wr[0]->GetId();
    wr[0]->AttachCb(std::move(WorkBuf[0]).IoCb);

    for (size_t i = 1; i < WorkBuf.size(); i++) {
        WorkBuf[i].Wr.sg_list = &WorkBuf[i].Sg;
        WorkBuf[i - 1].Wr.next = &WorkBuf[i].Wr;
        WorkBuf[i].Wr.wr_id = wr[i]->GetId();
        wr[i]->AttachCb(std::move(WorkBuf[i]).IoCb);
        wr[i]->ResetTimer();
    }

    return &WorkBuf[0].Wr;
}

size_t TIbVerbsBuilderImpl::GetVerbsNum() const noexcept {
    return WorkBuf.size();
}

// Creates builder for work requests, hint - number of expected verbs to preallocate memory
std::unique_ptr<IIbVerbsBuilder> CreateIbVerbsBuilder(size_t hint) noexcept {
    return std::make_unique<TIbVerbsBuilderImpl>(hint);
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

template<>
void Out<std::shared_ptr<NInterconnect::NRdma::TQueuePair>>(IOutputStream& os, const std::shared_ptr<NInterconnect::NRdma::TQueuePair>& qp) {
    if (qp) {
        os << "[";
        qp->Output(os);
        os << "]";
    } else {
        os << "[none]";
    }
}

IOutputStream& operator<<(IOutputStream& os, const std::shared_ptr<NInterconnect::NRdma::TQueuePair>& qp) {
    Out<std::shared_ptr<NInterconnect::NRdma::TQueuePair>>(os, qp);
    return os;
}

template<>
void Out<NInterconnect::NRdma::THandshakeData>(IOutputStream& os, const NInterconnect::NRdma::THandshakeData& hd) {
    ibv_gid gid;
    gid.global.subnet_prefix = hd.SubnetPrefix;
    gid.global.interface_id = hd.InterfaceId; 
    os << "[" << hd.QpNum << ", " << gid << ", " << hd.MtuIndex << "]";
}

IOutputStream& operator<<(IOutputStream& os, const NInterconnect::NRdma::THandshakeData& hd) {
    Out<NInterconnect::NRdma::THandshakeData>(os, hd);
    return os;
}
