#include "rdma.h"
#include "ctx.h"
#include <util/stream/output.h>

#include <util/datetime/base.h>
namespace NInterconnect::NRdma {

TQueuePair::~TQueuePair() {
    if (Qp) {
        ibv_destroy_qp(Qp);
    }
    if (Cq) {
        ibv_destroy_cq(Cq);
    }
}

int TQueuePair::Init(TRdmaCtx* ctx) noexcept {
    const ibv_device_attr& attr = ctx->GetDevAttr();
    Cq = ibv_create_cq(ctx->GetContext(), attr.max_cqe, nullptr, nullptr, 0);
    ibv_qp_init_attr qpInitAttr = {
        .send_cq = Cq,
        .recv_cq = Cq,
        .cap = {
            .max_send_wr = static_cast<ui32>(attr.max_qp_wr),
            .max_recv_wr = static_cast<ui32>(attr.max_qp_wr),
            .max_send_sge = static_cast<ui32>(attr.max_sge),
            .max_recv_sge = static_cast<ui32>(attr.max_sge),
            .max_inline_data = 0,
        },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0,
    };

    Qp = ibv_create_qp(ctx->GetProtDomain(), &qpInitAttr);
    if (Qp) {
        return 0;
    } else {
        return -1;
    }
}

int TQueuePair::ToRtsState(TRdmaCtx* ctx, ui32 qpNum, const ibv_gid& gid, ibv_mtu mtuIndex) noexcept {
    // ibv_modify_qp() returns 0 on success, or the value of errno on
    //  failure (which indicates the failure reason).

    {   // modify QP to INIT
        struct ibv_qp_attr qpAttr;
        memset(&qpAttr, 0, sizeof(qpAttr));

        qpAttr.qp_state = IBV_QPS_INIT;
        qpAttr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        qpAttr.pkey_index = 0;
        qpAttr.port_num = static_cast<ui8>(1); //TODO

        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (err) {
            return err;
        }
    }

    {   // modify QP to RTR
        struct ibv_qp_attr qpAttr;
        memset(&qpAttr, 0, sizeof(qpAttr));

        qpAttr.qp_state = IBV_QPS_RTR;
        qpAttr.path_mtu = mtuIndex;
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
        qpAttr.retry_cnt     = 7;
        qpAttr.rnr_retry     = 7;

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

void TQueuePair::ProcessCq() noexcept {
    const int wcBatchSize = 1;
    std::vector<ibv_wc> wcs(wcBatchSize);

    int i = 0;

    // Just for test.
    while (i < 10) {
        int numComp = ibv_poll_cq(Cq, wcBatchSize, &wcs.front());
        if (numComp < 0) {
            Cerr << "ibv_poll_cq failed: " << strerror(errno) << Endl;
            return;
        }
        Cerr << "DONE " << wcs.front().wr_id << " " << ibv_wc_status_str(wcs.front().status)  << " " << wcs.front().qp_num << Endl;
        Sleep(TDuration::Seconds(1));
        i++;
    }
}

}
