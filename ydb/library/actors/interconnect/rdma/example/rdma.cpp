#include "rdma.h"


#include <util/generic/vector.h>
#include <util/stream/output.h>


TContext::TContext(NInterconnect::NRdma::TRdmaCtx* ctx, std::shared_ptr<NInterconnect::NRdma::IMemPool> memPool)
    : Ctx(ctx)
    , MemPool(memPool)
    , Cq(nullptr)
    , Qp(nullptr)
{
}

TContext::~TContext() {
    if (Cq) {
        ibv_destroy_cq(Cq);
    }
    if (Qp) {
        ibv_destroy_qp(Qp);
    }
}

int TContext::InitQp() {    
    int err = ibv_query_port(Ctx->GetContext(), Ctx->GetPortNum(), &PortAttr);
    if (err) {
        Cerr << "ibv_query_port failed: " << strerror(errno) << Endl;
        return 1;
    }

    err = ibv_query_device(Ctx->GetContext(), &DevAttrs);
    if (err) {
        Cerr << "ibv_query_device failed: " << strerror(errno) << Endl;
        return 1;
    }

    Cq = ibv_create_cq(Ctx->GetContext(), DevAttrs.max_cqe, nullptr, nullptr, 0);
    if (!Cq) {
        Cerr << "ibv_create_cq failed: " << strerror(errno) << Endl;
        return 1;
    }

    ibv_qp_init_attr qpInitAttr = {
        .send_cq = Cq,
        .recv_cq = Cq,
        .cap = {
            .max_send_wr = static_cast<ui32>(DevAttrs.max_qp_wr),
            .max_recv_wr = static_cast<ui32>(DevAttrs.max_qp_wr),
            .max_send_sge = static_cast<ui32>(DevAttrs.max_sge),
            .max_recv_sge = static_cast<ui32>(DevAttrs.max_sge),
        },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0,
    };

    Qp = ibv_create_qp(Ctx->GetProtDomain(), &qpInitAttr);
    if (!Qp) {
        Cerr << "ibv_create_qp failed: " << strerror(errno) << Endl;
        return 1;
    }

    return 0;
}

ibv_qp_state GetQpState(ibv_qp* qp) {
    ibv_qp_attr qpAttr;
    ibv_qp_init_attr qpInitAttr;
    int ret = ibv_query_qp(qp, &qpAttr, IBV_QP_STATE, &qpInitAttr);
    if (ret) {
        Cerr << "ibv_query_qp failed: " << strerror(errno) << Endl;
        return IBV_QPS_ERR;
    }
    return qpAttr.qp_state;
}

int TContext::MoveQpToRTS(ibv_gid dstGidEntry, ui32 dstQpNum) {
    if (!Qp) {
        Cerr << "QP is not initialized" << Endl;
        return 1;
    }

    {   // modify QP to INIT
        struct ibv_qp_attr qpAttr = {
            .qp_state = IBV_QPS_INIT,
            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
            .pkey_index = 0,
            .port_num = static_cast<ui8>(Ctx->GetPortNum()),
        };
    
        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
        if (err) {
            Cerr << "ibv_modify_qp failed: " << strerror(errno) << Endl;
            return 1;
        }

        auto state = GetQpState(Qp);
        if (state != IBV_QPS_INIT) {
            Cerr << "QP state is not INIT after modification: " << (int)state << Endl;
            return 1;
        }
    }

    Cout << "QP in INIT" << Endl;

    {   // modify QP to RTR
        struct ibv_qp_attr qpAttr = {
            .qp_state = IBV_QPS_RTR,
            .path_mtu = PortAttr.active_mtu,
            .rq_psn = 0,
            .dest_qp_num = dstQpNum,
            .ah_attr = {
                .grh = {
                    .dgid = dstGidEntry,
                    .sgid_index = static_cast<ui8>(Ctx->GetGidIndex()),
                    .hop_limit = 1,
                },
                .sl = 0,
                .src_path_bits = 0,
                .is_global = 1,
                .port_num = static_cast<ui8>(Ctx->GetPortNum()),
            },
            .max_dest_rd_atomic = 1,
            .min_rnr_timer = 12,
        };
    
        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
        if (err) {
            Cerr << "ibv_modify_qp failed: " << strerror(errno) << Endl;
            return 1;
        }

        auto state = GetQpState(Qp);
        if (state != IBV_QPS_RTR) {
            Cerr << "QP state is not RTR after modification: " << (int)state << Endl;
            return 1;
        }
    }

    Cout << "QP in RTR" << Endl;

    {   // modify QP to RTS
        struct ibv_qp_attr  qpAttr = {
            .qp_state      = IBV_QPS_RTS,
            .sq_psn        = 0,
            .max_rd_atomic = 1,
            .timeout       = 14,
            .retry_cnt     = 7,
            .rnr_retry     = 7,
        };

        int err = ibv_modify_qp(Qp, &qpAttr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
        if (err) {
            Cerr << "ibv_modify_qp failed: " << strerror(errno) << Endl;
            return 1;
        }

        auto state = GetQpState(Qp);
        if (state != IBV_QPS_RTS) {
            Cerr << "QP state is not RTS after modification: " << (int)state << Endl;
            return 1;
        }
    }

    Cout << "QP in RTS" << Endl;

    return 0;
}


void SendRdmaReadWr(TContext& ctx, ui64 wrId, void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize) {
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
    
    if (ibv_post_send(ctx.Qp, &wr, &bad_wr)) {
        Cerr << "ibv_post_send failed: " << strerror(errno) << Endl;
    }
}
