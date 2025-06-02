#pragma once

#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

struct TContext {
    ibv_gid Gid;
    ui32 DeviceIndex;
    NInterconnect::NRdma::TRdmaCtx* Ctx;
    std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;
    ibv_port_attr PortAttr;
    ibv_device_attr DevAttrs;
    ibv_cq *Cq;
    ibv_qp *Qp;

    TContext(NInterconnect::NRdma::TRdmaCtx* ctx, std::shared_ptr<NInterconnect::NRdma::IMemPool> memPool);

    ~TContext();

    int InitQp();

    int MoveQpToRTS(ibv_gid dstGidEntry, ui32 dstQpNum);
};

void SendRdmaReadWr(TContext& ctx, ui64 wrId, void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize);
ibv_qp_state GetQpState(ibv_qp* qp);
