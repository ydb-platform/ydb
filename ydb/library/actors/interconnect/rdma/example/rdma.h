#pragma once

#include <ydb/library/actors/interconnect/rdma/rdma_link_manager.h>
#include <ydb/library/actors/interconnect/rdma/rdma_ctx.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

struct TContext {
    ibv_gid_entry Entry;
    ui32 DeviceIndex;
    NInterconnect::NRdma::TRdmaCtx* Ctx;
    std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;
    ibv_port_attr PortAttr;
    ibv_device_attr DevAttrs;
    ibv_cq *Cq;
    ibv_qp *Qp;

    TContext(ibv_gid_entry entry, ui32 deviceIndex, NInterconnect::NRdma::TRdmaCtx* ctx, std::shared_ptr<NInterconnect::NRdma::IMemPool> memPool);

    ~TContext();

    int InitQp();

    int MoveQpToRTS(ibv_gid dstGidEntry, ui32 dstQpNum);
};

std::tuple<ui32, ibv_gid_entry, NInterconnect::NRdma::TRdmaCtx*> GetRdmaCtx(ui32 gidIndex);
void SendRdmaReadWr(TContext& ctx, ui64 wrId, void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize);
ibv_qp_state GetQpState(ibv_qp* qp);
