#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

#include "ibdrv/include/infiniband/verbs.h"

namespace NInterconnect::NRdma {

class TRdmaCtx;

class TQueuePair: public TNonCopyable {
public:
    TQueuePair() = default;
    ~TQueuePair();
    int Init(TRdmaCtx* ctx) noexcept;
    int ToRtsState(TRdmaCtx* ctx, ui32 qpNum, const ibv_gid& gid, ibv_mtu mtuIndex) noexcept;
    int SendRdmaReadWr(ui64 wrId, void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize) noexcept;
    ui32 GetQpNum() const noexcept {
        return Qp->qp_num;
    }

    void ProcessCq() noexcept;

private:
    ibv_qp* Qp = nullptr;
    //Should not be here!
    ibv_cq* Cq = nullptr;
};


}
