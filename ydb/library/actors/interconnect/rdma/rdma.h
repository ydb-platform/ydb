#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

extern "C" {
struct ibv_qp;
struct ibv_cq; 
union ibv_gid; 
}

class IOutputStream;

namespace NInterconnect::NRdma {

class TRdmaCtx;

class TQueuePair: public NNonCopyable::TMoveOnly {
public:
    TQueuePair() = default;
    ~TQueuePair();
    int Init(TRdmaCtx* ctx) noexcept;
    int ToRtsState(TRdmaCtx* ctx, ui32 qpNum, const ibv_gid& gid, int mtuIndex) noexcept;
    int SendRdmaReadWr(ui64 wrId, void* mrAddr, ui32 mrlKey, void* dstAddr, ui32 dstRkey, ui32 dstSize) noexcept;
    ui32 GetQpNum() const noexcept;
    void Output(IOutputStream&) const noexcept;

    void ProcessCq() noexcept;

private:
    ibv_qp* Qp = nullptr;
    //Should not be here!
    ibv_cq* Cq = nullptr;
};

}
