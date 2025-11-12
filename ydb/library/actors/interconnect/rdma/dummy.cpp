#include "ctx.h"
#include "link_manager.h"
#include "rdma.h"

#include <util/stream/output.h>
#include <vector>

// This file containt dummy implementation of rdma low lewel function for unsupported platforms

namespace NInterconnect::NRdma {

const NLinkMgr::TCtxsMap& NLinkMgr::GetAllCtxs() {
    static NLinkMgr::TCtxsMap dummy;
    return dummy;
}

// Same is in ibverbs
const int TQueuePair::UnknownQpState = 7;

std::unique_ptr<IIbVerbsBuilder> CreateIbVerbsBuilder(size_t) noexcept { return {}; }

TQueuePair::~TQueuePair() { }

int TQueuePair::Init(TRdmaCtx*, ICq*, int) noexcept { return 1; }

ui32 TQueuePair::GetMinMtuIndex(ui32) const noexcept { return 0; }

size_t TQueuePair::GetDeviceIndex() const noexcept { return 0; }

ui32 TQueuePair::GetQpNum() const noexcept { return 0; }

int TQueuePair::ToRtsState(const THandshakeData&) noexcept { return 0; }

THandshakeData TQueuePair::GetHandshakeData() const noexcept {
    return THandshakeData {0, 0, 0, 0};
}

namespace NLinkMgr {
    TRdmaCtx* GetCtx(NInterconnect::TAddress const&) { return nullptr; }
    bool Init() { return false; }
}

}

template<>
void Out<std::shared_ptr<NInterconnect::NRdma::TQueuePair>>(IOutputStream& os, const std::shared_ptr<NInterconnect::NRdma::TQueuePair>&) {
    os << "[dummy qp]";
}

template<>
void Out<NInterconnect::NRdma::THandshakeData>(IOutputStream& os, const NInterconnect::NRdma::THandshakeData&) {
    os << "[dummy handshake data]";
}

IOutputStream& operator<<(IOutputStream& os, const NInterconnect::NRdma::THandshakeData& hd) {
    Out<NInterconnect::NRdma::THandshakeData>(os, hd);
    return os;
}
