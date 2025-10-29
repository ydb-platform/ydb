#include "ctx.h"
#include "link_manager.h"

#include <vector>

// This file containt dummy implementation of rdma low lewel function for unsupported platforms

namespace NInterconnect::NRdma {

const NLinkMgr::TCtxsMap& NLinkMgr::GetAllCtxs() {
    static NLinkMgr::TCtxsMap dummy;
    return dummy;
}

}
