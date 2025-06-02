#pragma once

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/generic/fwd.h>


struct in6_addr;

namespace NInterconnect::NRdma {
class TRdmaCtx;
}

// LinkManager is a component returning global context associated
// with RDMA device by given ipv6 address.
// In case of ipv4 address must be propogated to ipv6 
namespace NInterconnect::NRdma::NLinkMgr {

using TCtxsMap = std::vector<std::pair<ibv_gid, std::shared_ptr<NInterconnect::NRdma::TRdmaCtx>>>;

TRdmaCtx* GetCtx(int sockfd);
TRdmaCtx* GetCtx(const in6_addr& );
const TCtxsMap& GetAllCtxs();

}
