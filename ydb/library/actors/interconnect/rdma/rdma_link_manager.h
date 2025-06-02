#pragma once

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/generic/fwd.h>

#include <unordered_map>


struct in6_addr; 
struct ibv_gid_entry;

template <>
struct std::hash<ibv_gid> {
    std::size_t operator()(const ibv_gid& k) const;
};
template <>
struct std::equal_to<ibv_gid> {
    bool operator()(const ibv_gid& a, const ibv_gid& b) const;
};

namespace NInterconnect::NRdma {
class TRdmaCtx;
}

// LinkManager is a component returning global context associated
// with RDMA device by given ipv6 address.
// In case of ipv4 address must be propogated to ipv6 
namespace NInterconnect::NRdma::NLinkMgr {

using TCtxsMap = std::unordered_map<ibv_gid, std::shared_ptr<NInterconnect::NRdma::TRdmaCtx>>;

TRdmaCtx* GetCtx(int sockfd);
TRdmaCtx* GetCtx(const in6_addr& );
const TCtxsMap& GetAllCtxs();

}
