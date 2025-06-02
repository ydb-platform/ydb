#include "rdma_link_manager.h"
#include "rdma_ctx.h"

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/printf.h>

#include <errno.h>

#include <arpa/inet.h>

#include <unordered_map>
#include <memory>

#include <util/network/address.h>

std::size_t std::hash<ibv_gid>::operator()(const ibv_gid& k) const {
    using std::size_t;
    using std::hash;

    return hash<ui64>()(k.global.subnet_prefix)
        ^ hash<ui64>()(k.global.interface_id);
}

bool std::equal_to<ibv_gid>::operator()(const ibv_gid& a, const ibv_gid& b) const {
    return a.global.interface_id == b.global.interface_id
        && a.global.subnet_prefix == b.global.subnet_prefix;
}

namespace NInterconnect::NRdma {

TDeviceCtx::~TDeviceCtx() {
    ibv_dealloc_pd(ProtDomain);
    ibv_close_device(Context);
}

std::shared_ptr<TRdmaCtx> TRdmaCtx::Create(std::shared_ptr<TDeviceCtx> deviceCtx, ui32 portNum, int gidIndex) {
    const char* deviceName = ibv_get_device_name(deviceCtx->Context->device);

    ibv_device_attr devAttr;
    int err = ibv_query_device(deviceCtx->Context, &devAttr);
    if (err) {
        Cerr << "ibv_query_device failed on device " << deviceName << " : " << strerror(errno) << Endl;
        return nullptr;
    }

    ibv_port_attr portAttr;
    err = ibv_query_port(deviceCtx->Context, portNum, &portAttr);
    if (err) {
        Cerr << "ibv_query_port failed on device " << deviceName << " : " << strerror(errno) << Endl;
        return nullptr;
    }

    if (portAttr.link_layer != IBV_LINK_LAYER_ETHERNET) {
        Cerr << "Port " << (int)portNum << " on device " << deviceName << " is not RoCE" << Endl;
        return nullptr;
    }

    ibv_gid gid;
    err = ibv_query_gid(deviceCtx->Context, portNum, gidIndex, &gid);
    if (err) {
        Cerr << "ibv_query_gid failed on device " << deviceName << " : " << strerror(errno) << Endl;
        return nullptr;
    }

    if (gid.global.interface_id == 0) {
        // Cerr << "GID is not valid on device " << deviceName << ", port " << (int)portNum << ", gid index " << gidIndex << Endl;
        return nullptr;
    }

    TRdmaCtx* ctx = new TRdmaCtx(std::move(deviceCtx), devAttr, deviceName, portNum, portAttr, gidIndex, gid);
    return std::shared_ptr<TRdmaCtx>(ctx);
    // return std::make_shared<TRdmaCtx>(std::move(deviceCtx), devAttr, deviceName, portNum, portAttr, gidIndex, gid);
}

}

namespace NInterconnect::NRdma::NLinkMgr {

static class TRdmaLinkManager {
    using TCtxsMap = std::unordered_map<ibv_gid, std::shared_ptr<NInterconnect::NRdma::TRdmaCtx>>;
public:
    const TCtxsMap& GetAllCtxs() {
        return CtxMap;
    }

    TRdmaCtx* GetCtx(const ibv_gid* gid) {
        auto it = CtxMap.find(*gid);
        if (it == CtxMap.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    TRdmaLinkManager() {
        ScanDevices();
    }
    
private:
    TCtxsMap CtxMap;
    void ScanDevices() {
        int numDevices;
        int err;
        ibv_device** deviceList = ibv_get_device_list(&numDevices);
        if (!deviceList) {
            ErrNo = errno;
            Err = TString(strerror(errno));
            return;
        }

        for (int i = 0; i < numDevices; i++) {
            ibv_device* dev = deviceList[i];
            ibv_context* ctx = ibv_open_device(dev);
            if (!ctx) {
                Err = Sprintf("Failed to open ib device '%s'", ibv_get_device_name(dev));
                continue;
            }
            Y_DEFER{ ibv_free_device_list(deviceList); };

            ibv_pd* pd = ibv_alloc_pd(ctx);
            if (!pd) {
                ibv_close_device(ctx);
                continue;
            }

            auto deviceCtx = std::make_shared<TDeviceCtx>(ctx, pd);

            ibv_device_attr devAttrs;
            err = ibv_query_device(ctx, &devAttrs);

            if (err < 0) {
                continue;
            }

            for (uint8_t portNum = 1; portNum <= devAttrs.phys_port_cnt; portNum++) {
                ibv_port_attr portAttrs;
                err = ibv_query_port(ctx, portNum, &portAttrs);
                if (err == 0) {
                    for (int gidIndex = 0; gidIndex < portAttrs.gid_tbl_len; gidIndex++ ) {
                        auto ctx = TRdmaCtx::Create(deviceCtx, portNum, gidIndex);
                        if (!ctx) {
                            // Cerr << "Failed to create RDMA context for device " << ibv_get_device_name(dev) 
                            //      << ", port " << (int)portNum << ", gid index " << gidIndex << Endl;
                            continue;
                        }

                        char gidStr[INET6_ADDRSTRLEN];
                        inet_ntop(AF_INET6, &(ctx->GetGid()), gidStr, INET6_ADDRSTRLEN);
                        // fprintf(stderr, "RDMA device: %s\n", str);
                        Cerr << "Found RDMA device: device_name=" << ibv_get_device_name(dev) 
                                 << ", port=" << (int)portNum << ", gid_index=" << gidIndex << ", gid=" << gidStr << Endl;

                        auto [_, ok] = CtxMap.emplace(ctx->GetGid(), ctx);
                        if (!ok) {
                            Cerr << "Duplicate GID found: " << gidStr << Endl;
                            continue;
                        }
                    }
                }
            }
        }
    }

    int ErrNo = 0;
    TString Err;

} RdmaLinkManager;


void InitLinkManager();


TRdmaCtx* GetCtx(int sockfd) {
    sockaddr_storage addr;
    socklen_t addrLen = sizeof(addr);
    if (getsockname(sockfd, reinterpret_cast<sockaddr*>(&addr), &addrLen) < 0) {
        Cerr << "getsockname failed: " << strerror(errno) << Endl;
        return nullptr;
    }
    sockaddr_in6* addr_in6 = (sockaddr_in6*)&addr;
    char str[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &addr_in6->sin6_addr, str, INET6_ADDRSTRLEN);
    Cerr << "GetCtx: sockfd=" << sockfd << ", addr=" << str << Endl;
    return GetCtx(addr_in6->sin6_addr);
}

TRdmaCtx* GetCtx(const in6_addr& ip) {
    const ibv_gid* gid = reinterpret_cast<const ibv_gid*>(&ip);
    return RdmaLinkManager.GetCtx(gid);
}

const TCtxsMap& GetAllCtxs() {
    return RdmaLinkManager.GetAllCtxs();
}

} 
