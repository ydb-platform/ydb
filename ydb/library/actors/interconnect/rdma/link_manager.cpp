#include "link_manager.h"
#include "ctx.h"

#include <contrib/libs/ibdrv/include/infiniband/verbs.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/string/builder.h>

#include <errno.h>

#include <memory>

#include <util/network/address.h>

template <>
struct std::less<ibv_gid> {
    std::size_t operator()(const ibv_gid& a, const ibv_gid& b) const {
        return std::tie(a.global.subnet_prefix, a.global.interface_id) <
               std::tie(b.global.subnet_prefix, b.global.interface_id);
    }
};
template <>
struct std::equal_to<ibv_gid> {
    bool operator()(const ibv_gid& a, const ibv_gid& b) const {
        return a.global.interface_id == b.global.interface_id
            && a.global.subnet_prefix == b.global.subnet_prefix;
    }
};

namespace NInterconnect::NRdma::NLinkMgr {

static class TRdmaLinkManager {
public:
    const TCtxsMap& GetAllCtxs() {
        return CtxMap;
    }

    TRdmaCtx* GetCtx(const ibv_gid& gid) {
        auto it = std::lower_bound(
            CtxMap.begin(), CtxMap.end(),
            std::pair<ibv_gid, std::shared_ptr<NInterconnect::NRdma::TRdmaCtx>>{gid, nullptr},
            [](const auto& a, const auto& b) {
                return std::less<ibv_gid>()(a.first, b.first);
            }
        );
        if (it != CtxMap.end() && std::equal_to<ibv_gid>()(it->first, gid)) {
            return it->second.get();
        }
        Cerr << "No RDMA context found for GID: " << gid << Endl;
        return nullptr;
    }

    TRdmaLinkManager() {
        // Make sure libibverbs.so is present
        try {
            IbvDlOpen();
        } catch (std::exception& ex) {
            Cerr << "Unalbe to load ibverbs library: " << ex.what() << Endl;
            return;
        }
        ScanDevices();
    }
private:
    TCtxsMap CtxMap;

    void ScanDevices() {
        int numDevices = 0;
        int err;
        ibv_device** deviceList = ibv_get_device_list(&numDevices);
        if (!deviceList) {
            ErrNo = errno;
            Err = TString(strerror(errno));
            return;
        }

        Y_DEFER{ ibv_free_device_list(deviceList); };

        for (int i = 0; i < numDevices; i++) {
            ibv_device* dev = deviceList[i];
            ibv_context* ctx = ibv_open_device(dev);
            if (!ctx) {
                Err = Sprintf("Failed to open ib device '%s'", ibv_get_device_name(dev));
                continue;
            }

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
                            continue;
                        }

                        CtxMap.emplace_back(ctx->GetGid(), ctx);
                    }
                }
            }
        }
        std::sort(CtxMap.begin(), CtxMap.end(),
            [](const auto& a, const auto& b) {
                return std::less<ibv_gid>()(a.first, b.first);
            });

        // check for duplicates
        for (size_t i = 0; i < CtxMap.size(); ++i) {
            auto ctx = CtxMap[i].second;
            ctx->DeviceIndex = i;

            if (i > 0) {
                auto prevCtx = CtxMap[i - 1].second;
                if (std::equal_to<ibv_gid>()(prevCtx->GetGid(), ctx->GetGid())) {
                    Cerr << "Duplicate GID found: ctx1=" << prevCtx->ToString() << ", ctx2=" << ctx->ToString() << Endl;
                }
            }
        }
    }

    int ErrNo = 0;
    TString Err;

} RdmaLinkManager;

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
    return GetCtx(addr_in6->sin6_addr);
}

TRdmaCtx* GetCtx(const in6_addr& ip) {
    const ibv_gid* gid = reinterpret_cast<const ibv_gid*>(&ip);
    return RdmaLinkManager.GetCtx(*gid);
}

const TCtxsMap& GetAllCtxs() {
    return RdmaLinkManager.GetAllCtxs();
}

} 
