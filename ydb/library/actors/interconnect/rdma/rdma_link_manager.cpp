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

template <>
struct std::hash<ibv_gid>
{
    std::size_t operator()(const ibv_gid& k) const {
        using std::size_t;
        using std::hash;

        return hash<ui64>()(k.global.subnet_prefix)
            ^ hash<ui64>()(k.global.interface_id);
    }
};

template <>
struct std::equal_to<ibv_gid>
{
    bool operator()(const ibv_gid& a, const ibv_gid& b) const {
        return a.global.interface_id == b.global.interface_id
            && a.global.subnet_prefix == b.global.subnet_prefix;
    }
};

namespace NInterconnect::NRdma {

TRdmaCtx::~TRdmaCtx() {
    ibv_dealloc_pd(ProtDomain);
    ibv_close_device(Context);
}

}

namespace NInterconnect::NRdma::NLinkMgr {

static class TRdmaLinkManager {
public:
    TRdmaCtx* GetCtx(const ibv_gid* gid) {
        for (const auto& [entry, ctx]: CtxMap) {
            if (entry.gid.global.interface_id == gid->global.interface_id &&
                entry.gid.global.subnet_prefix == gid->global.subnet_prefix) {
                return ctx.get();
            }
        }
        return nullptr;
    }

    TCtxsMap GetAllCtxs() {
        TCtxsMap ctxs;
        for (const auto& [entry, ctx]: CtxMap) {
            ctxs.emplace_back(entry, ctx.get());
        }
        return ctxs;
    }

    TRdmaLinkManager() {
        ScanDevices();
    }
    
private:
    std::vector<std::pair<ibv_gid_entry, std::shared_ptr<NInterconnect::NRdma::TRdmaCtx>>> CtxMap;
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

            ibv_pd* pd = ibv_alloc_pd(ctx);
            if (!pd) {
                ibv_close_device(ctx);
                continue;
            }

            auto sharedCtx = std::make_shared<TRdmaCtx>(ctx, pd);

            ibv_device_attr devAttrs;
            err = ibv_query_device(ctx, &devAttrs);

            if (err < 0) {
                continue;
            }
        
            for (uint8_t portNum = 1; portNum <= devAttrs.phys_port_cnt; portNum++) {
                ibv_port_attr portAttrs;
                err = ibv_query_port(ctx, portNum, &portAttrs);
                if (err == 0) {
                    //Cerr << "port " << (int)port << " speed: " << (int)portAttrs.active_speed << " " << err << "len: " << portAttrs.gid_tbl_len << Endl;

                    for (int gidIndex = 0; gidIndex < portAttrs.gid_tbl_len; gidIndex++ ) {
                        ibv_gid gid;
                        err = ibv_query_gid(ctx, portNum, gidIndex, &gid);
                        // ibv_query_gid_ex(ctx, port, gidIndex, &entry, 0);
                        if (err == 0 && gid.global.interface_id) {

                            ibv_gid_entry entry{
                                .gid = gid,
                                .gid_index = static_cast<ui32>(gidIndex),
                                .port_num = portNum,
                            };

                            CtxMap.emplace_back(entry, sharedCtx);

                            // char str[INET6_ADDRSTRLEN];
                            // inet_ntop(AF_INET6, &(gid), str, INET6_ADDRSTRLEN);
                            
                            // fprintf(stderr, "%s\n", str);
                        }
                    }
                }
            }
        }
        
        ibv_free_device_list(deviceList);
    }

    int ErrNo = 0;
    TString Err;

} RdmaLinkManager;


void InitLinkManager();

TRdmaCtx* GetCtx(const in6_addr& ip) {
    const ibv_gid* gid = reinterpret_cast<const ibv_gid*>(&ip);
    return RdmaLinkManager.GetCtx(gid);
}

TCtxsMap GetAllCtxs() {
    return RdmaLinkManager.GetAllCtxs();
}


} 
