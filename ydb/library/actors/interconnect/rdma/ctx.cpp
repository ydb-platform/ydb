#include "ctx_impl.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <arpa/inet.h>

namespace NInterconnect::NRdma {

TDeviceCtx::TDeviceCtx(ibv_context* ctx, ibv_pd* pd)
    : Context(ctx)
    , ProtDomain(pd)
{}

TRdmaCtx::TRdmaCtx(
    std::shared_ptr<TDeviceCtx> deviceCtx, const ibv_device_attr& devAttr, const char* deviceName,
    ui32 portNum, const ibv_port_attr& portAttr, int gidIndex, const ibv_gid& gid
)
    : DeviceCtx(std::move(deviceCtx))
    , Impl(new TImpl(devAttr, deviceName, portNum, portAttr, gidIndex, gid))
{}

const ibv_device_attr& TRdmaCtx::GetDevAttr() const noexcept {
    return Impl->DevAttr;
}

const char* TRdmaCtx::GetDeviceName() const noexcept {
    return Impl->DeviceName;
}

ui32 TRdmaCtx::GetPortNum() const noexcept {
    return Impl->PortNum;
}

const ibv_port_attr& TRdmaCtx::GetPortAttr() const noexcept {
    return Impl->PortAttr;
}

int TRdmaCtx::GetGidIndex() const noexcept {
    return Impl->GidIndex;
}

const ibv_gid& TRdmaCtx::GetGid() const noexcept {
    return Impl->Gid;
}

size_t TRdmaCtx::GetDeviceIndex() const noexcept {
    return Impl->DeviceIndex;
}

TDeviceCtx::~TDeviceCtx() {
    ibv_dealloc_pd(ProtDomain);
    ibv_close_device(Context);
}

std::shared_ptr<TRdmaCtx> TRdmaCtx::Create(std::shared_ptr<TDeviceCtx> deviceCtx, ui32 portNum, int gidIndex) {
    const char* deviceName = ibv_get_device_name(deviceCtx->Context->device);

    ibv_device_attr devAttr;
    int err = ibv_query_device(deviceCtx->Context, &devAttr);
    if (err) {
        Cerr << "ibv_query_device failed on {device# " << deviceName << "} : " << strerror(errno) << Endl;
        return nullptr;
    }

    ibv_port_attr portAttr;
    err = ibv_query_port(deviceCtx->Context, portNum, &portAttr);
    if (err) {
        Cerr << "ibv_query_port failed on {device# " << deviceName << "} : " << strerror(errno) << Endl;
        return nullptr;
    }

    if (portAttr.link_layer != IBV_LINK_LAYER_ETHERNET) {
        Cerr << "{device# " << deviceName << ", port# " << (int)portNum << "} is not RoCE" << Endl;
        return nullptr;
    }

    ibv_gid gid;
    err = ibv_query_gid(deviceCtx->Context, portNum, gidIndex, &gid);
    if (err) {
        Cerr << "ibv_query_gid failed on {device# " << deviceName << ", port# " << (int)portNum << ", gidIndex# " << gidIndex << "} : " << strerror(errno) << Endl;
        return nullptr;
    }

    if (gid.global.interface_id == 0) {
        // there are a lot of devices with no GID, so we just skip them
        return nullptr;
    }

    TRdmaCtx* ctx = new TRdmaCtx(std::move(deviceCtx), devAttr, deviceName, portNum, portAttr, gidIndex, gid);
    return std::shared_ptr<TRdmaCtx>(ctx);
}

TString TRdmaCtx::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

void TRdmaCtx::Output(IOutputStream &str) const {
    str << "{device_name# " << GetDeviceName()
        << " port_num# " << GetPortNum()
        << " gid_index# " << GetGidIndex()
        << " gid# " << GetGid() << "}";
}

}

IOutputStream& operator<<(IOutputStream& os, const ibv_gid& gid) {
    char gidStr[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &gid, gidStr, INET6_ADDRSTRLEN);
    os << gidStr;
    return os;
}

IOutputStream& operator<<(IOutputStream& os, const NInterconnect::NRdma::TRdmaCtx& rdmaCtx) {
    rdmaCtx.Output(os);
    return os;
}
