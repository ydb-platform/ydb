#pragma once

#include <memory>
#include <util/generic/noncopyable.h>
#include <util/system/types.h>
#include <util/generic/string.h>

#include <contrib/libs/ibdrv/include/infiniband/verbs.h>

extern "C" {

struct ibv_context;
struct ibv_pd;

}

namespace NInterconnect::NRdma {

namespace NLinkMgr {
    class TRdmaLinkManager;
}

struct TDeviceCtx : public NNonCopyable::TNonCopyable {
    TDeviceCtx(ibv_context* ctx, ibv_pd* pd);

    ~TDeviceCtx();

    ibv_context* const Context;
    ibv_pd* const ProtDomain;
};

class TRdmaCtx : public NNonCopyable::TNonCopyable {
    friend class NLinkMgr::TRdmaLinkManager;
    TRdmaCtx(
        std::shared_ptr<TDeviceCtx> deviceCtx, ibv_device_attr devAttr, const char* deviceName,
        ui32 portNum, ibv_port_attr portAttr, int gidIndex, ibv_gid gid
    );

public:
    static std::shared_ptr<TRdmaCtx> Create(std::shared_ptr<TDeviceCtx> deviceCtx, ui32 portNum, int gidIndex);

    ~TRdmaCtx() = default;

    ibv_context* GetContext() const {
        return DeviceCtx->Context;
    }
    ibv_pd* GetProtDomain() const {
        return DeviceCtx->ProtDomain;
    }
    const ibv_device_attr& GetDevAttr() const {
        return DevAttr;
    }
    const char* GetDeviceName() const {
        return DeviceName;
    }
    ui32 GetPortNum() const {
        return PortNum;
    }
    const ibv_port_attr& GetPortAttr() const {
        return PortAttr;
    }
    int GetGidIndex() const {
        return GidIndex;
    }
    const ibv_gid& GetGid() const {
        return Gid;
    }
    size_t GetDeviceIndex() const {
        return DeviceIndex;
    }

    void Output(IOutputStream &str) const;
    TString ToString() const;

private:
    const std::shared_ptr<TDeviceCtx> DeviceCtx;
    const ibv_device_attr DevAttr;
    const char* DeviceName;
    const ui32 PortNum;
    const ibv_port_attr PortAttr;
    const int GidIndex;
    const ibv_gid Gid;
    size_t DeviceIndex;
};

}

IOutputStream& operator<<(IOutputStream& os, const ibv_gid& gid);
IOutputStream& operator<<(IOutputStream& os, const NInterconnect::NRdma::TRdmaCtx& rdmaCtx);
