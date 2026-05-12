#pragma once

#include <memory>
#include <util/generic/noncopyable.h>
#include <util/system/types.h>
#include <util/generic/string.h>


extern "C" {

struct ibv_context;
struct ibv_pd;
struct ibv_device_attr;
struct ibv_port_attr;
union ibv_gid;

}

namespace NInterconnect::NRdma {

namespace NLinkMgr {
    class TRdmaLinkManager;
}

// Open ibdrvlib. Throw exception in case of absent library, or other dlopen errors.
// Is used to check the library is present.
void IbvDlOpen();

struct TDeviceCtx : public NNonCopyable::TNonCopyable {
    TDeviceCtx(ibv_context* ctx, ibv_pd* pd);

    ~TDeviceCtx();

    ibv_context* const Context;
    ibv_pd* const ProtDomain;
};

class TRdmaCtx : public NNonCopyable::TNonCopyable {
    friend class NLinkMgr::TRdmaLinkManager;
    TRdmaCtx(
        std::shared_ptr<TDeviceCtx> deviceCtx, const ibv_device_attr& devAttr, const char* deviceName,
        ui32 portNum, const ibv_port_attr& portAttr, int gidIndex, const ibv_gid& gid
    );

    struct TImpl;
public:
    static std::shared_ptr<TRdmaCtx> Create(std::shared_ptr<TDeviceCtx> deviceCtx, ui32 portNum, int gidIndex);

    ~TRdmaCtx() = default;

    ibv_context* GetContext() const noexcept {
        return DeviceCtx->Context;
    }

    ibv_pd* GetProtDomain() const noexcept {
        return DeviceCtx->ProtDomain;
    }

    const ibv_device_attr& GetDevAttr() const noexcept;
    const char* GetDeviceName() const noexcept;
    ui32 GetPortNum() const noexcept;
    const ibv_port_attr& GetPortAttr() const noexcept;
    int GetGidIndex() const noexcept;
    const ibv_gid& GetGid() const noexcept;
    size_t GetDeviceIndex() const noexcept;

    void Output(IOutputStream &str) const;
    TString ToString() const;

private:
    const std::shared_ptr<TDeviceCtx> DeviceCtx;
    std::unique_ptr<TImpl> Impl;
};

}

IOutputStream& operator<<(IOutputStream& os, const ibv_gid& gid);
IOutputStream& operator<<(IOutputStream& os, const NInterconnect::NRdma::TRdmaCtx& rdmaCtx);
