#pragma once
#include "ctx.h"
#include <contrib/libs/ibdrv/include/infiniband/verbs.h>

namespace NInterconnect::NRdma {

struct TRdmaCtx::TImpl {
    TImpl(const ibv_device_attr& devAttr, const char* deviceName,
        ui32 portNum, const ibv_port_attr& portAttr, int gidIndex, const ibv_gid& gid) noexcept
    : DevAttr(devAttr)
    , DeviceName(deviceName)
    , PortNum(portNum)
    , PortAttr(portAttr)
    , GidIndex(gidIndex)
    , Gid(gid)
    {}

    const ibv_device_attr DevAttr;
    const char* DeviceName;
    const ui32 PortNum;
    const ibv_port_attr PortAttr;
    const int GidIndex;
    const ibv_gid Gid;
    size_t DeviceIndex;
};

}
