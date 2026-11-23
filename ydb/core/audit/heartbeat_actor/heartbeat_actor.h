#pragma once

#include <util/network/interface.h>

#include <functional>
#include <memory>

namespace NActors {
class IActor;
}

namespace NKikimr::NAudit {

class TAuditConfig;

using TNetworkInterfacesProvider = std::function<NAddr::TNetworkInterfaceList()>;

std::unique_ptr<NActors::IActor> CreateHeartbeatActor(const TAuditConfig& auditConfig,
    TNetworkInterfacesProvider networkInterfacesProvider = NAddr::GetNetworkInterfaces);

}   // namespace NKikimr::NAudit
