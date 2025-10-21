#pragma once

#include <util/datetime/base.h>

#include <memory>

namespace NActors {
class IActor;
}

namespace NKikimr::NAudit {

class TAuditConfig;

std::unique_ptr<NActors::IActor> CreateHeartbeatActor(const TAuditConfig& auditConfig);

}   // namespace NKikimr::NAudit
