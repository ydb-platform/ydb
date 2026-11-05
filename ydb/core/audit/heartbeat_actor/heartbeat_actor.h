#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <memory>

namespace NActors {
class IActor;
}

namespace NKikimrConfig {
class TAppConfig;
}

namespace NKikimr::NAudit {

class TAuditConfig;

// Returns the configured IP of this node, or an empty string if unavailable.
TString GetHeartbeatNodeAddress(const NKikimrConfig::TAppConfig& config, ui32 nodeId);

std::unique_ptr<NActors::IActor> CreateHeartbeatActor(const TAuditConfig& auditConfig, TString nodeAddress);

}   // namespace NKikimr::NAudit
