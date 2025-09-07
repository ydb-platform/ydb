#pragma once

#include <util/datetime/base.h>

#include <memory>

namespace NActors {
class IActor;
}

namespace NKikimr::NAudit {

std::unique_ptr<NActors::IActor> CreateHeartbeatActor(TDuration heartbeatInterval);

}   // namespace NKikimr::NAudit
