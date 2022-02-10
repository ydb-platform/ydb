#pragma once

#include <ydb/core/yq/libs/config/protos/audit.pb.h>

#include <library/cpp/actors/core/actor.h>

#include <util/system/types.h>


namespace NYq {

NActors::IActor* CreateMockYqAuditServiceActor(const NConfig::TAuditConfig& config);

} // namespace NYq
