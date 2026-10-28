#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <util/system/types.h>

namespace NKikimrPQ {
class TMirrorPartitionConfig;
}

namespace NKikimr::NPQ {

NActors::IActor* CreateTopicConfigurationMover(ui64 tabletId, const NActors::TActorId& tabletActor,
    const NKikimrPQ::TMirrorPartitionConfig& config);

} // namespace NKikimr::NPQ
