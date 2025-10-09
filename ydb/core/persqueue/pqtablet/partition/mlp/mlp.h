#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NMLP {


NActors::IActor* CreateConsumerActor(const NActors::TActorId& tabletActorId, const NActors::TActorId& partitionActorId);

}
