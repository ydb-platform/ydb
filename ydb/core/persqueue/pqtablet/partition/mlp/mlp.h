#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NMLP {


// MLP не работает если включена компактифкация по ключу!!! (иначе не понятно как прореживать скомпакченные значения)
NActors::IActor* CreateConsumerActor(const NActors::TActorId& tabletActorId, const NActors::TActorId& partitionActorId);

}
