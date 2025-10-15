#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <util/system/types.h>

namespace NKikimr::NPQ::NMLP {


// MLP не работает если включена компактифкация по ключу!!! (иначе не понятно как прореживать скомпакченные значения)
NActors::IActor* CreateConsumerActor(ui64 tabletId, const NActors::TActorId& tabletActorId, const NActors::TActorId& partitionActorId);

}
