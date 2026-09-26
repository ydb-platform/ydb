#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ {

NActors::TActorId MakeWriteSessionsQuoterId();
NActors::IActor* CreateWriteSessionsQuoter();

} // namespace NKikimr::NPQ
