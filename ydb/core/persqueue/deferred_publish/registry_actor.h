#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateDeferredPublishRegistryActor();

NActors::TActorId GetOrCreateDeferredPublishRegistryActorId(NActors::TActorSystem* actorSystem);

} // namespace NKikimr::NPQ::NDeferredPublish
