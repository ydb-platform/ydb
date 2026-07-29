#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

inline NActors::TActorId MakeDeferredPublishRegistryActorId() {
    return NActors::TActorId(0, "DefPubReg");
}

NActors::IActor* CreateDeferredPublishRegistryActor();

} // namespace NKikimr::NPQ::NDeferredPublish
