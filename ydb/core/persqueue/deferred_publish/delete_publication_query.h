#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateDeletePublicationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId);

} // namespace NKikimr::NPQ::NDeferredPublish
