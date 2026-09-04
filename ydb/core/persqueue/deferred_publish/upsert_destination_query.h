#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateUpsertDestinationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId,
    const TString& path,
    const TString& destinationBlob);

} // namespace NKikimr::NPQ::NDeferredPublish
