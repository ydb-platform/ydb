#pragma once

#include "events.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateFinalizePublicationActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId,
    EFinalizePublicationOp op,
    const TString& userToken);

} // namespace NKikimr::NPQ::NDeferredPublish
