#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateInsertPublicationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    const TString& extPublicationId,
    const TMaybe<TString>& writerIdentity,
    const TString& createdBy);

} // namespace NKikimr::NPQ::NDeferredPublish
