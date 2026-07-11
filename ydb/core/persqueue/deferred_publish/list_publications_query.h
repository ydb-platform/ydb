#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateListPublicationsQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    const TMaybe<TString>& writerIdentityFilter);

NActors::IActor* CreateDescribePublicationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId);

} // namespace NKikimr::NPQ::NDeferredPublish
