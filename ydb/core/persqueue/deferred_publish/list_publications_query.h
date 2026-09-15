#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

NActors::IActor* CreateListPublicationsQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    const TString& callerSid,
    const TMaybe<TString>& writerIdentityFilter);

} // namespace NKikimr::NPQ::NDeferredPublish
