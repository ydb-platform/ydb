#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NKikimr::NPQ {

struct TDeferredDestinationUpsertParams {
    ui64 IntPublicationId = 0;
    TString TopicPath;
    TString Database;
    ui32 PartitionId = 0;
    ui64 TabletId = 0;
};

NActors::IActor* CreateDeferredDestinationUpsertActor(
    const NActors::TActorId& writer,
    TDeferredDestinationUpsertParams params);

} // namespace NKikimr::NPQ
