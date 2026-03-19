#pragma once

#include <ydb/library/actors/core/actor.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NKikimr::NActorTracing {

    NActors::IActor* CreateActorTracingService();

    NActors::IActor* CreateTraceFetchGatherer(NActors::TActorId replyTo, TString localTrace, TVector<ui32> subtreeNodeIds);

} // namespace NKikimr::NActorTracing
