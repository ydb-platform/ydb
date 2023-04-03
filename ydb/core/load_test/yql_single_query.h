#pragma once

#include <library/cpp/actors/core/actor.h>

namespace NKikimr {

NActors::IActor *CreateYqlSingleQueryActor(NActors::TActorId parent, TString workingDir, TString query, TDuration timeout = TDuration::Seconds(10));

} // namespace NKikimr
