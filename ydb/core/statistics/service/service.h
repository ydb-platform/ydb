#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NStat {

struct TStatServiceSettings {
    TStatServiceSettings() = default;
};

NActors::TActorId MakeStatServiceID(ui32 node);

THolder<NActors::IActor> CreateStatService(const TStatServiceSettings& settings = TStatServiceSettings());

} // NKikimr::NStat
