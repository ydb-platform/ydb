#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NStat {

struct TStatServiceSettings {
    TStatServiceSettings() = default;
};

inline NActors::TActorId MakeStatServiceID(ui32 node) {
    const char x[12] = "StatService";
    return NActors::TActorId(node, TStringBuf(x, 12));
}

THolder<NActors::IActor> CreateStatService(const TStatServiceSettings& settings = TStatServiceSettings());

} // NKikimr::NStat
