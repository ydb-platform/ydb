#pragma once

#include <library/cpp/actors/core/actor.h>

namespace NKikimr {
namespace NStat {

inline NActors::TActorId MakeStatServiceID() {
    return NActors::TActorId(0, TStringBuf("Statistics"));
}

THolder<NActors::IActor> CreateStatService();

} // NStat
} // NKikimr
