#pragma once

#include <library/cpp/actors/core/actor.h>

namespace NKikimr {
namespace NStat {

inline NActors::TActorId MakeStatServiceID(ui32 node) {
    const char x[12] = "StatService";
    return NActors::TActorId(node, TStringBuf(x, 12));
}

THolder<NActors::IActor> CreateStatService();

} // NStat
} // NKikimr
