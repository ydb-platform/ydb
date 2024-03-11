#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NGraph {

using namespace NActors;

inline TActorId MakeGraphServiceId(ui32 node = 0) {
    char x[12] = {'g','r','a','p','h','s', 'v', 'c'};
    return TActorId(node, TStringBuf(x, 12));
}

IActor* CreateGraphService(const TString& database);

} // NGraph
} // NKikimr
