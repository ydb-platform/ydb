#pragma once
#include <library/cpp/actors/core/actor.h>

namespace NKikimr {

inline NActors::TActorId MakeNodeIdentifierServiceId() {
    char x[12] = {'n','o','d','e','i','d','e','n','t','i','f','r'};
    return TActorId(0, TStringBuf(x, 12));
}

NActors::IActor* CreateNodeIdentifier();

}
