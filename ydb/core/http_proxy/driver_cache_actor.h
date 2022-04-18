#pragma once

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NHttpProxy {

    NActors::IActor* CreateStaticDiscoveryActor(ui16 port);
} // namespace
