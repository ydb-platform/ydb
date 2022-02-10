#pragma once

#include "defs.h"
#include <library/cpp/actors/core/actor.h> 

namespace NKikimr {

    NActors::IActor *CreateFailureInjectionActor();

} // NKikimr
