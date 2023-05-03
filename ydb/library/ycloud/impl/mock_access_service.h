#pragma once
#include <library/cpp/actors/core/actor.h>

namespace NCloud {

NActors::IActor* CreateMockAccessServiceWithCache(); // for compatibility with older code

}
