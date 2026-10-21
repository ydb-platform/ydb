#pragma once
#include <ydb/library/actors/core/actor.h>

namespace NCloud {

NActors::IActor* CreateMockAccessServiceWithCache(); // for compatibility with older code

}
