#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

NActors::IActor* CreateDistributedRetroCollector();

} // namespace NKikimr
