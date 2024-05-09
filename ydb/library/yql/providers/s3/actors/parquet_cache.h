#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

NActors::TActorId ParquetCacheActorId();

NActors::IActor* CreateParquetCache();

} // namespace NYql::NDq
