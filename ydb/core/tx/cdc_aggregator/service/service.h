#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NCdcAggregator {

NActors::IActor* CreateCdcAggregatorService();

} // namespace NKikimr::NCdcAggregator
