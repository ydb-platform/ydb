#pragma once

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp {

NActors::IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters);

}  // namespace NKikimr::NKqp
