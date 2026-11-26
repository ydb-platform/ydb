#pragma once

#include <util/datetime/base.h>

namespace NActors {
class IActor;
}

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpComputeSchedulerService(TDuration updateFairSharePeriod);

} // namespace NKikimr::NKqp
