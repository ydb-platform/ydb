#pragma once

#if !defined(USE_OLD_SCHEDULER)
#   include <ydb/core/kqp/runtime/scheduler/new/kqp_compute_actor.h>
#else
#   include <ydb/core/kqp/runtime/scheduler/old/kqp_compute_scheduler.h>
#endif

namespace NKikimr::NKqp {

#if !defined(USE_OLD_SCHEDULER)
    template <class T>
    using TSchedulableComputeActorBase = NScheduler::TSchedulableComputeActorBase<T>;
    using TSchedulableOptions = NScheduler::TSchedulableActorHelper::TOptions;
#else
    template <class T>
    using TSchedulableComputeActorBase = NSchedulerOld::TSchedulableComputeActorBase<T>;
    using TSchedulableOptions = NSchedulerOld::TComputeActorSchedulingOptions;
#endif

} // namespace NKikimr::NKqp
