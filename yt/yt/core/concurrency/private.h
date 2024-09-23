#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TEnqueuedAction;

class TMpmcQueueImpl;
class TMpscQueueImpl;

template <class TQueueImpl>
class TInvokerQueue;

template <class TQueueImpl>
using TInvokerQueuePtr = TIntrusivePtr<TInvokerQueue<TQueueImpl>>;

using TMpmcInvokerQueue = TInvokerQueue<TMpmcQueueImpl>;
using TMpmcInvokerQueuePtr = TIntrusivePtr<TMpmcInvokerQueue>;

using TMpscInvokerQueue = TInvokerQueue<TMpscQueueImpl>;
using TMpscInvokerQueuePtr = TIntrusivePtr<TMpscInvokerQueue>;

template <class TQueueImpl>
class TSingleQueueSchedulerThread;

template <class TQueueImpl>
class TSuspendableSingleQueueSchedulerThread;

using TMpscSingleQueueSchedulerThread = TSingleQueueSchedulerThread<TMpscQueueImpl>;
using TMpscSingleQueueSchedulerThreadPtr = TIntrusivePtr<TMpscSingleQueueSchedulerThread>;

using TMpscSuspendableSingleQueueSchedulerThread = TSuspendableSingleQueueSchedulerThread<TMpscQueueImpl>;
using TMpscSuspendableSingleQueueSchedulerThreadPtr = TIntrusivePtr<TMpscSuspendableSingleQueueSchedulerThread>;

////////////////////////////////////////////////////////////////////////////////

class TFiber;
class TFls;

DECLARE_REFCOUNTED_CLASS(TSchedulerThread)

DECLARE_REFCOUNTED_CLASS(TFairShareInvokerQueue)
DECLARE_REFCOUNTED_CLASS(TFairShareQueueSchedulerThread)
DECLARE_REFCOUNTED_STRUCT(IFairShareCallbackQueue)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ConcurrencyLogger, "Concurrency");
inline const NProfiling::TProfiler ConcurrencyProfiler("/concurrency");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
