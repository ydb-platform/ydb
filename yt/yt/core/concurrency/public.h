#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TActionQueue)
DECLARE_REFCOUNTED_STRUCT(IThreadPool)

DECLARE_REFCOUNTED_STRUCT(ISuspendableActionQueue)

DECLARE_REFCOUNTED_CLASS(TPeriodicExecutor)
DECLARE_REFCOUNTED_CLASS(TRetryingPeriodicExecutor)
DECLARE_REFCOUNTED_CLASS(TScheduledExecutor)
DECLARE_REFCOUNTED_CLASS(TInvokerAlarm)

DECLARE_REFCOUNTED_CLASS(TAsyncSemaphore)
DECLARE_REFCOUNTED_CLASS(TProfiledAsyncSemaphore)

DECLARE_REFCOUNTED_STRUCT(IFairShareActionQueue)

DECLARE_REFCOUNTED_STRUCT(IQuantizedExecutor)

DECLARE_REFCOUNTED_CLASS(TAsyncLooper);

namespace NDetail {

DECLARE_REFCOUNTED_STRUCT(TDelayedExecutorEntry)

} // namespace NDetail

using TDelayedExecutorCookie = NDetail::TDelayedExecutorEntryPtr;

DECLARE_REFCOUNTED_CLASS(TThroughputThrottlerConfig)
DECLARE_REFCOUNTED_CLASS(TRelativeThroughputThrottlerConfig)
DECLARE_REFCOUNTED_CLASS(TPrefetchingThrottlerConfig)
DECLARE_REFCOUNTED_STRUCT(IThroughputThrottler)
DECLARE_REFCOUNTED_STRUCT(IReconfigurableThroughputThrottler)
DECLARE_REFCOUNTED_STRUCT(ITestableReconfigurableThroughputThrottler)

DECLARE_REFCOUNTED_STRUCT(IAsyncInputStream)
DECLARE_REFCOUNTED_STRUCT(IAsyncOutputStream)

DECLARE_REFCOUNTED_STRUCT(IFlushableAsyncOutputStream)

DECLARE_REFCOUNTED_STRUCT(IAsyncZeroCopyInputStream)
DECLARE_REFCOUNTED_STRUCT(IAsyncZeroCopyOutputStream)

DECLARE_REFCOUNTED_STRUCT(IFairShareThreadPool)

DECLARE_REFCOUNTED_CLASS(TAsyncStreamPipe)

DEFINE_ENUM(EWaitForStrategy,
    (WaitFor)
    (Get)
);

class TAsyncSemaphore;

DEFINE_ENUM(EExecutionStackKind,
    (Small) // 256 Kb (default)
    (Large) //   8 Mb
);

class TExecutionStack;

template <class TSignature>
class TCoroutine;

template <class T>
class TNonblockingQueue;

template <typename EQueue>
struct IEnumIndexedFairShareActionQueue;

template <typename EQueue>
using IEnumIndexedFairShareActionQueuePtr = TIntrusivePtr<IEnumIndexedFairShareActionQueue<EQueue>>;

DECLARE_REFCOUNTED_STRUCT(TLeaseEntry)
using TLease = TLeaseEntryPtr;

DECLARE_REFCOUNTED_STRUCT(IPollable)
DECLARE_REFCOUNTED_STRUCT(IPoller)
DECLARE_REFCOUNTED_STRUCT(IThreadPoolPoller)

DECLARE_REFCOUNTED_CLASS(TThread)

using TFiberId = size_t;
constexpr size_t InvalidFiberId = 0;

DEFINE_ENUM(EFiberState,
    (Created)
    (Running)
    (Introspecting)
    (Waiting)
    (Idle)
    (Finished)
);

using TFairShareThreadPoolTag = TString;

DECLARE_REFCOUNTED_STRUCT(IPoolWeightProvider)

DECLARE_REFCOUNTED_STRUCT(ITwoLevelFairShareThreadPool)

class TFiber;

DECLARE_REFCOUNTED_STRUCT(TFairThrottlerConfig)
DECLARE_REFCOUNTED_STRUCT(TFairThrottlerBucketConfig)

DECLARE_REFCOUNTED_STRUCT(IThrottlerIPC)
DECLARE_REFCOUNTED_STRUCT(IIPCBucket)

DECLARE_REFCOUNTED_CLASS(TFairThrottler)
DECLARE_REFCOUNTED_CLASS(TBucketThrottler)

DECLARE_REFCOUNTED_STRUCT(ICallbackProvider)

class TPropagatingStorage;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
