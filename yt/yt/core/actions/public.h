#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
class TCallback;

using TClosure = TCallback<void()>;

template <class TSignature>
class TCallbackList;

template <class T>
class TFuture;

template <>
class TFuture<void>;

template <class T>
class TPromise;

template <>
class TPromise<void>;

template <class T>
class TFutureHolder;

DECLARE_REFCOUNTED_STRUCT(IInvoker)
DECLARE_REFCOUNTED_STRUCT(IPrioritizedInvoker)
DECLARE_REFCOUNTED_STRUCT(ISuspendableInvoker)

template <class TInvoker>
class TGenericInvokerPool;

using IInvokerPool = TGenericInvokerPool<IInvoker>;
using IPrioritizedInvokerPool = TGenericInvokerPool<IPrioritizedInvoker>;
using ISuspendableInvokerPool = TGenericInvokerPool<ISuspendableInvoker>;

DECLARE_REFCOUNTED_TYPE(IInvokerPool)
DECLARE_REFCOUNTED_TYPE(IPrioritizedInvokerPool)
DECLARE_REFCOUNTED_TYPE(ISuspendableInvokerPool)
DECLARE_REFCOUNTED_CLASS(TDiagnosableInvokerPool)

DECLARE_REFCOUNTED_CLASS(TCancelableContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
