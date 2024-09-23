#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Provides access to indexed invokers.
/*
 *  Underlying invokers are supposed to share common state in the pool
 *  and work in cooperative way (e.g.: pool could share CPU between invokers fairly).
 *
 *  Interface is generic, but user is supposed to work with instantiations for well known
 *  invoker types: see below IInvokerPool, IPrioritizedInvokerPool, ISuspendableInvokerPool, etc.
*/
template <class TInvoker>
class TGenericInvokerPool
    : public virtual TRefCounted
{
public:
    //! Returns number of invokers in the pool.
    virtual int GetSize() const = 0;

    //! Returns reference to the invoker from the underlying storage by the integer #index.
    //! Parameter #index is supposed to take values in the [0, implementation-defined limit) range.
    const TIntrusivePtr<TInvoker>& GetInvoker(int index) const;

    //! Returns reference to the invoker from the underlying storage by the enum #index.
    //! Parameter #index is supposed to take values in the [0, implementation-defined limit) range.
    template <class E>
        requires TEnumTraits<E>::IsEnum
    const TIntrusivePtr<TInvoker>& GetInvoker(E index) const;

protected:
    //! Returns reference to the invoker from the underlying storage by the integer #index.
    virtual const TIntrusivePtr<TInvoker>& DoGetInvoker(int index) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IInvokerPool)
DEFINE_REFCOUNTED_TYPE(IPrioritizedInvokerPool)
DEFINE_REFCOUNTED_TYPE(ISuspendableInvokerPool)

////////////////////////////////////////////////////////////////////////////////

//! For each underlying invoker calls Suspend. Returns combined future.
TFuture<void> SuspendInvokerPool(const ISuspendableInvokerPoolPtr& suspendableInvokerPool);

//! For each underlying invoker calls Resume.
void ResumeInvokerPool(const ISuspendableInvokerPoolPtr& suspendableInvokerPool);

////////////////////////////////////////////////////////////////////////////////

class TDiagnosableInvokerPool
    : public IInvokerPool
{
public:
    struct TInvokerStatistics
    {
        i64 EnqueuedActionCount = 0;
        i64 DequeuedActionCount = 0;
        i64 ExecutedActionCount = 0;
        i64 WaitingActionCount = 0;
        TDuration TotalTimeEstimate;
    };

    //! Returns statistics of the invoker by the integer #index.
    //! Parameter #index is supposed to take values in the [0, implementation-defined limit) range.
    TInvokerStatistics GetInvokerStatistics(int index) const;

    //! Returns statistics of the invoker by the integer #index.
    //! Parameter #index is supposed to take values in the [0, implementation-defined limit) range.
    template <class E>
        requires TEnumTraits<E>::IsEnum
    TInvokerStatistics GetInvokerStatistics(E index) const;

    virtual void UpdateActionTimeRelevancyHalflife(TDuration newHalflife) = 0;

protected:
    virtual TInvokerStatistics DoGetInvokerStatistics(int index) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TDiagnosableInvokerPool)

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Helper is provided for step-by-step inferring of the TOutputInvoker given template arguments.
template <class TInvokerFunctor, class TInputInvoker>
struct TTransformInvokerPoolHelper
{
    using TInputInvokerPtr = TIntrusivePtr<TInputInvoker>;
    using TOutputInvokerPtr = std::invoke_result_t<TInvokerFunctor, TInputInvokerPtr>;
    using TOutputInvoker = typename TOutputInvokerPtr::TUnderlying;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Applies #functor (TInputInvoker is supposed to represent mapping from TInputInvokerPtr to TOutputInvokerPtr)
//! to all invokers in the #inputInvokerPool producing IGenericInvokerPool<TOutputInvoker>.
//! Output invoker pool is guaranteed to capture input invoker pool so feel free to chain TransformInvokerPool calls.
template <
    class TInvokerFunctor,
    class TInputInvoker,
    class TOutputInvoker = typename NDetail::TTransformInvokerPoolHelper<TInvokerFunctor, TInputInvoker>::TOutputInvoker>
TIntrusivePtr<TGenericInvokerPool<TOutputInvoker>> TransformInvokerPool(
    TIntrusivePtr<TGenericInvokerPool<TInputInvoker>> inputInvokerPool,
    TInvokerFunctor&& functor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INVOKER_POOL_INL_H_
#include "invoker_pool-inl.h"
#undef INVOKER_POOL_INL_H_
