#ifndef INVOKER_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include invoker_pool.h"
// For the sake of sane code completion.
#include "invoker_pool.h"
#endif
#undef INVOKER_POOL_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TDummyInvokerHolder
{ };

// TInvokerHolder represents any object type capable of holding underlying invokers.
// It only needs to outlive underlying invokers and is not used in other way.
// TInvokerPoolWrapper with TDummyInvokerHolder is used in tests.
template <class TInvoker, class TInvokerHolder = TDummyInvokerHolder>
class TInvokerPoolWrapper
    : public IGenericInvokerPool<TInvoker>
{
private:
    using TInvokerPtr = TIntrusivePtr<TInvoker>;

public:
    explicit TInvokerPoolWrapper(
        std::vector<TInvokerPtr> invokers,
        TInvokerHolder invokerHolder = TDummyInvokerHolder())
        : InvokerHolder_(std::move(invokerHolder))
        , Invokers_(std::move(invokers))
    { }

    int GetSize() const override
    {
        return Invokers_.size();
    }

protected:
    const TInvokerPtr& DoGetInvoker(int index) const override
    {
        YT_VERIFY(0 <= index && index < std::ssize(Invokers_));
        return Invokers_[index];
    }

private:
    const TInvokerHolder InvokerHolder_;
    std::vector<TInvokerPtr> Invokers_;
};

} // namespace NDetail

template <class TInvokerFunctor, class TInputInvoker, class TOutputInvoker>
TIntrusivePtr<IGenericInvokerPool<TOutputInvoker>> TransformInvokerPool(
    TIntrusivePtr<IGenericInvokerPool<TInputInvoker>> inputInvokerPool,
    TInvokerFunctor&& functor)
{
    const auto invokerCount = inputInvokerPool->GetSize();

    std::vector<TIntrusivePtr<TOutputInvoker>> invokers;
    invokers.reserve(invokerCount);
    for (int invokerIndex = 0; invokerIndex < invokerCount; ++invokerIndex) {
        invokers.push_back(functor(inputInvokerPool->GetInvoker(invokerIndex)));
    }

    return New<NYT::NDetail::TInvokerPoolWrapper<TOutputInvoker, TIntrusivePtr<IGenericInvokerPool<TInputInvoker>>>>(
        std::move(invokers),
        std::move(inputInvokerPool));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
