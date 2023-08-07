#include "cancelable_context.h"
#include "callback.h"
#include "invoker_detail.h"
#include "current_invoker.h"

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext::TCancelableInvoker
    : public TInvokerWrapper
{
public:
    TCancelableInvoker(
        TCancelableContextPtr context,
        IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , Context_(std::move(context))
    {
        YT_VERIFY(Context_);
    }

    void Invoke(TClosure callback) override
    {
        YT_ASSERT(callback);

        if (Context_->Canceled_) {
            return;
        }

        return UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this), callback = std::move(callback)] {
            if (Context_->Canceled_) {
                return;
            }

            TCurrentInvokerGuard guard(this);
            callback();
        }));
    }

private:
    const TCancelableContextPtr Context_;

};

////////////////////////////////////////////////////////////////////////////////

bool TCancelableContext::IsCanceled() const
{
    return Canceled_;
}

void TCancelableContext::Cancel(const TError& error)
{
    THashSet<TWeakPtr<TCancelableContext>> propagateToContexts;
    THashSet<TFuture<void>> propagateToFutures;
    {
        auto guard = Guard(SpinLock_);
        if (Canceled_) {
            return;
        }
        CancelationError_ = error;
        Canceled_ = true;
        PropagateToContexts_.swap(propagateToContexts);
        PropagateToFutures_.swap(propagateToFutures);
    }

    Handlers_.FireAndClear(error);

    for (const auto& weakContext : propagateToContexts) {
        auto context = weakContext.Lock();
        if (context) {
            context->Cancel(error);
        }
    }

    for (const auto& future : propagateToFutures) {
        future.Cancel(error);
    }
}

IInvokerPtr TCancelableContext::CreateInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TCancelableInvoker>(this, std::move(underlyingInvoker));
}

void TCancelableContext::SubscribeCanceled(const TCallback<void(const TError&)>& callback)
{
    auto guard = Guard(SpinLock_);
    if (Canceled_) {
        guard.Release();
        callback(CancelationError_);
        return;
    }
    Handlers_.Subscribe(callback);
}

void TCancelableContext::UnsubscribeCanceled(const TCallback<void(const TError&)>& /*callback*/)
{
    YT_ABORT();
}

void TCancelableContext::PropagateTo(const TCancelableContextPtr& context)
{
    auto weakContext = MakeWeak(context);

    {
        auto guard = Guard(SpinLock_);
        if (Canceled_) {
            guard.Release();
            context->Cancel(CancelationError_);
            return;
        }
        PropagateToContexts_.insert(context);
    }

    context->SubscribeCanceled(BIND_NO_PROPAGATE([=, this, weakThis = MakeWeak(this)] (const TError& /*error*/) {
        if (auto this_ = weakThis.Lock()) {
            auto guard = Guard(SpinLock_);
            PropagateToContexts_.erase(context);
        }
    }));
}

void TCancelableContext::PropagateTo(const TFuture<void>& future)
{
    {
        auto guard = Guard(SpinLock_);
        if (Canceled_) {
            guard.Release();
            future.Cancel(CancelationError_);
            return;
        }

        PropagateToFutures_.insert(future);
    }

    future.Subscribe(BIND_NO_PROPAGATE([=, this, weakThis = MakeWeak(this)] (const TError&) {
        if (auto this_ = weakThis.Lock()) {
            auto guard = Guard(SpinLock_);
            PropagateToFutures_.erase(future);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
