#include "cancelable_context.h"
#include "callback.h"
#include "invoker_detail.h"
#include "current_invoker.h"

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext::TCancelableInvoker
    : public TInvokerWrapper<false>
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

        auto currentTokenGuard = NDetail::MakeCancelableContextCurrentTokenGuard(Context_);

        if (Context_->Canceled_) {
            callback.Reset();
            return;
        }

        return UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            [
                this,
                this_ = MakeStrong(this),
                callback = std::move(callback)
            ] () mutable {
                auto currentTokenGuard = NDetail::MakeCancelableContextCurrentTokenGuard(Context_);

                if (Context_->Canceled_) {
                    callback.Reset();
                    return;
                }

                TCurrentInvokerGuard guard(this);
                callback();
            }));
    }

    void Invoke(TMutableRange<TClosure> callbacks) override
    {
        auto currentTokenGuard = NDetail::MakeCancelableContextCurrentTokenGuard(Context_);

        std::vector<TClosure> capturedCallbacks;
        capturedCallbacks.reserve(callbacks.size());
        for (auto& callback : callbacks) {
            capturedCallbacks.push_back(std::move(callback));
        }

        if (Context_->Canceled_) {
            capturedCallbacks.clear();
            return;
        }

        return UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            [
                this,
                this_ = MakeStrong(this),
                capturedCallbacks = std::move(capturedCallbacks)
            ] () mutable {
                auto currentTokenGuard = NDetail::MakeCancelableContextCurrentTokenGuard(Context_);

                if (Context_->Canceled_) {
                    capturedCallbacks.clear();
                    return;
                }

                TCurrentInvokerGuard guard(this);
                for (const auto& callback : capturedCallbacks) {
                    callback();
                }
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

const TError& TCancelableContext::GetCancelationError() const
{
    return CancelationError_;
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
        if (auto context = weakContext.Lock()) {
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
