#include "suspendable_invoker.h"

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/thread/lfqueue.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSuspendableInvoker
    : public TInvokerWrapper<true>
    , public virtual ISuspendableInvoker
{
public:
    explicit TSuspendableInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    void Invoke(TClosure callback) override
    {
        Queue_.Enqueue(std::move(callback));
        ScheduleMore();
    }

    void Invoke(TMutableRange<TClosure> callbacks) override
    {
        Queue_.EnqueueAll(std::move(callbacks));
        ScheduleMore();
    }

    TFuture<void> Suspend() override
    {
        YT_VERIFY(!Suspended_.exchange(true));
        {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(!FreeEvent_);
            FreeEvent_ = NewPromise<void>();
            if (ActiveInvocationCount_ == 0) {
                FreeEvent_.Set();
            }
        }
        return FreeEvent_;
    }

    void Resume() override
    {
        YT_VERIFY(Suspended_.exchange(false));
        TPromise<void> freeEvent;
        {
            auto guard = Guard(SpinLock_);
            freeEvent = std::move(FreeEvent_);
        }
        if (freeEvent && !freeEvent.IsSet()) {
            freeEvent.TrySet(TError(NYT::EErrorCode::Canceled, "Invoker resumed before suspension completed"));
        }
        ScheduleMore();
    }

    bool IsSuspended() override
    {
        return Suspended_;
    }

private:
    std::atomic<bool> Suspended_ = false;
    std::atomic<bool> SchedulingMore_ = false;
    std::atomic<int> ActiveInvocationCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    TLockFreeQueue<TClosure> Queue_;

    TPromise<void> FreeEvent_;

    // TODO(acid): Think how to merge this class with implementation in other invokers.
    class TInvocationGuard
    {
    public:
        explicit TInvocationGuard(TIntrusivePtr<TSuspendableInvoker> owner)
            : Owner_(std::move(owner))
        { }

        TInvocationGuard(TInvocationGuard&& other) = default;
        TInvocationGuard(const TInvocationGuard& other) = delete;

        void Reset()
        {
            Owner_.Reset();
        }

        ~TInvocationGuard()
        {
            if (Owner_) {
                Owner_->OnFinished();
            }
        }

    private:
        TIntrusivePtr<TSuspendableInvoker> Owner_;
    };


    void RunCallback(TClosure callback, TInvocationGuard invocationGuard)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        TOneShotContextSwitchGuard contextSwitchGuard([&] {
            invocationGuard.Reset();
            OnFinished();
        });
        callback();
    }

    void OnFinished()
    {
        YT_VERIFY(ActiveInvocationCount_ > 0);

        if (--ActiveInvocationCount_ == 0 && Suspended_) {
            auto guard = Guard(SpinLock_);
            if (FreeEvent_ && !FreeEvent_.IsSet()) {
                auto freeEvent = FreeEvent_;
                guard.Release();
                freeEvent.TrySet();
            }
        }
    }

    void ScheduleMore()
    {
        if (Suspended_ || SchedulingMore_.exchange(true)) {
            return;
        }

        while (!Suspended_) {
            ++ActiveInvocationCount_;
            TInvocationGuard guard(this);

            TClosure callback;
            if (Suspended_ || !Queue_.Dequeue(&callback)) {
                break;
            }

            UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
                &TSuspendableInvoker::RunCallback,
                MakeStrong(this),
                Passed(std::move(callback)),
                Passed(std::move(guard))));
        }

        SchedulingMore_ = false;
        if (!Queue_.IsEmpty()) {
            ScheduleMore();
        }
    }
};

ISuspendableInvokerPtr CreateSuspendableInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TSuspendableInvoker>(std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
