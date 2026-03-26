#include "serialized_invoker.h"

#include <yt/yt/core/actions/invoker_detail.h>
#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/misc/ring_queue.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TSerializedInvoker
    : public TInvokerWrapper<false>
    , public TInvokerProfilingWrapper
{
public:
    explicit TSerializedInvoker(
        IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    TSerializedInvoker(
        IInvokerPtr underlyingInvoker,
        const NProfiling::TTagSet& tagSet,
        NProfiling::IRegistryPtr registry)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , TInvokerProfilingWrapper(std::move(registry), "/serialized", tagSet)
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        auto wrappedCallback = WrapCallback(std::move(callback));

        auto guard = Guard(Lock_);
        if (Dead_) {
            return;
        }
        Queue_.push(std::move(wrappedCallback));
        TrySchedule(std::move(guard));
    }

    bool IsSerialized() const override
    {
        return true;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TRingQueue<TClosure> Queue_;
    bool CallbackScheduled_ = false;
    bool Dead_ = false;


    class TInvocationGuard
    {
    public:
        explicit TInvocationGuard(TIntrusivePtr<TSerializedInvoker> owner)
            : Owner_(std::move(owner))
        { }

        TInvocationGuard(TInvocationGuard&& other) = default;
        TInvocationGuard(const TInvocationGuard& other) = delete;

        void Activate()
        {
            YT_ASSERT(!Activated_);
            Activated_ = true;
        }

        void Reset()
        {
            Owner_.Reset();
        }

        ~TInvocationGuard()
        {
            if (Owner_) {
                Owner_->OnFinished(Activated_);
            }
        }

    private:
        TIntrusivePtr<TSerializedInvoker> Owner_;
        bool Activated_ = false;
    };

    void TrySchedule(TGuard<NThreading::TSpinLock>&& guard)
    {
        if (std::exchange(CallbackScheduled_, true)) {
            return;
        }
        guard.Release();
        UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TSerializedInvoker::RunCallback,
            MakeStrong(this),
            Passed(TInvocationGuard(this))));
    }

    void DrainQueue(TGuard<NThreading::TSpinLock>&& guard)
    {
        std::vector<TClosure> callbacks;
        while (!Queue_.empty()) {
            callbacks.push_back(std::move(Queue_.front()));
            Queue_.pop();
        }
        guard.Release();
        callbacks.clear();
    }

    void RunCallback(TInvocationGuard invocationGuard)
    {
        invocationGuard.Activate();

        TCurrentInvokerGuard currentInvokerGuard(this);
        TOneShotContextSwitchGuard contextSwitchGuard([&] {
            invocationGuard.Reset();
            OnFinished(true);
        });

        auto lockGuard = Guard(Lock_);

        if (Queue_.empty()) {
            return;
        }

        auto callback = std::move(Queue_.front());
        Queue_.pop();

        lockGuard.Release();

        callback();
    }

    void OnFinished(bool activated)
    {
        auto guard = Guard(Lock_);

        YT_VERIFY(std::exchange(CallbackScheduled_, false));

        if (activated) {
            if (!Queue_.empty()) {
                TrySchedule(std::move(guard));
            }
        } else {
            Dead_ = true;
            DrainQueue(std::move(guard));
        }
    }
};

IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker)
{
    if (underlyingInvoker->IsSerialized()) {
        return underlyingInvoker;
    }

    return New<TSerializedInvoker>(
        std::move(underlyingInvoker));
}

IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const NProfiling::TTagSet& tagSet,
    NProfiling::IRegistryPtr registry)
{
    if (underlyingInvoker->IsSerialized()) {
        return underlyingInvoker;
    }

    return New<TSerializedInvoker>(
        std::move(underlyingInvoker),
        tagSet,
        std::move(registry));
}

IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const std::string& invokerName,
    NProfiling::IRegistryPtr registry)
{
    return CreateSerializedInvoker(
        std::move(underlyingInvoker),
        NProfiling::TTagSet({{"invoker", invokerName}}),
        std::move(registry));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
