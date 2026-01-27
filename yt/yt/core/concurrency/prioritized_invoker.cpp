#include "prioritized_invoker.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TPrioritizedInvoker
    : public TInvokerWrapper<true>
    , public TInvokerProfilingWrapper
    , public virtual IPrioritizedInvoker
{
public:
    explicit TPrioritizedInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    TPrioritizedInvoker(IInvokerPtr underlyingInvoker, const NProfiling::TTagSet& tagSet, NProfiling::IRegistryPtr registry)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , TInvokerProfilingWrapper(std::move(registry), "/prioritized", tagSet)
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        auto wrappedCallback = WrapCallback(std::move(callback));
        UnderlyingInvoker_->Invoke(std::move(wrappedCallback));
    }

    void Invoke(TClosure callback, i64 priority) override
    {
        {
            auto guard = Guard(SpinLock_);
            Heap_.push_back({std::move(callback), priority, Counter_--});
            std::push_heap(Heap_.begin(), Heap_.end());
        }
        auto wrappedCallback = WrapCallback(BIND_NO_PROPAGATE(&TPrioritizedInvoker::DoExecute, MakeStrong(this)));
        UnderlyingInvoker_->Invoke(std::move(wrappedCallback));
    }

private:
    struct TEntry
    {
        TClosure Callback;
        i64 Priority;
        i64 Index;

        bool operator < (const TEntry& other) const
        {
            return std::tie(Priority, Index) < std::tie(other.Priority, other.Index);
        }
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<TEntry> Heap_;
    i64 Counter_ = 0;

    void DoExecute()
    {
        auto guard = Guard(SpinLock_);
        std::pop_heap(Heap_.begin(), Heap_.end());
        auto callback = std::move(Heap_.back().Callback);
        Heap_.pop_back();
        guard.Release();
        callback();
    }
};

IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker)
{
    return New<TPrioritizedInvoker>(
        std::move(underlyingInvoker));
}

IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker,
    const NProfiling::TTagSet& tagSet,
    NProfiling::IRegistryPtr registry)
{
    return New<TPrioritizedInvoker>(
        std::move(underlyingInvoker),
        std::move(tagSet),
        std::move(registry));
}

IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker,
    const std::string& invokerName,
    NProfiling::IRegistryPtr registry)
{
    return CreatePrioritizedInvoker(
        std::move(underlyingInvoker),
        NProfiling::TTagSet({{"invoker", invokerName}}),
        std::move(registry));
}

////////////////////////////////////////////////////////////////////////////////

class TFakePrioritizedInvoker
    : public TInvokerWrapper<true>
    , public virtual IPrioritizedInvoker
{
public:
    explicit TFakePrioritizedInvoker(IInvokerPtr underlyingInvoker)
        : TInvokerWrapper(std::move(underlyingInvoker))
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback, i64 /*priority*/) override
    {
        Invoke(std::move(callback));
    }

    void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(std::move(callback));
    }
};

IPrioritizedInvokerPtr CreateFakePrioritizedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TFakePrioritizedInvoker>(std::move(underlyingInvoker));
}

////////////////////////////////////////////////////////////////////////////////

class TFixedPriorityInvoker
    : public TInvokerWrapper<false>
{
public:
    TFixedPriorityInvoker(
        IPrioritizedInvokerPtr underlyingInvoker,
        i64 priority)
        : TInvokerWrapper(underlyingInvoker)
        , UnderlyingInvoker_(std::move(underlyingInvoker))
        , Priority_(priority)
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        return UnderlyingInvoker_->Invoke(std::move(callback), Priority_);
    }

private:
    const IPrioritizedInvokerPtr UnderlyingInvoker_;
    const i64 Priority_;
};

IInvokerPtr CreateFixedPriorityInvoker(
    IPrioritizedInvokerPtr underlyingInvoker,
    i64 priority)
{
    return New<TFixedPriorityInvoker>(
        std::move(underlyingInvoker),
        priority);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
