#include "invoker_util.h"
#include "invoker.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/system_invokers.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <stack>

namespace NYT {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    void Invoke(TClosure callback) override
    {
        static TFlsSlot<TFiberState> StateSlot;
        auto& state = *StateSlot;

        // We optimize invocation of recursion-free callbacks, i.e. those that do
        // not invoke anything in our invoker. This is done by introducing the AlreadyInvoking
        // flag which allows us to handle such case without allocation of the deferred
        // callback queue.

        if (state.AlreadyInvoking) {
            // Ensure deferred callback queue exists and push our callback into it.
            if (!state.DeferredCallbacks) {
                state.DeferredCallbacks.emplace();
            }
            state.DeferredCallbacks->push(std::move(callback));
        } else {
            // We are the outermost callback; execute synchronously.
            state.AlreadyInvoking = true;
            callback();
            callback.Reset();
            // If some callbacks were deferred, execute them until the queue is drained.
            // Note that some of the callbacks may defer new callbacks, which is perfectly valid.
            if (state.DeferredCallbacks) {
                while (!state.DeferredCallbacks->empty()) {
                    state.DeferredCallbacks->front()();
                    state.DeferredCallbacks->pop();
                }
                // Reset queue to reduce fiber memory footprint.
                state.DeferredCallbacks.reset();
            }
            state.AlreadyInvoking = false;
        }
    }

    void Invoke(TMutableRange<TClosure> callbacks) override
    {
        for (auto& callback : callbacks) {
            Invoke(std::move(callback));
        }
    }

    bool CheckAffinity(const IInvokerPtr& invoker) const override
    {
        return invoker.Get() == this;
    }

    bool IsSerialized() const override
    {
        return true;
    }

    TThreadId GetThreadId() const override
    {
        return InvalidThreadId;
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver /*waitTimeObserver*/) override
    { }

private:
    struct TFiberState
    {
        bool AlreadyInvoking = false;
        std::optional<TRingQueue<TClosure>> DeferredCallbacks;
    };
};

IInvokerPtr GetSyncInvoker()
{
    return LeakyRefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

class TNullInvoker
    : public IInvoker
{
public:
    void Invoke(TClosure /*callback*/) override
    { }

    void Invoke(TMutableRange<TClosure> /*callbacks*/) override
    { }

    bool CheckAffinity(const IInvokerPtr& /*invoker*/) const override
    {
        return false;
    }

    bool IsSerialized() const override
    {
        // Null invoker never executes any callbacks,
        // so formally it is serialized.
        return true;
    }

    TThreadId GetThreadId() const override
    {
        return InvalidThreadId;
    }

    void RegisterWaitTimeObserver(TWaitTimeObserver /*waitTimeObserver*/) override
    { }
};

IInvokerPtr GetNullInvoker()
{
    return LeakyRefCountedSingleton<TNullInvoker>();
}

IInvokerPtr GetFinalizerInvoker()
{
    return NConcurrency::GetFinalizerInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
