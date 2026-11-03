#pragma once

#include <util/generic/hash_set.h>
#include <util/system/spinlock.h>

namespace NYdb::NBS::NGrpc {

////////////////////////////////////////////////////////////////////////////////

class TRequestHandlerBase
{
private:
    std::atomic_uint64_t RefCount = 1;

public:
    virtual ~TRequestHandlerBase() = default;

    virtual void Process(bool ok) = 0;
    virtual void Cancel() = 0;

    void* AcquireCompletionTag();
    void ReleaseCompletionTag();
};

using TRequestHandlerPtr = std::unique_ptr<TRequestHandlerBase>;

template <typename TRequestHandler>
class TRequestsInFlight final
{
protected:
    THashSet<TRequestHandler*> Requests;
    TAdaptiveLock RequestsLock;
    bool ShouldStop = false;

public:
    size_t GetCount() const
    {
        with_lock (RequestsLock) {
            return Requests.size();
        }
    }

    bool Register(TRequestHandler* handler)
    {
        with_lock (RequestsLock) {
            if (ShouldStop) {
                return false;
            }

            auto res = Requests.emplace(handler);
            Y_ABORT_UNLESS(res.second);
        }

        return true;
    }

    void Unregister(TRequestHandler* handler)
    {
        with_lock (RequestsLock) {
            auto it = Requests.find(handler);
            Y_ABORT_UNLESS(it != Requests.end());
            Requests.erase(it);
        }
    }

    void Shutdown()
    {
        with_lock (RequestsLock) {
            ShouldStop = true;

            for (auto* handler: Requests) {
                handler->Cancel();
            }
        }
        TSpinWait sw;
        for (;;) {
            if (GetCount() == 0) {
                break;
            }
            sw.Sleep();
        }
    }

    template <std::invocable<TRequestHandler*> TUnaryFunction>
    void ForEach(TUnaryFunction f)
    {
        with_lock (RequestsLock) {
            for (auto* handler: Requests) {
                f(handler);
            }
        }
    }
};

}   // namespace NYdb::NBS::NGrpc
