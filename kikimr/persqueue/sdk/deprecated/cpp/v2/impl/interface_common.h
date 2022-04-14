#pragma once

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NPersQueue {

class TPQLibPrivate;

const TString& GetCancelReason();

class TSyncDestroyed {
protected:
    TSyncDestroyed(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib);

public:
    // Get future that is signalled after object's destructor.
    // Non-pqlib threads can wait on this future to ensure that
    // all async operations are finished.
    virtual NThreading::TFuture<void> Destroyed() noexcept {
        return DestroyedPromise.GetFuture();
    }

    // This destructor will be executed after real object's destructor
    // in which it is expected to set all owned promises.
    virtual ~TSyncDestroyed();

    virtual void Cancel() = 0;

    void SetDestroyEventRef(std::shared_ptr<void> ref) {
        DestroyEventRef = std::move(ref);
    }

protected:
    // Needed for proper PQLib deinitialization
    void DestroyPQLibRef();

protected:
    NThreading::TPromise<void> DestroyedPromise = NThreading::NewPromise<void>();
    std::shared_ptr<void> DestroyEventRef;
    TIntrusivePtr<TPQLibPrivate> PQLib;
    bool IsCanceling = false;
    TAdaptiveLock DestroyLock;
};

} // namespace NPersQueue
