#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NYdb::NPersQueue {

class TImplTracker {
public:
    struct TWire {
        TImplTracker* Owner;

        TWire()
            : Owner(nullptr) {
        }

        TWire(TImplTracker& owner)
            : Owner(&owner) {
            Owner->Increment();
        }

        ~TWire() {
            if (Owner) {
                Owner->Decrement();
            }
        }

        TWire(const TWire& that) : Owner(that.Owner) {
            if (Owner)
                Owner->Increment();
        }
        TWire(TWire&& that)
            : Owner(std::exchange(that.Owner, nullptr)) {
        }
        TWire& operator=(const TWire& that) {
            if (Owner == that.Owner) {
                return *this;
            }
            if (Owner) {
                Owner->Decrement();
            }
            Owner = that.Owner;
            if (Owner) {
                Owner->Increment();
            }
            return *this;
        }
        TWire& operator=(TWire&& that) {
            if (this == &that) {
                return *this;
            }
            if (Owner == that.Owner) {
                if (that.Owner) {
                    that.Owner->Decrement();
                }
                that.Owner = nullptr;
                return *this;
            }
            if (Owner) {
                Owner->Decrement();
            }
            Owner = std::exchange(that.Owner, nullptr);
            return *this;
        }

        operator bool() const {
            return Owner;
        }
    };

public:
    ~TImplTracker() {
        // to synchronize with last Decrement() in other thread
        TGuard guard(Lock);
    }

    std::shared_ptr<TWire> MakeTrackedWire() {
        return std::make_shared<TWire>(*this);
    }

    void Increment() {
        with_lock(Lock) {
            Y_VERIFY(!AwaitingCompletion());
            ++RefCount;
        }
    }

    void Decrement() {
        with_lock(Lock) {
            --RefCount;
            if (RefCount == 0 && AwaitingCompletion()) {
                CompletionPromise.SetValue();
            }
        }
    }

    NThreading::TFuture<void> AsyncComplete() {
        with_lock(Lock) {
            if (!AwaitingCompletion()) {
                CompletionPromise = NThreading::NewPromise();
            }
            if (RefCount == 0) {
                CompletionPromise.SetValue();
            }
            return CompletionPromise.GetFuture();
        }
    }

private:
    inline bool AwaitingCompletion() const {
        return CompletionPromise.Initialized();
    }

private:
    TSpinLock Lock;
    size_t RefCount = 0;
    NThreading::TPromise<void> CompletionPromise;
};

}
