#pragma once

#include "yql_panic.h"

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NYql {

class TMultiResourceLock : private TNonCopyable {
private:
    struct TLock : public TThrRefBase {
        typedef TIntrusivePtr<TLock> TPtr;

        bool IsUnique() const {
            return RefCount() == 1;
        }

        TMutex Mutex_;
    };

public:
    struct TResourceLock : private TNonCopyable {
        TResourceLock(TMultiResourceLock& owner, TLock::TPtr lock, TString resourceId)
            : Owner_(owner)
            , Lock_(std::move(lock))
            , ResourceId_(std::move(resourceId))
        {
            Y_ENSURE(Lock_);
            Lock_->Mutex_.Acquire();
        }

        TResourceLock(TResourceLock&& other)
            : Owner_(other.Owner_)
            , Lock_(std::move(other.Lock_))
            , ResourceId_(std::move(other.ResourceId_))
        {

        }

        TResourceLock& operator=(TResourceLock&&) = delete;

        ~TResourceLock() {
            if (!Lock_) {
                return;
            }

            Lock_->Mutex_.Release();
            // decrement ref count before TryCleanup
            Lock_ = nullptr;
            Owner_.TryCleanup(ResourceId_);
        }

    private:
        TMultiResourceLock& Owner_;
        TLock::TPtr Lock_;
        TString ResourceId_;
    };

    TResourceLock Acquire(TString resourceId);

    template <typename F>
    auto RunWithLock(TString resourceId, const F& f) -> decltype(f()) {
        auto lock = Acquire(std::move(resourceId));
        return f();
    }

    ~TMultiResourceLock();

private:
    TLock::TPtr ProvideResourceLock(const TString& resourceId);
    void TryCleanup(const TString& resourceId);

private:
    TMutex Guard_;
    TMap<TString, TLock::TPtr> Locks_;
};

}
