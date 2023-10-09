#include "multi_resource_lock.h"

using namespace NYql;

TMultiResourceLock::TResourceLock TMultiResourceLock::Acquire(TString resourceId) {
    TLock::TPtr lock = ProvideResourceLock(resourceId);

    // resource-specific mutex should be locked outside of Guard_ lock
    return { *this, std::move(lock), std::move(resourceId) };
}

TMultiResourceLock::~TMultiResourceLock() {
    with_lock(Guard_) {
        Y_ABORT_UNLESS(Locks_.empty(), "~TMultiResourceLock: we still have %lu unreleased locks", Locks_.size());
    }
}

TMultiResourceLock::TLock::TPtr TMultiResourceLock::ProvideResourceLock(const TString& resourceId) {
    with_lock(Guard_) {
        auto it = Locks_.find(resourceId);
        if (it == Locks_.end()) {
            it = Locks_.emplace(resourceId, MakeIntrusive<TLock>()).first;
        }

        // important: ref count will be incremented under lock
        // in this case we have guarantee TryCleanup will not erase this resource just after exit from this method and before entering lock->Mutex_.Acquire()
        return it->second;
    }
}

void TMultiResourceLock::TryCleanup(const TString& resourceId) {
    with_lock(Guard_) {
        auto it = Locks_.find(resourceId);
        if (it == Locks_.end()) {
            return;
        }

        if (it->second->IsUnique()) {
            Locks_.erase(it);
        }
    }
}
