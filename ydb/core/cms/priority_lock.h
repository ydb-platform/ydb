#pragma once

#include "defs.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>

#include <list>

namespace NKikimr::NCms {

template<typename T>
void AddPriorityLock(std::list<T>& locks, T&& lock) {
    auto pos = LowerBound(locks.begin(), locks.end(), lock, [](auto &l, auto &r) {
        return l.Priority < r.Priority;
    });
    locks.insert(pos, std::move(lock));
}

template<typename T>
void RemovePriorityLocks(std::list<T>& locks, i32 priority) {
    auto begin = LowerBoundBy(locks.begin(), locks.end(), priority, [](auto &l) {
        return l.Priority;
    });

    auto end = UpperBoundBy(locks.begin(), locks.end(), priority, [](auto &l) {
        return l.Priority;
    });

    Y_ABORT_UNLESS(begin != end);
    locks.erase(begin, end);
}

template<typename T>
bool HasSameOrHigherPriorityLock(const std::list<T>& locks, i32 priority) {
    if (locks.empty()) {
        return false;
    }

    if (HasAppData() && AppData()->FeatureFlags.GetEnableCmsLocksPriority()) {
        return locks.begin()->Priority <= priority;
    } else {
        return true; // only one lock is allowed despite of its priority
    }
}

} // namespace NKikimr::NCms
