#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/noncopyable.h>

namespace NKikimr::NOlap::NCounters {

class TInflightGuard: private TNonCopyable {
private:
    const NMonitoring::TDynamicCounters::TCounterPtr Counter;

public:
    explicit TInflightGuard(const NMonitoring::TDynamicCounters::TCounterPtr& counter)
        : Counter(counter)
    {
        Counter->Inc();
    }

    ~TInflightGuard() {
        Counter->Dec();
    }
};

}   // namespace NKikimr::NOlap::NCounters
