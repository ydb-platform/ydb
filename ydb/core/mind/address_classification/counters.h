#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>

namespace NKikimr::NNetClassifier::NCounters {

using namespace NMonitoring;

struct TNetClassifierCounters : TAtomicRefCount<TNetClassifierCounters>, TNonCopyable {
    using TPtr = TIntrusivePtr<TNetClassifierCounters>;
    using TCounterPtr = TDynamicCounters::TCounterPtr;

    TNetClassifierCounters(TIntrusivePtr<TDynamicCounters> counters);

    TCounterPtr GoodConfigNotificationsCount;
    TCounterPtr BrokenConfigNotificationsCount;
    TCounterPtr SubscribersCount;
    TCounterPtr NetDataSourceType;

    TCounterPtr NetDataUpdateLagSeconds;
};

} // namespace NKikimr::NNetClassifier::NCounters
