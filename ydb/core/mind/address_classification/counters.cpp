#include "counters.h"

namespace NKikimr::NNetClassifier::NCounters {

#define SETUP_SIMPLE_COUNTER(name, derived) \
    name(counters->GetCounter(#name, derived))

TNetClassifierCounters::TNetClassifierCounters(TIntrusivePtr<TDynamicCounters> counters)
    : SETUP_SIMPLE_COUNTER(GoodConfigNotificationsCount, true)
    , SETUP_SIMPLE_COUNTER(BrokenConfigNotificationsCount, true)
    , SETUP_SIMPLE_COUNTER(SubscribersCount, false)
    , SETUP_SIMPLE_COUNTER(NetDataSourceType, false)
    , SETUP_SIMPLE_COUNTER(NetDataUpdateLagSeconds, false)
{
}

#undef SETUP_SIMPLE_COUNTER

} // namespace NKikimr::NNetClassifier::NCounters
