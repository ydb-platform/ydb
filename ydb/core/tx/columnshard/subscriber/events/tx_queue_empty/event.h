#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard::NSubscriber {
class TEventTxPlanQueueEmpty: public ISubscriptionEvent {
public:
    TEventTxPlanQueueEmpty()
        : ISubscriptionEvent(EEventType::PlanQueueEmpty)
    {
    }
};


}