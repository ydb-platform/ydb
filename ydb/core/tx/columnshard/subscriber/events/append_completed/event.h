#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard::NSubscriber {

class TEventAppendCompleted: public ISubscriptionEvent {
private:
    using TBase = ISubscriptionEvent;
    virtual TString DoDebugString() const override;

public:
    TEventAppendCompleted()
        : TBase(EEventType::AppendCompleted)
    {
    }
};

}
