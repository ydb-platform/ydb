#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <memory>
#include <set>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NColumnShard::NSubscriber {

class ISubscriber {
public:
    virtual void OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev, TColumnShard& shard) = 0;
    virtual std::set<EEventType> GetEventTypes() const = 0;
    virtual bool IsFinished() const = 0;

    virtual ~ISubscriber() = default;
};

}