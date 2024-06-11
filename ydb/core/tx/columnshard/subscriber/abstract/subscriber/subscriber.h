#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <memory>
#include <set>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NColumnShard::NSubscriber {

class ISubscriber {
private:
    virtual bool DoOnEvent(const std::shared_ptr<ISubscriptionEvent>& ev, TColumnShard& shard) = 0;
public:
    bool OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev, TColumnShard& shard) {
        return DoOnEvent(ev, shard);
    }

    virtual std::set<EEventType> GetEventTypes() const = 0;
    virtual bool IsFinished() const = 0;

    virtual ~ISubscriber() = default;
};

}