#pragma once

#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <ydb/core/tx/columnshard/subscriber/protos/subscriber.pb.h>

#include <memory>
#include <set>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NColumnShard::NSubscriber {

class ISubscriber {
private:
    virtual bool DoOnEvent(const std::shared_ptr<ISubscriptionEvent>& ev, TColumnShard& shard) = 0;
    virtual bool DoDeserializeFromProto(const NKikimrColumnShardSubscriberProto::TSubscriberState&) {
        return true;
    }
    virtual void DoSerializeToProto(NKikimrColumnShardSubscriberProto::TSubscriberState& proto) const {

    }

public:
    bool OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev, TColumnShard& shard) {
        return DoOnEvent(ev, shard);
    }

    virtual std::set<EEventType> GetEventTypes() const = 0;
    virtual bool IsFinished() const = 0;

    virtual ~ISubscriber() = default;

    bool DeserializeFromProto(const NKikimrColumnShardSubscriberProto::TSubscriberState& proto);
    void SerializeToProto(NKikimrColumnShardSubscriberProto::TSubscriberState& proto) const;
};

}
